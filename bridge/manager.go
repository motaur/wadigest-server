// Package bridge manages per-user whatsmeow sessions.
// Each user is identified by a UUID token; their WA session lives at
// sessions/{token}/wa-session.db. A per-token mutex prevents two concurrent
// HTTP requests from opening the same session file simultaneously.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

type Message struct {
	ChatJID   string `json:"chat_jid"`
	ChatName  string `json:"chat_name"`
	Sender    string `json:"sender"`
	IsFromMe  bool   `json:"is_from_me"`
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp"` // unix millis
	IsGroup   bool   `json:"is_group"`
}

type Group struct {
	JID  string `json:"jid"`
	Name string `json:"name"`
}

type SyncResult struct {
	Messages []Message `json:"messages"`
	Groups   []Group   `json:"groups"`
}

// PairEvent is streamed to the caller during QR/phone pairing.
type PairEvent struct {
	Type    string `json:"type"`    // "qr" | "code" | "success" | "error"
	Payload string `json:"payload"` // QR string, phone code, or error message
}

// Manager holds per-token mutexes and the sessions directory path.
type Manager struct {
	sessionsDir string
	locks       sync.Map // token (string) → *sync.Mutex
}

func NewManager(sessionsDir string) *Manager {
	return &Manager{sessionsDir: sessionsDir}
}

func (m *Manager) tokenDir(token string) string {
	return fmt.Sprintf("%s/%s", m.sessionsDir, token)
}

func (m *Manager) sessionPath(token string) string {
	return fmt.Sprintf("%s/wa-session.db", m.tokenDir(token))
}

// lock acquires the per-token mutex and returns an unlock func.
func (m *Manager) lock(token string) func() {
	v, _ := m.locks.LoadOrStore(token, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// openContainer opens (or creates) the whatsmeow session store for the token.
// Caller is responsible for closing the container.
func (m *Manager) openContainer(token string) (*sqlstore.Container, error) {
	if err := os.MkdirAll(m.tokenDir(token), 0700); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)", m.sessionPath(token))
	return sqlstore.New(context.Background(), "sqlite", dsn, waLog.Noop)
}

// PairQR starts QR pairing. Events are sent on the returned channel.
// The channel is closed when pairing succeeds, fails, or times out.
// Caller must hold the token (from a prior call) and pass it in.
func (m *Manager) PairQR(ctx context.Context, token string) (<-chan PairEvent, error) {
	// Held for the whole pairing lifetime (through the post-pair history
	// wait below), so a Sync() call the frontend fires right after "success"
	// can't open a second connection to the same session while this one is
	// still finishing up.
	unlock := m.lock(token)

	// Blow away any stale session so whatsmeow gets fresh noise keys.
	_ = os.RemoveAll(m.tokenDir(token))

	container, err := m.openContainer(token)
	if err != nil {
		unlock()
		return nil, err
	}

	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		container.Close()
		unlock()
		return nil, err
	}

	client := whatsmeow.NewClient(device, waLog.Noop)
	qrChan, err := client.GetQRChannel(ctx)
	if err != nil {
		container.Close()
		unlock()
		return nil, err
	}
	connected := waitForConnected(client)
	historyDone := m.captureHistorySync(client, token)
	if err := client.Connect(); err != nil {
		container.Close()
		unlock()
		return nil, err
	}

	out := make(chan PairEvent, 8)
	go func() {
		defer unlock()
		defer close(out)
		defer client.Disconnect()
		defer container.Close()
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				out <- PairEvent{Type: "qr", Payload: evt.Code}
			case "success":
				out <- PairEvent{Type: "success"}
				awaitPostPairHandshake(connected, historyDone)
				return
			case "timeout":
				out <- PairEvent{Type: "error", Payload: "qr_timeout"}
				return
			default:
				out <- PairEvent{Type: "error", Payload: qrEventMessage(evt)}
				return
			}
		}
	}()
	return out, nil
}

// waitForConnected registers a handler that reports once the client reaches
// the "Connected" state, and must be called before client.Connect().
func waitForConnected(client *whatsmeow.Client) <-chan struct{} {
	connected := make(chan struct{})
	var closeOnce sync.Once
	client.AddEventHandler(func(rawEvt interface{}) {
		if _, ok := rawEvt.(*events.Connected); ok {
			closeOnce.Do(func() { close(connected) })
		}
	})
	return connected
}

// captureHistorySync registers a handler that saves every history-sync batch
// the phone sends after pairing into a per-token cache file (see
// appendHistorySyncCache), and returns a channel that's closed once the
// phone reports the sync is complete. Without this, the initial batch of
// historical messages whatsmeow downloads right after pairing is delivered
// exactly once and is lost forever if nothing consumes it.
func (m *Manager) captureHistorySync(client *whatsmeow.Client, token string) <-chan struct{} {
	done := make(chan struct{})
	var closeOnce sync.Once
	client.AddEventHandler(func(rawEvt interface{}) {
		evt, ok := rawEvt.(*events.HistorySync)
		if !ok || evt.Data == nil {
			return
		}
		_ = m.appendHistorySyncCache(token, parseHistorySync(client, evt.Data))
		if evt.Data.GetProgress() >= 100 {
			closeOnce.Do(func() { close(done) })
		}
	})
	return done
}

// awaitPostPairHandshake gives whatsmeow time to finish the post-pairing
// handshake before the caller disconnects. Disconnecting right after
// PairSuccess — before the client reaches "Connected" — leaves the phone's
// WhatsApp app stuck on "Logging in..." and then reports a link failure.
// It then waits for the phone to finish sending historical messages (see
// captureHistorySync), since that only happens once per pairing.
func awaitPostPairHandshake(connected, historyDone <-chan struct{}) {
	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		return
	}
	select {
	case <-historyDone:
	case <-time.After(15 * time.Second):
	}
	time.Sleep(2 * time.Second)
}

// qrEventMessage turns any non-code/success/timeout QRChannelItem into a
// human-readable error string, so pairing failures (e.g. a stale QR getting
// scanned) reach the client instead of the socket closing silently.
func qrEventMessage(evt whatsmeow.QRChannelItem) string {
	if evt.Error != nil {
		return evt.Error.Error()
	}
	return evt.Event
}

// buildMessage converts a whatsmeow message event — live or parsed out of a
// history-sync batch — into our Message type. ok is false for events with
// no extractable text (reactions, receipts, protocol messages, etc.).
func buildMessage(v *events.Message, groupNames map[types.JID]string) (msg Message, ok bool) {
	text := extractText(v)
	if text == "" {
		return Message{}, false
	}
	chatName := v.Info.PushName
	if v.Info.IsGroup {
		chatName = groupNames[v.Info.Chat]
	}
	return Message{
		ChatJID:   v.Info.Chat.String(),
		ChatName:  chatName,
		Sender:    v.Info.PushName,
		IsFromMe:  v.Info.IsFromMe,
		Text:      text,
		Timestamp: v.Info.Timestamp.UnixMilli(),
		IsGroup:   v.Info.IsGroup,
	}, true
}

// parseHistorySync converts a decoded history-sync blob into our Message
// type. Conversation names come from the blob itself (GetJoinedGroups isn't
// available yet this early in the pairing flow).
func parseHistorySync(client *whatsmeow.Client, data *waHistorySync.HistorySync) []Message {
	var out []Message
	groupNames := make(map[types.JID]string)
	for _, conv := range data.GetConversations() {
		chatJID, err := types.ParseJID(conv.GetID())
		if err != nil {
			continue
		}
		if name := conv.GetName(); name != "" {
			groupNames[chatJID] = name
		}
		for _, hm := range conv.GetMessages() {
			webMsg := hm.GetMessage()
			if webMsg == nil {
				continue
			}
			evt, err := client.ParseWebMessage(chatJID, webMsg)
			if err != nil {
				continue
			}
			if msg, ok := buildMessage(evt, groupNames); ok {
				out = append(out, msg)
			}
		}
	}
	return out
}

func (m *Manager) historySyncCachePath(token string) string {
	return fmt.Sprintf("%s/history_sync.json", m.tokenDir(token))
}

// appendHistorySyncCache persists messages from a history-sync batch to
// disk. The pairing websocket only streams PairEvents to the client, so
// this is how the messages survive until the next Sync() call picks them up.
func (m *Manager) appendHistorySyncCache(token string, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	path := m.historySyncCachePath(token)
	var existing []Message
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &existing)
	}
	data, err := json.Marshal(append(existing, msgs...))
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// consumeHistorySyncCache reads and deletes the cached history-sync messages
// for token, if any were saved during pairing.
func (m *Manager) consumeHistorySyncCache(token string) []Message {
	path := m.historySyncCachePath(token)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	os.Remove(path)
	var msgs []Message
	_ = json.Unmarshal(data, &msgs)
	return msgs
}

// PairPhone starts phone-number pairing. The pairing code is sent as a
// PairEvent{Type:"code"}, then success/error follow.
func (m *Manager) PairPhone(ctx context.Context, token, phone string) (<-chan PairEvent, error) {
	// See PairQR for why this is held through the post-pair history wait.
	unlock := m.lock(token)

	_ = os.RemoveAll(m.tokenDir(token))

	container, err := m.openContainer(token)
	if err != nil {
		unlock()
		return nil, err
	}

	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		container.Close()
		unlock()
		return nil, err
	}

	client := whatsmeow.NewClient(device, waLog.Noop)
	qrChan, err := client.GetQRChannel(ctx)
	if err != nil {
		container.Close()
		unlock()
		return nil, err
	}
	connected := waitForConnected(client)
	historyDone := m.captureHistorySync(client, token)
	if err := client.Connect(); err != nil {
		container.Close()
		unlock()
		return nil, err
	}

	out := make(chan PairEvent, 8)

	// Request the phone pairing code; runs concurrently with the qrChan loop.
	go func() {
		code, err := client.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
		if err != nil {
			out <- PairEvent{Type: "error", Payload: "pair_phone: " + err.Error()}
			return
		}
		out <- PairEvent{Type: "code", Payload: code}
	}()

	go func() {
		defer unlock()
		defer close(out)
		defer client.Disconnect()
		defer container.Close()
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				// QR codes are still emitted on this channel during phone
				// pairing; irrelevant here since we're pairing via code.
			case "success":
				out <- PairEvent{Type: "success"}
				awaitPostPairHandshake(connected, historyDone)
				return
			case "timeout":
				out <- PairEvent{Type: "error", Payload: "pair_timeout"}
				return
			default:
				out <- PairEvent{Type: "error", Payload: qrEventMessage(evt)}
				return
			}
		}
	}()

	return out, nil
}

// Sync opens the session, connects, waits for buffered messages, then
// disconnects. waitSeconds controls the receive window (10 is a good default).
func (m *Manager) Sync(token string, waitSeconds int) (SyncResult, error) {
	unlock := m.lock(token)
	defer unlock()

	container, err := m.openContainer(token)
	if err != nil {
		return SyncResult{}, err
	}
	defer container.Close()

	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		return SyncResult{}, err
	}
	if device.ID == nil {
		return SyncResult{}, fmt.Errorf("not_paired: no device ID in session")
	}

	var mu sync.Mutex
	var messages []Message
	groupNames := make(map[types.JID]string)

	client := whatsmeow.NewClient(device, waLog.Noop)
	client.AddEventHandler(func(rawEvt interface{}) {
		switch v := rawEvt.(type) {
		case *events.Message:
			mu.Lock()
			if msg, ok := buildMessage(v, groupNames); ok {
				messages = append(messages, msg)
			}
			mu.Unlock()
		case *events.GroupInfo:
			if v.Name != nil && v.Name.Name != "" {
				mu.Lock()
				groupNames[v.JID] = v.Name.Name
				mu.Unlock()
			}
		case *events.HistorySync:
			// Rare here (the initial batch normally arrives during pairing
			// and is cached — see captureHistorySync), but handle it in case
			// the phone sends another batch during a later sync.
			if v.Data == nil {
				return
			}
			historyMsgs := parseHistorySync(client, v.Data)
			mu.Lock()
			messages = append(messages, historyMsgs...)
			mu.Unlock()
		}
	})

	if err := client.Connect(); err != nil {
		return SyncResult{}, fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect()

	var groups []Group
	if gs, err := client.GetJoinedGroups(context.Background()); err == nil {
		mu.Lock()
		for _, g := range gs {
			if g.Name != "" {
				groups = append(groups, Group{JID: g.JID.String(), Name: g.Name})
				groupNames[g.JID] = g.Name
			}
		}
		mu.Unlock()
	}

	time.Sleep(time.Duration(waitSeconds) * time.Second)

	mu.Lock()
	// Historical messages the phone delivered right after pairing (see
	// captureHistorySync) — the pairing websocket has no way to stream them
	// straight to the client, so they wait here until the first sync.
	messages = append(messages, m.consumeHistorySyncCache(token)...)

	// Patch chat names that arrived before GetJoinedGroups returned.
	for i := range messages {
		if messages[i].IsGroup && messages[i].ChatName == "" {
			var jid types.JID
			if err := json.Unmarshal([]byte(`"`+messages[i].ChatJID+`"`), &jid); err == nil {
				if n, ok := groupNames[jid]; ok {
					messages[i].ChatName = n
				}
			}
		}
	}
	mu.Unlock()

	return SyncResult{Messages: messages, Groups: groups}, nil
}

// SessionExists reports whether a session file exists for the token.
func (m *Manager) SessionExists(token string) bool {
	_, err := os.Stat(m.sessionPath(token))
	return err == nil
}

// DeleteSession removes all files for the token and unregisters its mutex.
func (m *Manager) DeleteSession(token string) error {
	unlock := m.lock(token)
	defer unlock()
	err := os.RemoveAll(m.tokenDir(token))
	m.locks.Delete(token)
	return err
}

func extractText(evt *events.Message) string {
	msg := evt.Message
	if msg == nil {
		return ""
	}
	if c := msg.GetConversation(); c != "" {
		return c
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	if img := msg.GetImageMessage(); img != nil && img.GetCaption() != "" {
		return "[image] " + img.GetCaption()
	}
	if vid := msg.GetVideoMessage(); vid != nil && vid.GetCaption() != "" {
		return "[video] " + vid.GetCaption()
	}
	return ""
}
