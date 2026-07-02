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
	// Blow away any stale session so whatsmeow gets fresh noise keys.
	_ = os.RemoveAll(m.tokenDir(token))

	container, err := m.openContainer(token)
	if err != nil {
		return nil, err
	}

	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		container.Close()
		return nil, err
	}

	client := whatsmeow.NewClient(device, waLog.Noop)
	qrChan, err := client.GetQRChannel(ctx)
	if err != nil {
		container.Close()
		return nil, err
	}
	if err := client.Connect(); err != nil {
		container.Close()
		return nil, err
	}

	out := make(chan PairEvent, 8)
	go func() {
		defer close(out)
		defer client.Disconnect()
		defer container.Close()
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				out <- PairEvent{Type: "qr", Payload: evt.Code}
			case "success":
				out <- PairEvent{Type: "success"}
				return
			case "timeout":
				out <- PairEvent{Type: "error", Payload: "qr_timeout"}
				return
			}
		}
	}()
	return out, nil
}

// PairPhone starts phone-number pairing. The pairing code is sent as a
// PairEvent{Type:"code"}, then success/error follow.
func (m *Manager) PairPhone(ctx context.Context, token, phone string) (<-chan PairEvent, error) {
	_ = os.RemoveAll(m.tokenDir(token))

	container, err := m.openContainer(token)
	if err != nil {
		return nil, err
	}

	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		container.Close()
		return nil, err
	}

	client := whatsmeow.NewClient(device, waLog.Noop)
	qrChan, err := client.GetQRChannel(ctx)
	if err != nil {
		container.Close()
		return nil, err
	}
	if err := client.Connect(); err != nil {
		container.Close()
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
		defer close(out)
		defer client.Disconnect()
		defer container.Close()
		for evt := range qrChan {
			switch evt.Event {
			case "success":
				out <- PairEvent{Type: "success"}
				return
			case "timeout":
				out <- PairEvent{Type: "error", Payload: "pair_timeout"}
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
			text := extractText(v)
			if text == "" {
				return
			}
			mu.Lock()
			chatName := v.Info.PushName
			if v.Info.IsGroup {
				if n, ok := groupNames[v.Info.Chat]; ok {
					chatName = n
				} else {
					chatName = ""
				}
			}
			messages = append(messages, Message{
				ChatJID:   v.Info.Chat.String(),
				ChatName:  chatName,
				Sender:    v.Info.PushName,
				IsFromMe:  v.Info.IsFromMe,
				Text:      text,
				Timestamp: v.Info.Timestamp.UnixMilli(),
				IsGroup:   v.Info.IsGroup,
			})
			mu.Unlock()
		case *events.GroupInfo:
			if v.Name != nil && v.Name.Name != "" {
				mu.Lock()
				groupNames[v.JID] = v.Name.Name
				mu.Unlock()
			}
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

	// Patch chat names that arrived before GetJoinedGroups returned.
	mu.Lock()
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
