package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wadigest/server/bridge"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type Handler struct {
	mgr *bridge.Manager
}

func NewHandler(sessionsDir string) *Handler {
	return &Handler{mgr: bridge.NewManager(sessionsDir)}
}

func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()

	// Pairing
	r.Post("/pair/qr", h.startQR)
	r.Post("/pair/phone", h.startPhone)
	r.Get("/pair/{token}/ws", h.pairStream)

	// Per-session operations
	r.Post("/sync/{token}", h.sync)
	r.Delete("/session/{token}", h.deleteSession)

	return r
}

// POST /api/pair/qr  → {token}
// Creates a fresh UUID token; client opens /api/pair/{token}/ws next.
func (h *Handler) startQR(w http.ResponseWriter, _ *http.Request) {
	token := uuid.NewString()
	jsonOK(w, map[string]string{"token": token, "mode": "qr"})
}

// POST /api/pair/phone  body:{phone}  → {token}
func (h *Handler) startPhone(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone string `json:"phone"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Phone) == "" {
		http.Error(w, `{"error":"phone required"}`, http.StatusBadRequest)
		return
	}
	token := uuid.NewString()
	jsonOK(w, map[string]string{"token": token, "mode": "phone", "phone": body.Phone})
}

// GET /api/pair/{token}/ws?mode=qr|phone[&phone=...]
// Upgrades to WebSocket and streams PairEvents until success or error.
func (h *Handler) pairStream(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")
	mode := r.URL.Query().Get("mode")
	phone := r.URL.Query().Get("phone")

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // any origin; CORS is handled in main
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	ctx := conn.CloseRead(context.Background())

	var events <-chan bridge.PairEvent
	switch mode {
	case "phone":
		if phone == "" {
			_ = wsjson.Write(ctx, conn, bridge.PairEvent{Type: "error", Payload: "phone param required"})
			return
		}
		events, err = h.mgr.PairPhone(ctx, token, phone)
	default:
		events, err = h.mgr.PairQR(ctx, token)
	}
	if err != nil {
		_ = wsjson.Write(ctx, conn, bridge.PairEvent{Type: "error", Payload: err.Error()})
		return
	}

	for evt := range events {
		if err := wsjson.Write(ctx, conn, evt); err != nil {
			return
		}
		if evt.Type == "success" || evt.Type == "error" {
			break
		}
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

// POST /api/sync/{token}  body:{wait_seconds?}  → SyncResult
func (h *Handler) sync(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")

	if !h.mgr.SessionExists(token) {
		http.Error(w, `{"error":"session not found"}`, http.StatusNotFound)
		return
	}

	var body struct {
		WaitSeconds int `json:"wait_seconds"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.WaitSeconds <= 0 {
		body.WaitSeconds = 10
	}
	// Allow override via query param too (useful for testing).
	if ws := r.URL.Query().Get("wait"); ws != "" {
		if n, err := strconv.Atoi(ws); err == nil && n > 0 {
			body.WaitSeconds = n
		}
	}

	result, err := h.mgr.Sync(token, body.WaitSeconds)
	if err != nil {
		code := http.StatusInternalServerError
		if strings.Contains(err.Error(), "not_paired") {
			code = http.StatusUnauthorized
		}
		jsonErr(w, err.Error(), code)
		return
	}
	jsonOK(w, result)
}

// DELETE /api/session/{token}
func (h *Handler) deleteSession(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")
	if err := h.mgr.DeleteSession(token); err != nil {
		jsonErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]bool{"ok": true})
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
