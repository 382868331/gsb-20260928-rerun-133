package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"debugservice/internal/server"
)

func statusOf(h http.Handler, method, path string, body any) (int, map[string]any) {
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

func TestHTTPAttachControlAndIdempotentContinue(t *testing.T) {
	h := server.New().Routes()
	create := request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "count"})
	pauseID := int(create["pauseId"].(float64))

	attach := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "alice", "role": "controller"})
	if attach["role"] != "controller" {
		t.Fatalf("attach = %#v", attach)
	}
	second := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "bob", "role": "controller"})
	if second["role"] != "observer" || second["controller"] != "alice" {
		t.Fatalf("second attach = %#v", second)
	}

	code, _ := statusOf(h, http.MethodPost, "/sessions/1/continue", map[string]any{"clientId": "bob", "pauseId": pauseID})
	if code != http.StatusForbidden {
		t.Fatalf("observer continue status = %d", code)
	}

	first := request(t, h, http.MethodPost, "/sessions/1/continue", map[string]any{"clientId": "alice", "pauseId": pauseID, "requestId": "r1"})
	if first["terminated"] != true {
		t.Fatalf("controller continue = %#v", first)
	}
	again := request(t, h, http.MethodPost, "/sessions/1/continue", map[string]any{"clientId": "alice", "pauseId": pauseID, "requestId": "r1"})
	if again["duplicate"] != true || again["terminated"] != true {
		t.Fatalf("duplicate continue = %#v", again)
	}

	// Reattach after termination: trusted reason and last pause are kept.
	reattach := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "carol", "role": "controller"})
	snapshot, ok := reattach["snapshot"].(map[string]any)
	if !ok || snapshot["state"] != "terminated" || snapshot["reason"] != "exit" {
		t.Fatalf("reattach snapshot = %#v", reattach)
	}
	lastPause, ok := snapshot["lastPause"].(map[string]any)
	if !ok || lastPause["pauseId"].(float64) == 0 {
		t.Fatalf("lastPause missing: %#v", snapshot)
	}
}

func TestHTTPDetachReleasesControl(t *testing.T) {
	h := server.New().Routes()
	request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "loop"})
	request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "alice", "role": "controller"})
	request(t, h, http.MethodPost, "/sessions/1/detach", map[string]any{"clientId": "alice"})

	attach := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "bob", "role": "controller"})
	if attach["role"] != "controller" {
		t.Fatalf("bob should control after detach: %#v", attach)
	}
}
