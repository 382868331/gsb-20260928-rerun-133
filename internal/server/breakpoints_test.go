package server_test

import (
	"net/http"
	"testing"

	"debugservice/internal/server"
)

func TestHTTPBreakpointCandidateFlow(t *testing.T) {
	h := server.New().Routes()
	request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "loop"})
	request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "alice", "role": "controller"})
	request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "bob", "role": "controller"})

	// Observer prepares a candidate; everyone can view it.
	preview := request(t, h, http.MethodPost, "/sessions/1/breakpoints/preview", map[string]any{
		"clientId": "bob",
		"changes":  []any{map[string]any{"line": 2, "enabled": true, "condition": "i >= 2"}},
	})
	if preview["stale"] != false {
		t.Fatalf("preview = %#v", preview)
	}
	view := request(t, h, http.MethodGet, "/sessions/1/breakpoints/candidate", nil)
	if view["candidate"].(map[string]any)["preparedBy"] != "bob" {
		t.Fatalf("candidate = %#v", view)
	}

	// Observer cannot adopt; the controller can.
	code, _ := statusOf(h, http.MethodPost, "/sessions/1/breakpoints/adopt", map[string]any{"clientId": "bob"})
	if code != http.StatusForbidden {
		t.Fatalf("observer adopt status = %d", code)
	}
	adopted := request(t, h, http.MethodPost, "/sessions/1/breakpoints/adopt", map[string]any{"clientId": "alice", "requestId": "a1"})
	if adopted["version"].(float64) != 1 {
		t.Fatalf("adopted = %#v", adopted)
	}
	dup := request(t, h, http.MethodPost, "/sessions/1/breakpoints/adopt", map[string]any{"clientId": "alice", "requestId": "a1"})
	if dup["version"].(float64) != 1 {
		t.Fatalf("idempotent adopt = %#v", dup)
	}

	// A new candidate goes stale after a source reload; the conflict
	// response carries the re-check details.
	request(t, h, http.MethodPost, "/sessions/1/breakpoints/preview", map[string]any{
		"clientId": "alice",
		"changes":  []any{map[string]any{"line": 3, "enabled": true}},
	})
	request(t, h, http.MethodPost, "/sessions/1/reload", nil)
	code, body := statusOf(h, http.MethodPost, "/sessions/1/breakpoints/adopt", map[string]any{"clientId": "alice"})
	if code != http.StatusConflict || body["recheck"] == nil {
		t.Fatalf("stale adopt = %d %#v", code, body)
	}
}
