package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"debugservice/internal/server"
)

func rawRequest(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
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
	request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "loop", "breakpoints": []int{2}})

	alice := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"client": "alice"})
	if alice["role"] != "controller" || alice["token"] == "" {
		t.Fatalf("alice attach = %#v", alice)
	}
	bob := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"client": "bob"})
	if bob["role"] != "observer" {
		t.Fatalf("bob attach = %#v", bob)
	}
	snapshot := alice["snapshot"].(map[string]any)
	pauseID := int(snapshot["pauseId"].(float64))
	token := alice["token"].(string)

	// Observer cannot advance execution.
	code, denied := rawRequest(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": pauseID, "token": bob["token"]})
	if code != http.StatusForbidden {
		t.Fatalf("observer continue status=%d body=%#v", code, denied)
	}
	// Observer can still query.
	vars := request(t, h, http.MethodPost, "/sessions/1/variables",
		map[string]any{"pauseId": pauseID, "names": []string{"i"}})
	if vars["vars"] == nil {
		t.Fatalf("observer variables = %#v", vars)
	}

	// Controller continues idempotently; a replay after "reconnect" must not
	// advance the program again.
	first := request(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": pauseID, "token": token, "requestId": "r1"})
	if first["paused"] == nil {
		t.Fatalf("first continue = %#v", first)
	}
	replay := request(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": pauseID, "token": token, "requestId": "r1"})
	if replay["duplicate"] != true {
		t.Fatalf("replay = %#v", replay)
	}
	firstPause := first["paused"].(map[string]any)["pauseId"]
	replayPause := replay["paused"].(map[string]any)["pauseId"]
	if firstPause != replayPause {
		t.Fatalf("replay advanced the program: first=%v replay=%v", firstPause, replayPause)
	}

	// Reconnect with the same client id returns the same token and role.
	again := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"client": "alice"})
	if again["token"] != token || again["role"] != "controller" {
		t.Fatalf("reconnect = %#v", again)
	}

	// Controller releases; observer takes over and can advance.
	request(t, h, http.MethodPost, "/sessions/1/release", map[string]any{"token": token})
	code, _ = rawRequest(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": int(firstPause.(float64)), "token": token})
	if code != http.StatusConflict {
		t.Fatalf("continue after release status=%d", code)
	}
	claim := request(t, h, http.MethodPost, "/sessions/1/claim", map[string]any{"token": bob["token"]})
	if claim["role"] != "controller" {
		t.Fatalf("claim = %#v", claim)
	}
	taken := request(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": int(firstPause.(float64)), "token": bob["token"]})
	if taken["paused"] == nil && taken["terminated"] != true {
		t.Fatalf("continue after takeover = %#v", taken)
	}
}

func TestHTTPReconnectAfterExitSeesReasonAndLastPause(t *testing.T) {
	h := server.New().Routes()
	request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "raise"})
	alice := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"client": "alice"})
	pauseID := int(alice["snapshot"].(map[string]any)["pauseId"].(float64))

	result := request(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": pauseID, "token": alice["token"], "requestId": "exit-1"})
	if result["terminated"] != true || result["reason"] != "exception" {
		t.Fatalf("continue = %#v", result)
	}

	// A new client attaching after the exit observes the trusted end state.
	late := request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"client": "carol"})
	snapshot := late["snapshot"].(map[string]any)
	if snapshot["state"] != "terminated" || snapshot["reason"] != "exception" {
		t.Fatalf("late snapshot = %#v", snapshot)
	}
	lastPause := snapshot["lastPause"].(map[string]any)
	if int(lastPause["pauseId"].(float64)) != pauseID {
		t.Fatalf("lastPause = %#v", lastPause)
	}

	// Replaying the exit request reports the recorded outcome as duplicate.
	replay := request(t, h, http.MethodPost, "/sessions/1/continue",
		map[string]any{"pauseId": pauseID, "token": alice["token"], "requestId": "exit-1"})
	if replay["duplicate"] != true || replay["terminated"] != true || replay["reason"] != "exception" {
		t.Fatalf("replayed exit = %#v", replay)
	}
}
