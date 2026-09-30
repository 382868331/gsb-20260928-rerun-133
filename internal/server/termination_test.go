package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"debugservice/internal/server"
)

// TestTerminatedSnapshotExposesReasonAndLastPause verifies the protocol keeps
// answering after executor exit: terminal reason and the last pause are both
// queryable, while a stale pause identity is rejected.
func TestTerminatedSnapshotExposesReasonAndLastPause(t *testing.T) {
	h := newTestHandler()
	create := request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "raise"})
	pauseID := int(create["pauseId"].(float64))

	request(t, h, http.MethodPost, "/sessions/1/continue", map[string]any{"pauseId": pauseID})

	snap := request(t, h, http.MethodGet, "/sessions/1", nil)
	if snap["state"] != "terminated" || snap["reason"] != "exception" {
		t.Fatalf("terminal snapshot = %#v", snap)
	}
	last, ok := snap["lastPause"].(map[string]any)
	if !ok {
		t.Fatalf("lastPause missing: %#v", snap)
	}
	if last["pauseId"].(float64) != float64(pauseID) || last["state"] != "stopped" {
		t.Fatalf("lastPause = %#v", last)
	}

	body, _ := json.Marshal(map[string]any{"pauseId": pauseID, "names": []string{"ready"}})
	req := httptest.NewRequest(http.MethodPost, "/sessions/1/variables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusGone {
		t.Fatalf("stale identity after exit status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func newTestHandler() http.Handler {
	return server.New().Routes()
}
