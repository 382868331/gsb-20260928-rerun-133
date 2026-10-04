package server_test

import (
	"net/http"
	"testing"

	"debugservice/internal/server"
)

func TestHTTPBreakpointCandidates(t *testing.T) {
	h := server.New().Routes()
	create := request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "loop"})
	pauseID := int(create["pauseId"].(float64))
	request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "alice", "role": "controller"})
	request(t, h, http.MethodPost, "/sessions/1/attach", map[string]any{"clientId": "bob", "role": "observer"})

	// Observers cannot stage; the controller can.
	code, _ := statusOf(h, http.MethodPost, "/sessions/1/breakpoints/candidates", map[string]any{
		"clientId": "bob", "breakpoints": []map[string]any{{"line": 2, "enabled": true}},
	})
	if code != http.StatusForbidden {
		t.Fatalf("observer stage status = %d", code)
	}
	staged := request(t, h, http.MethodPost, "/sessions/1/breakpoints/candidates", map[string]any{
		"clientId": "alice", "breakpoints": []map[string]any{{"line": 2, "enabled": true}},
	})
	candidateID := int(staged["id"].(float64))

	// Observers can preview candidates and versions.
	preview := request(t, h, http.MethodGet, "/sessions/1/breakpoints", nil)
	if preview["version"].(float64) != 1 || len(preview["candidates"].([]any)) != 1 {
		t.Fatalf("preview = %#v", preview)
	}

	// Source change flags the candidate; silent adoption is refused.
	request(t, h, http.MethodPost, "/sessions/1/source", map[string]any{"clientId": "alice", "validLines": []int{3}})
	code, _ = statusOf(h, http.MethodPost, "/sessions/1/breakpoints/adopt", map[string]any{"clientId": "alice", "candidateId": candidateID})
	if code != http.StatusConflict {
		t.Fatalf("stale adopt status = %d", code)
	}
	flagged := request(t, h, http.MethodGet, "/sessions/1/breakpoints", nil)
	recheck := flagged["candidates"].([]any)[0].(map[string]any)["recheck"].(map[string]any)
	if recheck["reason"] != "source-changed" {
		t.Fatalf("recheck = %#v", recheck)
	}

	// Refresh, adopt, and verify the pause reports the version in effect.
	request(t, h, http.MethodPost, "/sessions/1/breakpoints/refresh", map[string]any{"clientId": "alice", "candidateId": candidateID})
	adopted := request(t, h, http.MethodPost, "/sessions/1/breakpoints/adopt", map[string]any{"clientId": "alice", "candidateId": candidateID})
	if adopted["id"].(float64) != 2 || adopted["effectivePauseId"].(float64) != float64(pauseID) {
		t.Fatalf("adopted = %#v", adopted)
	}
	resumed := request(t, h, http.MethodPost, "/sessions/1/continue", map[string]any{"clientId": "alice", "pauseId": pauseID})
	paused := resumed["paused"].(map[string]any)
	if paused["breakpointVersion"].(float64) != 2 || paused["line"].(float64) != 2 {
		t.Fatalf("paused = %#v", paused)
	}
}
