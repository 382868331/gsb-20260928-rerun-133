package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"debugservice/internal/server"
)

func request(t *testing.T, h http.Handler, method, path string, body any) map[string]any {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code >= 400 {
		t.Fatalf("%s %s: status=%d body=%s", method, path, rec.Code, rec.Body.String())
	}
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	out["status"] = float64(rec.Code)
	return out
}

func TestHTTPAbnormalExitAndDuplicateContinue(t *testing.T) {
	h := server.New().Routes()
	create := request(t, h, http.MethodPost, "/sessions", map[string]any{"program": "raise"})
	pauseID := int(create["pauseId"].(float64))

	result := request(t, h, http.MethodPost, "/sessions/1/continue", map[string]any{"pauseId": pauseID})
	if result["terminated"] != true || result["reason"] != "exception" {
		t.Fatalf("continue result = %#v", result)
	}

	req := httptest.NewRequest(http.MethodPost, "/sessions/1/continue", bytes.NewReader([]byte(`{"pauseId":999}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusGone {
		t.Fatalf("late continue status = %d body=%s", rec.Code, rec.Body.String())
	}
}
