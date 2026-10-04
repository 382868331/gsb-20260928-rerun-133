package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"

	"debugservice/internal/executor"
	"debugservice/internal/session"
)

type Server struct {
	mu       sync.Mutex
	sessions map[string]*session.Session
}

func New() *Server {
	return &Server{sessions: map[string]*session.Session{}}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /programs", s.listPrograms)
	mux.HandleFunc("POST /sessions", s.createSession)
	mux.HandleFunc("GET /sessions/{id}", s.withSession(s.getSession))
	mux.HandleFunc("POST /sessions/{id}/attach", s.withSession(s.attachSession))
	mux.HandleFunc("POST /sessions/{id}/detach", s.withSession(s.detachSession))
	mux.HandleFunc("POST /sessions/{id}/wait", s.withSession(s.waitSession))
	mux.HandleFunc("POST /sessions/{id}/continue", s.withSession(s.continueSession))
	mux.HandleFunc("POST /sessions/{id}/pause", s.withSession(s.pauseSession))
	mux.HandleFunc("POST /sessions/{id}/breakpoints", s.withSession(s.setBreakpoints))
	mux.HandleFunc("POST /sessions/{id}/breakpoints/preview", s.withSession(s.previewBreakpoints))
	mux.HandleFunc("GET /sessions/{id}/breakpoints/candidate", s.withSession(s.getCandidate))
	mux.HandleFunc("POST /sessions/{id}/breakpoints/adopt", s.withSession(s.adoptCandidate))
	mux.HandleFunc("POST /sessions/{id}/reload", s.withSession(s.reloadSource))
	mux.HandleFunc("POST /sessions/{id}/variables", s.withSession(s.variables))
	return logging(mux)
}

func (s *Server) listPrograms(w http.ResponseWriter, _ *http.Request) {
	programs := executor.BuiltinPrograms()
	names := make([]string, 0, len(programs))
	for name := range programs {
		names = append(names, name)
	}
	writeJSON(w, http.StatusOK, map[string]any{"programs": names})
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Program     string `json:"program"`
		Breakpoints []int  `json:"breakpoints"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	programs := executor.BuiltinPrograms()
	program, ok := programs[body.Program]
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("unknown program"))
		return
	}
	s.mu.Lock()
	id := strconv.Itoa(len(s.sessions) + 1)
	engine := executor.NewVirtual(program)
	sess := session.New(id, engine)
	s.sessions[id] = sess
	s.mu.Unlock()
	if len(body.Breakpoints) > 0 {
		_, _ = sess.SetBreakpoints(body.Breakpoints)
	}
	writeJSON(w, http.StatusCreated, sess.Snapshot())
}

type handler func(http.ResponseWriter, *http.Request, *session.Session)

func (s *Server) withSession(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		sess := s.sessions[r.PathValue("id")]
		s.mu.Unlock()
		if sess == nil {
			writeError(w, http.StatusNotFound, errors.New("unknown session"))
			return
		}
		h(w, r, sess)
	}
}

func (s *Server) getSession(w http.ResponseWriter, _ *http.Request, sess *session.Session) {
	writeJSON(w, http.StatusOK, sess.Snapshot())
}

func (s *Server) attachSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		ClientID string `json:"clientId"`
		Role     string `json:"role"`
		Takeover bool   `json:"takeover"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.Attach(body.ClientID, body.Role == session.RoleController, body.Takeover)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) detachSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		ClientID string `json:"clientId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	sess.Detach(body.ClientID)
	writeJSON(w, http.StatusOK, sess.Snapshot())
}

func (s *Server) waitSession(w http.ResponseWriter, _ *http.Request, sess *session.Session) {
	writeJSON(w, http.StatusOK, sess.Wait())
}

func (s *Server) continueSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		PauseID   int    `json:"pauseId"`
		ClientID  string `json:"clientId"`
		RequestID string `json:"requestId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.ContinueEx(body.ClientID, body.PauseID, body.RequestID)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) pauseSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		ClientID  string `json:"clientId"`
		RequestID string `json:"requestId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.PauseEx(body.ClientID, body.RequestID)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) setBreakpoints(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Lines     []int  `json:"lines"`
		ClientID  string `json:"clientId"`
		RequestID string `json:"requestId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.SetBreakpointsEx(body.ClientID, body.Lines, body.RequestID)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) variables(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		PauseID int      `json:"pauseId"`
		Names   []string `json:"names"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.Variables(body.PauseID, body.Names)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"vars": result})
}

func (s *Server) previewBreakpoints(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Changes  []executor.Breakpoint `json:"changes"`
		ClientID string                `json:"clientId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	view, err := sess.PreviewBreakpoints(body.ClientID, body.Changes)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) getCandidate(w http.ResponseWriter, _ *http.Request, sess *session.Session) {
	view, err := sess.Candidate()
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) adoptCandidate(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		ClientID  string `json:"clientId"`
		RequestID string `json:"requestId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, recheck, err := sess.AdoptCandidate(body.ClientID, body.RequestID)
	if err != nil {
		if errors.Is(err, session.ErrStaleCandidate) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "recheck": recheck})
			return
		}
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) reloadSource(w http.ResponseWriter, _ *http.Request, sess *session.Session) {
	version, err := sess.NoteSourceChange()
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sourceVersion": version})
}

func writeSessionError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, session.ErrRunning):
		status = http.StatusConflict
	case errors.Is(err, session.ErrTerminated):
		status = http.StatusGone
	case errors.Is(err, session.ErrPauseMismatch), errors.Is(err, session.ErrInvalidPause):
		status = http.StatusConflict
	case errors.Is(err, session.ErrNotController):
		status = http.StatusForbidden
	case errors.Is(err, session.ErrUnknownClient):
		status = http.StatusBadRequest
	case errors.Is(err, session.ErrNoCandidate):
		status = http.StatusConflict
	default:
		writeError(w, status, err)
		return false
	}
	writeError(w, status, err)
	return false
}

func decodeBody(r *http.Request, target any) error {
	if r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(target)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
