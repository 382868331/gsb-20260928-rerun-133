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
	mux.HandleFunc("POST /sessions/{id}/wait", s.withSession(s.waitSession))
	mux.HandleFunc("POST /sessions/{id}/continue", s.withSession(s.continueSession))
	mux.HandleFunc("POST /sessions/{id}/pause", s.withSession(s.pauseSession))
	mux.HandleFunc("POST /sessions/{id}/breakpoints", s.withSession(s.setBreakpoints))
	mux.HandleFunc("POST /sessions/{id}/variables", s.withSession(s.variables))
	mux.HandleFunc("POST /sessions/{id}/attach", s.withSession(s.attachSession))
	mux.HandleFunc("POST /sessions/{id}/release", s.withSession(s.releaseSession))
	mux.HandleFunc("POST /sessions/{id}/claim", s.withSession(s.claimSession))
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

// attachSession registers (or re-registers after a disconnect) a client and
// answers with its token, role, and a fresh snapshot so a reconnecting
// client immediately observes the current pause or termination state.
func (s *Server) attachSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Client string `json:"client"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if body.Client == "" {
		writeError(w, http.StatusBadRequest, errors.New("client id is required"))
		return
	}
	writeJSON(w, http.StatusOK, sess.Attach(body.Client))
}

// releaseSession drops execution control; any attached client may then claim.
func (s *Server) releaseSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Token string `json:"token"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := sess.Release(body.Token); err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"controller": ""})
}

// claimSession lets an attached observer take over vacant execution control.
func (s *Server) claimSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Token string `json:"token"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := sess.Claim(body.Token); err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"role": string(session.RoleController)})
}

func (s *Server) waitSession(w http.ResponseWriter, _ *http.Request, sess *session.Session) {
	writeJSON(w, http.StatusOK, sess.Wait())
}

func (s *Server) continueSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		PauseID   int    `json:"pauseId"`
		Token     string `json:"token"`
		RequestID string `json:"requestId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.ContinueAs(body.Token, body.PauseID, body.RequestID)
	if err != nil {
		writeSessionErrorDuplicate(w, err, result.Duplicate)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) pauseSession(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Token     string `json:"token"`
		RequestID string `json:"requestId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.PauseAs(body.Token, body.RequestID)
	if err != nil {
		writeSessionErrorDuplicate(w, err, result.Duplicate)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) setBreakpoints(w http.ResponseWriter, r *http.Request, sess *session.Session) {
	var body struct {
		Lines []int  `json:"lines"`
		Token string `json:"token"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := sess.SetBreakpointsAs(body.Token, body.Lines)
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

func writeSessionError(w http.ResponseWriter, err error) bool {
	return writeSessionErrorDuplicate(w, err, false)
}

// writeSessionErrorDuplicate maps session errors to statuses and annotates
// idempotent replays so a client can tell an already-processed request from
// a fresh rejection.
func writeSessionErrorDuplicate(w http.ResponseWriter, err error, duplicate bool) bool {
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
	case errors.Is(err, session.ErrNotController), errors.Is(err, session.ErrUnknownClient):
		status = http.StatusForbidden
	case errors.Is(err, session.ErrNoController), errors.Is(err, session.ErrControlHeld):
		status = http.StatusConflict
	default:
		writeError(w, status, err)
		return false
	}
	if duplicate {
		writeJSON(w, status, map[string]any{"error": err.Error(), "duplicate": true})
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
