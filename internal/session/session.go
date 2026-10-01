package session

import (
	"errors"
	"sync"

	"debugservice/internal/executor"
)

var (
	ErrRunning       = errors.New("session is running")
	ErrTerminated    = errors.New("session is terminated")
	ErrPauseMismatch = errors.New("pause id does not match the current pause")
	ErrInvalidPause  = errors.New("pause id is required")
	ErrUnknownClient = errors.New("unknown client token")
	ErrNotController = errors.New("client does not hold control of the session")
	ErrNoController  = errors.New("session has no controller; claim control first")
	ErrControlHeld   = errors.New("control is held by another client")
)

type State string

const (
	StateStopped    State = "stopped"
	StateRunning    State = "running"
	StateTerminated State = "terminated"
)

// Snapshot is an immutable view emitted for one state transition.
type Snapshot struct {
	State     State            `json:"state"`
	Reason    string           `json:"reason,omitempty"`
	PauseID   int              `json:"pauseId,omitempty"`
	Line      int              `json:"line,omitempty"`
	PC        int              `json:"pc,omitempty"`
	Vars      map[string]int64 `json:"vars,omitempty"`
	LastPause *Snapshot        `json:"lastPause,omitempty"`
	// Controller names the client currently holding execution control.
	// Empty when no client has attached (legacy open mode) or control has
	// been released and is waiting to be claimed.
	Controller string `json:"controller,omitempty"`
}

type WaitResult = Snapshot

type ContinueResult struct {
	Paused     *Snapshot `json:"paused,omitempty"`
	Terminated bool      `json:"terminated,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	// Duplicate reports that this outcome was recorded for an earlier
	// request carrying the same requestId; the program was not advanced
	// a second time.
	Duplicate bool `json:"duplicate,omitempty"`
}

// PauseResult is the outcome of a pause request.
type PauseResult struct {
	WaitResult
	Duplicate bool `json:"duplicate,omitempty"`
}

// Role describes how an attached client may interact with the session.
type Role string

const (
	RoleController Role = "controller"
	RoleObserver   Role = "observer"
)

// AttachResult is returned when a client attaches or re-attaches to a
// session, including after a disconnect.
type AttachResult struct {
	Token    string   `json:"token"`
	Role     Role     `json:"role"`
	Snapshot Snapshot `json:"snapshot"`
}

type clientInfo struct {
	token string
}

// storedRequest records the outcome of an idempotent request so a replay
// (for example after a reconnect) can be explained without advancing the
// program a second time.
type storedRequest struct {
	done        chan struct{}
	continueRes ContinueResult
	pauseRes    PauseResult
	err         error
}

type Engine interface {
	executor.Executor
}

type waiter chan WaitResult

type command struct {
	apply  func(*Session)
	closer bool
}

// Session serializes every request in one event loop. Continue and variable
// reads issued concurrently therefore observe one transition each; they never
// interleave into a mixed snapshot.
type Session struct {
	id     string
	engine Engine

	cmds       chan command
	done       chan struct{}
	terminated chan struct{}
	closeOnce  sync.Once
	// submitMu guards closing and pairs admission with teardown. A command
	// admitted by submit is always consumed by the loop, so no request is
	// stranded in cmds when the executor exits.
	submitMu sync.Mutex
	closing  bool
	inflight sync.WaitGroup
	// terminalMu serializes requests answered directly after the loop exits.
	terminalMu sync.Mutex
	readyWG    sync.WaitGroup
	readyDone  bool
	wg         sync.WaitGroup
	waiters    map[waiter]struct{}
	state      State
	reason     string
	pc         int
	line       int
	vars       map[string]int64
	pauseID    int
	lastPause  *Snapshot
	breakpoint map[int]bool
	// clients tracks every attached client by its caller-chosen id.
	// controller names the client id holding execution control; empty
	// means control is released and can be claimed. When no client has
	// ever attached the session runs in legacy open mode and mutating
	// requests are admitted without a token.
	clients    map[string]*clientInfo
	controller string
	// requests remembers recent idempotent request outcomes, keyed by
	// requestId, so replays are answered instead of re-executed.
	requests   map[string]*storedRequest
	requestSeq []string
}

func New(id string, engine Engine) *Session {
	s := &Session{
		id:         id,
		engine:     engine,
		cmds:       make(chan command, 32),
		done:       make(chan struct{}),
		terminated: make(chan struct{}),
		waiters:    map[waiter]struct{}{},
		state:      StateRunning,
		breakpoint: map[int]bool{},
		clients:    map[string]*clientInfo{},
		requests:   map[string]*storedRequest{},
	}
	events := make(chan executor.Event, 8)
	s.readyWG.Add(1)
	s.wg.Add(1)
	go s.loop(events)
	engine.Start(events)
	s.readyWG.Wait()
	return s
}

func (s *Session) ID() string { return s.id }

func (s *Session) loop(events <-chan executor.Event) {
	defer func() {
		close(s.done)
		s.wg.Done()
	}()
	for {
		select {
		case event := <-events:
			s.onEvent(event)
			continue
		default:
		}
		select {
		case event := <-events:
			s.onEvent(event)
		case cmd := <-s.cmds:
			if cmd.closer {
				s.handleClose(events)
				return
			}
			cmd.apply(s)
		}
	}
}

// handleClose shuts a live executor down, waits for its terminal event, and
// then tears the loop down. A close while already terminated tears down right
// away, so Shutdown always releases the session.
func (s *Session) handleClose(events <-chan executor.Event) {
	if s.state != StateTerminated {
		s.engine.Shutdown()
		for s.state != StateTerminated {
			select {
			case event := <-events:
				s.onEvent(event)
			case cmd := <-s.cmds:
				if !cmd.closer {
					cmd.apply(s)
				}
			}
		}
	}
	s.teardown(events)
}

// teardown stops admitting new commands, waits for every admitted command to
// be consumed, and drains the channel defensively. Commands racing with the
// switch observe closing and answer directly from submit.
func (s *Session) teardown(events <-chan executor.Event) {
	s.submitMu.Lock()
	s.closing = true
	s.submitMu.Unlock()
	s.inflight.Wait()
drain:
	for {
		select {
		case cmd := <-s.cmds:
			if cmd.apply != nil {
				cmd.apply(s)
			}
		case event := <-events:
			s.onEvent(event)
		default:
			break drain
		}
	}
}

func (s *Session) onEvent(event executor.Event) {
	switch event.Type {
	case executor.EventStopped:
		s.state = StateStopped
		s.reason = event.Reason
		s.pc = event.PC
		s.line = event.Line
		s.vars = cloneVars(event.Vars)
		s.pauseID = event.PauseID
		s.lastPause = &Snapshot{
			State: StateStopped, Reason: event.Reason, PauseID: event.PauseID,
			Line: event.Line, PC: event.PC, Vars: cloneVars(event.Vars),
		}
		s.notify(Snapshot{
			State: StateStopped, Reason: event.Reason, PauseID: s.pauseID,
			Line: event.Line, PC: event.PC, Vars: cloneVars(event.Vars),
			LastPause: s.lastPauseRef(),
		})
	case executor.EventTerminated:
		s.state = StateTerminated
		s.reason = event.Reason
		s.pc = event.PC
		s.line = event.Line
		s.vars = cloneVars(event.Vars)
		s.pauseID = 0
		s.notify(WaitResult{State: StateTerminated, Reason: event.Reason, Line: event.Line, LastPause: s.lastPauseRef()})
		for w := range s.waiters {
			s.deliver(w)
		}
		close(s.terminated)
	}
	if !s.readyDone {
		s.readyDone = true
		s.readyWG.Done()
	}
}

func (s *Session) notify(r WaitResult) {
	for w := range s.waiters {
		w <- r
		close(w)
		delete(s.waiters, w)
	}
}

func (s *Session) deliver(w waiter) {
	select {
	case w <- WaitResult{State: StateTerminated, Reason: s.reason, Line: s.line, LastPause: s.lastPauseRef()}:
		close(w)
	default:
	}
	delete(s.waiters, w)
}

func (s *Session) lastPauseRef() *Snapshot {
	if s.lastPause == nil {
		return nil
	}
	ref := *s.lastPause
	if s.lastPause.Vars != nil {
		ref.Vars = cloneVars(s.lastPause.Vars)
	}
	return &ref
}

func (s *Session) submit(apply func(*Session)) {
	s.submitMu.Lock()
	if s.closing {
		s.submitMu.Unlock()
		// The loop is gone or leaving. Serialize late answers so they still
		// observe a coherent terminal state and never park on cmds.
		<-s.done
		s.terminalMu.Lock()
		apply(s)
		s.terminalMu.Unlock()
		return
	}
	s.inflight.Add(1)
	s.submitMu.Unlock()
	select {
	case s.cmds <- command{apply: apply}:
	case <-s.done:
		apply(s)
	}
	s.inflight.Done()
}

// Wait blocks until the next stop or termination. All waiters are released on
// executor exit, including abnormal exits.
func (s *Session) Wait() WaitResult {
	w := make(waiter, 1)
	result := make(chan WaitResult, 1)
	s.submit(func(s *Session) {
		if s.state == StateTerminated {
			s.deliver(w)
		} else {
			s.waiters[w] = struct{}{}
		}
	})
	go func() { result <- <-w }()
	return <-result
}

// Snapshot returns the current state. Variables are only included for a live
// pause matching pauseId.
func (s *Session) Snapshot() Snapshot {
	result := make(chan Snapshot, 1)
	s.submit(func(s *Session) {
		result <- s.snapshotLocked()
	})
	return <-result
}

// snapshotLocked builds the current snapshot. It must run inside the event
// loop (or after teardown under terminalMu).
func (s *Session) snapshotLocked() Snapshot {
	snapshot := Snapshot{
		State: s.state, Reason: s.reason, PauseID: s.pauseID,
		Line: s.line, PC: s.pc, Controller: s.controller,
	}
	if s.state == StateStopped {
		snapshot.Vars = cloneVars(s.vars)
	}
	if s.state == StateTerminated {
		snapshot.LastPause = s.lastPauseRef()
	}
	return snapshot
}

// controlErrorLocked enforces execution-control ownership. In legacy open
// mode (no client ever attached) every caller may advance the program. Once
// clients attach, only the controller's token may mutate execution.
func (s *Session) controlErrorLocked(token string) error {
	if len(s.clients) == 0 {
		return nil
	}
	if s.controller == "" {
		return ErrNoController
	}
	if token == "" {
		return ErrNotController
	}
	if info := s.clients[s.controller]; info != nil && info.token == token {
		return nil
	}
	return ErrNotController
}

// rememberLocked stores an idempotent request outcome, evicting the oldest
// entries past a small bound.
func (s *Session) rememberLocked(requestID string, stored *storedRequest) {
	const maxRemembered = 128
	s.requests[requestID] = stored
	s.requestSeq = append(s.requestSeq, requestID)
	for len(s.requestSeq) > maxRemembered {
		oldest := s.requestSeq[0]
		s.requestSeq = s.requestSeq[1:]
		delete(s.requests, oldest)
	}
}

// Continue resumes only the pause identified by pauseId. A duplicate continue
// or a request from the previous pause cannot operate the new pause.
func (s *Session) Continue(pauseID int) (ContinueResult, error) {
	return s.ContinueAs("", pauseID, "")
}

// ContinueAs is Continue with execution-control and idempotency semantics.
// token must belong to the current controller once any client has attached.
// A non-empty requestID makes the request idempotent: a replay after a
// reconnect receives the recorded outcome with Duplicate set and never
// advances the program again.
func (s *Session) ContinueAs(token string, pauseID int, requestID string) (ContinueResult, error) {
	if pauseID == 0 {
		return ContinueResult{}, ErrInvalidPause
	}
	type admission struct {
		stored *storedRequest
		replay bool
	}
	admitCh := make(chan admission, 1)
	errCh := make(chan error, 1)
	w := make(waiter, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.requests[requestID]; ok {
				admitCh <- admission{stored: stored, replay: true}
				return
			}
		}
		var stored *storedRequest
		if requestID != "" {
			stored = &storedRequest{done: make(chan struct{})}
			s.rememberLocked(requestID, stored)
		}
		admitCh <- admission{stored: stored}
		if err := s.controlErrorLocked(token); err != nil {
			errCh <- err
			return
		}
		switch {
		case s.state == StateTerminated:
			errCh <- ErrTerminated
		case s.state == StateRunning:
			errCh <- ErrRunning
		case pauseID != s.pauseID:
			errCh <- ErrPauseMismatch
		default:
			currentPauseID := s.pauseID
			s.waiters[w] = struct{}{}
			s.engine.UpdateBreakpoints(s.breakpoint)
			s.state = StateRunning
			s.pauseID = 0
			s.engine.Resume(currentPauseID, s.breakpoint)
		}
	})
	adm := <-admitCh
	if adm.replay {
		stored := adm.stored
		<-stored.done
		result := stored.continueRes
		result.Duplicate = true
		return result, stored.err
	}
	finish := func(result ContinueResult, err error) (ContinueResult, error) {
		if adm.stored != nil {
			adm.stored.continueRes = result
			adm.stored.err = err
			close(adm.stored.done)
		}
		return result, err
	}
	select {
	case err := <-errCh:
		return finish(ContinueResult{}, err)
	case r := <-w:
		if r.State == StateTerminated {
			return finish(ContinueResult{Terminated: true, Reason: r.Reason}, nil)
		}
		return finish(ContinueResult{Paused: &r}, nil)
	case <-s.terminated:
		return finish(ContinueResult{}, ErrTerminated)
	}
}

// Pause asks a running executor to stop at the next safe point. If already
// stopped, the caller receives the current pause rather than invalidating it.
func (s *Session) Pause() (WaitResult, error) {
	result, err := s.PauseAs("", "")
	return result.WaitResult, err
}

// PauseAs is Pause with execution-control and idempotency semantics, mirroring
// ContinueAs: token must be the controller's once clients exist, and a
// non-empty requestID makes the request replayable without side effects.
func (s *Session) PauseAs(token string, requestID string) (PauseResult, error) {
	type admission struct {
		stored *storedRequest
		replay bool
	}
	admitCh := make(chan admission, 1)
	w := make(waiter, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.requests[requestID]; ok {
				admitCh <- admission{stored: stored, replay: true}
				return
			}
		}
		var stored *storedRequest
		if requestID != "" {
			stored = &storedRequest{done: make(chan struct{})}
			s.rememberLocked(requestID, stored)
		}
		admitCh <- admission{stored: stored}
		if err := s.controlErrorLocked(token); err != nil {
			errCh <- err
			return
		}
		switch s.state {
		case StateTerminated:
			errCh <- ErrTerminated
		case StateStopped:
			w <- WaitResult{State: StateStopped, PauseID: s.pauseID, Reason: s.reason, Line: s.line, PC: s.pc, Vars: cloneVars(s.vars)}
			close(w)
		default:
			s.waiters[w] = struct{}{}
			s.engine.Pause()
		}
	})
	adm := <-admitCh
	if adm.replay {
		stored := adm.stored
		<-stored.done
		result := stored.pauseRes
		result.Duplicate = true
		return result, stored.err
	}
	finish := func(result PauseResult, err error) (PauseResult, error) {
		if adm.stored != nil {
			adm.stored.pauseRes = result
			adm.stored.err = err
			close(adm.stored.done)
		}
		return result, err
	}
	select {
	case err := <-errCh:
		return finish(PauseResult{}, err)
	case r := <-w:
		return finish(PauseResult{WaitResult: r}, nil)
	}
}

// SetBreakpoints replaces the complete breakpoint set. While running it is
// handed to the executor and takes effect at the next safe point; while
// stopped it takes effect at the current safe position.
func (s *Session) SetBreakpoints(lines []int) (map[string]any, error) {
	return s.SetBreakpointsAs("", lines)
}

// SetBreakpointsAs is SetBreakpoints guarded by execution control: once any
// client has attached, only the controller's token may change breakpoints.
func (s *Session) SetBreakpointsAs(token string, lines []int) (map[string]any, error) {
	next := make(map[int]bool, len(lines))
	for _, line := range lines {
		next[line] = true
	}
	result := make(chan map[string]any, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if s.state == StateTerminated {
			errCh <- ErrTerminated
			return
		}
		if err := s.controlErrorLocked(token); err != nil {
			errCh <- err
			return
		}
		s.breakpoint = next
		pending := false
		if s.state == StateRunning {
			pending = s.engine.UpdateBreakpoints(next)
		}
		result <- map[string]any{"breakpoints": lines, "pending": pending}
	})
	select {
	case err := <-errCh:
		return nil, err
	case r := <-result:
		return r, nil
	}
}

// Variables reads variables from exactly one pause. While running it fails
// instead of exposing a prior pause; a stale pauseId cannot read a new pause.
func (s *Session) Variables(pauseID int, names []string) (map[string]int64, error) {
	if pauseID == 0 {
		return nil, ErrInvalidPause
	}
	result := make(chan map[string]int64, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		switch {
		case s.state == StateTerminated:
			errCh <- ErrTerminated
		case s.state == StateRunning:
			errCh <- ErrRunning
		case pauseID != s.pauseID:
			errCh <- ErrPauseMismatch
		default:
			out := map[string]int64{}
			if len(names) == 0 {
				for name, value := range s.vars {
					out[name] = value
				}
			} else {
				for _, name := range names {
					if value, ok := s.vars[name]; ok {
						out[name] = value

					}
				}
			}
			result <- out
		}
	})
	select {
	case err := <-errCh:
		return nil, err
	case r := <-result:
		return r, nil
	}
}

// Shutdown terminates a paused executor and waits for the event loop to drain
// every request that was admitted before the teardown started.
func (s *Session) Shutdown() {
	s.closeOnce.Do(func() {
		s.submitMu.Lock()
		if s.closing {
			s.submitMu.Unlock()
			return
		}
		s.inflight.Add(1)
		s.submitMu.Unlock()
		s.cmds <- command{closer: true}
		s.inflight.Done()
	})
	s.wg.Wait()
}

func (s *Session) Close() { s.Shutdown() }

func cloneVars(in map[string]int64) map[string]int64 {
	if in == nil {
		return nil
	}
	out := make(map[string]int64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}
