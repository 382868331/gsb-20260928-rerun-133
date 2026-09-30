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
}

type WaitResult = Snapshot

type ContinueResult struct {
	Paused     *Snapshot `json:"paused,omitempty"`
	Terminated bool      `json:"terminated,omitempty"`
	Reason     string    `json:"reason,omitempty"`
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
		snapshot := Snapshot{
			State: s.state, Reason: s.reason, PauseID: s.pauseID,
			Line: s.line, PC: s.pc,
		}
		if s.state == StateStopped {
			snapshot = Snapshot{
				State: s.state, Reason: s.reason, PauseID: s.pauseID,
				Line: s.line, PC: s.pc, Vars: cloneVars(s.vars),
			}
		}
		if s.state == StateTerminated {
			snapshot.LastPause = s.lastPauseRef()
		}
		result <- snapshot
	})
	return <-result
}

// Continue resumes only the pause identified by pauseId. A duplicate continue
// or a request from the previous pause cannot operate the new pause.
func (s *Session) Continue(pauseID int) (ContinueResult, error) {
	if pauseID == 0 {
		return ContinueResult{}, ErrInvalidPause
	}
	errCh := make(chan error, 1)
	w := make(waiter, 1)
	s.submit(func(s *Session) {
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
	select {
	case err := <-errCh:
		return ContinueResult{}, err
	case r := <-w:
		if r.State == StateTerminated {
			return ContinueResult{Terminated: true, Reason: r.Reason}, nil
		}
		return ContinueResult{Paused: &r}, nil
	case <-s.terminated:
		return ContinueResult{}, ErrTerminated
	}
}

// Pause asks a running executor to stop at the next safe point. If already
// stopped, the caller receives the current pause rather than invalidating it.
func (s *Session) Pause() (WaitResult, error) {
	w := make(waiter, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
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
	select {
	case err := <-errCh:
		return WaitResult{}, err
	case r := <-w:
		return r, nil
	}
}

// SetBreakpoints replaces the complete breakpoint set. While running it is
// handed to the executor and takes effect at the next safe point; while
// stopped it takes effect at the current safe position.
func (s *Session) SetBreakpoints(lines []int) (map[string]any, error) {
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
