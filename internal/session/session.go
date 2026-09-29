// Package session owns the debugger state machine: sessions, pause
// generations, breakpoints and executor lifecycle.
package session

import (
	"sort"
	"sync"

	"dbgprotocol/internal/virtual"
)

// Event is an asynchronous notification delivered to the client.
type Event struct {
	Type     string `json:"type"`
	PauseID  int64  `json:"pauseId,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Line     int    `json:"line,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
	Message  string `json:"message,omitempty"`
}

// Engine is the executor surface consumed by a session. Other executors
// (remote, simulator, ...) can be plugged in here without changing the
// request semantics exposed to clients.
type Engine interface {
	Run(virtual.Control)
	Lines() []int
	Variables() map[string]int64
}

// HoldReleaser is implemented by engines that support the launch-time
// holdBeforeLine test aid.
type HoldReleaser interface {
	ReleaseHold()
}

const (
	StatusPaused  = "paused"
	StatusRunning = "running"
	StatusExited  = "exited"
)

// Session is a single debug session bound to one engine.
type Session struct {
	ID     string
	engine Engine

	emit func(Event)

	mu        sync.Mutex
	status    string
	pauseID   int64
	line      int
	breakers  map[int]bool
	validLine map[int]bool
	step      bool
	resumeCh  chan virtual.Resume
	doneCh    chan struct{} // closed when the session is torn down
	exitedCh  chan struct{} // closed when the engine exits
	exitCode  int
	exitMsg   string
	closed    bool
}

// New wires a session to an engine. emit receives ordered lifecycle events.
func New(id string, engine Engine, emit func(Event)) *Session {
	valid := map[int]bool{}
	for _, ln := range engine.Lines() {
		valid[ln] = true
	}
	return &Session{
		ID:        id,
		engine:    engine,
		emit:      emit,
		breakers:  map[int]bool{},
		validLine: valid,
		doneCh:    make(chan struct{}),
		exitedCh:  make(chan struct{}),
	}
}

// Start launches the engine goroutine. The entry pause is reported
// asynchronously via the event callback.
func (s *Session) Start() {
	go func() {
		s.engine.Run(s)
	}()
}

// Close tears the session down and unblocks a paused engine.
func (s *Session) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	resume := s.resumeCh
	close(s.doneCh)
	s.mu.Unlock()

	if resume != nil {
		select {
		case resume <- virtual.Resume{OK: false}:
		default:
		}
	}
}

// Paused implements virtual.Control. It opens a new pause generation and
// blocks until that exact generation is resumed or the session ends.
func (s *Session) Paused(reason string, line int) virtual.Resume {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return virtual.Resume{OK: false}
	}
	s.pauseID++
	id := s.pauseID
	s.status = StatusPaused
	s.line = line
	s.step = false
	s.resumeCh = make(chan virtual.Resume, 1)
	ch := s.resumeCh
	ev := Event{Type: "stopped", PauseID: id, Reason: reason, Line: line}
	s.mu.Unlock()

	s.emit(ev)

	select {
	case r := <-ch:
		return r
	case <-s.doneCh:
		return virtual.Resume{OK: false}
	}
}

// Decision implements virtual.Control. Breakpoints and single-step requests
// are evaluated only at instruction boundaries, which are the safe points.
func (s *Session) Decision(line int) virtual.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return virtual.Decision{Abort: true}
	}
	if s.breakers[line] {
		s.step = false
		return virtual.Decision{Pause: true, Reason: "breakpoint"}
	}
	if s.step {
		s.step = false
		return virtual.Decision{Pause: true, Reason: "step"}
	}
	return virtual.Decision{}
}

// Exit implements virtual.Control. Normal and abnormal termination both
// finish every waiter.
func (s *Session) Exit(code int, message string) {
	s.mu.Lock()
	if s.status == StatusExited {
		s.mu.Unlock()
		return
	}
	s.status = StatusExited
	s.exitCode = code
	s.exitMsg = message
	s.step = false
	s.resumeCh = nil
	close(s.exitedCh)
	ev := Event{Type: "terminated", ExitCode: code, Message: message}
	s.mu.Unlock()

	s.emit(ev)
}

// Snapshot is the state returned to inspection requests.
type Snapshot struct {
	Status  string
	PauseID int64
	Line    int
}

func (s *Session) snapshotLocked() Snapshot {
	return Snapshot{Status: s.status, PauseID: s.pauseID, Line: s.line}
}

// State returns the current state without pause-generation checks.
func (s *Session) State() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

// SetBreakpoints atomically replaces the breakpoint set. Unknown lines are
// rejected. The change only affects instruction boundaries reached later,
// so an in-flight instruction can never observe a half-applied set.
func (s *Session) SetBreakpoints(lines []int) (verified []int, rejected []int) {
	next := map[int]bool{}
	seen := map[int]bool{}
	for _, ln := range lines {
		if seen[ln] {
			continue
		}
		seen[ln] = true
		if s.validLine[ln] {
			next[ln] = true
			verified = append(verified, ln)
		} else {
			rejected = append(rejected, ln)
		}
	}
	sort.Ints(verified)
	s.mu.Lock()
	s.breakers = next
	s.mu.Unlock()
	return verified, rejected
}

// Breakpoints returns the currently active breakpoint lines.
func (s *Session) Breakpoints() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, 0, len(s.breakers))
	for ln := range s.breakers {
		out = append(out, ln)
	}
	sort.Ints(out)
	return out
}

// RequirePaused validates that pauseID names the current pause generation.
// Variables/stack frames may only be read against a live pause: reading
// while running would risk a mixed snapshot, and an old generation must
// never drive a newer pause.
func (s *Session) RequirePaused(pauseID int64) (Snapshot, *Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.snapshotLocked()
	if s.status == StatusExited {
		return snap, &Error{Code: "ProcessExited", Message: s.exitMsg, ExitCode: s.exitCode}
	}
	if s.status == StatusRunning {
		return snap, &Error{Code: "NotPaused", Message: "execution is running; state can only be inspected while paused"}
	}
	if pauseID != 0 && pauseID != snap.PauseID {
		return snap, &Error{Code: "StalePause", Message: "pause generation has changed", CurrentPauseID: snap.PauseID}
	}
	return snap, nil
}

// Variables returns a single atomic snapshot taken from the paused engine.
func (s *Session) Variables(pauseID int64) (map[string]int64, int, *Error) {
	snap, perr := s.RequirePaused(pauseID)
	if perr != nil {
		return nil, snap.Line, perr
	}
	// The engine goroutine is blocked on this pause, so the copy it returns
	// cannot be interleaved with variable mutations.
	return s.engine.Variables(), snap.Line, nil
}

// Resume continues or single-steps the named pause generation. The engine is
// only unblocked when the returned commit function runs, letting the
// protocol layer write the response and continued event before any next
// stop event. Nil commit means nothing was resumed.
func (s *Session) Resume(pauseID int64, action string) (Snapshot, func(), *Error) {
	s.mu.Lock()
	if s.status == StatusExited {
		snap := s.snapshotLocked()
		s.mu.Unlock()
		return snap, nil, &Error{Code: "ProcessExited", Message: s.exitMsg, ExitCode: s.exitCode}
	}
	if s.status != StatusPaused {
		snap := s.snapshotLocked()
		s.mu.Unlock()
		return snap, nil, &Error{Code: "NotPaused", Message: "execution is not paused; duplicate resume ignored"}
	}
	if pauseID != 0 && pauseID != s.pauseID {
		snap := s.snapshotLocked()
		s.mu.Unlock()
		return snap, nil, &Error{Code: "StalePause", Message: "pause generation has changed", CurrentPauseID: snap.PauseID}
	}
	s.status = StatusRunning
	s.step = action == "next"
	id := s.pauseID
	ch := s.resumeCh
	s.resumeCh = nil
	s.mu.Unlock()

	commit := func() {
		ch <- virtual.Resume{Action: action, OK: true}
	}
	return Snapshot{Status: StatusRunning, PauseID: id}, commit, nil
}

// WaitExit blocks until the engine exits or the session is torn down.
func (s *Session) WaitExit() (code int, msg string, exited bool) {
	select {
	case <-s.exitedCh:
	case <-s.doneCh:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exitCode, s.exitMsg, s.status == StatusExited
}

// ReleaseHold unblocks an executor parked by holdBeforeLine. It is a no-op
// for engines without hold support.
func (s *Session) ReleaseHold() {
	if hr, ok := s.engine.(HoldReleaser); ok {
		hr.ReleaseHold()
	}
}

// Error is the structured failure returned to protocol clients.
type Error struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	ExitCode       int    `json:"exitCode,omitempty"`
	CurrentPauseID int64  `json:"currentPauseId,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }
