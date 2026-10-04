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
	ErrNotController = errors.New("another client holds control of this session")
	ErrUnknownClient = errors.New("client id is required")
)

const (
	RoleController = "controller"
	RoleObserver   = "observer"
)

type State string

const (
	StateStopped    State = "stopped"
	StateRunning    State = "running"
	StateTerminated State = "terminated"
)

// Snapshot is an immutable view emitted for one state transition.
type Snapshot struct {
	State      State            `json:"state"`
	Reason     string           `json:"reason,omitempty"`
	PauseID    int              `json:"pauseId,omitempty"`
	Line       int              `json:"line,omitempty"`
	PC         int              `json:"pc,omitempty"`
	Vars       map[string]int64 `json:"vars,omitempty"`
	LastPause  *Snapshot        `json:"lastPause,omitempty"`
	Controller string           `json:"controller,omitempty"`
	// BreakpointVersion is the breakpoint version this pause actually ran
	// under, so every stop can be checked against the version in effect.
	BreakpointVersion int `json:"breakpointVersion,omitempty"`
	// ConditionError carries the diagnostic when the stop was caused by a
	// broken breakpoint condition.
	ConditionError string `json:"conditionError,omitempty"`
	// HitCount is the accumulated hit count of the breakpoint that
	// produced this pause, so every stop can be checked against the hit
	// basis recorded by the executor.
	HitCount int `json:"hitCount,omitempty"`
}

type WaitResult = Snapshot

type ContinueResult struct {
	Paused     *Snapshot `json:"paused,omitempty"`
	Terminated bool      `json:"terminated,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Duplicate  bool      `json:"duplicate,omitempty"`
}

// AttachResult reports the role a client received and the authoritative
// snapshot at attach time, so a reconnecting client immediately observes the
// current pause or the trusted termination state.
type AttachResult struct {
	ClientID   string   `json:"clientId"`
	Role       string   `json:"role"`
	Controller string   `json:"controller,omitempty"`
	Snapshot   Snapshot `json:"snapshot"`
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
	hitCount   int
	lastPause  *Snapshot
	// bpSpecs is the authoritative breakpoint set of the current version.
	bpSpecs []BreakpointSpec
	// bpVersion numbers the authoritative set; bpPauseVersion records the
	// version the current pause ran under; bpPendingVersion is the version
	// pushed to the executor at the last resume.
	bpVersion        int
	bpPauseVersion   int
	bpPendingVersion int
	bpAdoptedBy      string
	bpEffectivePause int
	// bpHits and bpCondErrs accumulate hits and condition diagnostics for
	// the current version only; they are archived into bpHistory when a
	// new version is installed.
	bpHits     map[int]int
	bpCondErrs map[int]string
	bpHistory  []VersionRecord
	// candidates holds staged breakpoint changes awaiting adoption.
	candidates      []*Candidate
	nextCandidateID int
	// controller is the client that owns execution advancement. Observers
	// may read state but cannot resume, pause, or move breakpoints.
	controller string
	// completed stores the outcome of idempotent requests by request id so
	// a reconnect-triggered retry is explained instead of re-executed.
	completed map[string]requestOutcome
}

type requestOutcome struct {
	continueResult *ContinueResult
	pauseResult    *WaitResult
	breakpoints    map[string]any
	candidate      *Candidate
	version        *VersionRecord
	err            error
}

func New(id string, engine Engine) *Session {
	s := &Session{
		id:               id,
		engine:           engine,
		cmds:             make(chan command, 32),
		done:             make(chan struct{}),
		terminated:       make(chan struct{}),
		waiters:          map[waiter]struct{}{},
		state:            StateRunning,
		bpPendingVersion: 1,
		bpVersion:        1,
		bpHits:           map[int]int{},
		bpCondErrs:       map[int]string{},
		completed:        map[string]requestOutcome{},
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
		s.hitCount = event.HitCount
		s.bpPauseVersion = s.bpPendingVersion
		switch event.Reason {
		case executor.ReasonBreakpoint:
			s.bpHits[event.Line]++
		case executor.ReasonConditionError:
			// A broken condition keeps its diagnostic; it is neither a
			// hit nor a silent miss.
			s.bpCondErrs[event.Line] = event.ConditionError
		}
		s.flagCandidatesOnPauseLocked()
		s.lastPause = &Snapshot{
			State: StateStopped, Reason: event.Reason, PauseID: event.PauseID,
			Line: event.Line, PC: event.PC, Vars: cloneVars(event.Vars),
			BreakpointVersion: s.bpPauseVersion, ConditionError: event.ConditionError,
			HitCount: event.HitCount,
		}
		s.notify(Snapshot{
			State: StateStopped, Reason: event.Reason, PauseID: s.pauseID,
			Line: event.Line, PC: event.PC, Vars: cloneVars(event.Vars),
			LastPause:         s.lastPauseRef(),
			BreakpointVersion: s.bpPauseVersion, ConditionError: event.ConditionError,
			HitCount: event.HitCount,
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

// snapshotLocked builds the authoritative view from inside the event loop.
func (s *Session) snapshotLocked() Snapshot {
	snapshot := Snapshot{
		State: s.state, Reason: s.reason, PauseID: s.pauseID,
		Line: s.line, PC: s.pc, Controller: s.controller,
		BreakpointVersion: s.bpPauseVersion,
		HitCount:          s.hitCount,
	}
	if s.state == StateStopped {
		snapshot.Vars = cloneVars(s.vars)
	}
	if s.state == StateTerminated {
		snapshot.LastPause = s.lastPauseRef()
	}
	return snapshot
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

// Attach registers a client with the session and resolves its role. The
// first client asking for control becomes the controller; everyone else is a
// read-only observer. A reconnecting client reattaches with the same id and
// immediately receives the authoritative snapshot, including the trusted
// termination reason and last pause after the program has exited. takeover
// lets a new client claim control from a controller that went away.
func (s *Session) Attach(clientID string, wantController bool, takeover bool) (AttachResult, error) {
	if clientID == "" {
		return AttachResult{}, ErrUnknownClient
	}
	result := make(chan AttachResult, 1)
	s.submit(func(s *Session) {
		role := RoleObserver
		switch {
		case s.controller == clientID:
			role = RoleController
		case wantController && (s.controller == "" || takeover):
			s.controller = clientID
			role = RoleController
		}
		result <- AttachResult{
			ClientID: clientID, Role: role,
			Controller: s.controller, Snapshot: s.snapshotLocked(),
		}
	})
	return <-result, nil
}

// Detach releases control held by clientID. Execution state is untouched, so
// a paused program stays paused until a new controller takes over.
func (s *Session) Detach(clientID string) {
	done := make(chan struct{})
	s.submit(func(s *Session) {
		if s.controller == clientID {
			s.controller = ""
		}
		close(done)
	})
	<-done
}

func (s *Session) Continue(pauseID int) (ContinueResult, error) {
	return s.ContinueEx("", pauseID, "")
}

// ContinueEx resumes only the pause identified by pauseId, on behalf of
// clientID. A duplicate continue or a request from the previous pause cannot
// operate the new pause. When a controller holds the session, only that
// client may resume. A non-empty requestID makes the call idempotent: a
// retry after reconnect reports the stored outcome with Duplicate set
// instead of advancing the program again.
func (s *Session) ContinueEx(clientID string, pauseID int, requestID string) (ContinueResult, error) {
	if pauseID == 0 {
		return ContinueResult{}, ErrInvalidPause
	}
	type outcome struct {
		result  ContinueResult
		err     error
		pending bool
	}
	outCh := make(chan outcome, 1)
	w := make(waiter, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.completed[requestID]; ok {
				result := ContinueResult{}
				if stored.continueResult != nil {
					result = *stored.continueResult
				}
				result.Duplicate = true
				outCh <- outcome{result: result, err: stored.err}
				return
			}
		}
		fail := func(err error) {
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: err}
			}
			outCh <- outcome{err: err}
		}
		switch {
		case s.state == StateTerminated:
			fail(ErrTerminated)
		case s.controller != "" && clientID != s.controller:
			fail(ErrNotController)
		case s.state == StateRunning:
			fail(ErrRunning)
		case pauseID != s.pauseID:
			fail(ErrPauseMismatch)
		default:
			if requestID != "" {
				// Mark as in-flight so a retry arriving while the
				// program runs is explained instead of re-executed.
				s.completed[requestID] = requestOutcome{err: ErrRunning}
			}
			currentPauseID := s.pauseID
			s.waiters[w] = struct{}{}
			s.engine.UpdateBreakpoints(executorBreakpoints(s.bpSpecs))
			s.state = StateRunning
			s.pauseID = 0
			s.bpPendingVersion = s.bpVersion
			s.engine.Resume(currentPauseID, executorBreakpoints(s.bpSpecs))
			outCh <- outcome{pending: true}
		}
	})
	out := <-outCh
	if !out.pending {
		return out.result, out.err
	}
	var result ContinueResult
	var err error
	select {
	case r := <-w:
		if r.State == StateTerminated {
			result = ContinueResult{Terminated: true, Reason: r.Reason}
		} else {
			result = ContinueResult{Paused: &r}
		}
	case <-s.terminated:
		err = ErrTerminated
	}
	if requestID != "" {
		stored := result
		storedErr := err
		s.submit(func(s *Session) {
			s.completed[requestID] = requestOutcome{continueResult: &stored, err: storedErr}
		})
	}
	return result, err
}

func (s *Session) Pause() (WaitResult, error) {
	return s.PauseEx("", "")
}

// PauseEx asks a running executor to stop at the next safe point. If already
// stopped, the caller receives the current pause rather than invalidating
// it. Only the controller may request a pause; observers get
// ErrNotController. A non-empty requestID makes the call idempotent.
func (s *Session) PauseEx(clientID string, requestID string) (WaitResult, error) {
	type outcome struct {
		result  WaitResult
		err     error
		pending bool
	}
	outCh := make(chan outcome, 1)
	w := make(waiter, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.completed[requestID]; ok {
				result := WaitResult{}
				if stored.pauseResult != nil {
					result = *stored.pauseResult
				}
				outCh <- outcome{result: result, err: stored.err}
				return
			}
		}
		fail := func(err error) {
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: err}
			}
			outCh <- outcome{err: err}
		}
		if s.controller != "" && clientID != s.controller {
			fail(ErrNotController)
			return
		}
		switch s.state {
		case StateTerminated:
			fail(ErrTerminated)
		case StateStopped:
			result := WaitResult{State: StateStopped, PauseID: s.pauseID, Reason: s.reason, Line: s.line, PC: s.pc, Vars: cloneVars(s.vars)}
			if requestID != "" {
				s.completed[requestID] = requestOutcome{pauseResult: &result}
			}
			outCh <- outcome{result: result}
		default:
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: ErrRunning}
			}
			s.waiters[w] = struct{}{}
			s.engine.Pause()
			outCh <- outcome{pending: true}
		}
	})
	out := <-outCh
	if !out.pending {
		return out.result, out.err
	}
	var result WaitResult
	var err error
	select {
	case r := <-w:
		result = r
	case <-s.terminated:
		err = ErrTerminated
	}
	if requestID != "" {
		stored := result
		storedErr := err
		s.submit(func(s *Session) {
			s.completed[requestID] = requestOutcome{pauseResult: &stored, err: storedErr}
		})
	}
	return result, err
}

func (s *Session) SetBreakpoints(lines []int) (map[string]any, error) {
	return s.SetBreakpointsEx("", lines, "")
}

// SetBreakpointsEx replaces the complete breakpoint set. While running it is
// handed to the executor and takes effect at the next safe point; while
// stopped it takes effect at the current safe position. Only the controller
// may change breakpoints. A non-empty requestID makes the call idempotent:
// a retry reports the stored result instead of re-applying.
func (s *Session) SetBreakpointsEx(clientID string, lines []int, requestID string) (map[string]any, error) {
	result := make(chan map[string]any, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.completed[requestID]; ok {
				if stored.err != nil {
					errCh <- stored.err
				} else {
					result <- stored.breakpoints
				}
				return
			}
		}
		fail := func(err error) {
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: err}
			}
			errCh <- err
		}
		if s.state == StateTerminated {
			fail(ErrTerminated)
			return
		}
		if s.controller != "" && clientID != s.controller {
			fail(ErrNotController)
			return
		}
		specs := make([]BreakpointSpec, 0, len(lines))
		for _, line := range lines {
			specs = append(specs, BreakpointSpec{Line: line, Enabled: true})
		}
		pending := s.state == StateRunning
		s.installVersionLocked(specs, clientID)
		applied := map[string]any{"breakpoints": lines, "pending": pending, "version": s.bpVersion}
		if requestID != "" {
			s.completed[requestID] = requestOutcome{breakpoints: applied}
		}
		result <- applied
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
