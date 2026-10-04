package session

import (
	"errors"
	"sort"
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
	// ErrStaleCandidate rejects adopting a candidate whose pause, source, or
	// base version moved on; the caller must re-verify locations and hits.
	ErrStaleCandidate = errors.New("candidate is stale: re-verify locations and hit records")
	ErrNoCandidate    = errors.New("no breakpoint candidate is prepared")
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
	// BreakpointVersion is the adopted breakpoint version in effect: the
	// version a pause actually used, or the latest adopted version while
	// running.
	BreakpointVersion int `json:"breakpointVersion,omitempty"`
	// Hits are cumulative breakpoint hit counts per line since start, so
	// after a resume already-occurred hits stay distinguishable from
	// candidate edits that are not adopted yet.
	Hits map[int]int `json:"hits,omitempty"`
	// Diagnostic explains a condition evaluation error at this pause.
	Diagnostic string `json:"diagnostic,omitempty"`
	// SourceVersion increments on every source reload; candidates prepared
	// against an older source version are stale.
	SourceVersion int `json:"sourceVersion,omitempty"`
}

// Candidate is a previewable, not-yet-adopted breakpoint version. Any
// client may prepare or inspect it; only the controller may adopt it.
type Candidate struct {
	ID                int                         `json:"id"`
	BaseVersion       int                         `json:"baseVersion"`
	BasePauseSeq      int                         `json:"-"`
	BaseSourceVersion int                         `json:"-"`
	BaseHits          map[int]int                 `json:"-"`
	Items             map[int]executor.Breakpoint `json:"items"`
	PreparedBy        string                      `json:"preparedBy,omitempty"`
}

// RecheckInfo lists what must be re-verified before a stale candidate can
// be adopted again: the candidate locations and the hit records that
// accumulated since the candidate was prepared.
type RecheckInfo struct {
	Reason string      `json:"reason"`
	Lines  []int       `json:"lines"`
	Hits   map[int]int `json:"hits,omitempty"`
}

// CandidateView is the read model observers and controllers both see.
type CandidateView struct {
	Candidate Candidate    `json:"candidate"`
	Stale     bool         `json:"stale"`
	Recheck   *RecheckInfo `json:"recheck,omitempty"`
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
	pauseSeq   int
	lastPause  *Snapshot
	// breakpoints are the currently adopted version; candidate holds a
	// previewable next version that only the controller may adopt.
	breakpoints   map[int]executor.Breakpoint
	bpVersion     int
	pauseVersion  int
	hits          map[int]int
	diagnostic    string
	sourceVersion int
	candidate     *Candidate
	candidateSeq  int
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
	adopt          map[string]any
	recheck        *RecheckInfo
	err            error
}

func New(id string, engine Engine) *Session {
	s := &Session{
		id:          id,
		engine:      engine,
		cmds:        make(chan command, 32),
		done:        make(chan struct{}),
		terminated:  make(chan struct{}),
		waiters:     map[waiter]struct{}{},
		state:       StateRunning,
		breakpoints: map[int]executor.Breakpoint{},
		hits:        map[int]int{},
		completed:   map[string]requestOutcome{},
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
		s.pauseSeq++
		s.pauseVersion = event.BreakpointVersion
		s.diagnostic = event.Diagnostic
		if event.Hits != nil {
			s.hits = cloneHits(event.Hits)
		}
		s.lastPause = &Snapshot{
			State: StateStopped, Reason: event.Reason, PauseID: event.PauseID,
			Line: event.Line, PC: event.PC, Vars: cloneVars(event.Vars),
			BreakpointVersion: event.BreakpointVersion, Diagnostic: event.Diagnostic,
		}
		s.notify(Snapshot{
			State: StateStopped, Reason: event.Reason, PauseID: s.pauseID,
			Line: event.Line, PC: event.PC, Vars: cloneVars(event.Vars),
			LastPause: s.lastPauseRef(), BreakpointVersion: event.BreakpointVersion,
			Diagnostic: event.Diagnostic, Hits: cloneHits(s.hits),
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
		SourceVersion: s.sourceVersion, Hits: cloneHits(s.hits),
	}
	if s.state == StateStopped {
		snapshot.Vars = cloneVars(s.vars)
		snapshot.BreakpointVersion = s.pauseVersion
		snapshot.Diagnostic = s.diagnostic
	} else {
		snapshot.BreakpointVersion = s.bpVersion
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
			set := executor.BreakpointSet{Version: s.bpVersion, Items: cloneBreakpoints(s.breakpoints)}
			s.engine.UpdateBreakpoints(set)
			s.state = StateRunning
			s.pauseID = 0
			s.engine.Resume(currentPauseID, set)
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

// SetBreakpointsEx replaces the complete breakpoint set with plain enabled
// breakpoints and adopts it immediately as the next version. While running
// it is handed to the executor and takes effect at the next safe point;
// while stopped it takes effect at the current safe position. Only the
// controller may change breakpoints. A non-empty requestID makes the call
// idempotent: a retry reports the stored result instead of re-applying.
func (s *Session) SetBreakpointsEx(clientID string, lines []int, requestID string) (map[string]any, error) {
	items := make(map[int]executor.Breakpoint, len(lines))
	for _, line := range lines {
		items[line] = executor.Breakpoint{Line: line, Enabled: true}
	}
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
		pending := s.adoptLocked(items)
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

// adoptLocked installs items as the next adopted version and pushes it to
// the executor when the program is running. It reports whether the update
// is still pending at the executor.
func (s *Session) adoptLocked(items map[int]executor.Breakpoint) bool {
	s.bpVersion++
	s.breakpoints = cloneBreakpoints(items)
	if s.state == StateRunning {
		return s.engine.UpdateBreakpoints(executor.BreakpointSet{Version: s.bpVersion, Items: cloneBreakpoints(items)})
	}
	return false
}

// PreviewBreakpoints prepares a candidate version by applying changes on
// top of the currently adopted set. Any client, including observers, may
// prepare and inspect a candidate; adoption is restricted to the
// controller. The candidate is bound to the pause, source version, and
// base version at preparation time.
func (s *Session) PreviewBreakpoints(clientID string, changes []executor.Breakpoint) (CandidateView, error) {
	result := make(chan CandidateView, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if s.state == StateTerminated {
			errCh <- ErrTerminated
			return
		}
		items := cloneBreakpoints(s.breakpoints)
		for _, change := range changes {
			items[change.Line] = change
		}
		s.candidateSeq++
		s.candidate = &Candidate{
			ID:                s.candidateSeq,
			BaseVersion:       s.bpVersion,
			BasePauseSeq:      s.pauseSeq,
			BaseSourceVersion: s.sourceVersion,
			BaseHits:          cloneHits(s.hits),
			Items:             items,
			PreparedBy:        clientID,
		}
		result <- s.candidateViewLocked()
	})
	select {
	case err := <-errCh:
		return CandidateView{}, err
	case view := <-result:
		return view, nil
	}
}

// CandidateViewLocked describes the prepared candidate and, when stale,
// exactly which locations and hit records need re-verification.
func (s *Session) candidateViewLocked() CandidateView {
	view := CandidateView{Candidate: *s.candidate}
	view.Candidate.Items = cloneBreakpoints(s.candidate.Items)
	if recheck := s.staleReasonLocked(); recheck != nil {
		view.Stale = true
		view.Recheck = recheck
	}
	return view
}

// staleReasonLocked reports why the prepared candidate no longer matches
// the session: a newer pause, a source reload, or a newer adopted version.
// It returns nil when the candidate is still adoptable.
func (s *Session) staleReasonLocked() *RecheckInfo {
	c := s.candidate
	if c == nil {
		return nil
	}
	reason := ""
	switch {
	case c.BaseSourceVersion != s.sourceVersion:
		reason = "source changed since the candidate was prepared"
	case c.BasePauseSeq != s.pauseSeq:
		reason = "a new pause occurred since the candidate was prepared"
	case c.BaseVersion != s.bpVersion:
		reason = "a newer breakpoint version was adopted since the candidate was prepared"
	default:
		return nil
	}
	lines := make([]int, 0, len(c.Items))
	for line := range c.Items {
		lines = append(lines, line)
	}
	sortInts(lines)
	hits := map[int]int{}
	for line, count := range s.hits {
		if count-c.BaseHits[line] > 0 {
			hits[line] = count - c.BaseHits[line]
		}
	}
	return &RecheckInfo{Reason: reason, Lines: lines, Hits: hits}
}

// Candidate returns the current candidate view, including staleness and
// re-verification details. Observers may inspect it; only the controller
// can adopt.
func (s *Session) Candidate() (CandidateView, error) {
	result := make(chan CandidateView, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if s.candidate == nil {
			errCh <- ErrNoCandidate
			return
		}
		result <- s.candidateViewLocked()
	})
	select {
	case err := <-errCh:
		return CandidateView{}, err
	case view := <-result:
		return view, nil
	}
}

// AdoptResult describes an adopted candidate version.
type AdoptResult struct {
	Version int                         `json:"version"`
	Items   map[int]executor.Breakpoint `json:"items"`
	Pending bool                        `json:"pending"`
}

// AdoptCandidate promotes the prepared candidate to the next authoritative
// version. Only the current controller may adopt; a late adopt from a
// controller that already handed over control fails with ErrNotController
// and never touches the current version. A stale candidate fails with
// ErrStaleCandidate and the re-verification details. A non-empty requestID
// makes the call idempotent.
func (s *Session) AdoptCandidate(clientID string, requestID string) (AdoptResult, *RecheckInfo, error) {
	type outcome struct {
		result  AdoptResult
		recheck *RecheckInfo
		err     error
	}
	outCh := make(chan outcome, 1)
	s.submit(func(s *Session) {
		if requestID != "" {
			if stored, ok := s.completed[requestID]; ok {
				out := outcome{err: stored.err, recheck: stored.recheck}
				if stored.adopt != nil {
					out.result = adoptResultFromMap(stored.adopt)
				}
				outCh <- out
				return
			}
		}
		fail := func(err error, recheck *RecheckInfo) {
			if requestID != "" {
				s.completed[requestID] = requestOutcome{err: err, recheck: recheck}
			}
			outCh <- outcome{err: err, recheck: recheck}
		}
		switch {
		case s.state == StateTerminated:
			fail(ErrTerminated, nil)
		case s.controller != "" && clientID != s.controller:
			fail(ErrNotController, nil)
		case s.candidate == nil:
			fail(ErrNoCandidate, nil)
		default:
			if recheck := s.staleReasonLocked(); recheck != nil {
				fail(ErrStaleCandidate, recheck)
				return
			}
			pending := s.adoptLocked(s.candidate.Items)
			result := AdoptResult{Version: s.bpVersion, Items: cloneBreakpoints(s.breakpoints), Pending: pending}
			s.candidate = nil
			if requestID != "" {
				s.completed[requestID] = requestOutcome{adopt: adoptResultToMap(result)}
			}
			outCh <- outcome{result: result}
		}
	})
	out := <-outCh
	return out.result, out.recheck, out.err
}

// NoteSourceChange records a source reload. Candidates prepared before it
// become stale and must be re-verified; already-occurred hits are kept.
func (s *Session) NoteSourceChange() (int, error) {
	result := make(chan int, 1)
	errCh := make(chan error, 1)
	s.submit(func(s *Session) {
		if s.state == StateTerminated {
			errCh <- ErrTerminated
			return
		}
		s.sourceVersion++
		result <- s.sourceVersion
	})
	select {
	case err := <-errCh:
		return 0, err
	case version := <-result:
		return version, nil
	}
}

func adoptResultToMap(r AdoptResult) map[string]any {
	return map[string]any{"version": r.Version, "items": r.Items, "pending": r.Pending}
}

func adoptResultFromMap(m map[string]any) AdoptResult {
	out := AdoptResult{}
	if v, ok := m["version"].(int); ok {
		out.Version = v
	}
	if items, ok := m["items"].(map[int]executor.Breakpoint); ok {
		out.Items = items
	}
	if p, ok := m["pending"].(bool); ok {
		out.Pending = p
	}
	return out
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

func cloneHits(in map[int]int) map[int]int {
	if in == nil {
		return nil
	}
	out := make(map[int]int, len(in))
	for line, count := range in {
		out[line] = count
	}
	return out
}

func cloneBreakpoints(in map[int]executor.Breakpoint) map[int]executor.Breakpoint {
	out := make(map[int]executor.Breakpoint, len(in))
	for line, bp := range in {
		out[line] = bp
	}
	return out
}

func sortInts(values []int) {
	sort.Ints(values)
}
