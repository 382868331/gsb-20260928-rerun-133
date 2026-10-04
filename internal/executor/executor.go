package executor

// Event is the only way a running executor talks back to the debug session.
// The session owns all client-visible state, so swapping in another executor
// (remote debugger, simulator, ...) does not change client request semantics.
type Event struct {
	Type    EventType        `json:"type"`
	Reason  string           `json:"reason,omitempty"`
	PauseID int              `json:"pauseId,omitempty"`
	PC      int              `json:"pc,omitempty"`
	Line    int              `json:"line,omitempty"`
	Vars    map[string]int64 `json:"vars,omitempty"`
	// BreakpointVersion identifies the adopted breakpoint version that was
	// in effect when this event was produced, so the session can attribute
	// every pause to exactly one version.
	BreakpointVersion int `json:"breakpointVersion,omitempty"`
	// Diagnostic carries a condition evaluation error. A breakpoint whose
	// condition fails to evaluate still stops the program; the diagnostic
	// explains why instead of silently counting the stop as a miss.
	Diagnostic string `json:"diagnostic,omitempty"`
	// Hits reports cumulative breakpoint hit counts per line since start.
	Hits map[int]int `json:"hits,omitempty"`
}

// Breakpoint describes one location: whether it is enabled, an optional
// condition expression ("i >= 3"), and a hit-count convention (stop only
// when the hit count reaches a multiple of HitCount; 0 means every hit).
type Breakpoint struct {
	Line      int    `json:"line"`
	Enabled   bool   `json:"enabled"`
	Condition string `json:"condition,omitempty"`
	HitCount  int    `json:"hitCount,omitempty"`
}

// BreakpointSet is one adopted, immutable breakpoint version.
type BreakpointSet struct {
	Version int                `json:"version"`
	Items   map[int]Breakpoint `json:"items"`
}

type EventType string

const (
	EventStopped    EventType = "stopped"
	EventTerminated EventType = "terminated"
)

const (
	ReasonEntry      = "entry"
	ReasonBreakpoint = "breakpoint"
	ReasonPause      = "pause"
	ReasonExit       = "exit"
	ReasonException  = "exception"
)

// Executor is the adapter contract between the protocol service and a concrete
// execution engine. Methods are safe to call concurrently; every state change
// is reported through the event channel passed to Start.
type Executor interface {
	// Start launches execution asynchronously. The executor must emit an
	// initial stopped event and block there until Resume is called. When the
	// program ends it emits exactly one terminated event.
	Start(events chan<- Event)
	// Resume continues from a stopped state. breakpoints are the complete,
	// authoritative breakpoint version to honor at subsequent safe points.
	Resume(pauseID int, breakpoints BreakpointSet)
	// Pause requests a stop at the next safe point (instruction boundary).
	Pause()
	// UpdateBreakpoints installs a complete breakpoint version at the next
	// safe point. It returns false when the executor has already exited; in
	// that case it must not block.
	UpdateBreakpoints(breakpoints BreakpointSet) bool
	// Done is closed when the executor has exited.
	Done() <-chan struct{}
	// Shutdown stops a paused executor and makes it emit terminated.
	Shutdown()
}
