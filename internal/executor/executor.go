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
	// ConditionError carries the diagnostic when a breakpoint condition
	// fails to evaluate. The stop is reported instead of being silently
	// treated as a miss.
	ConditionError string `json:"conditionError,omitempty"`
	// HitCount is the accumulated hit count of the breakpoint that
	// produced this stop, so the pause can be checked against the hit
	// basis it fired on.
	HitCount int `json:"hitCount,omitempty"`
}

type EventType string

const (
	EventStopped    EventType = "stopped"
	EventTerminated EventType = "terminated"
)

const (
	ReasonEntry          = "entry"
	ReasonBreakpoint     = "breakpoint"
	ReasonPause          = "pause"
	ReasonExit           = "exit"
	ReasonException      = "exception"
	ReasonConditionError = "condition-error"
)

// Breakpoint is one installed breakpoint: a line, an enable flag, an
// optional condition expression evaluated against the current variables,
// and an optional hit-count target. HitCount <= 1 stops on every hit;
// HitCount N stops on every Nth hit. A condition evaluation error always
// stops with a diagnostic, even before the target is reached.
type Breakpoint struct {
	Line      int    `json:"line"`
	Enabled   bool   `json:"enabled"`
	Condition string `json:"condition,omitempty"`
	HitCount  int    `json:"hitCount,omitempty"`
}

// Executor is the adapter contract between the protocol service and a concrete
// execution engine. Methods are safe to call concurrently; every state change
// is reported through the event channel passed to Start.
type Executor interface {
	// Start launches execution asynchronously. The executor must emit an
	// initial stopped event and block there until Resume is called. When the
	// program ends it emits exactly one terminated event.
	Start(events chan<- Event)
	// Resume continues from a stopped state. breakpoints are the complete,
	// authoritative breakpoint set to honor at subsequent safe points.
	Resume(pauseID int, breakpoints map[int]Breakpoint)
	// Pause requests a stop at the next safe point (instruction boundary).
	Pause()
	// UpdateBreakpoints installs a complete breakpoint set at the next safe
	// point. It returns false when the executor has already exited; in that
	// case it must not block.
	UpdateBreakpoints(breakpoints map[int]Breakpoint) bool
	// HitCounts returns the number of condition passes accumulated per
	// breakpoint line under the currently installed set, including passes
	// since the last stop that did not reach the hit-count target. Counts
	// carry over while a breakpoint spec is unchanged; a changed,
	// re-enabled, or re-added breakpoint restarts from zero, and removed
	// lines are absent. It is safe to call concurrently with execution.
	HitCounts() map[int]int
	// Done is closed when the executor has exited.
	Done() <-chan struct{}
	// Shutdown stops a paused executor and makes it emit terminated.
	Shutdown()
}
