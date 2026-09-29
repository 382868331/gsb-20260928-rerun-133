// Package virtual implements a built-in deterministic executor used to
// exercise the debug protocol without attaching to a real process.
package virtual

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Instruction is one statement of the virtual program.
//
// Supported ops:
//   - set:  Var = Value
//   - add:  Var = operand(A) + operand(B), where an operand is either a
//     variable name or a base-10 integer literal
//   - abort: stop the executor with a non-zero exit code and a message
//   - exit:  stop the executor with the given code
type Instruction struct {
	Line  int    `json:"line"`
	Op    string `json:"op"`
	Var   string `json:"var,omitempty"`
	Value int64  `json:"value,omitempty"`
	A     string `json:"a,omitempty"`
	B     string `json:"b,omitempty"`
	Code  int    `json:"code,omitempty"`
	Msg   string `json:"message,omitempty"`
}

// DefaultProgram is a small program with arithmetic and a clean exit.
func DefaultProgram() []Instruction {
	return []Instruction{
		{Line: 1, Op: "set", Var: "x", Value: 0},
		{Line: 2, Op: "set", Var: "n", Value: 5},
		{Line: 3, Op: "add", Var: "x", A: "x", B: "n"},
		{Line: 4, Op: "set", Var: "n", Value: -1},
		{Line: 5, Op: "add", Var: "x", A: "x", B: "n"},
		{Line: 6, Op: "set", Var: "done", Value: 1},
		{Line: 7, Op: "exit", Code: 0},
	}
}

// ParseProgram converts the loosely typed launch argument into instructions.
func ParseProgram(raw []map[string]any) ([]Instruction, error) {
	if len(raw) == 0 {
		return DefaultProgram(), nil
	}
	prog := make([]Instruction, 0, len(raw))
	for i, item := range raw {
		ins := Instruction{}
		if v, ok := item["line"].(float64); ok {
			ins.Line = int(v)
		}
		if ins.Line <= 0 {
			return nil, fmt.Errorf("instruction %d: line must be a positive integer", i)
		}
		op, _ := item["op"].(string)
		ins.Op = strings.ToLower(strings.TrimSpace(op))
		ins.Var, _ = item["var"].(string)
		ins.A, _ = item["a"].(string)
		ins.B, _ = item["b"].(string)
		ins.Msg, _ = item["message"].(string)
		if v, ok := item["value"].(float64); ok {
			ins.Value = int64(v)
		}
		if v, ok := item["code"].(float64); ok {
			ins.Code = int(v)
		}
		switch ins.Op {
		case "set":
			if ins.Var == "" {
				return nil, fmt.Errorf("instruction %d: set requires var", i)
			}
		case "add":
			if ins.Var == "" || ins.A == "" || ins.B == "" {
				return nil, fmt.Errorf("instruction %d: add requires var, a and b", i)
			}
		case "exit":
		case "abort":
		default:
			return nil, fmt.Errorf("instruction %d: unknown op %q", i, ins.Op)
		}
		prog = append(prog, ins)
	}
	return prog, nil
}

// Decision tells the executor what to do at an instruction boundary.
type Decision struct {
	Pause  bool
	Reason string
	Abort  bool
}

// Resume is the answer to a paused-state request.
type Resume struct {
	Action string // "continue" or "next"
	OK     bool   // false when the session is being torn down
}

// Control is the bridge implemented by the debug session.
type Control interface {
	// Paused reports a pause and blocks until the session resumes it.
	Paused(reason string, line int) Resume
	// Decision is invoked at every instruction boundary while running.
	Decision(line int) Decision
	// Exit reports normal or abnormal executor termination.
	Exit(code int, message string)
}

// Executor runs a virtual program.
type Executor struct {
	Program []Instruction
	vars    map[string]int64

	// HoldBeforeLine blocks Run immediately before executing this line.
	// It is a test aid that lets script clients race inspection requests
	// against a guaranteed-running executor. Zero disables the hold.
	HoldBeforeLine int
	holdOnce       bool
	Release        chan struct{}
	releaseOnce    sync.Once

	// StepHook, when set, is invoked immediately before executing each
	// instruction. It exists to make races deterministic in tests.
	StepHook func(line int)
}

// New constructs an executor with an empty variable table.
func New(program []Instruction) *Executor {
	return &Executor{Program: program, vars: map[string]int64{}, Release: make(chan struct{}, 1)}
}

// Lines returns the valid source lines of the program.
func (e *Executor) Lines() []int {
	lines := make([]int, len(e.Program))
	for i, ins := range e.Program {
		lines[i] = ins.Line
	}
	return lines
}

// Variables returns a defensive copy of the current variables.
func (e *Executor) Variables() map[string]int64 {
	out := make(map[string]int64, len(e.vars))
	for k, v := range e.vars {
		out[k] = v
	}
	return out
}

// ReleaseHold unblocks a Run parked at HoldBeforeLine.
func (e *Executor) ReleaseHold() {
	e.releaseOnce.Do(func() { e.Release <- struct{}{} })
}

func (e *Executor) operand(name string) (int64, error) {
	if v, err := strconv.ParseInt(name, 10, 64); err == nil {
		return v, nil
	}
	v, ok := e.vars[name]
	if !ok {
		return 0, fmt.Errorf("unknown variable %q", name)
	}
	return v, nil
}

// Run drives the program until it exits or the session tears down.
func (e *Executor) Run(ctl Control) {
	pc := 0
	if res := ctl.Paused("entry", e.Program[pc].Line); !res.OK {
		return
	}
	for pc < len(e.Program) {
		if e.HoldBeforeLine != 0 && !e.holdOnce && e.Program[pc].Line == e.HoldBeforeLine {
			e.holdOnce = true
			<-e.Release
		}
		if e.StepHook != nil {
			e.StepHook(e.Program[pc].Line)
		}
		ins := e.Program[pc]
		switch ins.Op {
		case "set":
			e.vars[ins.Var] = ins.Value
		case "add":
			a, err := e.operand(ins.A)
			if err != nil {
				ctl.Exit(1, err.Error())
				return
			}
			b, err := e.operand(ins.B)
			if err != nil {
				ctl.Exit(1, err.Error())
				return
			}
			e.vars[ins.Var] = a + b
		case "abort":
			msg := ins.Msg
			if msg == "" {
				msg = "virtual executor aborted"
			}
			code := ins.Code
			if code == 0 {
				code = 1
			}
			ctl.Exit(code, msg)
			return
		case "exit":
			ctl.Exit(ins.Code, "")
			return
		}
		pc++
		if pc >= len(e.Program) {
			ctl.Exit(0, "")
			return
		}
		dec := ctl.Decision(e.Program[pc].Line)
		if dec.Abort {
			return
		}
		if dec.Pause {
			if res := ctl.Paused(dec.Reason, e.Program[pc].Line); !res.OK {
				return
			}
		}
	}
}
