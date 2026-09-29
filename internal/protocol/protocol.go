// Package protocol implements the line-delimited JSON debug protocol.
//
// Each request is a JSON object terminated by '\n':
//
//	{"id":1,"command":"launch","arguments":{...}}
//
// Replies and events share a framing envelope and are also newline
// delimited JSON values.
package protocol

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"dbgprotocol/internal/session"
	"dbgprotocol/internal/virtual"
)

const ProtocolVersion = "1.0"

// Request is one client message. Arguments stay raw until dispatch.
type Request struct {
	ID        int64           `json:"id"`
	Command   string          `json:"command"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// Message is the common wire envelope for responses and events.
type Message struct {
	Type string `json:"type"`

	// Response fields.
	RequestID int64      `json:"requestId,omitempty"`
	Command   string     `json:"command,omitempty"`
	Success   bool       `json:"success,omitempty"`
	Body      any        `json:"body,omitempty"`
	Error     *bodyError `json:"error,omitempty"`

	// Event fields.
	Event string         `json:"event,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

type bodyError struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	ExitCode       int    `json:"exitCode,omitempty"`
	CurrentPauseID int64  `json:"currentPauseId,omitempty"`
}

type launchArgs struct {
	// Executor selects the backend. Only "virtual" exists today; the field
	// keeps client requests stable as more executors are added.
	Executor       string           `json:"executor"`
	Program        []map[string]any `json:"program"`
	HoldBeforeLine int              `json:"holdBeforeLine"`
}

type pauseArgs struct {
	PauseID int64  `json:"pauseId"`
	Action  string `json:"action"`
}

type breakpointsArgs struct {
	Lines []int `json:"lines"`
}

// Server accepts client connections and owns their sessions.
type Server struct {
	mu       sync.Mutex
	sessions map[string]*session.Session
	nextID   int
}

func NewServer() *Server {
	return &Server{sessions: map[string]*session.Session{}}
}

// Listen serves until the listener closes.
func (s *Server) Listen(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	c := &connection{
		server: s,
		conn:   conn,
		out:    bufio.NewWriter(conn),
	}
	c.run()
}

// ServeConn runs one protocol session on an already established
// connection (used with net.Pipe and unix sockets in tests/tools).
func (s *Server) ServeConn(conn net.Conn) {
	s.serveConn(conn)
}

type connection struct {
	server  *Server
	conn    net.Conn
	writeMu sync.Mutex
	out     *bufio.Writer
	closed  bool
	sess    *session.Session
}

func (c *connection) run() {
	defer c.conn.Close()
	defer func() {
		if c.sess != nil {
			c.sess.Close()
		}
	}()

	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			c.sendError(0, "", parseError(err.Error()))
			continue
		}
		reply, start, perr := c.dispatch(req)
		if perr != nil {
			c.sendError(req.ID, req.Command, perr)
			continue
		}
		c.sendOK(req.ID, req.Command, reply)
		if start != nil {
			start()
		}
	}
	c.markClosed()
}

func parseError(msg string) *session.Error {
	return &session.Error{Code: "ParseError", Message: msg}
}

func (c *connection) emit(ev session.Event) {
	data := map[string]any{}
	if ev.PauseID != 0 {
		data["pauseId"] = ev.PauseID
	}
	if ev.Reason != "" {
		data["reason"] = ev.Reason
	}
	if ev.Line != 0 {
		data["line"] = ev.Line
	}
	if ev.Type == "terminated" {
		data["exitCode"] = ev.ExitCode
		if ev.Message != "" {
			data["message"] = ev.Message
		}
	}
	c.write(Message{Type: "event", Event: ev.Type, Data: data})
}

// dispatch returns the reply body. start, when non-nil, runs strictly after
// the reply has been written, which keeps launch response ahead of the
// entry stopped event.
func (c *connection) dispatch(req Request) (body any, start func(), perr *session.Error) {
	switch req.Command {
	case "initialize":
		return map[string]any{
			"protocolVersion": ProtocolVersion,
			"executors":       []string{"virtual"},
			"commands": []string{
				"initialize", "launch", "setBreakpoints", "continue",
				"variables", "stackTrace", "status", "releaseHold", "disconnect",
			},
		}, nil, nil

	case "launch":
		if c.sess != nil {
			return nil, nil, &session.Error{Code: "AlreadyLaunched", Message: "this connection already owns a session"}
		}
		var args launchArgs
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return nil, nil, &session.Error{Code: "InvalidArguments", Message: err.Error()}
		}
		if args.Executor != "" && args.Executor != "virtual" {
			return nil, nil, &session.Error{Code: "UnknownExecutor", Message: fmt.Sprintf("executor %q is not registered", args.Executor)}
		}
		program, err := virtual.ParseProgram(args.Program)
		if err != nil {
			return nil, nil, &session.Error{Code: "InvalidProgram", Message: err.Error()}
		}
		engine := virtual.New(program)
		engine.HoldBeforeLine = args.HoldBeforeLine
		c.server.mu.Lock()
		c.server.nextID++
		id := fmt.Sprintf("s-%d", c.server.nextID)
		sess := session.New(id, engine, c.emit)
		c.sess = sess
		c.server.sessions[id] = sess
		c.server.mu.Unlock()

		return map[string]any{
			"sessionId":  id,
			"executor":   "virtual",
			"status":     session.StatusPaused,
			"pauseId":    int64(1),
			"line":       program[0].Line,
			"validLines": engine.Lines(),
		}, sess.Start, nil

	case "setBreakpoints":
		if c.sess == nil {
			return nil, nil, notLaunched()
		}
		var args breakpointsArgs
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return nil, nil, &session.Error{Code: "InvalidArguments", Message: err.Error()}
		}
		verified, rejected := c.sess.SetBreakpoints(args.Lines)
		return map[string]any{"verified": verified, "rejected": rejected}, nil, nil

	case "continue":
		if c.sess == nil {
			return nil, nil, notLaunched()
		}
		var args pauseArgs
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return nil, nil, &session.Error{Code: "InvalidArguments", Message: err.Error()}
		}
		action := args.Action
		if action == "" {
			action = "continue"
		}
		if action != "continue" && action != "next" {
			return nil, nil, &session.Error{Code: "InvalidArguments", Message: "action must be continue or next"}
		}
		snap, commit, serr := c.sess.Resume(args.PauseID, action)
		if serr != nil {
			return nil, nil, serr
		}
		return map[string]any{"status": snap.Status, "pauseId": snap.PauseID}, func() {
			c.emit(session.Event{Type: "continued", PauseID: snap.PauseID})
			commit()
		}, nil

	case "variables":
		if c.sess == nil {
			return nil, nil, notLaunched()
		}
		var args pauseArgs
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return nil, nil, &session.Error{Code: "InvalidArguments", Message: err.Error()}
		}
		vars, line, serr := c.sess.Variables(args.PauseID)
		if serr != nil {
			return nil, nil, serr
		}
		return map[string]any{"variables": vars, "line": line}, nil, nil

	case "stackTrace":
		if c.sess == nil {
			return nil, nil, notLaunched()
		}
		var args pauseArgs
		if err := json.Unmarshal(req.Arguments, &args); err != nil {
			return nil, nil, &session.Error{Code: "InvalidArguments", Message: err.Error()}
		}
		snap, serr := c.sess.RequirePaused(args.PauseID)
		if serr != nil {
			return nil, nil, serr
		}
		return map[string]any{"frames": []map[string]any{{"index": 0, "line": snap.Line}}}, nil, nil

	case "status":
		if c.sess == nil {
			return nil, nil, notLaunched()
		}
		snap := c.sess.State()
		return map[string]any{"status": snap.Status, "pauseId": snap.PauseID, "line": snap.Line}, nil, nil

	case "releaseHold":
		if c.sess == nil {
			return nil, nil, notLaunched()
		}
		// Test aid for launch arguments holdBeforeLine.
		c.sess.ReleaseHold()
		return map[string]any{"ok": true}, nil, nil

	case "disconnect":
		if c.sess != nil {
			c.sess.Close()
		}
		return map[string]any{"ok": true}, nil, nil

	default:
		return nil, nil, &session.Error{Code: "UnknownCommand", Message: fmt.Sprintf("unknown command %q", req.Command)}
	}
}

func notLaunched() *session.Error {
	return &session.Error{Code: "NotLaunched", Message: "send launch before debugging commands"}
}

func (c *connection) write(m Message) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed {
		return
	}
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	data = append(data, '\n')
	if _, err := c.out.Write(data); err != nil {
		c.closed = true
		return
	}
	if err := c.out.Flush(); err != nil {
		c.closed = true
	}
}

func (c *connection) markClosed() {
	c.writeMu.Lock()
	c.closed = true
	c.writeMu.Unlock()
}

func (c *connection) sendOK(id int64, command string, body any) {
	c.write(Message{Type: "response", RequestID: id, Command: command, Success: true, Body: body})
}

func (c *connection) sendError(id int64, command string, e *session.Error) {
	c.write(Message{
		Type:      "response",
		RequestID: id,
		Command:   command,
		Success:   false,
		Error: &bodyError{
			Code:           e.Code,
			Message:        e.Message,
			ExitCode:       e.ExitCode,
			CurrentPauseID: e.CurrentPauseID,
		},
	})
}
