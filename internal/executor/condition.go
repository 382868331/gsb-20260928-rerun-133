package executor

import (
	"fmt"
	"strconv"
	"strings"
)

// EvalCondition evaluates a breakpoint condition against the current
// variables. Supported forms: "name" (truthy) and "name <op> <integer>"
// with <op> one of ==, !=, <, <=, >, >=. An empty condition always passes.
// Any parse or evaluation problem is returned as an error so the caller can
// surface a diagnostic instead of treating the breakpoint as a miss.
func EvalCondition(condition string, vars map[string]int64) (bool, error) {
	expr := strings.TrimSpace(condition)
	if expr == "" {
		return true, nil
	}
	for _, op := range []string{"==", "!=", "<=", ">=", "<", ">"} {
		if idx := strings.Index(expr, op); idx > 0 {
			name := strings.TrimSpace(expr[:idx])
			raw := strings.TrimSpace(expr[idx+len(op):])
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return false, fmt.Errorf("condition %q: right side %q is not an integer", condition, raw)
			}
			left, ok := vars[name]
			if !ok {
				return false, fmt.Errorf("condition %q: unknown variable %q", condition, name)
			}
			switch op {
			case "==":
				return left == value, nil
			case "!=":
				return left != value, nil
			case "<":
				return left < value, nil
			case "<=":
				return left <= value, nil
			case ">":
				return left > value, nil
			case ">=":
				return left >= value, nil
			}
		}
	}
	if value, ok := vars[expr]; ok {
		return value != 0, nil
	}
	return false, fmt.Errorf("condition %q: unknown variable %q", condition, expr)
}
