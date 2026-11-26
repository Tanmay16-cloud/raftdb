package engine

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"raftdb/internal/parser"
)

// evalFn evaluates a compiled expression against a row.
type evalFn func(Row) (Value, error)

// scopeCol names one column of the rows an expression is evaluated against.
type scopeCol struct {
	table string // table name or alias
	name  string
}

// scope lists the columns of a row, in order. A join's scope is its
// inputs' scopes placed side by side.
type scope []scopeCol

func tableScope(t *Table, name string) scope {
	s := make(scope, len(t.Columns))
	for i, c := range t.Columns {
		s[i] = scopeCol{name, c.Name}
	}
	return s
}

// resolve finds the column a reference names. Resolving names once, at plan
// time, is where "no such column" and "ambiguous column" errors come from;
// at run time a column reference is just a slice index.
func (s scope) resolve(ref *parser.ColumnRef) (int, error) {
	found := -1
	for i, c := range s {
		if c.name != ref.Column || (ref.Table != "" && c.table != ref.Table) {
			continue
		}
		if found >= 0 {
			return -1, fmt.Errorf("column reference %q is ambiguous", ref.String())
		}
		found = i
	}
	if found >= 0 {
		return found, nil
	}
	if ref.Table != "" && !slicesContainsFunc(s, func(c scopeCol) bool { return c.table == ref.Table }) {
		return -1, fmt.Errorf("missing FROM-clause entry for table %q", ref.Table)
	}
	return -1, fmt.Errorf("column %q does not exist", ref.String())
}

func slicesContainsFunc(s scope, f func(scopeCol) bool) bool {
	for _, c := range s {
		if f(c) {
			return true
		}
	}
	return false
}

// compiler turns parsed expressions into evalFns.
type compiler struct {
	scope scope
	where string // the clause being compiled, for error messages

	// agg is set when compiling the SELECT list, HAVING and ORDER BY of an
	// aggregate query. Those expressions run on the aggregate's output rows:
	// the GROUP BY values followed by one value per aggregate call.
	agg *aggState
}

type aggState struct {
	input     scope       // scope of the rows being grouped
	groups    []string    // canonical text of each GROUP BY expression
	groupCols map[int]int // input column -> output slot, for GROUP BY columns
	calls     []aggCall
}

type aggCall struct {
	name string // count, sum, avg, min or max
	arg  evalFn // nil for count(*)
	text string // canonical text, to share one slot among repeats
}

var aggregates = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}

func slot(i int) evalFn { return func(r Row) (Value, error) { return r[i], nil } }

func constant(v Value) evalFn { return func(Row) (Value, error) { return v, nil } }

func (c *compiler) compile(e parser.Expr) (evalFn, error) {
	if c.agg != nil {
		if f, ok, err := c.compileGrouped(e); ok || err != nil {
			return f, err
		}
	}
	switch e := e.(type) {
	case *parser.IntLit:
		return constant(e.Value), nil
	case *parser.FloatLit:
		return constant(e.Value), nil
	case *parser.StringLit:
		return constant(e.Value), nil
	case *parser.BoolLit:
		return constant(e.Value), nil
	case *parser.NullLit:
		return constant(nil), nil
	case *parser.ColumnRef:
		i, err := c.scope.resolve(e)
		if err != nil {
			return nil, err
		}
		return slot(i), nil
	case *parser.Unary:
		return c.compileUnary(e)
	case *parser.Binary:
		return c.compileBinary(e)
	case *parser.IsNull:
		x, err := c.compile(e.X)
		if err != nil {
			return nil, err
		}
		return func(r Row) (Value, error) {
			v, err := x(r)
			if err != nil {
				return nil, err
			}
			return (v == nil) != e.Not, nil
		}, nil
	case *parser.In:
		return c.compileIn(e)
	case *parser.Between:
		// x BETWEEN lo AND hi is x >= lo AND x <= hi.
		var f evalFn
		var err error
		b := &parser.Binary{Op: "AND",
			Left:  &parser.Binary{Op: ">=", Left: e.X, Right: e.Low},
			Right: &parser.Binary{Op: "<=", Left: e.X, Right: e.High}}
		if e.Not {
			f, err = c.compile(&parser.Unary{Op: "NOT", X: b})
		} else {
			f, err = c.compile(b)
		}
		return f, err
	case *parser.Call:
		if aggregates[e.Name] {
			return nil, fmt.Errorf("aggregate functions are not allowed in %s", c.where)
		}
		return c.compileCall(e)
	}
	return nil, fmt.Errorf("unsupported expression %s", e)
}

// compileGrouped handles the expressions that mean something special in an
// aggregate query: GROUP BY expressions, aggregate calls, and column
// references (which must be grouped). ok is false for anything else, which
// compiles as usual.
func (c *compiler) compileGrouped(e parser.Expr) (f evalFn, ok bool, err error) {
	a := c.agg
	text := e.String()
	for i, g := range a.groups {
		if g == text {
			return slot(i), true, nil
		}
	}
	switch e := e.(type) {
	case *parser.ColumnRef:
		i, err := a.input.resolve(e)
		if err != nil {
			return nil, true, err
		}
		if s, ok := a.groupCols[i]; ok {
			return slot(s), true, nil
		}
		return nil, true, fmt.Errorf("column %q must appear in the GROUP BY clause or be used in an aggregate function", e.String())
	case *parser.Call:
		if !aggregates[e.Name] {
			return nil, false, nil
		}
		for i, call := range a.calls {
			if call.text == text {
				return slot(len(a.groups) + i), true, nil
			}
		}
		call := aggCall{name: e.Name, text: text}
		switch {
		case e.Star && e.Name != "count":
			return nil, true, fmt.Errorf("%s(*) is not allowed; only count(*) is", e.Name)
		case !e.Star && len(e.Args) != 1:
			return nil, true, fmt.Errorf("%s takes exactly one argument", e.Name)
		case !e.Star:
			inner := &compiler{scope: a.input, where: "the argument of an aggregate function"}
			if call.arg, err = inner.compile(e.Args[0]); err != nil {
				return nil, true, err
			}
		}
		a.calls = append(a.calls, call)
		return slot(len(a.groups) + len(a.calls) - 1), true, nil
	}
	return nil, false, nil
}

func (c *compiler) compileUnary(e *parser.Unary) (evalFn, error) {
	x, err := c.compile(e.X)
	if err != nil {
		return nil, err
	}
	if e.Op == "NOT" {
		return func(r Row) (Value, error) {
			v, err := x(r)
			if err != nil || v == nil {
				return nil, err
			}
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("argument of NOT must be BOOL, not %s", typeName(v))
			}
			return !b, nil
		}, nil
	}
	return func(r Row) (Value, error) {
		v, err := x(r)
		if err != nil {
			return nil, err
		}
		switch n := v.(type) {
		case nil:
			return nil, nil
		case int64:
			if n == math.MinInt64 {
				return nil, errOverflow
			}
			return -n, nil
		case float64:
			return -n, nil
		}
		return nil, fmt.Errorf("operator - cannot be applied to %s", typeName(v))
	}, nil
}

func (c *compiler) compileBinary(e *parser.Binary) (evalFn, error) {
	l, err := c.compile(e.Left)
	if err != nil {
		return nil, err
	}
	r, err := c.compile(e.Right)
	if err != nil {
		return nil, err
	}
	switch e.Op {
	case "AND", "OR":
		return logical(e.Op, l, r), nil
	}
	var op func(a, b Value) (Value, error)
	switch e.Op {
	case "=", "!=", "<", "<=", ">", ">=":
		op = func(a, b Value) (Value, error) { return compareOp(e.Op, a, b) }
	case "+", "-", "*", "/", "%":
		op = func(a, b Value) (Value, error) { return arith(e.Op, a, b) }
	case "||":
		op = func(a, b Value) (Value, error) { return FormatValue(a) + FormatValue(b), nil }
	case "LIKE", "NOT LIKE":
		op = func(a, b Value) (Value, error) {
			s, ok1 := a.(string)
			p, ok2 := b.(string)
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("LIKE needs TEXT operands, not %s and %s", typeName(a), typeName(b))
			}
			return like(s, p) == (e.Op == "LIKE"), nil
		}
	default:
		return nil, fmt.Errorf("unsupported operator %s", e.Op)
	}
	// Every operator except AND and OR returns NULL if an operand is NULL.
	return func(row Row) (Value, error) {
		a, err := l(row)
		if err != nil {
			return nil, err
		}
		b, err := r(row)
		if err != nil || a == nil || b == nil {
			return nil, err
		}
		return op(a, b)
	}, nil
}

// logical implements AND and OR with SQL's three-valued logic, where NULL
// means "unknown": FALSE AND NULL is FALSE, but TRUE AND NULL is NULL.
// The right side is skipped when the left decides the answer.
func logical(op string, l, r evalFn) evalFn {
	decisive := op == "OR" // TRUE decides OR; FALSE decides AND
	return func(row Row) (Value, error) {
		a, err := l(row)
		if err != nil {
			return nil, err
		}
		if err := checkBool(op, a); err != nil {
			return nil, err
		}
		if a == decisive {
			return decisive, nil
		}
		b, err := r(row)
		if err != nil {
			return nil, err
		}
		if err := checkBool(op, b); err != nil {
			return nil, err
		}
		if b == decisive {
			return decisive, nil
		}
		if a == nil || b == nil {
			return nil, nil
		}
		return !decisive, nil
	}
}

func checkBool(op string, v Value) error {
	switch v.(type) {
	case nil, bool:
		return nil
	}
	return fmt.Errorf("argument of %s must be BOOL, not %s", op, typeName(v))
}

func compareOp(op string, a, b Value) (Value, error) {
	c, err := compareValues(a, b)
	if err != nil {
		return nil, err
	}
	switch op {
	case "=":
		return c == 0, nil
	case "!=":
		return c != 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	}
	return c >= 0, nil
}

var (
	errOverflow  = errors.New("integer out of range")
	errDivByZero = errors.New("division by zero")
)

// arith applies an arithmetic operator to two non-NULL values. Integer
// arithmetic stays integer (so 7 / 2 is 3, as in PostgreSQL) and reports
// overflow instead of wrapping around; mixing in a float gives a float.
func arith(op string, a, b Value) (Value, error) {
	x, xi := a.(int64)
	y, yi := b.(int64)
	if xi && yi {
		switch op {
		case "+":
			s := x + y
			if (x > 0 && y > 0 && s < 0) || (x < 0 && y < 0 && s >= 0) {
				return nil, errOverflow
			}
			return s, nil
		case "-":
			d := x - y
			if (x >= 0 && y < 0 && d < 0) || (x < 0 && y > 0 && d >= 0) {
				return nil, errOverflow
			}
			return d, nil
		case "*":
			if x == 0 || y == 0 {
				return int64(0), nil
			}
			p := x * y
			if p/y != x || (x == -1 && y == math.MinInt64) || (y == -1 && x == math.MinInt64) {
				return nil, errOverflow
			}
			return p, nil
		case "/":
			if y == 0 {
				return nil, errDivByZero
			}
			if x == math.MinInt64 && y == -1 {
				return nil, errOverflow
			}
			return x / y, nil
		default: // %
			if y == 0 {
				return nil, errDivByZero
			}
			if y == -1 {
				return int64(0), nil
			}
			return x % y, nil
		}
	}
	fx, ok1 := toFloat(a)
	fy, ok2 := toFloat(b)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("operator %s cannot be applied to %s and %s", op, typeName(a), typeName(b))
	}
	switch op {
	case "+":
		return fx + fy, nil
	case "-":
		return fx - fy, nil
	case "*":
		return fx * fy, nil
	case "/":
		if fy == 0 {
			return nil, errDivByZero
		}
		return fx / fy, nil
	default:
		if fy == 0 {
			return nil, errDivByZero
		}
		return math.Mod(fx, fy), nil
	}
}

// like matches s against a LIKE pattern, where % matches any run of
// characters and _ matches exactly one. When a later part of the pattern
// fails, it backtracks to the most recent % and lets it absorb one more
// character, which keeps the matching linear in the common cases.
func like(s, pattern string) bool {
	si, pi := 0, 0
	starP, starS := -1, 0
	for si < len(s) {
		if pi < len(pattern) {
			pc, pn := utf8.DecodeRuneInString(pattern[pi:])
			sc, sn := utf8.DecodeRuneInString(s[si:])
			switch {
			case pc == '%':
				starP, starS = pi, si
				pi += pn
				continue
			case pc == '_' || pc == sc:
				pi += pn
				si += sn
				continue
			}
		}
		if starP < 0 {
			return false
		}
		_, sn := utf8.DecodeRuneInString(s[starS:])
		starS += sn
		si, pi = starS, starP+1
	}
	return strings.Trim(pattern[pi:], "%") == ""
}

func (c *compiler) compileIn(e *parser.In) (evalFn, error) {
	x, err := c.compile(e.X)
	if err != nil {
		return nil, err
	}
	list := make([]evalFn, len(e.List))
	for i, item := range e.List {
		if list[i], err = c.compile(item); err != nil {
			return nil, err
		}
	}
	// x IN (a, b) is x = a OR x = b, with the same NULL rules: TRUE if any
	// item matches, otherwise NULL if x or any item is NULL, otherwise FALSE.
	return func(r Row) (Value, error) {
		v, err := x(r)
		if err != nil || v == nil {
			return nil, err
		}
		sawNull := false
		for _, f := range list {
			w, err := f(r)
			if err != nil {
				return nil, err
			}
			if w == nil {
				sawNull = true
				continue
			}
			c, err := compareValues(v, w)
			if err != nil {
				return nil, err
			}
			if c == 0 {
				return !e.Not, nil
			}
		}
		if sawNull {
			return nil, nil
		}
		return e.Not, nil
	}, nil
}

// scalar functions: name -> (argument count, or -1 for one or more; implementation)
var functions = map[string]struct {
	args int
	fn   func([]Value) (Value, error)
}{
	"lower": {1, textFunc(strings.ToLower)},
	"upper": {1, textFunc(strings.ToUpper)},
	"length": {1, func(a []Value) (Value, error) {
		return withText(a[0], func(s string) Value { return int64(utf8.RuneCountInString(s)) })
	}},
	"abs": {1, func(a []Value) (Value, error) {
		switch x := a[0].(type) {
		case nil:
			return nil, nil
		case int64:
			if x == math.MinInt64 {
				return nil, errOverflow
			}
			if x < 0 {
				return -x, nil
			}
			return x, nil
		case float64:
			return math.Abs(x), nil
		}
		return nil, fmt.Errorf("abs needs a number, not %s", typeName(a[0]))
	}},
	"round": {1, func(a []Value) (Value, error) {
		switch x := a[0].(type) {
		case nil, int64:
			return x, nil
		case float64:
			return math.Round(x), nil
		}
		return nil, fmt.Errorf("round needs a number, not %s", typeName(a[0]))
	}},
	"coalesce": {-1, func(a []Value) (Value, error) {
		for _, v := range a {
			if v != nil {
				return v, nil
			}
		}
		return nil, nil
	}},
}

func textFunc(f func(string) string) func([]Value) (Value, error) {
	return func(a []Value) (Value, error) { return withText(a[0], func(s string) Value { return f(s) }) }
}

func withText(v Value, f func(string) Value) (Value, error) {
	switch s := v.(type) {
	case nil:
		return nil, nil
	case string:
		return f(s), nil
	}
	return nil, fmt.Errorf("expected TEXT, got %s", typeName(v))
}

func (c *compiler) compileCall(e *parser.Call) (evalFn, error) {
	fn, ok := functions[e.Name]
	if !ok {
		return nil, fmt.Errorf("function %s does not exist", e.Name)
	}
	if e.Star || (fn.args >= 0 && len(e.Args) != fn.args) || (fn.args < 0 && len(e.Args) == 0) {
		return nil, fmt.Errorf("wrong number of arguments to %s", e.Name)
	}
	args := make([]evalFn, len(e.Args))
	for i, a := range e.Args {
		var err error
		if args[i], err = c.compile(a); err != nil {
			return nil, err
		}
	}
	return func(r Row) (Value, error) {
		vals := make([]Value, len(args))
		for i, a := range args {
			var err error
			if vals[i], err = a(r); err != nil {
				return nil, err
			}
		}
		return fn.fn(vals)
	}, nil
}

// columnRefs calls fn for every column reference in e, including those
// inside function arguments.
func columnRefs(e parser.Expr, fn func(*parser.ColumnRef)) {
	switch e := e.(type) {
	case *parser.ColumnRef:
		fn(e)
	case *parser.Unary:
		columnRefs(e.X, fn)
	case *parser.Binary:
		columnRefs(e.Left, fn)
		columnRefs(e.Right, fn)
	case *parser.IsNull:
		columnRefs(e.X, fn)
	case *parser.In:
		columnRefs(e.X, fn)
		for _, x := range e.List {
			columnRefs(x, fn)
		}
	case *parser.Between:
		columnRefs(e.X, fn)
		columnRefs(e.Low, fn)
		columnRefs(e.High, fn)
	case *parser.Call:
		for _, x := range e.Args {
			columnRefs(x, fn)
		}
	}
}

// hasAggregate reports whether e contains an aggregate call.
func hasAggregate(e parser.Expr) bool {
	found := false
	var walk func(parser.Expr)
	walk = func(e parser.Expr) {
		switch e := e.(type) {
		case *parser.Unary:
			walk(e.X)
		case *parser.Binary:
			walk(e.Left)
			walk(e.Right)
		case *parser.IsNull:
			walk(e.X)
		case *parser.In:
			walk(e.X)
			for _, x := range e.List {
				walk(x)
			}
		case *parser.Between:
			walk(e.X)
			walk(e.Low)
			walk(e.High)
		case *parser.Call:
			if aggregates[e.Name] {
				found = true
			}
			for _, x := range e.Args {
				walk(x)
			}
		}
	}
	walk(e)
	return found
}

// isConstant reports whether e has no column references or aggregates, so
// it can be evaluated once at plan time.
func isConstant(e parser.Expr) bool {
	refs := false
	columnRefs(e, func(*parser.ColumnRef) { refs = true })
	return !refs && !hasAggregate(e)
}

// evalConstant evaluates a constant expression.
func evalConstant(e parser.Expr) (Value, error) {
	f, err := (&compiler{where: "this context"}).compile(e)
	if err != nil {
		return nil, err
	}
	return f(nil)
}

// conjuncts splits a WHERE condition at its top-level ANDs.
func conjuncts(e parser.Expr) []parser.Expr {
	if e == nil {
		return nil
	}
	if b, ok := e.(*parser.Binary); ok && b.Op == "AND" {
		return append(conjuncts(b.Left), conjuncts(b.Right)...)
	}
	return []parser.Expr{e}
}

// andAll joins conditions with AND; nil for none.
func andAll(es []parser.Expr) parser.Expr {
	var out parser.Expr
	for _, e := range es {
		if out == nil {
			out = e
		} else {
			out = &parser.Binary{Op: "AND", Left: out, Right: e}
		}
	}
	return out
}
