package engine

import (
	"fmt"
	"slices"
	"strings"
)

// A query plan is a tree of planNodes. Executing it follows the iterator
// (or "Volcano") model: opening the root opens its inputs, and each call to
// next pulls one row up through the tree. Most operators stream, holding
// one row at a time; sort, hash join (its build side), aggregation and the
// inner side of a nested-loop join must see all their input first.
type planNode interface {
	open(tx *txn) (iterator, error)
	explain() string
	inputs() []planNode
}

// iterator yields rows; next returns a nil row when there are no more.
type iterator interface {
	next() (Row, error)
}

type iterFunc func() (Row, error)

func (f iterFunc) next() (Row, error) { return f() }

// explainPlan renders a plan as an indented tree, like PostgreSQL's EXPLAIN.
func explainPlan(n planNode) []string {
	var lines []string
	var walk func(n planNode, depth int)
	walk = func(n planNode, depth int) {
		prefix := ""
		if depth > 0 {
			prefix = strings.Repeat("   ", depth-1) + "-> "
		}
		lines = append(lines, prefix+n.explain())
		for _, in := range n.inputs() {
			walk(in, depth+1)
		}
	}
	walk(n, 0)
	return lines
}

func drain(it iterator) ([]Row, error) {
	var rows []Row
	for {
		r, err := it.next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			return rows, nil
		}
		rows = append(rows, r)
	}
}

// ---------------------------------------------------------------------------
// Leaves

// valuesNode produces one empty row, for SELECT without FROM.
type valuesNode struct{}

func (valuesNode) open(*txn) (iterator, error) {
	done := false
	return iterFunc(func() (Row, error) {
		if done {
			return nil, nil
		}
		done = true
		return Row{}, nil
	}), nil
}
func (valuesNode) explain() string    { return "Result" }
func (valuesNode) inputs() []planNode { return nil }

// accessPath is how a scan reaches a table's rows.
type accessPath struct {
	index  *Index // nil: walk the rows themselves, in primary key order
	lo, hi []byte // bounds on the encoded key value; nil means unbounded
	point  bool   // lo and hi bound a single value
	cond   string // the conditions that set the bounds, for EXPLAIN
}

// rowKeyValue carries a row's encoded primary key alongside its values,
// for UPDATE and DELETE. It never reaches the user.
type rowKeyValue string

// scanNode reads a table's rows through an access path.
type scanNode struct {
	table   *Table
	alias   string
	path    accessPath
	withKey bool // append each row's encoded primary key as a rowKeyValue
}

func (n *scanNode) explain() string {
	name := n.table.Name
	if n.alias != n.table.Name {
		name += " " + n.alias
	}
	switch p := n.path; {
	case p.index != nil:
		return fmt.Sprintf("Index Scan using %s on %s (%s)", p.index.Name, name, p.cond)
	case p.point:
		return fmt.Sprintf("Primary Key Lookup on %s (%s)", name, p.cond)
	case p.lo != nil || p.hi != nil:
		return fmt.Sprintf("Primary Key Range Scan on %s (%s)", name, p.cond)
	}
	return "Seq Scan on " + name
}

func (n *scanNode) inputs() []planNode { return nil }

func (n *scanNode) open(tx *txn) (iterator, error) {
	t := n.table
	rp := rowPrefix(t.ID)
	prefix := rp
	if n.path.index != nil {
		prefix = indexPrefix(n.path.index.ID)
	}
	start := append(slices.Clip(prefix), n.path.lo...)
	end := prefixEnd(prefix)
	if n.path.hi != nil {
		end = append(slices.Clip(prefix), n.path.hi...)
	}
	it := tx.scan(start, end)

	return iterFunc(func() (Row, error) {
		k, v, ok, err := it.Next()
		if err != nil || !ok {
			return nil, err
		}
		pk := k[len(rp):]
		if n.path.index != nil {
			// An index entry's value is the row's primary key; fetch the row.
			pk = v
			if v, ok, err = tx.kv.Get(rowKey(t.ID, pk)); err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("engine: index %q points to a missing row", n.path.index.Name)
			}
		}
		row, err := decodeRow(v, len(t.Columns))
		if err != nil {
			return nil, err
		}
		if n.withKey {
			row = append(row, rowKeyValue(pk))
		}
		return row, nil
	}), nil
}

// ---------------------------------------------------------------------------
// Streaming operators

// filterNode passes on the rows for which pred is TRUE (not FALSE or NULL).
type filterNode struct {
	input planNode
	pred  evalFn
	cond  string
}

func (n *filterNode) explain() string    { return "Filter: " + n.cond }
func (n *filterNode) inputs() []planNode { return []planNode{n.input} }

func (n *filterNode) open(tx *txn) (iterator, error) {
	in, err := n.input.open(tx)
	if err != nil {
		return nil, err
	}
	return iterFunc(func() (Row, error) {
		for {
			r, err := in.next()
			if err != nil || r == nil {
				return nil, err
			}
			v, err := n.pred(r)
			if err != nil {
				return nil, err
			}
			if err := checkBool("WHERE", v); err != nil {
				return nil, err
			}
			if v == true {
				return r, nil
			}
		}
	}), nil
}

// projectNode computes the output columns.
type projectNode struct {
	input planNode
	exprs []evalFn
	names []string
}

func (n *projectNode) explain() string    { return "Project: " + strings.Join(n.names, ", ") }
func (n *projectNode) inputs() []planNode { return []planNode{n.input} }

func (n *projectNode) open(tx *txn) (iterator, error) {
	in, err := n.input.open(tx)
	if err != nil {
		return nil, err
	}
	return iterFunc(func() (Row, error) {
		r, err := in.next()
		if err != nil || r == nil {
			return nil, err
		}
		out := make(Row, len(n.exprs))
		for i, f := range n.exprs {
			if out[i], err = f(r); err != nil {
				return nil, err
			}
		}
		return out, nil
	}), nil
}

// distinctNode drops rows whose first n columns repeat an earlier row's.
type distinctNode struct {
	input planNode
	n     int
}

func (n *distinctNode) explain() string    { return "Distinct" }
func (n *distinctNode) inputs() []planNode { return []planNode{n.input} }

func (n *distinctNode) open(tx *txn) (iterator, error) {
	in, err := n.input.open(tx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	return iterFunc(func() (Row, error) {
		for {
			r, err := in.next()
			if err != nil || r == nil {
				return nil, err
			}
			k := hashKey(r[:n.n]...)
			if !seen[k] {
				seen[k] = true
				return r, nil
			}
		}
	}), nil
}

// limitNode skips offset rows, then passes on at most limit (-1: no limit).
type limitNode struct {
	input         planNode
	limit, offset int64
}

func (n *limitNode) explain() string {
	s := "Limit"
	if n.limit >= 0 {
		s += fmt.Sprintf(" %d", n.limit)
	}
	if n.offset > 0 {
		s += fmt.Sprintf(" offset %d", n.offset)
	}
	return s
}
func (n *limitNode) inputs() []planNode { return []planNode{n.input} }

func (n *limitNode) open(tx *txn) (iterator, error) {
	in, err := n.input.open(tx)
	if err != nil {
		return nil, err
	}
	skipped, returned := int64(0), int64(0)
	return iterFunc(func() (Row, error) {
		for skipped < n.offset {
			r, err := in.next()
			if err != nil || r == nil {
				return nil, err
			}
			skipped++
		}
		if n.limit >= 0 && returned >= n.limit {
			return nil, nil // stop without reading the rest of the input
		}
		r, err := in.next()
		if r != nil {
			returned++
		}
		return r, err
	}), nil
}

// ---------------------------------------------------------------------------
// Blocking operators

type sortKey struct {
	col  int
	desc bool
}

// sortNode reads all its input and sorts it. The sort is stable, so rows
// that tie keep their input order.
type sortNode struct {
	input planNode
	keys  []sortKey
	desc  string
}

func (n *sortNode) explain() string    { return "Sort: " + n.desc }
func (n *sortNode) inputs() []planNode { return []planNode{n.input} }

func (n *sortNode) open(tx *txn) (iterator, error) {
	in, err := n.input.open(tx)
	if err != nil {
		return nil, err
	}
	rows, err := drain(in)
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(rows, func(a, b Row) int {
		for _, k := range n.keys {
			c := sortCompare(a[k.col], b[k.col])
			if k.desc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return 0
	})
	return sliceIter(rows), nil
}

func sliceIter(rows []Row) iterator {
	i := 0
	return iterFunc(func() (Row, error) {
		if i == len(rows) {
			return nil, nil
		}
		i++
		return rows[i-1], nil
	})
}

// nestedLoopJoinNode pairs every left row with every right row and keeps
// the pairs that satisfy cond (all of them, for a cross join). The right
// input is read once and kept in memory. Cost: O(left x right).
type nestedLoopJoinNode struct {
	left, right planNode
	cond        evalFn // nil for a cross join
	desc        string
}

func (n *nestedLoopJoinNode) explain() string {
	if n.cond == nil {
		return "Nested Loop Join (cross)"
	}
	return "Nested Loop Join: " + n.desc
}
func (n *nestedLoopJoinNode) inputs() []planNode { return []planNode{n.left, n.right} }

func (n *nestedLoopJoinNode) open(tx *txn) (iterator, error) {
	rin, err := n.right.open(tx)
	if err != nil {
		return nil, err
	}
	rights, err := drain(rin)
	if err != nil {
		return nil, err
	}
	lin, err := n.left.open(tx)
	if err != nil {
		return nil, err
	}
	var left Row
	j := len(rights)
	return iterFunc(func() (Row, error) {
		for {
			if j == len(rights) {
				if left, err = lin.next(); err != nil || left == nil {
					return nil, err
				}
				j = 0
				if len(rights) == 0 {
					continue
				}
			}
			r := append(slices.Clip(left), rights[j]...)
			j++
			if n.cond == nil {
				return r, nil
			}
			v, err := n.cond(r)
			if err != nil {
				return nil, err
			}
			if v == true {
				return r, nil
			}
		}
	}), nil
}

// hashJoinNode joins on leftKey = rightKey. It builds a hash table from the
// right input, then streams the left input and looks each row up: O(left +
// right) instead of O(left x right). NULL keys never match, as with =.
// residual holds any other join conditions.
type hashJoinNode struct {
	left, right       planNode
	leftKey, rightKey evalFn // rightKey runs on right rows alone
	residual          evalFn // may be nil
	desc              string
}

func (n *hashJoinNode) explain() string    { return "Hash Join: " + n.desc }
func (n *hashJoinNode) inputs() []planNode { return []planNode{n.left, n.right} }

func (n *hashJoinNode) open(tx *txn) (iterator, error) {
	rin, err := n.right.open(tx)
	if err != nil {
		return nil, err
	}
	table := make(map[string][]Row)
	for {
		r, err := rin.next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		k, err := n.rightKey(r)
		if err != nil {
			return nil, err
		}
		if k != nil {
			hk := hashKey(k)
			table[hk] = append(table[hk], r)
		}
	}
	lin, err := n.left.open(tx)
	if err != nil {
		return nil, err
	}
	var left Row
	var matches []Row
	return iterFunc(func() (Row, error) {
		for {
			for len(matches) > 0 {
				r := append(slices.Clip(left), matches[0]...)
				matches = matches[1:]
				if n.residual == nil {
					return r, nil
				}
				v, err := n.residual(r)
				if err != nil {
					return nil, err
				}
				if v == true {
					return r, nil
				}
			}
			if left, err = lin.next(); err != nil || left == nil {
				return nil, err
			}
			k, err := n.leftKey(left)
			if err != nil {
				return nil, err
			}
			if k != nil {
				matches = table[hashKey(k)]
			}
		}
	}), nil
}

// aggregateNode groups its input by the GROUP BY values and computes the
// aggregates for each group in a hash table. Each output row holds the
// group's values followed by its aggregate results.
type aggregateNode struct {
	input  planNode
	groups []evalFn
	calls  []aggCall
	desc   string
}

func (n *aggregateNode) explain() string    { return n.desc }
func (n *aggregateNode) inputs() []planNode { return []planNode{n.input} }

type group struct {
	vals Row
	accs []accumulator
}

func (n *aggregateNode) open(tx *txn) (iterator, error) {
	in, err := n.input.open(tx)
	if err != nil {
		return nil, err
	}
	groups := make(map[string]*group)
	var order []*group // first-seen order, so output is deterministic
	newGroup := func(vals Row) *group {
		g := &group{vals: vals, accs: make([]accumulator, len(n.calls))}
		for i, c := range n.calls {
			g.accs[i].fn = c.name
		}
		order = append(order, g)
		return g
	}
	for {
		r, err := in.next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			break
		}
		vals := make(Row, len(n.groups))
		for i, f := range n.groups {
			if vals[i], err = f(r); err != nil {
				return nil, err
			}
		}
		k := hashKey(vals...)
		g, ok := groups[k]
		if !ok {
			g = newGroup(vals)
			groups[k] = g
		}
		for i, c := range n.calls {
			var v Value = true // count(*) counts every row
			if c.arg != nil {
				if v, err = c.arg(r); err != nil {
					return nil, err
				}
			}
			if err := g.accs[i].add(v); err != nil {
				return nil, err
			}
		}
	}
	// Without GROUP BY, an aggregate query returns one row even for no input:
	// SELECT count(*) FROM empty is 0.
	if len(n.groups) == 0 && len(order) == 0 {
		newGroup(Row{})
	}
	rows := make([]Row, len(order))
	for i, g := range order {
		r := append(Row{}, g.vals...)
		for j := range g.accs {
			r = append(r, g.accs[j].result())
		}
		rows[i] = r
	}
	return sliceIter(rows), nil
}

// accumulator computes one aggregate. NULL inputs are ignored, and every
// aggregate except count returns NULL when it saw no non-NULL input.
type accumulator struct {
	fn    string
	count int64
	sum   Value // int64 until a float arrives
	best  Value // min or max so far
}

func (a *accumulator) add(v Value) error {
	if v == nil {
		return nil
	}
	a.count++
	switch a.fn {
	case "sum", "avg":
		if _, ok := toFloat(v); !ok {
			return fmt.Errorf("%s needs numbers, not %s", a.fn, typeName(v))
		}
		if a.sum == nil {
			a.sum = v
			return nil
		}
		s, err := arith("+", a.sum, v)
		if err != nil {
			return err
		}
		a.sum = s
	case "min", "max":
		if a.best == nil {
			a.best = v
			return nil
		}
		c, err := compareValues(v, a.best)
		if err != nil {
			return err
		}
		if (a.fn == "min" && c < 0) || (a.fn == "max" && c > 0) {
			a.best = v
		}
	}
	return nil
}

func (a *accumulator) result() Value {
	switch a.fn {
	case "count":
		return a.count
	case "sum":
		return a.sum
	case "avg":
		if a.count == 0 {
			return nil
		}
		s, _ := toFloat(a.sum)
		return s / float64(a.count)
	}
	return a.best
}
