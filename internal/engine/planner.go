package engine

import (
	"bytes"
	"fmt"
	"strings"

	"raftdb/internal/parser"
)

// query is a planned SELECT. Output rows may carry extra hidden columns
// after the visible ones (ORDER BY keys that aren't in the SELECT list).
type query struct {
	root    planNode
	columns []string
}

func (db *DB) selectRows(s *parser.Select) (*Result, error) {
	q, err := db.planSelect(s)
	if err != nil {
		return nil, err
	}
	it, err := q.root.open(db)
	if err != nil {
		return nil, err
	}
	rows, err := drain(it)
	if err != nil {
		return nil, err
	}
	n := len(q.columns)
	for i, r := range rows {
		rows[i] = r[:n:n]
	}
	return &Result{Columns: q.columns, Rows: rows, Message: fmt.Sprintf("SELECT %d", len(rows))}, nil
}

func (db *DB) explain(s *parser.Select) (*Result, error) {
	q, err := db.planSelect(s)
	if err != nil {
		return nil, err
	}
	res := &Result{Columns: []string{"QUERY PLAN"}, Message: "EXPLAIN"}
	for _, line := range explainPlan(q.root) {
		res.Rows = append(res.Rows, Row{line})
	}
	return res, nil
}

// source is one table in the FROM clause.
type source struct {
	table *Table
	name  string // alias, or the table name
	scope scope
	start int // offset of its first column in the joined row
}

// cond is one conjunct of the WHERE clause (or of a JOIN's ON clause), with
// the set of sources it references as a bitmask.
type cond struct {
	expr parser.Expr
	refs uint64
	used bool
}

const maxJoinTables = 64

// planSelect builds the plan for a SELECT:
//
//	scans (with their own filters) -> joins -> remaining filters
//	-> aggregate -> HAVING filter -> project -> distinct -> sort -> limit
func (db *DB) planSelect(s *parser.Select) (*query, error) {
	root, sc, err := db.planFrom(s)
	if err != nil {
		return nil, err
	}

	// Choose how to compile the SELECT list, HAVING and ORDER BY: directly
	// against the joined rows, or against the aggregate's output.
	c := &compiler{scope: sc, where: "WHERE"}
	isAgg := s.GroupBy != nil || s.Having != nil
	for _, it := range s.Items {
		isAgg = isAgg || (!it.Star && hasAggregate(it.Expr))
	}
	for _, o := range s.OrderBy {
		isAgg = isAgg || hasAggregate(o.Expr)
	}
	var agg *aggState
	var groups []evalFn
	if isAgg {
		agg = &aggState{input: sc, groupCols: make(map[int]int)}
		gc := &compiler{scope: sc, where: "GROUP BY"}
		for i, g := range s.GroupBy {
			f, err := gc.compile(g)
			if err != nil {
				return nil, err
			}
			groups = append(groups, f)
			agg.groups = append(agg.groups, g.String())
			if ref, ok := g.(*parser.ColumnRef); ok {
				col, _ := sc.resolve(ref)
				if _, dup := agg.groupCols[col]; !dup {
					agg.groupCols[col] = i
				}
			}
		}
		c = &compiler{agg: agg, where: "this context"}
	}

	// SELECT list.
	var exprs []evalFn
	var names []string
	for _, it := range s.Items {
		if it.Star {
			if isAgg {
				return nil, fmt.Errorf("SELECT * cannot be combined with GROUP BY or aggregate functions")
			}
			if len(sc) == 0 {
				return nil, fmt.Errorf("SELECT * with no tables specified is not valid")
			}
			for i, col := range sc {
				exprs = append(exprs, slot(i))
				names = append(names, col.name)
			}
			continue
		}
		c.where = "the SELECT list"
		f, err := c.compile(it.Expr)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, f)
		names = append(names, columnName(it))
	}
	visible := len(exprs)

	// HAVING.
	var having evalFn
	if s.Having != nil {
		c.where = "HAVING"
		if having, err = c.compile(s.Having); err != nil {
			return nil, err
		}
	}

	// ORDER BY: an output column (by position, name or identical
	// expression), or else an extra hidden column computed by the projection.
	var keys []sortKey
	var sortDesc []string
	for _, o := range s.OrderBy {
		col, err := outputColumn(o.Expr, s.Items, names[:visible])
		if err != nil {
			return nil, err
		}
		if col < 0 {
			if s.Distinct {
				return nil, fmt.Errorf("for SELECT DISTINCT, ORDER BY expressions must appear in the select list")
			}
			c.where = "ORDER BY"
			f, err := c.compile(o.Expr)
			if err != nil {
				return nil, err
			}
			col = len(exprs)
			exprs = append(exprs, f)
		}
		keys = append(keys, sortKey{col, o.Desc})
		d := o.Expr.String()
		if o.Desc {
			d += " DESC"
		}
		sortDesc = append(sortDesc, d)
	}

	if isAgg {
		desc := "Aggregate"
		if len(groups) > 0 {
			desc = "Hash Aggregate: group by " + strings.Join(agg.groups, ", ")
		}
		root = &aggregateNode{input: root, groups: groups, calls: agg.calls, desc: desc}
		if having != nil {
			root = &filterNode{input: root, pred: having, cond: s.Having.String()}
		}
	}
	root = &projectNode{input: root, exprs: exprs, names: names}
	if s.Distinct {
		root = &distinctNode{input: root, n: visible}
	}
	if keys != nil {
		root = &sortNode{input: root, keys: keys, desc: strings.Join(sortDesc, ", ")}
	}
	if s.Limit != nil || s.Offset != nil {
		limit, err := nonNegativeInt(s.Limit, "LIMIT", -1)
		if err != nil {
			return nil, err
		}
		offset, err := nonNegativeInt(s.Offset, "OFFSET", 0)
		if err != nil {
			return nil, err
		}
		root = &limitNode{input: root, limit: limit, offset: offset}
	}
	return &query{root: root, columns: names[:visible]}, nil
}

// columnName is the output name of a SELECT item, as PostgreSQL names it.
func columnName(it parser.SelectItem) string {
	if it.Alias != "" {
		return it.Alias
	}
	switch e := it.Expr.(type) {
	case *parser.ColumnRef:
		return e.Column
	case *parser.Call:
		return e.Name
	}
	return "?column?"
}

// outputColumn finds the SELECT item an ORDER BY expression refers to: a
// position (ORDER BY 2), an output name (ORDER BY total), or the same
// expression. It returns -1 if there is none.
func outputColumn(e parser.Expr, items []parser.SelectItem, names []string) (int, error) {
	if n, ok := e.(*parser.IntLit); ok {
		if n.Value < 1 || n.Value > int64(len(names)) {
			return 0, fmt.Errorf("ORDER BY position %d is not in select list", n.Value)
		}
		return int(n.Value - 1), nil
	}
	if ref, ok := e.(*parser.ColumnRef); ok && ref.Table == "" {
		found := -1
		for i, name := range names {
			if name == ref.Column {
				if found >= 0 && items[found].Expr.String() != items[i].Expr.String() {
					return 0, fmt.Errorf("ORDER BY %q is ambiguous", ref.Column)
				}
				if found < 0 {
					found = i
				}
			}
		}
		if found >= 0 {
			return found, nil
		}
	}
	text := e.String()
	col := 0
	for _, it := range items {
		if it.Star {
			return -1, nil // positions after * are not worth tracking
		}
		if it.Expr.String() == text {
			return col, nil
		}
		col++
	}
	return -1, nil
}

func nonNegativeInt(e parser.Expr, clause string, def int64) (int64, error) {
	if e == nil {
		return def, nil
	}
	if !isConstant(e) {
		return 0, fmt.Errorf("%s must be a constant", clause)
	}
	v, err := evalConstant(e)
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, not %s", clause, literal(v))
	}
	return n, nil
}

// planFrom plans the FROM clause and WHERE conditions, returning the plan
// and the scope of the rows it produces.
//
// Each WHERE conjunct (and, since joins here are inner joins, each ON
// conjunct) is placed as low in the tree as possible: a condition on one
// table filters that table's scan, and may choose its access path ("predicate
// pushdown"); a condition linking tables runs at the first join where all
// of them are present. Tables are joined in the order written.
func (db *DB) planFrom(s *parser.Select) (planNode, scope, error) {
	if s.From == nil {
		if s.Where == nil {
			return valuesNode{}, nil, nil
		}
		c := &compiler{where: "WHERE"}
		pred, err := c.compile(s.Where)
		if err != nil {
			return nil, nil, err
		}
		return &filterNode{input: valuesNode{}, pred: pred, cond: s.Where.String()}, nil, nil
	}

	refs := append([]parser.TableRef{*s.From}, make([]parser.TableRef, 0, len(s.Joins))...)
	for _, j := range s.Joins {
		refs = append(refs, j.Table)
	}
	if len(refs) > maxJoinTables {
		return nil, nil, fmt.Errorf("too many tables in FROM (the limit is %d)", maxJoinTables)
	}
	var sources []*source
	var full scope
	for _, ref := range refs {
		t, err := db.table(ref.Name)
		if err != nil {
			return nil, nil, err
		}
		name := ref.Name
		if ref.Alias != "" {
			name = ref.Alias
		}
		for _, other := range sources {
			if other.name == name {
				return nil, nil, fmt.Errorf("table name %q specified more than once", name)
			}
		}
		src := &source{table: t, name: name, scope: tableScope(t, name), start: len(full)}
		sources = append(sources, src)
		full = append(full, src.scope...)
	}
	sourceOf := func(col int) int {
		for i := len(sources) - 1; i >= 0; i-- {
			if col >= sources[i].start {
				return i
			}
		}
		return 0
	}
	// refsOf computes which sources an expression uses, resolving its
	// columns in the given scope (a prefix of the full scope).
	refsOf := func(e parser.Expr, sc scope) (uint64, error) {
		var mask uint64
		var err error
		columnRefs(e, func(ref *parser.ColumnRef) {
			col, rerr := sc.resolve(ref)
			if rerr != nil {
				if err == nil {
					err = rerr
				}
				return
			}
			mask |= 1 << sourceOf(col)
		})
		return mask, err
	}

	var conds []*cond
	add := func(e parser.Expr, sc scope, clause string) error {
		if hasAggregate(e) {
			return fmt.Errorf("aggregate functions are not allowed in %s", clause)
		}
		for _, part := range conjuncts(e) {
			mask, err := refsOf(part, sc)
			if err != nil {
				return err
			}
			conds = append(conds, &cond{expr: part, refs: mask})
		}
		return nil
	}
	if err := add(s.Where, full, "WHERE"); err != nil {
		return nil, nil, err
	}
	for i, j := range s.Joins {
		// An ON clause may only use the tables joined so far.
		visible := full[:sources[i+1].start+len(sources[i+1].scope)]
		if j.On != nil {
			if err := add(j.On, visible, "JOIN conditions"); err != nil {
				return nil, nil, err
			}
		}
	}

	// Scan each table with the conditions that involve only it.
	nodes := make([]planNode, len(sources))
	for i, src := range sources {
		var local []parser.Expr
		for _, c := range conds {
			if c.refs == 1<<i {
				local = append(local, c.expr)
				c.used = true
			}
		}
		node, err := db.planTableScan(src.table, src.name, andAll(local), false)
		if err != nil {
			return nil, nil, err
		}
		nodes[i] = node
	}

	// Join left to right.
	root := nodes[0]
	have := uint64(1)
	for i := 1; i < len(sources); i++ {
		bit := uint64(1) << i
		combined := full[:sources[i].start+len(sources[i].scope)]
		var here []*cond
		for _, c := range conds {
			if !c.used && c.refs&bit != 0 && c.refs&^(have|bit) == 0 {
				here = append(here, c)
				c.used = true
			}
		}
		node, err := planJoin(root, nodes[i], here, combined, full[:sources[i].start], sources[i].scope, have, bit, refsOf)
		if err != nil {
			return nil, nil, err
		}
		root = node
		have |= bit
	}

	// Whatever is left references no table at all (WHERE 1 = 1).
	var rest []parser.Expr
	for _, c := range conds {
		if !c.used {
			rest = append(rest, c.expr)
		}
	}
	if len(rest) > 0 {
		e := andAll(rest)
		pred, err := (&compiler{scope: full, where: "WHERE"}).compile(e)
		if err != nil {
			return nil, nil, err
		}
		root = &filterNode{input: root, pred: pred, cond: e.String()}
	}
	return root, full, nil
}

// planJoin joins the plan so far (left) with one more table (right). If a
// condition has the form left_expr = right_expr, it uses a hash join on
// it; otherwise a nested-loop join.
func planJoin(left, right planNode, conds []*cond, combined, leftScope, rightScope scope,
	have, bit uint64, refsOf func(parser.Expr, scope) (uint64, error)) (planNode, error) {

	for i, c := range conds {
		b, ok := c.expr.(*parser.Binary)
		if !ok || b.Op != "=" {
			continue
		}
		lm, err := refsOf(b.Left, combined)
		if err != nil {
			return nil, err
		}
		rm, err := refsOf(b.Right, combined)
		if err != nil {
			return nil, err
		}
		lexpr, rexpr := b.Left, b.Right
		switch {
		case lm != 0 && lm&^have == 0 && rm == bit:
		case rm != 0 && rm&^have == 0 && lm == bit:
			lexpr, rexpr = rexpr, lexpr
		default:
			continue
		}
		lk, err := (&compiler{scope: leftScope, where: "JOIN conditions"}).compile(lexpr)
		if err != nil {
			return nil, err
		}
		rk, err := (&compiler{scope: rightScope, where: "JOIN conditions"}).compile(rexpr)
		if err != nil {
			return nil, err
		}
		var others []parser.Expr
		for j, o := range conds {
			if j != i {
				others = append(others, o.expr)
			}
		}
		n := &hashJoinNode{left: left, right: right, leftKey: lk, rightKey: rk, desc: c.expr.String()}
		if e := andAll(others); e != nil {
			if n.residual, err = (&compiler{scope: combined, where: "JOIN conditions"}).compile(e); err != nil {
				return nil, err
			}
			n.desc += " AND " + e.String()
		}
		return n, nil
	}

	n := &nestedLoopJoinNode{left: left, right: right}
	var all []parser.Expr
	for _, c := range conds {
		all = append(all, c.expr)
	}
	if e := andAll(all); e != nil {
		var err error
		if n.cond, err = (&compiler{scope: combined, where: "JOIN conditions"}).compile(e); err != nil {
			return nil, err
		}
		n.desc = e.String()
	}
	return n, nil
}

// planTableScan plans a scan of one table filtered by where (which may be
// nil). withKey makes each row carry its primary key, for UPDATE and DELETE.
func (db *DB) planTableScan(t *Table, name string, where parser.Expr, withKey bool) (planNode, error) {
	sc := tableScope(t, name)
	var root planNode = &scanNode{table: t, alias: name, path: chooseAccess(t, sc, conjuncts(where)), withKey: withKey}
	if where == nil {
		return root, nil
	}
	if hasAggregate(where) {
		return nil, fmt.Errorf("aggregate functions are not allowed in WHERE")
	}
	pred, err := (&compiler{scope: sc, where: "WHERE"}).compile(where)
	if err != nil {
		return nil, err
	}
	// The filter re-checks every condition, including those the access path
	// already guarantees. That costs a little CPU and keeps the access path
	// logic free to be approximate.
	return &filterNode{input: root, pred: pred, cond: where.String()}, nil
}

// bounds collects the range that conditions place on one column, in the
// encoded key space: keys in [lo, hi) match.
type bounds struct {
	lo, hi []byte
	eq     bool
	conds  []string
}

// chooseAccess picks how to read a table given the conditions on it. It is
// rule-based (there are no table statistics yet), preferring, in order:
//
//  1. primary key = constant           (one row)
//  2. unique index column = constant   (at most one row)
//  3. indexed column = constant
//  4. a range on the primary key
//  5. a range on an indexed column
//  6. a full scan
func chooseAccess(t *Table, sc scope, conds []parser.Expr) accessPath {
	cols := make(map[int]*bounds)
	for _, e := range conds {
		for _, c := range columnBounds(e, sc) {
			v, ok := coerce(c.val, t.Columns[c.col].Type)
			if !ok || v == nil {
				continue // e.g. an INT column against 2.5: leave it to the filter
			}
			b := cols[c.col]
			if b == nil {
				b = &bounds{}
				cols[c.col] = b
			}
			b.add(c.op, appendKey(nil, v), e.String())
		}
	}

	best, bestScore := accessPath{}, 0
	consider := func(col int, idx *Index, eqScore, rangeScore int) {
		b := cols[col]
		if b == nil {
			return
		}
		score := rangeScore
		if b.eq {
			score = eqScore
		}
		if score > bestScore {
			lo := b.lo
			if lo == nil {
				lo = []byte{tagNull + 1} // a range never matches NULL
			}
			best = accessPath{index: idx, lo: lo, hi: b.hi, point: b.eq && idx == nil, cond: strings.Join(b.conds, " AND ")}
			bestScore = score
		}
	}
	if t.PK >= 0 {
		consider(t.PK, nil, 6, 3)
	}
	for _, idx := range t.Indexes {
		eq := 4
		if idx.Unique {
			eq = 5
		}
		consider(idx.Column, idx, eq, 2)
	}
	return best
}

func (b *bounds) add(op string, key []byte, text string) {
	raiseLo := func(k []byte) {
		if b.lo == nil || bytes.Compare(k, b.lo) > 0 {
			b.lo = k
		}
	}
	lowerHi := func(k []byte) {
		if k == nil {
			return
		}
		if b.hi == nil || bytes.Compare(k, b.hi) < 0 {
			b.hi = k
		}
	}
	switch op {
	case "=":
		raiseLo(key)
		lowerHi(prefixEnd(key))
		b.eq = true
	case ">":
		raiseLo(prefixEnd(key))
	case ">=":
		raiseLo(key)
	case "<":
		lowerHi(key)
	case "<=":
		lowerHi(prefixEnd(key))
	}
	// BETWEEN contributes two bounds from one condition; list it once.
	if n := len(b.conds); n == 0 || b.conds[n-1] != text {
		b.conds = append(b.conds, text)
	}
}

type columnBound struct {
	col int
	op  string
	val Value
}

var flipped = map[string]string{"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

// columnBounds recognises conditions an access path can use: column op
// constant, constant op column, and column BETWEEN constant AND constant.
func columnBounds(e parser.Expr, sc scope) []columnBound {
	column := func(e parser.Expr) (int, bool) {
		ref, ok := e.(*parser.ColumnRef)
		if !ok {
			return 0, false
		}
		i, err := sc.resolve(ref)
		return i, err == nil
	}
	value := func(e parser.Expr) (Value, bool) {
		if !isConstant(e) {
			return nil, false
		}
		v, err := evalConstant(e)
		return v, err == nil
	}
	switch e := e.(type) {
	case *parser.Binary:
		if _, ok := flipped[e.Op]; !ok {
			return nil
		}
		if col, ok := column(e.Left); ok {
			if v, ok := value(e.Right); ok {
				return []columnBound{{col, e.Op, v}}
			}
		}
		if col, ok := column(e.Right); ok {
			if v, ok := value(e.Left); ok {
				return []columnBound{{col, flipped[e.Op], v}}
			}
		}
	case *parser.Between:
		if e.Not {
			return nil
		}
		col, ok := column(e.X)
		lo, ok1 := value(e.Low)
		hi, ok2 := value(e.High)
		if ok && ok1 && ok2 {
			return []columnBound{{col, ">=", lo}, {col, "<=", hi}}
		}
	}
	return nil
}
