// Package engine executes SQL statements on top of the storage engine.
//
// It keeps a catalog of tables and indexes, stores rows and index entries
// as key/value pairs in the B+ tree, plans each query (choosing between a
// full scan, a primary-key lookup or an index scan, and between a hash join
// and a nested-loop join), and runs the plan as a tree of iterators.
package engine

import (
	"cmp"
	"fmt"
	"math"
	"strconv"
	"strings"

	"raftdb/internal/parser"
)

// Value is a SQL value: nil for NULL, or an int64, float64, string or bool.
type Value = any

// Row is one row of values.
type Row []Value

func typeName(v Value) string {
	switch v.(type) {
	case nil:
		return "NULL"
	case int64:
		return "INT"
	case float64:
		return "FLOAT"
	case string:
		return "TEXT"
	case bool:
		return "BOOL"
	}
	return fmt.Sprintf("%T", v)
}

// FormatValue renders a value for display.
func FormatValue(v Value) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	}
	return fmt.Sprint(v)
}

// literal renders a value as SQL, for error messages and EXPLAIN.
func literal(v Value) string {
	if s, ok := v.(string); ok {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	return FormatValue(v)
}

func toFloat(v Value) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// compareValues orders two non-NULL values. Numbers compare by value
// whatever their type; other types must match.
func compareValues(a, b Value) (int, error) {
	switch x := a.(type) {
	case int64:
		if y, ok := b.(int64); ok {
			return cmp.Compare(x, y), nil
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), nil
		}
	case bool:
		if y, ok := b.(bool); ok {
			return cmp.Compare(boolInt(x), boolInt(y)), nil
		}
	}
	fx, ok1 := toFloat(a)
	fy, ok2 := toFloat(b)
	if ok1 && ok2 {
		return cmp.Compare(fx, fy), nil
	}
	return 0, fmt.Errorf("cannot compare %s with %s", typeName(a), typeName(b))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// sortCompare is the total order used by ORDER BY. As in PostgreSQL, NULLs
// sort after every other value (so first in descending order). Values of
// different types, which a well-typed query never mixes, order by type.
func sortCompare(a, b Value) int {
	if a == nil || b == nil {
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return 1
		}
		return -1
	}
	if c, err := compareValues(a, b); err == nil {
		return c
	}
	return strings.Compare(typeName(a), typeName(b))
}

// coerce converts v to a column's type. INT values widen to FLOAT; nothing
// else converts implicitly. NULL fits any type (NOT NULL is checked
// separately).
func coerce(v Value, t parser.Type) (Value, bool) {
	switch x := v.(type) {
	case nil:
		return nil, true
	case int64:
		switch t {
		case parser.TypeInt:
			return x, true
		case parser.TypeFloat:
			return float64(x), true
		}
	case float64:
		return x, t == parser.TypeFloat
	case string:
		return x, t == parser.TypeText
	case bool:
		return x, t == parser.TypeBool
	}
	return nil, false
}

// hashKey returns a string that is equal for two values exactly when SQL
// considers them equal (1 and 1.0 included). It keys hash joins, GROUP BY
// and DISTINCT.
func hashKey(vals ...Value) string {
	var b []byte
	for _, v := range vals {
		if f, ok := v.(float64); ok && f == math.Trunc(f) && math.Abs(f) < 1<<62 {
			v = int64(f)
		}
		b = appendKey(b, v)
	}
	return string(b)
}
