package engine

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"raftdb/internal/storage"
)

// TestIndexesMatchFullScans is a differential test of the planner. Two
// databases receive the same random writes; one has secondary indexes and
// the other does not. Random queries must return the same rows from both,
// so every index scan, primary key range and index maintenance path is
// checked against a plain full scan. A small buffer pool and frequent
// checkpoints keep the storage engine evicting while this runs.
func TestIndexesMatchFullScans(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runDifferential(t, seed) })
	}
}

func runDifferential(t *testing.T, seed int64) {
	opts := storage.Options{PoolPages: 8, CheckpointFrames: 50}
	open := func(name string) *DB {
		db, err := OpenWithOptions(filepath.Join(t.TempDir(), name), opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	plain, indexed := open("plain.db"), open("indexed.db")
	const schema = "CREATE TABLE t (id INT PRIMARY KEY, a INT, b FLOAT, c TEXT, d BOOL)"
	mustExec(t, plain, schema)
	mustExec(t, indexed, schema+`;
		CREATE INDEX t_a ON t (a);
		CREATE INDEX t_b ON t (b);
		CREATE INDEX t_c ON t (c);
		CREATE INDEX t_d ON t (d)`)

	rng := rand.New(rand.NewSource(seed))
	// Small value domains make duplicates, NULLs and boundary hits common.
	intLit := func() string {
		if rng.Intn(8) == 0 {
			return "NULL"
		}
		return fmt.Sprint(rng.Intn(21) - 10)
	}
	numLit := func() string { // INT or FLOAT, to exercise mixed comparisons
		if rng.Intn(2) == 0 {
			return intLit()
		}
		return fmt.Sprintf("%.1f", float64(rng.Intn(41)-20)/2)
	}
	textLit := func() string {
		if rng.Intn(8) == 0 {
			return "NULL"
		}
		return "'" + []string{"", "a", "ab", "abc", "b", "ba", "z"}[rng.Intn(7)] + "'"
	}
	boolLit := func() string { return []string{"true", "false", "NULL"}[rng.Intn(3)] }
	floatLit := func() string {
		s := numLit()
		if s != "NULL" && !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	ops := []string{"=", "<", "<=", ">", ">=", "!="}
	cond := func() string {
		col := []string{"id", "a", "b", "c", "d"}[rng.Intn(5)]
		lit := map[string]func() string{"id": intLit, "a": numLit, "b": numLit, "c": textLit, "d": boolLit}[col]
		switch rng.Intn(6) {
		case 0:
			return fmt.Sprintf("%s BETWEEN %s AND %s", col, lit(), lit())
		case 1:
			return fmt.Sprintf("%s IS NULL", col)
		case 2: // constant on the left
			return fmt.Sprintf("%s %s %s", lit(), ops[rng.Intn(len(ops))], col)
		}
		return fmt.Sprintf("%s %s %s", col, ops[rng.Intn(len(ops))], lit())
	}
	where := func() string {
		w := cond()
		for rng.Intn(2) == 0 {
			w += []string{" AND ", " AND ", " OR "}[rng.Intn(3)] + cond()
		}
		return w
	}

	both := func(sql string) {
		t.Helper()
		r1, err1 := plain.Query(sql)
		r2, err2 := indexed.Query(sql)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("%s\nwithout indexes: %v\nwith indexes: %v", sql, err1, err2)
		}
		if err1 != nil {
			return
		}
		g1, g2 := sortedRows(r1), sortedRows(r2)
		if g1 != g2 {
			plan := ""
			if r, err := indexed.Query("EXPLAIN " + sql); err == nil {
				plan = render(r)
			}
			t.Fatalf("%s\nwithout indexes:\n%s\nwith indexes:\n%s\nplan:\n%s", sql, g1, g2, plan)
		}
	}

	for step := 0; step < 1500; step++ {
		switch r := rng.Intn(20); {
		case r < 6:
			var rows []string
			for n := rng.Intn(4) + 1; n > 0; n-- {
				rows = append(rows, fmt.Sprintf("(%d, %s, %s, %s, %s)",
					rng.Intn(300), intLit(), floatLit(), textLit(), boolLit()))
			}
			both("INSERT INTO t VALUES " + strings.Join(rows, ", "))
		case r < 8:
			set := []string{
				"a = " + intLit(), "b = " + floatLit(), "c = " + textLit(), "d = " + boolLit(),
				"a = a + 1", "c = c || 'a'", fmt.Sprintf("id = id + %d", rng.Intn(5)),
			}[rng.Intn(7)]
			both("UPDATE t SET " + set + " WHERE " + where())
		case r < 9:
			both("DELETE FROM t WHERE " + where())
		case r < 10:
			// A transaction that may be rolled back, to exercise index
			// entries written and then discarded.
			both("BEGIN")
			both(fmt.Sprintf("INSERT INTO t VALUES (%d, %s, %s, %s, %s)", rng.Intn(300)+300, intLit(), floatLit(), textLit(), boolLit()))
			both("UPDATE t SET a = " + intLit() + " WHERE " + where())
			if rng.Intn(2) == 0 {
				both("ROLLBACK")
			} else {
				both("COMMIT")
			}
		default:
			both("SELECT * FROM t WHERE " + where())
		}
		if step%50 == 49 {
			// Garbage-collect old versions. Without it the tree fills with
			// dead versions (each UPDATE rewrites every row it matches);
			// with it, vacuum itself must never change what a query returns.
			for _, db := range []*DB{plain, indexed} {
				if _, err := db.Query("VACUUM"); err != nil {
					t.Fatal(err)
				}
			}
		}
		if step%250 == 0 {
			checkIndexes(t, indexed)
		}
	}
	checkIndexes(t, indexed)
	both("SELECT * FROM t")
}

// sortedRows renders a result's rows in sorted order, so results that differ
// only in row order compare equal.
func sortedRows(r *Result) string {
	lines := strings.Split(render(r), "\n")
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}
