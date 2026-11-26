package engine

import (
	"fmt"
	"path/filepath"
	"testing"
)

func benchDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	benchExec(b, db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, s TEXT); CREATE INDEX t_a ON t (a)")
	return db
}

func benchExec(b *testing.B, db *DB, sql string) {
	b.Helper()
	if _, err := db.Exec(sql); err != nil {
		b.Fatalf("%s: %v", sql, err)
	}
}

func insertSQL(i int) string {
	return fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'some text value here')", i, i%100)
}

// Each INSERT is its own transaction, so each pays for an fsync.
func BenchmarkInsertAutocommit(b *testing.B) {
	db := benchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchExec(b, db, insertSQL(i))
	}
}

// INSERTs grouped 1000 to a transaction: the cost of the SQL layer, MVCC
// and the B+ tree without the fsync.
func BenchmarkInsertBatched(b *testing.B) {
	db := benchDB(b)
	b.ResetTimer()
	benchExec(b, db, "BEGIN")
	for i := 0; i < b.N; i++ {
		if i > 0 && i%1000 == 0 {
			benchExec(b, db, "COMMIT; BEGIN")
		}
		benchExec(b, db, insertSQL(i))
	}
	benchExec(b, db, "COMMIT")
}

// A primary key lookup in its own read-only transaction.
func BenchmarkPointSelect(b *testing.B) {
	db := benchDB(b)
	benchExec(b, db, "BEGIN")
	for i := 0; i < 10000; i++ {
		benchExec(b, db, insertSQL(i))
	}
	benchExec(b, db, "COMMIT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchExec(b, db, fmt.Sprintf("SELECT s FROM t WHERE id = %d", i%10000))
	}
}
