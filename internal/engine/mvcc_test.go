package engine

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func sessionQuery(t *testing.T, s *Session, sql string) string {
	t.Helper()
	r, err := s.Query(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return render(r)
}

func sessionExec(t *testing.T, s *Session, sql string) {
	t.Helper()
	if _, err := s.Exec(sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func expectConflict(t *testing.T, err error, what string) {
	t.Helper()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("%s: got %v, want a serialization conflict", what, err)
	}
}

func accountsDB(t *testing.T) (*DB, *Session, *Session) {
	db, _ := openTemp(t)
	mustExec(t, db, `CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT);
		CREATE UNIQUE INDEX accounts_owner ON accounts (owner);
		INSERT INTO accounts VALUES (1, 'asha', 100), (2, 'rohan', 50)`)
	a, b := db.Session(), db.Session()
	t.Cleanup(func() { a.Close(); b.Close() })
	return db, a, b
}

func TestNoDirtyReads(t *testing.T) {
	_, a, b := accountsDB(t)
	sessionExec(t, a, "BEGIN; UPDATE accounts SET balance = 0 WHERE id = 1")
	if got := sessionQuery(t, b, "SELECT balance FROM accounts WHERE id = 1"); got != "100" {
		t.Fatalf("uncommitted update visible to another session: %s", got)
	}
	sessionExec(t, a, "ROLLBACK")
	if got := sessionQuery(t, b, "SELECT balance FROM accounts WHERE id = 1"); got != "100" {
		t.Fatalf("after rollback: %s", got)
	}
}

func TestRepeatableReadsAndNoPhantoms(t *testing.T) {
	_, a, b := accountsDB(t)
	sessionExec(t, a, "BEGIN")
	before := sessionQuery(t, a, "SELECT id, balance FROM accounts ORDER BY id")
	sessionExec(t, b, "UPDATE accounts SET balance = 999 WHERE id = 1")
	sessionExec(t, b, "INSERT INTO accounts VALUES (3, 'meera', 10)")
	sessionExec(t, b, "DELETE FROM accounts WHERE id = 2")
	if got := sessionQuery(t, a, "SELECT id, balance FROM accounts ORDER BY id"); got != before {
		t.Fatalf("snapshot changed inside a transaction:\nbefore %q\nafter  %q", before, got)
	}
	// The index sees the same snapshot as the table.
	if got := sessionQuery(t, a, "SELECT id FROM accounts WHERE owner = 'meera'"); got != "" {
		t.Fatalf("phantom through the index: %q", got)
	}
	if got := sessionQuery(t, a, "SELECT id FROM accounts WHERE owner = 'rohan'"); got != "2" {
		t.Fatalf("deleted row vanished from the snapshot's index: %q", got)
	}
	sessionExec(t, a, "COMMIT")
	if got := sessionQuery(t, a, "SELECT id, balance FROM accounts ORDER BY id"); got != "1 | 999\n3 | 10" {
		t.Fatalf("after commit: %q", got)
	}
}

func TestReadYourOwnWrites(t *testing.T) {
	_, a, _ := accountsDB(t)
	sessionExec(t, a, `BEGIN; INSERT INTO accounts VALUES (3, 'meera', 10);
		UPDATE accounts SET balance = balance + 1; DELETE FROM accounts WHERE id = 2`)
	if got := sessionQuery(t, a, "SELECT id, balance FROM accounts ORDER BY id"); got != "1 | 101\n3 | 11" {
		t.Fatalf("own writes: %q", got)
	}
	if got := sessionQuery(t, a, "SELECT id FROM accounts WHERE owner = 'meera'"); got != "3" {
		t.Fatalf("own index entry: %q", got)
	}
	sessionExec(t, a, "COMMIT")
}

// The classic lost update: two transactions read a balance and both write
// back a new one. Writes are buffered until COMMIT, so both updates run;
// the first to commit wins and the second must fail instead of silently
// overwriting it.
func TestLostUpdateIsPrevented(t *testing.T) {
	db, a, b := accountsDB(t)
	sessionExec(t, a, "BEGIN")
	sessionExec(t, b, "BEGIN")
	sessionExec(t, a, "UPDATE accounts SET balance = balance - 30 WHERE id = 1")
	sessionExec(t, b, "UPDATE accounts SET balance = balance - 20 WHERE id = 1")
	sessionExec(t, a, "COMMIT")
	_, err := b.Query("COMMIT")
	expectConflict(t, err, "second commit of a concurrently updated row")
	if b.InTransaction() {
		t.Fatal("a failed COMMIT must end the transaction")
	}
	if got := runQuery(t, db, "SELECT balance FROM accounts WHERE id = 1"); got != "70" {
		t.Fatalf("balance = %s", got)
	}

	// Deleting a row that another transaction changed and committed after
	// this one began is a conflict too.
	sessionExec(t, b, "BEGIN")
	sessionExec(t, a, "UPDATE accounts SET balance = 0 WHERE id = 2")
	sessionExec(t, b, "DELETE FROM accounts WHERE id = 2")
	_, err = b.Query("COMMIT")
	expectConflict(t, err, "delete of a row updated after the snapshot")
}

func TestConcurrentInsertsOfTheSameKey(t *testing.T) {
	db, a, b := accountsDB(t)
	sessionExec(t, a, "BEGIN; INSERT INTO accounts VALUES (3, 'meera', 1)")
	sessionExec(t, b, "BEGIN; INSERT INTO accounts VALUES (3, 'kabir', 1)")
	sessionExec(t, a, "COMMIT")
	_, err := b.Query("COMMIT")
	expectConflict(t, err, "concurrent insert of the same primary key")

	// Different primary keys but the same unique value: the unique index
	// key carries no primary key, so the two inserts collide on it.
	sessionExec(t, a, "BEGIN; INSERT INTO accounts VALUES (4, 'zoya', 1)")
	sessionExec(t, b, "BEGIN; INSERT INTO accounts VALUES (5, 'zoya', 1)")
	sessionExec(t, a, "COMMIT")
	_, err = b.Query("COMMIT")
	expectConflict(t, err, "concurrent insert of the same unique value")
	if got := runQuery(t, db, "SELECT id FROM accounts WHERE owner = 'zoya'"); got != "4" {
		t.Fatalf("zoya: %q", got)
	}
	checkIndexes(t, db)

	// Different keys and values don't conflict.
	sessionExec(t, a, "BEGIN; INSERT INTO accounts VALUES (6, 'ira', 1)")
	sessionExec(t, b, "BEGIN; INSERT INTO accounts VALUES (7, 'dev', 1)")
	sessionExec(t, a, "COMMIT")
	sessionExec(t, b, "COMMIT")
}

// Snapshot isolation permits write skew. Two doctors are on call and at
// least one must stay; each checks that the other is on call and signs
// off. The transactions write different rows, so both commit.
func TestWriteSkewIsPossible(t *testing.T) {
	db, a, b := accountsDB(t)
	mustExec(t, db, `CREATE TABLE oncall (doctor TEXT PRIMARY KEY, on_call BOOL);
		INSERT INTO oncall VALUES ('alice', true), ('bob', true)`)
	for _, s := range []*Session{a, b} {
		sessionExec(t, s, "BEGIN")
		if got := sessionQuery(t, s, "SELECT count(*) FROM oncall WHERE on_call"); got != "2" {
			t.Fatalf("on call: %s", got)
		}
	}
	sessionExec(t, a, "UPDATE oncall SET on_call = false WHERE doctor = 'alice'")
	sessionExec(t, b, "UPDATE oncall SET on_call = false WHERE doctor = 'bob'")
	sessionExec(t, a, "COMMIT")
	sessionExec(t, b, "COMMIT")
	if got := runQuery(t, db, "SELECT count(*) FROM oncall WHERE on_call"); got != "0" {
		t.Fatalf("expected the write-skew anomaly, got %s on call", got)
	}
}

func TestSchemaChangesConflictWithConcurrentWrites(t *testing.T) {
	db, a, b := accountsDB(t)

	// CREATE INDEX commits first: the concurrent insert, which its backfill
	// could not see, must not commit.
	sessionExec(t, a, "BEGIN; INSERT INTO accounts VALUES (3, 'meera', 10)")
	sessionExec(t, b, "CREATE INDEX accounts_balance ON accounts (balance)")
	_, err := a.Query("COMMIT")
	expectConflict(t, err, "insert concurrent with a committed CREATE INDEX")
	checkIndexes(t, db)

	// The insert commits first: the schema change must not.
	sessionExec(t, b, "BEGIN; DROP TABLE accounts")
	sessionExec(t, a, "INSERT INTO accounts VALUES (4, 'kabir', 1)")
	_, err = b.Query("COMMIT")
	expectConflict(t, err, "DROP TABLE concurrent with a committed insert")
	if got := runQuery(t, db, "SELECT count(*) FROM accounts"); got != "3" {
		t.Fatalf("accounts: %s rows", got)
	}

	// A schema change inside a transaction is invisible to others until it
	// commits.
	sessionExec(t, a, "BEGIN; CREATE TABLE secret (x INT)")
	if _, err := b.Query("SELECT * FROM secret"); err == nil {
		t.Fatal("uncommitted CREATE TABLE visible to another session")
	}
	if db.Session().Table("secret") != nil {
		t.Fatal("uncommitted table listed in another session's catalog")
	}
	sessionExec(t, a, "COMMIT")
	sessionQuery(t, b, "SELECT * FROM secret")
}

func TestOpenTransactionIsRolledBackOnReopen(t *testing.T) {
	db, path := openTemp(t)
	mustExec(t, db, fixture)
	other := db.Session()
	sessionExec(t, other, "BEGIN; DELETE FROM orders; INSERT INTO orders VALUES (99, 1, 1.0)")
	// Closing the database flushes every page, including the open
	// transaction's versions, so recovery has to undo them.
	db.Close()
	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := runQuery(t, db2, "SELECT oid FROM orders ORDER BY oid"); got != "10\n11\n12\n13" {
		t.Fatalf("orders after reopen: %q", got)
	}
	checkIndexes(t, db2)
}

func TestVacuumStatement(t *testing.T) {
	db := fixtureDB(t)
	for i := 0; i < 5; i++ {
		mustExec(t, db, "UPDATE users SET score = score + 1")
	}
	mustExec(t, db, "DELETE FROM orders WHERE oid > 11")
	contents := func() string {
		return runQuery(t, db, "SELECT * FROM users ORDER BY id") + "\n" + runQuery(t, db, "SELECT * FROM orders ORDER BY oid")
	}
	before := contents()
	r, err := db.Query("VACUUM")
	if err != nil {
		t.Fatal(err)
	}
	if r.Message == "VACUUM 0" {
		t.Fatal("vacuum removed nothing")
	}
	if got := contents(); got != before {
		t.Fatalf("vacuum changed the data:\n%s\nwant:\n%s", got, before)
	}
	checkIndexes(t, db)

	// Versions an open snapshot still needs survive a vacuum.
	reader := db.Session()
	sessionExec(t, reader, "BEGIN")
	snapshot := sessionQuery(t, reader, "SELECT sum(score) FROM users")
	mustExec(t, db, "UPDATE users SET score = 0")
	mustExec(t, db, "VACUUM")
	if got := sessionQuery(t, reader, "SELECT sum(score) FROM users"); got != snapshot {
		t.Fatalf("vacuum broke an open snapshot: %s, want %s", got, snapshot)
	}
	sessionExec(t, reader, "COMMIT")
	if _, err := db.Exec("BEGIN; VACUUM"); err == nil || !strings.Contains(err.Error(), "inside a transaction") {
		t.Fatalf("VACUUM in a transaction: %v", err)
	}
}

// TestConcurrentTransfers runs bank transfers from several goroutines, each
// with its own session, retrying on conflicts, while readers repeatedly
// check that money is neither created nor destroyed: every snapshot must
// see the same total. Lost updates would break the total; so would any
// reader seeing half of a transfer.
func TestConcurrentTransfers(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bank.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const accounts, initial = 10, 1000
	mustExec(t, db, "CREATE TABLE acct (id INT PRIMARY KEY, balance INT); CREATE INDEX acct_balance ON acct (balance)")
	var rows []string
	for i := 0; i < accounts; i++ {
		rows = append(rows, fmt.Sprintf("(%d, %d)", i, initial))
	}
	mustExec(t, db, "INSERT INTO acct VALUES "+strings.Join(rows, ", "))
	want := fmt.Sprint(accounts * initial)

	var conflicts, committed atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s := db.Session()
			defer s.Close()
			rng := rand.New(rand.NewSource(int64(w)))
			for done := 0; done < 40; {
				from, to := rng.Intn(accounts), rng.Intn(accounts)
				if from == to {
					continue
				}
				amount := rng.Intn(50) + 1
				_, err := s.Exec(fmt.Sprintf(`BEGIN;
					UPDATE acct SET balance = balance - %[3]d WHERE id = %[1]d;
					UPDATE acct SET balance = balance + %[3]d WHERE id = %[2]d;
					COMMIT`, from, to, amount))
				switch {
				case errors.Is(err, ErrConflict):
					conflicts.Add(1)
					s.Close() // the failed statement already rolled back; this is a no-op
				case err != nil:
					errs <- err
					return
				default:
					committed.Add(1)
					done++
				}
			}
		}(w)
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := db.Session()
			for i := 0; i < 60; i++ {
				res, err := s.Query("SELECT sum(balance) FROM acct")
				if err != nil {
					errs <- err
					return
				}
				if got := render(res); got != want {
					errs <- fmt.Errorf("a snapshot saw a total of %s, want %s", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := runQuery(t, db, "SELECT sum(balance), count(*) FROM acct"); got != want+" | 10" {
		t.Fatalf("final total: %s", got)
	}
	checkIndexes(t, db)
	t.Logf("%d transfers committed, %d conflicts retried", committed.Load(), conflicts.Load())
}
