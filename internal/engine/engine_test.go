package engine

import (
	"path/filepath"
	"strings"
	"testing"

	"raftdb/internal/storage"
)

func openTemp(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// render formats a result as one line per row, values separated by " | ".
func render(r *Result) string {
	lines := make([]string, len(r.Rows))
	for i, row := range r.Rows {
		vals := make([]string, len(row))
		for j, v := range row {
			vals[j] = FormatValue(v)
		}
		lines[i] = strings.Join(vals, " | ")
	}
	return strings.Join(lines, "\n")
}

func runQuery(t *testing.T, db *DB, sql string) string {
	t.Helper()
	r, err := db.Query(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return render(r)
}

const fixture = `
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, city TEXT, score FLOAT);
CREATE INDEX users_age ON users (age);
CREATE UNIQUE INDEX users_name ON users (name);
INSERT INTO users VALUES
	(1, 'Asha', 22, 'Pilani', 8.5),
	(2, 'Rohan', 17, 'Goa', 6.0),
	(3, 'Meera', 30, 'Pilani', 9.25),
	(4, 'Kabir', NULL, 'Delhi', NULL),
	(5, 'Zoya', 22, NULL, 7.0);
CREATE TABLE orders (oid INT PRIMARY KEY, uid INT, amount FLOAT);
CREATE INDEX orders_uid ON orders (uid);
INSERT INTO orders VALUES (10, 1, 99.5), (11, 1, 10), (12, 3, 5.25), (13, 9, 1);
CREATE TABLE tags (tag TEXT, uid INT);
INSERT INTO tags VALUES ('admin', 1), ('new', 2), ('new', 5);
`

func fixtureDB(t *testing.T) *DB {
	db, _ := openTemp(t)
	mustExec(t, db, fixture)
	return db
}

func TestQueries(t *testing.T) {
	db := fixtureDB(t)
	cases := []struct{ sql, want string }{
		// Scans, filters, NULL semantics
		{"SELECT name FROM users WHERE age > 20 ORDER BY id", "Asha\nMeera\nZoya"},
		{"SELECT name FROM users WHERE age IS NULL", "Kabir"},
		{"SELECT name FROM users WHERE age != 22 ORDER BY name", "Meera\nRohan"},
		{"SELECT count(*) FROM users WHERE city = NULL", "0"},
		{"SELECT name FROM users WHERE city IN ('Goa', 'Delhi') ORDER BY name", "Kabir\nRohan"},
		{"SELECT name FROM users WHERE name LIKE '%a' ORDER BY name", "Asha\nMeera\nZoya"},
		{"SELECT name FROM users WHERE age BETWEEN 18 AND 25 ORDER BY name", "Asha\nZoya"},
		{"SELECT name FROM users WHERE NOT (age < 25) ORDER BY name", "Meera"},

		// Primary key lookups and ranges
		{"SELECT name FROM users WHERE id = 3", "Meera"},
		{"SELECT name FROM users WHERE id >= 4", "Kabir\nZoya"},
		{"SELECT name FROM users WHERE id > 1 AND id < 4", "Rohan\nMeera"},
		{"SELECT name FROM users WHERE id = 99", ""},

		// Index scans, including the type mismatch the planner must leave to the filter
		{"SELECT name FROM users WHERE age = 22 ORDER BY name", "Asha\nZoya"},
		{"SELECT name FROM users WHERE age = 22.5", ""},
		{"SELECT name FROM users WHERE age < 22.5 ORDER BY name", "Asha\nRohan\nZoya"},
		{"SELECT id FROM users WHERE name = 'Meera'", "3"},

		// Expressions and functions
		{"SELECT id * 2 + 1, name || '!' FROM users WHERE id = 2", "5 | Rohan!"},
		{"SELECT upper(name), length(city) FROM users WHERE id = 1", "ASHA | 6"},
		{"SELECT coalesce(city, 'none') FROM users WHERE id = 5", "none"},
		{"SELECT 7 / 2, 7.0 / 2, 7 % 3, -abs(-4), round(2.5)", "3 | 3.5 | 1 | -4 | 3"},
		{"SELECT 1 + NULL, NULL = NULL, NULL IS NULL", "NULL | NULL | true"},
		{"SELECT false AND NULL, true OR NULL", "false | true"},

		// Ordering, DISTINCT, LIMIT
		{"SELECT name, age FROM users ORDER BY age DESC, name", "Kabir | NULL\nMeera | 30\nAsha | 22\nZoya | 22\nRohan | 17"},
		{"SELECT name FROM users ORDER BY 1 LIMIT 2", "Asha\nKabir"},
		{"SELECT name AS n FROM users ORDER BY n DESC LIMIT 1 OFFSET 1", "Rohan"},
		{"SELECT DISTINCT age FROM users ORDER BY age", "17\n22\n30\nNULL"},
		{"SELECT DISTINCT city FROM users WHERE city IS NOT NULL ORDER BY city", "Delhi\nGoa\nPilani"},

		// Aggregates
		{"SELECT count(*), count(age), sum(age), min(age), max(age) FROM users", "5 | 4 | 91 | 17 | 30"},
		{"SELECT avg(score) FROM users", "7.6875"},
		{"SELECT count(*), sum(age) FROM users WHERE id > 100", "0 | NULL"},
		{"SELECT city, count(*) FROM users GROUP BY city ORDER BY city", "Delhi | 1\nGoa | 1\nPilani | 2\nNULL | 1"},
		{"SELECT age, count(*) AS c FROM users GROUP BY age HAVING count(*) > 1", "22 | 2"},
		{"SELECT city, max(score) - min(score) FROM users WHERE city = 'Pilani' GROUP BY city", "Pilani | 0.75"},

		// Joins
		{"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid ORDER BY o.oid",
			"Asha | 99.5\nAsha | 10\nMeera | 5.25"},
		{"SELECT name, sum(amount) AS total FROM users JOIN orders ON id = uid GROUP BY name ORDER BY total DESC",
			"Asha | 109.5\nMeera | 5.25"},
		{"SELECT u.name, t.tag FROM users u, tags t WHERE u.id = t.uid AND t.tag = 'new' ORDER BY u.name",
			"Rohan | new\nZoya | new"},
		{"SELECT count(*) FROM users, tags", "15"},
		{"SELECT u.name FROM users u JOIN orders o ON u.id = o.uid AND o.amount > 50", "Asha"},
		{"SELECT a.name, b.name FROM users a JOIN users b ON a.age = b.age AND a.id < b.id", "Asha | Zoya"},
		{"SELECT t.tag, o.oid FROM tags t JOIN users u ON t.uid = u.id JOIN orders o ON o.uid = u.id ORDER BY o.oid",
			"admin | 10\nadmin | 11"},
		{"SELECT u.name FROM users u JOIN orders o ON u.age < o.amount ORDER BY u.name, o.oid LIMIT 3",
			"Asha\nMeera\nRohan"},

		// SELECT without FROM, tables without a primary key
		{"SELECT 'x' || 'y'", "xy"},
		{"SELECT tag, uid FROM tags ORDER BY uid DESC", "new | 5\nnew | 2\nadmin | 1"},
	}
	for _, c := range cases {
		if got := runQuery(t, db, c.sql); got != c.want {
			t.Errorf("%s\n got: %q\nwant: %q", c.sql, got, c.want)
		}
	}
}

func TestColumnNames(t *testing.T) {
	db := fixtureDB(t)
	r, err := db.Query("SELECT id, name AS who, count(*), age + 1, u.city FROM users u GROUP BY id, name, age, u.city")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(r.Columns, ",")
	if !strings.HasPrefix(got, "id,who,count,") || !strings.HasSuffix(got, ",city") {
		t.Fatalf("columns = %q", got)
	}
}

func TestWrites(t *testing.T) {
	db := fixtureDB(t)
	steps := []struct{ sql, msg string }{
		{"INSERT INTO users (id, name) VALUES (6, 'Ira')", "INSERT 1"},
		{"UPDATE users SET age = age + 1, city = 'Jaipur' WHERE age >= 22", "UPDATE 3"},
		{"UPDATE users SET id = id + 100 WHERE id = 2", "UPDATE 1"},
		{"DELETE FROM users WHERE city IS NULL", "DELETE 1"},
		{"INSERT INTO tags VALUES ('x', 1), ('x', 1)", "INSERT 2"},
		{"DELETE FROM tags WHERE tag = 'x'", "DELETE 2"},
	}
	for _, s := range steps {
		r, err := db.Query(s.sql)
		if err != nil {
			t.Fatalf("%s: %v", s.sql, err)
		}
		if r.Message != s.msg {
			t.Errorf("%s: message %q, want %q", s.sql, r.Message, s.msg)
		}
	}
	want := "1 | Asha | 23 | Jaipur\n3 | Meera | 31 | Jaipur\n4 | Kabir | NULL | Delhi\n5 | Zoya | 23 | Jaipur\n102 | Rohan | 17 | Goa"
	if got := runQuery(t, db, "SELECT id, name, age, city FROM users ORDER BY id"); got != want {
		t.Errorf("table after writes:\n got: %q\nwant: %q", got, want)
	}
	// The indexes followed every change, including the primary key update.
	for sql, want := range map[string]string{
		"SELECT name FROM users WHERE age = 22":         "",
		"SELECT name FROM users WHERE age = 23":         "Asha\nZoya",
		"SELECT id FROM users WHERE name = 'Rohan'":     "102",
		"SELECT count(*) FROM users WHERE name = 'Ira'": "0",
		"SELECT count(*) FROM tags":                     "3",
	} {
		if got := runQuery(t, db, sql); got != want {
			t.Errorf("%s: got %q, want %q", sql, got, want)
		}
	}
	checkIndexes(t, db)
}

func TestExplain(t *testing.T) {
	db := fixtureDB(t)
	cases := []struct{ sql, want string }{
		{"SELECT * FROM users", "Seq Scan on users"},
		{"SELECT * FROM users WHERE id = 3", "Primary Key Lookup on users ((id = 3))"},
		{"SELECT * FROM users WHERE id > 3", "Primary Key Range Scan on users ((id > 3))"},
		{"SELECT * FROM users WHERE 3 < id AND id <= 9", "Primary Key Range Scan on users ((3 < id) AND (id <= 9))"},
		{"SELECT * FROM users WHERE age = 22", "Index Scan using users_age on users ((age = 22))"},
		{"SELECT * FROM users WHERE age BETWEEN 1 AND 5", "Index Scan using users_age on users ((age BETWEEN 1 AND 5))"},
		// A unique index beats a non-unique one; the primary key beats both.
		{"SELECT * FROM users WHERE age = 22 AND name = 'Asha'", "Index Scan using users_name on users ((name = 'Asha'))"},
		{"SELECT * FROM users WHERE name = 'Asha' AND id = 1", "Primary Key Lookup on users ((id = 1))"},
		// An equality on an index beats a range on the primary key.
		{"SELECT * FROM users WHERE id > 1 AND age = 22", "Index Scan using users_age on users ((age = 22))"},
		// Conditions an index cannot answer.
		{"SELECT * FROM users WHERE age + 1 = 23", "Seq Scan on users"},
		{"SELECT * FROM users WHERE age = 22 OR id = 1", "Seq Scan on users"},
		{"SELECT * FROM users WHERE city = 'Goa'", "Seq Scan on users"},
		{"SELECT * FROM users WHERE age = NULL", "Seq Scan on users"},
		// Joins
		{"SELECT * FROM users u JOIN orders o ON u.id = o.uid", "Hash Join: (u.id = o.uid)"},
		{"SELECT * FROM users u JOIN orders o ON u.age < o.amount", "Nested Loop Join"},
		{"SELECT * FROM users, tags", "Nested Loop Join (cross)"},
	}
	for _, c := range cases {
		got := runQuery(t, db, "EXPLAIN "+c.sql)
		if !strings.Contains(got, c.want) {
			t.Errorf("EXPLAIN %s\nplan:\n%s\nwant a line containing %q", c.sql, got, c.want)
		}
	}

	// The whole plan, to pin down the operator order and that the WHERE
	// condition is pushed below the join, where an index can answer it.
	want := strings.Join([]string{
		"Limit 2",
		"-> Sort: total DESC",
		"   -> Project: name, total",
		"      -> Hash Aggregate: group by name",
		"         -> Hash Join: (id = uid)",
		"            -> Filter: (age > 18)",
		"               -> Index Scan using users_age on users ((age > 18))",
		"            -> Seq Scan on orders",
	}, "\n")
	got := runQuery(t, db, "EXPLAIN SELECT name, sum(amount) AS total FROM users JOIN orders ON id = uid WHERE age > 18 GROUP BY name ORDER BY total DESC LIMIT 2")
	if got != want {
		t.Errorf("full plan:\n%s", got)
	}
}

func TestErrors(t *testing.T) {
	db := fixtureDB(t)
	cases := []struct{ sql, want string }{
		{"SELECT * FROM nope", `table "nope" does not exist`},
		{"SELECT nope FROM users", `column "nope" does not exist`},
		{"SELECT x.id FROM users", `missing FROM-clause entry for table "x"`},
		{"SELECT id FROM users, orders, users", `table name "users" specified more than once`},
		{"SELECT name FROM users u JOIN users v ON u.id = v.id", `column reference "name" is ambiguous`},
		{"CREATE TABLE users (a INT)", `table "users" already exists`},
		{"CREATE TABLE t2 (a INT PRIMARY KEY, b INT PRIMARY KEY)", `multiple primary keys for table "t2" are not allowed`},
		{"CREATE TABLE t2 (a INT, a TEXT)", `column "a" specified more than once`},
		{"CREATE INDEX users_age ON users (city)", `index "users_age" already exists`},
		{"CREATE INDEX i ON users (nope)", `column "nope" of table "users" does not exist`},
		{"DROP INDEX nope", `index "nope" does not exist`},
		{"DROP TABLE nope", `table "nope" does not exist`},
		{"INSERT INTO users VALUES (1, 'Dup', 1, 'x', 1.0)", "duplicate key value violates primary key: id = 1 already exists"},
		{"INSERT INTO users VALUES (9, 'Asha', 1, 'x', 1.0)", `duplicate key value violates unique index "users_name": name = 'Asha' already exists`},
		{"INSERT INTO users (id) VALUES (9)", `null value in column "name" violates not-null constraint`},
		{"INSERT INTO users VALUES (NULL, 'P', 1, 'x', 1.0)", `null value in column "id" violates not-null constraint`},
		{"INSERT INTO users (id, name) VALUES ('nine', 'P')", `column "id" is INT, but the value 'nine' is TEXT`},
		{"INSERT INTO users (id, name) VALUES (9.5, 'P')", `column "id" is INT, but the value 9.5 is FLOAT`},
		{"INSERT INTO users VALUES (9, 'P')", "INSERT has 2 values but 5 target columns"},
		{"INSERT INTO users (id, id) VALUES (9, 9)", `column "id" specified more than once`},
		{"UPDATE users SET nope = 1", `column "nope" of table "users" does not exist`},
		{"UPDATE users SET name = 'Asha' WHERE id = 2", `violates unique index "users_name"`},
		{"UPDATE users SET id = 1 WHERE id = 2", "violates primary key"},
		{"SELECT name, count(*) FROM users", `column "name" must appear in the GROUP BY clause or be used in an aggregate function`},
		{"SELECT * FROM users GROUP BY id", "SELECT * cannot be combined with GROUP BY or aggregate functions"},
		{"SELECT * FROM users WHERE count(*) > 1", "aggregate functions are not allowed in WHERE"},
		{"SELECT sum(*) FROM users", "sum(*) is not allowed; only count(*) is"},
		{"SELECT nope(1)", "function nope does not exist"},
		{"SELECT upper(1)", "expected TEXT, got INT"},
		{"SELECT 1 + 'a'", "operator + cannot be applied to INT and TEXT"},
		{"SELECT name FROM users WHERE age", "argument of WHERE must be BOOL, not INT"},
		{"SELECT 1 / 0", "division by zero"},
		{"SELECT 9223372036854775807 + 1", "out of range"},
		{"SELECT name FROM users ORDER BY 5", "ORDER BY position 5 is not in select list"},
		{"SELECT DISTINCT name FROM users ORDER BY age", "for SELECT DISTINCT, ORDER BY expressions must appear in the select list"},
		{"SELECT name FROM users LIMIT -1", "LIMIT must be a non-negative integer"},
		{"SELECT name FROM users LIMIT age", "LIMIT must be a constant"},
		{"SELECT *", "SELECT * with no tables specified is not valid"},
		{"COMMIT", "no transaction in progress"},
		{"ROLLBACK", "no transaction in progress"},
	}
	for _, c := range cases {
		_, err := db.Query(c.sql)
		if err == nil {
			t.Errorf("%s: no error, want %q", c.sql, c.want)
		} else if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s:\n got: %v\nwant: %s", c.sql, err, c.want)
		}
	}
	// None of the failed statements changed anything.
	if got := runQuery(t, db, "SELECT count(*), sum(id) FROM users"); got != "5 | 15" {
		t.Errorf("users changed: %s", got)
	}
	checkIndexes(t, db)
}

// A statement is atomic: if the third row of an INSERT fails, the first two
// are not kept, and neither are their index entries.
func TestStatementAtomicity(t *testing.T) {
	db := fixtureDB(t)
	_, err := db.Query("INSERT INTO users (id, name, age) VALUES (20, 'A2', 50), (21, 'B2', 50), (1, 'C2', 50)")
	if err == nil {
		t.Fatal("expected a duplicate key error")
	}
	if got := runQuery(t, db, "SELECT count(*) FROM users WHERE age = 50"); got != "0" {
		t.Fatalf("partial insert visible through the index: %s", got)
	}
	if got := runQuery(t, db, "SELECT count(*) FROM users"); got != "5" {
		t.Fatalf("partial insert visible: %s rows", got)
	}
	_, err = db.Query("UPDATE users SET age = 100 / (age - 22)")
	if err == nil {
		t.Fatal("expected division by zero")
	}
	if got := runQuery(t, db, "SELECT sum(age) FROM users"); got != "91" {
		t.Fatalf("partial update visible: sum(age) = %s", got)
	}
	checkIndexes(t, db)
}

func TestTransactions(t *testing.T) {
	db := fixtureDB(t)
	mustExec(t, db, "BEGIN; DELETE FROM orders; CREATE TABLE tmp (a INT); INSERT INTO tmp VALUES (1)")
	if !db.InTransaction() {
		t.Fatal("not in a transaction after BEGIN")
	}
	if got := runQuery(t, db, "SELECT count(*) FROM orders"); got != "0" {
		t.Fatalf("own delete not visible inside the transaction: %s", got)
	}
	mustExec(t, db, "ROLLBACK")
	if got := runQuery(t, db, "SELECT count(*) FROM orders"); got != "4" {
		t.Fatalf("rollback did not restore orders: %s", got)
	}
	if db.Table("tmp") != nil {
		t.Fatal("rollback did not undo CREATE TABLE")
	}

	mustExec(t, db, "BEGIN; DELETE FROM orders WHERE oid = 13; DROP INDEX users_age; COMMIT")
	if got := runQuery(t, db, "SELECT count(*) FROM orders"); got != "3" {
		t.Fatalf("commit lost: %s", got)
	}
	if got := runQuery(t, db, "EXPLAIN SELECT * FROM users WHERE age = 1"); !strings.Contains(got, "Seq Scan") {
		t.Fatalf("dropped index still used:\n%s", got)
	}

	// An error inside a transaction rolls the whole transaction back.
	mustExec(t, db, "BEGIN; INSERT INTO orders VALUES (50, 1, 1.0)")
	_, err := db.Query("INSERT INTO orders VALUES (50, 1, 1.0)")
	if err == nil || !strings.Contains(err.Error(), "the transaction was rolled back") {
		t.Fatalf("error inside transaction: %v", err)
	}
	if db.InTransaction() {
		t.Fatal("still in a transaction after a failed statement")
	}
	if got := runQuery(t, db, "SELECT count(*) FROM orders WHERE oid = 50"); got != "0" {
		t.Fatal("first insert of the failed transaction survived")
	}
	if _, err := db.Query("BEGIN; BEGIN"); err == nil {
		t.Fatal("nested BEGIN allowed")
	}
}

func TestDropTable(t *testing.T) {
	db := fixtureDB(t)
	mustExec(t, db, "DROP TABLE users")
	mustExec(t, db, "DROP TABLE IF EXISTS users")
	mustExec(t, db, "CREATE TABLE users (id INT PRIMARY KEY, x TEXT)")
	if got := runQuery(t, db, "SELECT count(*) FROM users"); got != "0" {
		t.Fatalf("recreated table has old rows: %s", got)
	}
	// Dropping must remove the rows and index entries from storage, not just the catalog.
	for _, prefix := range []string{"r", "x"} {
		n := 0
		it := readTxn(t, db).scan([]byte(prefix), prefixEnd([]byte(prefix)))
		for {
			_, _, ok, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			n++
		}
		want := map[string]int{"r": 7, "x": 4}[prefix] // orders and tags rows; the orders_uid index
		if n != want {
			t.Errorf("%d keys with prefix %q after drop, want %d", n, prefix, want)
		}
	}
}

func TestCreateIndexOnExistingData(t *testing.T) {
	db := fixtureDB(t)
	mustExec(t, db, "CREATE INDEX users_city ON users (city)")
	if got := runQuery(t, db, "SELECT name FROM users WHERE city = 'Pilani' ORDER BY name"); got != "Asha\nMeera" {
		t.Fatalf("new index: %q", got)
	}
	if _, err := db.Query("CREATE UNIQUE INDEX u_age ON users (age)"); err == nil {
		t.Fatal("unique index created over duplicate values")
	}
	if db.Table("users").Indexes[len(db.Table("users").Indexes)-1].Name == "u_age" {
		t.Fatal("failed index left in the catalog")
	}
	// Unique indexes allow any number of NULLs, as in PostgreSQL.
	mustExec(t, db, "CREATE UNIQUE INDEX users_score ON users (score)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (7, 'N1'), (8, 'N2')")
	checkIndexes(t, db)
}

func TestPersistence(t *testing.T) {
	db, path := openTemp(t)
	mustExec(t, db, fixture)
	mustExec(t, db, "BEGIN; INSERT INTO orders VALUES (99, 1, 1.0)") // never committed
	db.Close()

	db2, err := OpenWithOptions(path, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := runQuery(t, db2, "SELECT count(*) FROM orders"); got != "4" {
		t.Fatalf("orders after reopen: %s (the open transaction must be discarded)", got)
	}
	if got := runQuery(t, db2, "SELECT name FROM users WHERE age = 22 ORDER BY name"); got != "Asha\nZoya" {
		t.Fatalf("index after reopen: %q", got)
	}
	if got := runQuery(t, db2, "EXPLAIN SELECT * FROM users WHERE name = 'x'"); !strings.Contains(got, "users_name") {
		t.Fatalf("index missing from catalog after reopen:\n%s", got)
	}
	// IDs keep counting from where they were: a new table must not reuse one.
	mustExec(t, db2, "CREATE TABLE more (a INT); INSERT INTO more VALUES (1)")
	if got := runQuery(t, db2, "SELECT count(*) FROM orders"); got != "4" {
		t.Fatalf("new table collided with an old one: orders has %s rows", got)
	}
	if got := runQuery(t, db2, "SELECT count(*) FROM tags"); got != "3" {
		t.Fatalf("hidden row IDs: tags has %s rows", got)
	}
	mustExec(t, db2, "INSERT INTO tags VALUES ('z', 0)")
	if got := runQuery(t, db2, "SELECT count(*) FROM tags"); got != "4" {
		t.Fatalf("hidden row ID reused after reopen: tags has %s rows", got)
	}
}

func TestSchemaString(t *testing.T) {
	db := fixtureDB(t)
	got := db.Table("users").String()
	want := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, city TEXT, score FLOAT);\n" +
		"CREATE INDEX users_age ON users (age);\n" +
		"CREATE UNIQUE INDEX users_name ON users (name);"
	if got != want {
		t.Fatalf("schema:\n%s\nwant:\n%s", got, want)
	}
}

// checkIndexes verifies that every index holds exactly one entry per row,
// keyed by the row's current value and pointing at the row's primary key.
// It reads everything from one snapshot, and closes it so that it doesn't
// hold back VACUUM.
func checkIndexes(t *testing.T, db *DB) {
	t.Helper()
	tx, err := db.begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.rollback()
	for _, tbl := range tx.tables {
		want := map[string]bool{}
		it := tx.scan(rowPrefix(tbl.ID), prefixEnd(rowPrefix(tbl.ID)))
		var rows []Row
		var pks [][]byte
		for {
			k, v, ok, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			row, err := decodeRow(v, len(tbl.Columns))
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
			pks = append(pks, append([]byte(nil), k[len(rowPrefix(tbl.ID)):]...))
		}
		for _, idx := range tbl.Indexes {
			clear(want)
			for i, row := range rows {
				want[string(indexKey(idx, row[idx.Column], pks[i]))+"\x00"+string(pks[i])] = true
			}
			it := tx.scan(indexPrefix(idx.ID), prefixEnd(indexPrefix(idx.ID)))
			n := 0
			for {
				k, v, ok, err := it.Next()
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				n++
				if !want[string(k)+"\x00"+string(v)] {
					t.Fatalf("index %s: stray entry %x -> %x", idx.Name, k, v)
				}
			}
			if n != len(rows) {
				t.Fatalf("index %s has %d entries for %d rows", idx.Name, n, len(rows))
			}
		}
	}
}

// Constraints are checked against the statement's final state, not row by
// row, so shifting every key succeeds whichever order the rows are visited
// in: primary key order, or through an index in the reverse order.
func TestUpdateShiftsKeys(t *testing.T) {
	db, _ := openTemp(t)
	mustExec(t, db, `CREATE TABLE s (id INT PRIMARY KEY, rank INT, u INT);
		CREATE INDEX s_rank ON s (rank);
		CREATE UNIQUE INDEX s_u ON s (u);
		INSERT INTO s VALUES (1, 3, 10), (2, 2, 20), (3, 1, 30)`)
	mustExec(t, db, "UPDATE s SET id = id + 1")                // primary key order
	mustExec(t, db, "UPDATE s SET id = id + 1 WHERE rank > 0") // index order: 4, 3, 2
	mustExec(t, db, "UPDATE s SET u = u + 10 WHERE rank > 0")
	if got := runQuery(t, db, "SELECT id, u FROM s ORDER BY id"); got != "3 | 20\n4 | 30\n5 | 40" {
		t.Fatalf("after shifting keys: %q", got)
	}
	// A real collision in the final state is still an error.
	if _, err := db.Query("UPDATE s SET id = 9"); err == nil || !strings.Contains(err.Error(), "violates primary key") {
		t.Fatalf("collision: %v", err)
	}
	if _, err := db.Query("UPDATE s SET u = 30 WHERE id = 3"); err == nil || !strings.Contains(err.Error(), "violates unique index") {
		t.Fatalf("unique collision with an untouched row: %v", err)
	}
	checkIndexes(t, db)
}

// readTxn returns a read-only transaction on the latest committed state,
// for tests that inspect the stored keys directly.
func readTxn(t *testing.T, db *DB) *txn {
	t.Helper()
	tx, err := db.begin(true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.rollback() })
	return tx
}
