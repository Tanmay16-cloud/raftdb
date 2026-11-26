package parser

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// Each case gives SQL as a person might write it and the canonical form the
// parser should print back. Fully parenthesised output makes the parser's
// grouping decisions visible.
var canonicalCases = []struct{ in, want string }{
	// Operator precedence and associativity
	{"SELECT 1 + 2 * 3", "SELECT (1 + (2 * 3))"},
	{"SELECT (1 + 2) * 3", "SELECT ((1 + 2) * 3)"},
	{"SELECT 1 - 2 - 3", "SELECT ((1 - 2) - 3)"},
	{"SELECT 8 / 4 % 3", "SELECT ((8 / 4) % 3)"},
	{"SELECT a OR b AND c", "SELECT (a OR (b AND c))"},
	{"SELECT NOT a = 1 AND b", "SELECT ((NOT (a = 1)) AND b)"},
	{"SELECT -a * b", "SELECT ((-a) * b)"},
	{"SELECT - - 5", "SELECT (-(-5))"},
	{"SELECT a || b = c", "SELECT ((a || b) = c)"},
	{"SELECT a + 1 < b * 2 OR c", "SELECT (((a + 1) < (b * 2)) OR c)"},
	{"SELECT x BETWEEN 1 AND 10 AND y", "SELECT ((x BETWEEN 1 AND 10) AND y)"},
	{"SELECT x NOT BETWEEN a + 1 AND b", "SELECT (x NOT BETWEEN (a + 1) AND b)"},
	{"SELECT a NOT IN (1, 2, 3) OR b IS NOT NULL", "SELECT ((a NOT IN (1, 2, 3)) OR (b IS NOT NULL))"},
	{"SELECT a IN (b) AND c IS NULL", "SELECT ((a IN (b)) AND (c IS NULL))"},
	{"SELECT name LIKE 'A%' AND name NOT LIKE '%z'", "SELECT ((name LIKE 'A%') AND (name NOT LIKE '%z'))"},
	{"SELECT a <> b, a != b, a <= b, a >= b", "SELECT (a != b), (a != b), (a <= b), (a >= b)"},

	// Literals
	{"SELECT 'it''s', 1.5, 2e3, 7, TRUE, false, NULL", "SELECT 'it''s', 1.5, 2000.0, 7, TRUE, FALSE, NULL"},
	{"SELECT 1 -- a comment\n/* another */ + 2", "SELECT (1 + 2)"},

	// SELECT
	{"select * from users", "SELECT * FROM users"},
	{"SELECT DISTINCT city FROM users;", "SELECT DISTINCT city FROM users"},
	{
		"SELECT id, name AS n, age + 1 years FROM users u WHERE age >= 18 ORDER BY age DESC, id ASC LIMIT 10 OFFSET 20",
		"SELECT id, name AS n, (age + 1) AS years FROM users AS u WHERE (age >= 18) ORDER BY age DESC, id LIMIT 10 OFFSET 20",
	},
	{
		"SELECT u.name, COUNT(*) FROM users u INNER JOIN orders o ON u.id = o.user_id, regions GROUP BY u.name HAVING count(*) > 5",
		"SELECT u.name, count(*) FROM users AS u JOIN orders AS o ON (u.id = o.user_id), regions GROUP BY u.name HAVING (count(*) > 5)",
	},
	{"SELECT lower(name), now(), coalesce(a, b, 0) FROM t", "SELECT lower(name), now(), coalesce(a, b, 0) FROM t"},

	// Identifiers: unquoted names fold to lower case; quoted names are exact.
	{`SELECT "Full Name", "select", Email FROM "My Table"`, `SELECT "Full Name", "select", email FROM "My Table"`},
	{`SELECT "abc", key FROM t`, `SELECT abc, "key" FROM t`},

	// Data definition
	{
		"create table if not exists users (id int primary key, name varchar not null, score real, active boolean)",
		"CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT NOT NULL, score FLOAT, active BOOL)",
	},
	{"CREATE TABLE if (key text)", `CREATE TABLE "if" ("key" TEXT)`},
	{"create unique index idx_email on users (email)", "CREATE UNIQUE INDEX idx_email ON users (email)"},
	{"CREATE INDEX IF NOT EXISTS i ON t (c)", "CREATE INDEX IF NOT EXISTS i ON t (c)"},
	{"drop table if exists users", "DROP TABLE IF EXISTS users"},
	{"DROP INDEX idx_email", "DROP INDEX idx_email"},

	// Data manipulation
	{
		"insert into users (id, name) values (1, 'Asha'), (2, 'Rohan')",
		"INSERT INTO users (id, name) VALUES (1, 'Asha'), (2, 'Rohan')",
	},
	{"INSERT INTO t VALUES (1 + 1, NULL)", "INSERT INTO t VALUES ((1 + 1), NULL)"},
	{
		"UPDATE users SET score = score * 1.1, active = TRUE WHERE id = 7",
		"UPDATE users SET score = (score * 1.1), active = TRUE WHERE (id = 7)",
	},
	{"DELETE FROM users WHERE NOT active", "DELETE FROM users WHERE (NOT active)"},
	{"DELETE FROM users", "DELETE FROM users"},

	// Transactions
	{"begin transaction", "BEGIN"},
	{"COMMIT;", "COMMIT"},
	{"rollback", "ROLLBACK"},
}

func TestCanonicalForm(t *testing.T) {
	for _, c := range canonicalCases {
		stmt, err := Parse(c.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", c.in, err)
			continue
		}
		if got := stmt.String(); got != c.want {
			t.Errorf("Parse(%q).String()\n got: %s\nwant: %s", c.in, got, c.want)
		}
	}
}

// Parsing the canonical form must give back exactly the same tree.
func TestRoundTrip(t *testing.T) {
	for _, c := range canonicalCases {
		first, err := Parse(c.in)
		if err != nil {
			continue // reported by TestCanonicalForm
		}
		second, err := Parse(first.String())
		if err != nil {
			t.Errorf("canonical form %q does not parse: %v", first.String(), err)
			continue
		}
		if !reflect.DeepEqual(first, second) {
			t.Errorf("round trip changed the tree for %q", c.in)
		}
	}
}

func TestTreeShape(t *testing.T) {
	stmt, err := Parse("SELECT name FROM users WHERE age > 18 AND city = 'Pilani'")
	if err != nil {
		t.Fatal(err)
	}
	want := &Select{
		Items: []SelectItem{{Expr: &ColumnRef{Column: "name"}}},
		From:  &TableRef{Name: "users"},
		Where: &Binary{
			Op:    "AND",
			Left:  &Binary{Op: ">", Left: &ColumnRef{Column: "age"}, Right: &IntLit{Value: 18}},
			Right: &Binary{Op: "=", Left: &ColumnRef{Column: "city"}, Right: &StringLit{Value: "Pilani"}},
		},
	}
	if !reflect.DeepEqual(stmt, want) {
		t.Fatalf("got %#v", stmt)
	}
}

func TestErrors(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SELECT FROM users", "line 1, column 8: expected expression, got keyword FROM"},
		{"SELECT * FROM", "line 1, column 14: expected table name, got end of input"},
		{"SELECT * FROM t WHERE", "line 1, column 22: expected expression, got end of input"},
		{"SELECT * FROM t\nWHERE a = ;", `line 2, column 11: expected expression, got ";"`},
		{"INSERT INTO t VALUES (1, 2", `line 1, column 27: expected "," or ")", got end of input`},
		{"CREATE TABLE t (id BLOB)", "line 1, column 20: unknown column type BLOB"},
		{"CREATE TABLE t (id INT,)", "line 1, column 24: expected column name, got \")\""},
		{"SELECT 99999999999999999999", "line 1, column 8: integer out of range"},
		{"SELECT 1e999", "line 1, column 8: number out of range"},
		{"SELECT 'abc", "line 1, column 8: unterminated string literal"},
		{`SELECT ""`, "line 1, column 8: zero-length quoted identifier"},
		{"SELECT 1 /* open", "line 1, column 10: unterminated /* comment"},
		{"SELECT #", "line 1, column 8: unexpected character '#'"},
		{"SELECT a FROM t t2 t3", `line 1, column 20: expected ";" or end of input, got identifier t3`},
		{"SELEC * FROM t", "line 1, column 1: expected a statement, got identifier SELEC"},
		{"SELECT * FROM a INNER b", "line 1, column 23: expected JOIN, got identifier b"},
		{"SELECT a IS 5", "line 1, column 13: expected NULL, got number 5"},
		{"SELECT a BETWEEN 1 OR 2", "line 1, column 20: expected AND, got keyword OR"},
		{"UPDATE t SET a == 1", "line 1, column 17: expected expression, got \"=\""},
		{"SELECT 1; SELECT 2", "expected one statement, found 2"},
		{"", "expected one statement, found 0"},
	}
	for _, c := range cases {
		_, err := Parse(c.in)
		if err == nil {
			t.Errorf("Parse(%q) succeeded, want error %q", c.in, c.want)
			continue
		}
		var perr *Error
		if !errors.As(err, &perr) {
			t.Errorf("Parse(%q) returned %T, want *Error", c.in, err)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("Parse(%q)\n got: %v\nwant: %s", c.in, err, c.want)
		}
	}
}

func TestParseAll(t *testing.T) {
	script := `
		-- set up
		CREATE TABLE users (id INT PRIMARY KEY, name TEXT);;
		INSERT INTO users VALUES (1, 'Asha');
		/* query */ SELECT name FROM users
	`
	stmts, err := ParseAll(script)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3", len(stmts))
	}
	for i, want := range []any{&CreateTable{}, &Insert{}, &Select{}} {
		if reflect.TypeOf(stmts[i]) != reflect.TypeOf(want) {
			t.Errorf("statement %d is %T, want %T", i, stmts[i], want)
		}
	}
}

// FuzzParse feeds the parser random input. For any input, the parser must not
// panic, must report failures as *Error, and if it succeeds, its canonical
// output must parse back into an identical tree.
//
// Run with: go test ./internal/parser -fuzz=FuzzParse -fuzztime=60s
func FuzzParse(f *testing.F) {
	for _, c := range canonicalCases {
		f.Add(c.in)
	}
	f.Fuzz(func(t *testing.T, src string) {
		stmts, err := ParseAll(src)
		if err != nil {
			var perr *Error
			if !errors.As(err, &perr) {
				t.Fatalf("error of type %T: %v", err, err)
			}
			return
		}
		for _, s := range stmts {
			text := s.String()
			again, err := Parse(text)
			if err != nil {
				t.Fatalf("canonical form %q (from %q) does not parse: %v", text, src, err)
			}
			if !reflect.DeepEqual(s, again) {
				t.Fatalf("round trip changed the tree:\n input: %q\n canonical: %q\n again: %q", src, text, again.String())
			}
		}
	})
}
