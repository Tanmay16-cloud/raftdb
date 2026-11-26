package main

import (
	"path/filepath"
	"strings"
	"testing"

	"raftdb/internal/engine"
)

func TestStatementComplete(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT 1;", true},
		{"SELECT 1;  \n", true},
		{"SELECT 1", false},
		{"SELECT ';'", false},
		{"SELECT ';';", true},
		{"SELECT 'it''s;'", false},
		{"SELECT 'it''s';", true},
		{`SELECT "a;b" FROM t`, false},
		{"SELECT 'unterminated;", false},
		{"SELECT 1; -- done", true},
		{"SELECT 1 -- not yet;", false},
		{"SELECT 1 /* ; */", false},
		{"SELECT 1; /* trailing */", true},
		{"SELECT 1 /* open ;", false},
		{"BEGIN; SELECT 1", false},
		{"BEGIN; SELECT 1;", true},
	}
	for _, c := range cases {
		if got := statementComplete(c.sql); got != c.want {
			t.Errorf("statementComplete(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestShellSession(t *testing.T) {
	db, err := engine.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var out strings.Builder
	sh := &shell{db: db, out: &out}
	sh.run(strings.NewReader(strings.Join([]string{
		"\xef\xbb\xbfCREATE TABLE users (id INT PRIMARY KEY, name TEXT,",
		"  score FLOAT);",
		"INSERT INTO users VALUES (1, 'Asha', 9.5), (22, 'Rohan', NULL);",
		"SELECT id, name, score FROM users ORDER BY id;",
		"SELECT nope FROM users;",
		`\d`,
		`\d users`,
		"SELECT count(*) FROM users", // no semicolon at end of input
	}, "\n")))

	want := strings.Join([]string{
		"CREATE TABLE",
		"INSERT 2",
		"id | name  | score",
		"---+-------+------",
		" 1 | Asha  |   9.5",
		"22 | Rohan |  NULL",
		"(2 rows)",
		`ERROR: column "nope" does not exist`,
		"table | columns | indexes",
		"------+---------+--------",
		"users |       3 |       0",
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score FLOAT);",
		"count",
		"-----",
		"    2",
		"(1 row)",
		"",
	}, "\n")
	if out.String() != want {
		t.Fatalf("session output:\n%s\nwant:\n%s", out.String(), want)
	}
}
