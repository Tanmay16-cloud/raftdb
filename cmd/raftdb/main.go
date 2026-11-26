// Command raftdb is raftdb's SQL shell and server.
//
//	raftdb [-db file]                          shell on a local, unreplicated database
//	raftdb serve -id N -cluster 1=host:port,...  run node N of a cluster
//	raftdb connect -cluster host:port,...        shell on a cluster
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"raftdb/internal/engine"
	"raftdb/internal/parser"
)

const help = `Enter SQL statements ending with ';' (they may span several lines).
BEGIN; ... COMMIT; groups statements into a transaction, and VACUUM; removes
old row versions that no transaction can see any more.
  \d             list tables
  \d <table>     show a table's schema and indexes
  \parse <sql>   show how the parser reads a statement, without running it
  \timing        toggle printing how long each statement takes
  \stats         show storage counters (local) or the node's Raft status (cluster)
  \?             show this message
  \q             quit`

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			serve(os.Args[2:])
			return
		case "connect":
			connect(os.Args[2:])
			return
		}
	}
	path := flag.String("db", "raftdb.db", "path to the database file")
	flag.Parse()

	db, err := engine.Open(*path)
	if err != nil {
		fatal("open:", err)
	}
	defer db.Close()
	runShell(localDB{db}, *path)
}

func fatal(args ...any) {
	fmt.Fprintln(os.Stderr, args...)
	os.Exit(1)
}

func runShell(db database, name string) {
	// Prompts and the banner only make sense when a person is typing.
	interactive := false
	if fi, err := os.Stdin.Stat(); err == nil {
		interactive = fi.Mode()&os.ModeCharDevice != 0
	}
	s := &shell{db: db, out: os.Stdout, interactive: interactive}
	if interactive {
		fmt.Printf("raftdb (%s). Type \\? for help.\n", name)
	}
	s.run(os.Stdin)
}

// database is what the shell runs against: a local database or a session
// on a cluster.
type database interface {
	Exec(sql string) ([]*engine.Result, error)
	InTransaction() bool
	Tables() ([]*engine.Table, error)
	Stats() (string, error)
}

type localDB struct{ db *engine.DB }

func (l localDB) Exec(sql string) ([]*engine.Result, error) { return l.db.Exec(sql) }
func (l localDB) InTransaction() bool                       { return l.db.InTransaction() }
func (l localDB) Tables() ([]*engine.Table, error)          { return l.db.Tables(), nil }
func (l localDB) Stats() (string, error) {
	st := l.db.Stats()
	return fmt.Sprintf("cache hits %d, misses %d, evictions %d, commits %d, rollbacks %d, checkpoints %d",
		st.CacheHits, st.CacheMisses, st.Evictions, st.Commits, st.Rollbacks, st.Checkpoints), nil
}

type shell struct {
	db          database
	out         io.Writer
	interactive bool
	timing      bool
}

func (s *shell) run(r io.Reader) {
	in := bufio.NewScanner(r)
	in.Buffer(make([]byte, 64*1024), 1<<20)
	var buf strings.Builder
	for {
		if s.interactive {
			s.prompt(buf.Len() > 0)
		}
		if !in.Scan() {
			if strings.TrimSpace(buf.String()) != "" {
				s.execSQL(buf.String()) // a final statement without ';'
			}
			return
		}
		// Windows shells can prefix piped input with a UTF-8 byte-order mark.
		line := strings.TrimPrefix(in.Text(), "\xef\xbb\xbf")
		if buf.Len() == 0 && strings.HasPrefix(strings.TrimSpace(line), `\`) {
			if !s.meta(strings.TrimSpace(line)) {
				return
			}
			continue
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
		if statementComplete(buf.String()) {
			s.execSQL(buf.String())
			buf.Reset()
		}
	}
}

func (s *shell) prompt(continuation bool) {
	switch {
	case continuation:
		fmt.Fprint(s.out, "raftdb-> ")
	case s.db.InTransaction():
		fmt.Fprint(s.out, "raftdb*> ")
	default:
		fmt.Fprint(s.out, "raftdb=> ")
	}
}

func (s *shell) execSQL(sql string) {
	start := time.Now()
	results, err := s.db.Exec(sql)
	elapsed := time.Since(start)
	for _, r := range results {
		printResult(s.out, r)
	}
	if err != nil {
		fmt.Fprintln(s.out, "ERROR:", err)
	}
	if s.timing {
		fmt.Fprintf(s.out, "Time: %.3f ms\n", float64(elapsed.Microseconds())/1000)
	}
}

// meta runs a backslash command. It returns false to quit.
func (s *shell) meta(line string) bool {
	cmd, arg, _ := strings.Cut(line, " ")
	arg = strings.TrimSpace(arg)
	switch cmd {
	case `\q`:
		return false
	case `\?`, `\h`, `\help`:
		fmt.Fprintln(s.out, help)
	case `\d`:
		tables, err := s.db.Tables()
		if err != nil {
			fmt.Fprintln(s.out, "ERROR:", err)
			break
		}
		if arg == "" {
			res := &engine.Result{Columns: []string{"table", "columns", "indexes"}}
			for _, t := range tables {
				res.Rows = append(res.Rows, engine.Row{t.Name, int64(len(t.Columns)), int64(len(t.Indexes))})
			}
			printTable(s.out, res)
			break
		}
		var found *engine.Table
		for _, name := range []string{strings.ToLower(arg), arg} {
			for _, t := range tables {
				if t.Name == name && found == nil {
					found = t
				}
			}
		}
		if found != nil {
			fmt.Fprintln(s.out, found)
		} else {
			fmt.Fprintf(s.out, "ERROR: table %q does not exist\n", arg)
		}
	case `\parse`:
		stmts, err := parser.ParseAll(arg)
		if err != nil {
			fmt.Fprintln(s.out, "ERROR:", err)
			break
		}
		for _, st := range stmts {
			fmt.Fprintf(s.out, "%T\n  %s\n", st, st)
		}
	case `\timing`:
		s.timing = !s.timing
		fmt.Fprintf(s.out, "Timing is %s.\n", map[bool]string{true: "on", false: "off"}[s.timing])
	case `\stats`:
		if st, err := s.db.Stats(); err != nil {
			fmt.Fprintln(s.out, "ERROR:", err)
		} else {
			fmt.Fprintln(s.out, st)
		}
	default:
		fmt.Fprintf(s.out, "unknown command %s; type \\? for help\n", cmd)
	}
	return true
}

// statementComplete reports whether sql ends with a semicolon that is not
// inside a string, a quoted identifier or a comment, ignoring trailing
// whitespace and comments.
func statementComplete(sql string) bool {
	complete := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case c == '\'' || c == '"':
			// Skip to the closing quote; a doubled quote is an escaped one.
			j := i + 1
			for ; j < len(sql); j++ {
				if sql[j] == c {
					if j+1 < len(sql) && sql[j+1] == c {
						j++
						continue
					}
					break
				}
			}
			if j >= len(sql) {
				return false // unterminated: keep reading lines
			}
			i, complete = j, false
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return false
			}
			i += end + 3
		case c == ';':
			complete = true
		case c != ' ' && c != '\t' && c != '\n' && c != '\r':
			complete = false
		}
	}
	return complete
}

func printResult(w io.Writer, r *engine.Result) {
	if r.Columns == nil {
		fmt.Fprintln(w, r.Message)
		return
	}
	printTable(w, r)
	if len(r.Columns) == 1 && r.Columns[0] == "QUERY PLAN" {
		return
	}
	if len(r.Rows) == 1 {
		fmt.Fprintln(w, "(1 row)")
	} else {
		fmt.Fprintf(w, "(%d rows)\n", len(r.Rows))
	}
}

// printTable prints rows in aligned columns, numbers right-aligned, in the
// style of psql.
func printTable(w io.Writer, r *engine.Result) {
	cells := make([][]string, len(r.Rows))
	widths := make([]int, len(r.Columns))
	numeric := make([]bool, len(r.Columns))
	for i, c := range r.Columns {
		widths[i] = width(c)
		numeric[i] = true
	}
	for i, row := range r.Rows {
		cells[i] = make([]string, len(row))
		for j, v := range row {
			cells[i][j] = engine.FormatValue(v)
			widths[j] = max(widths[j], width(cells[i][j]))
			switch v.(type) {
			case int64, float64, nil:
			default:
				numeric[j] = false
			}
		}
	}
	var b strings.Builder
	line := func(vals []string, header bool) {
		for j, v := range vals {
			if j > 0 {
				b.WriteString(" | ")
			}
			gap := widths[j] - width(v)
			left := 0 // spaces before the value; the rest go after it
			switch {
			case header:
				left = gap / 2 // column names are centred
			case numeric[j]:
				left = gap
			}
			if j == len(vals)-1 {
				gap = left // no trailing spaces at the end of a line
			}
			b.WriteString(strings.Repeat(" ", left) + v + strings.Repeat(" ", gap-left))
		}
		b.WriteString("\n")
	}
	line(r.Columns, true)
	for j := range r.Columns {
		if j > 0 {
			b.WriteString("-+-")
		}
		b.WriteString(strings.Repeat("-", widths[j]))
	}
	b.WriteString("\n")
	for _, row := range cells {
		line(row, false)
	}
	io.WriteString(w, b.String())
}

func width(s string) int { return len([]rune(s)) }
