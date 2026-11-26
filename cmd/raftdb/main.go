// Command raftdb is, for now, an interactive shell over the storage engine
// and the SQL parser. It will become the database server once the executor
// and Raft layers exist.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"raftdb/internal/parser"
	"raftdb/internal/storage"
)

const help = `commands:
  put <key> <value>     store a value (the value may contain spaces)
  get <key>             print a value
  del <key>             delete a key
  scan [start] [end]    list keys in [start, end)
  parse <sql>           show how the SQL parser reads a statement
  help                  show this message
  exit                  quit`

func main() {
	path := flag.String("db", "raftdb.db", "path to the database file")
	flag.Parse()

	tree, err := storage.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer tree.Close()

	fmt.Printf("raftdb storage shell (%s)\n%s\n", *path, help)
	in := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !in.Scan() {
			return
		}
		// Windows shells can prefix piped input with a UTF-8 byte-order mark.
		line := strings.TrimSpace(strings.TrimPrefix(in.Text(), "\xef\xbb\xbf"))
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if err := run(tree, line, fields); err != nil {
			if err == errExit {
				return
			}
			fmt.Println("error:", err)
		}
	}
}

var errExit = fmt.Errorf("exit")

func run(tree *storage.BTree, line string, f []string) error {
	switch cmd := strings.ToLower(f[0]); {
	case cmd == "exit" || cmd == "quit":
		return errExit
	case cmd == "help":
		fmt.Println(help)
	case cmd == "parse" && len(f) >= 2:
		// Use the raw line so spacing inside string literals is kept.
		stmts, err := parser.ParseAll(strings.TrimSpace(line[len(f[0]):]))
		if err != nil {
			return err
		}
		for _, s := range stmts {
			fmt.Printf("%T\n  %s\n", s, s)
		}
	case cmd == "put" && len(f) >= 3:
		if err := tree.Put([]byte(f[1]), []byte(strings.Join(f[2:], " "))); err != nil {
			return err
		}
		return tree.Sync()
	case cmd == "get" && len(f) == 2:
		v, ok, err := tree.Get([]byte(f[1]))
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("(not found)")
		} else {
			fmt.Println(string(v))
		}
	case cmd == "del" && len(f) == 2:
		ok, err := tree.Delete([]byte(f[1]))
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("(not found)")
		}
		return tree.Sync()
	case cmd == "scan" && len(f) <= 3:
		var start, end []byte
		if len(f) > 1 {
			start = []byte(f[1])
		}
		if len(f) > 2 {
			end = []byte(f[2])
		}
		n := 0
		err := tree.Scan(start, end, func(k, v []byte) bool {
			fmt.Printf("%s = %s\n", k, v)
			n++
			return true
		})
		fmt.Printf("(%d keys)\n", n)
		return err
	default:
		return fmt.Errorf("unrecognised command; type help")
	}
	return nil
}
