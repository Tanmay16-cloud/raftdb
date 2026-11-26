package server_test

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"raftdb/internal/client"
	"raftdb/internal/engine"
	"raftdb/internal/server"
)

func TestResponseEncoding(t *testing.T) {
	resp := &server.Response{
		Results: []*engine.Result{
			{Columns: []string{"a", "b"}, Rows: []engine.Row{
				{nil, true}, {int64(-42), 3.5}, {"text", false}, {int64(1 << 62), ""},
			}},
			{Message: "INSERT 3"},
		},
		Code: server.CodeNotLeader, Err: "boom", InTx: true, Leader: 3, LeaderAddr: "127.0.0.1:7003",
		Tables: []*engine.Table{{ID: 1, Name: "t", PK: 0, Columns: []engine.Column{{Name: "id"}}}},
		Status: &server.NodeStatus{ID: 2, Role: "follower", Term: 9, Commit: 100, Applied: 99},
	}
	b, err := resp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got server.Response
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&got, resp) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, *resp)
	}
	for n := range len(b) {
		var r server.Response
		if r.UnmarshalBinary(b[:n]) == nil {
			t.Fatalf("truncated response (%d of %d bytes) decoded", n, len(b))
		}
	}
}

// testCluster runs nodes as real gRPC servers on localhost.
type testCluster struct {
	t     *testing.T
	addrs map[uint64]string
	dirs  map[uint64]string
	procs map[uint64]*server.Process
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startCluster(t *testing.T, n int) *testCluster {
	c := &testCluster{t: t, addrs: map[uint64]string{}, dirs: map[uint64]string{}, procs: map[uint64]*server.Process{}}
	for id := uint64(1); id <= uint64(n); id++ {
		c.addrs[id] = freeAddr(t)
		c.dirs[id] = filepath.Join(t.TempDir(), fmt.Sprint(id))
	}
	for id := range c.addrs {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, p := range c.procs {
			p.Stop()
		}
	})
	return c
}

func (c *testCluster) start(id uint64) {
	p, err := server.Start(server.NodeConfig{ID: id, Addrs: c.addrs, Dir: c.dirs[id], TickInterval: 5 * time.Millisecond})
	if err != nil {
		c.t.Fatal(err)
	}
	c.procs[id] = p
}

func (c *testCluster) stop(id uint64) {
	c.procs[id].Stop()
	delete(c.procs, id)
}

func (c *testCluster) leader() uint64 {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, p := range c.procs {
			if p.Node().Status().Leader == id {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

// addrList lists followers first, so the client must be redirected.
func (c *testCluster) addrList() []string {
	lead := c.leader()
	var out []string
	for id, a := range c.addrs {
		if id != lead {
			out = append(out, a)
		}
	}
	return append(out, c.addrs[lead])
}

// must returns v, or panics (failing the test) on an error.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func cell(t *testing.T, s *client.Session, sql string) string {
	t.Helper()
	res := must(s.Query(sql))
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("%s: %v", sql, res.Rows)
	}
	return fmt.Sprint(res.Rows[0][0])
}

func TestClientRoutesToLeader(t *testing.T) {
	c := startCluster(t, 3)
	cl := must(client.Dial(c.addrList(), client.Options{}))
	defer cl.Close()
	s := cl.Session()
	defer s.Close()
	must(s.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, score FLOAT, ok BOOL)"))
	must(s.Exec("INSERT INTO users VALUES (1, 'asha', 9.5, TRUE), (2, NULL, -1.25, FALSE)"))
	res := must(s.Query("SELECT id, name, score, ok FROM users ORDER BY id"))
	want := []engine.Row{{int64(1), "asha", 9.5, true}, {int64(2), nil, -1.25, false}}
	if !reflect.DeepEqual(res.Rows, want) || !reflect.DeepEqual(res.Columns, []string{"id", "name", "score", "ok"}) {
		t.Fatalf("got %v %v", res.Columns, res.Rows)
	}
	tables := must(s.Tables())
	if len(tables) != 1 || tables[0].Name != "users" || len(tables[0].Columns) != 4 {
		t.Fatalf("tables: %+v", tables)
	}
	// SQL errors come back as errors, with the code and message.
	_, err := s.Exec("SELEC 1")
	var e *client.Error
	if !errors.As(err, &e) || e.Code != server.CodeError {
		t.Fatalf("syntax error: %v", err)
	}
}

func TestSessionsAndTransactions(t *testing.T) {
	c := startCluster(t, 3)
	cl := must(client.Dial(c.addrList(), client.Options{}))
	defer cl.Close()
	a, b := cl.Session(), cl.Session()
	defer a.Close()
	defer b.Close()
	must(a.Exec("CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)"))
	must(a.Exec("BEGIN"))
	must(a.Exec("INSERT INTO kv VALUES (1, 'one')"))
	if !a.InTransaction() {
		t.Fatal("session lost its transaction between requests")
	}
	if got := cell(t, b, "SELECT COUNT(*) FROM kv"); got != "0" {
		t.Fatalf("uncommitted row visible: %s", got)
	}
	must(a.Exec("COMMIT"))
	if got := cell(t, b, "SELECT COUNT(*) FROM kv"); got != "1" {
		t.Fatalf("committed row not visible: %s", got)
	}

	// Two sessions update the same row: the second commit conflicts.
	must(a.Exec("BEGIN; UPDATE kv SET v = 'a' WHERE k = 1"))
	must(b.Exec("BEGIN; UPDATE kv SET v = 'b' WHERE k = 1"))
	must(a.Exec("COMMIT"))
	if _, err := b.Exec("COMMIT"); !client.IsConflict(err) {
		t.Fatalf("second commit: %v", err)
	}
	// Closing a session rolls back its open transaction on the server.
	a.Exec("BEGIN; INSERT INTO kv VALUES (2, 'two')")
	a.Close()
	if got := cell(t, b, "SELECT COUNT(*) FROM kv"); got != "1" {
		t.Fatalf("abandoned transaction committed: %s", got)
	}
}

func TestClientFailover(t *testing.T) {
	c := startCluster(t, 3)
	cl := must(client.Dial(c.addrList(), client.Options{}))
	defer cl.Close()
	s := cl.Session()
	defer s.Close()
	must(s.Exec("CREATE TABLE t (id INT PRIMARY KEY)"))
	for i := range 10 {
		must(s.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i)))
	}

	// A transaction open on the leader dies with it.
	tx := cl.Session()
	must(tx.Exec("BEGIN; INSERT INTO t VALUES (100)"))

	old := c.leader()
	c.stop(old)
	killed := time.Now()

	// The first write may be ambiguous (the connection broke mid-request);
	// after that the client finds the new leader by itself.
	succeeded := 10
	var firstOK time.Duration
	for i := 10; i < 30; i++ {
		_, err := s.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
		switch {
		case err == nil:
			succeeded++
			if firstOK == 0 {
				firstOK = time.Since(killed)
			}
		case errors.Is(err, client.ErrConnectionLost):
		default:
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if succeeded < 29 {
		t.Fatalf("only %d inserts succeeded", succeeded)
	}
	n := cell(t, s, "SELECT COUNT(*) FROM t WHERE id < 100")
	if n != fmt.Sprint(succeeded) && n != fmt.Sprint(succeeded+1) {
		t.Fatalf("%s rows, but %d inserts succeeded", n, succeeded)
	}
	t.Logf("writes resumed %v after the leader was killed", firstOK.Round(time.Millisecond))

	if _, err := tx.Exec("COMMIT"); !errors.Is(err, client.ErrConnectionLost) || tx.InTransaction() {
		t.Fatalf("commit of a transaction on the dead leader: %v", err)
	}
	if got := cell(t, s, "SELECT COUNT(*) FROM t WHERE id = 100"); got != "0" {
		t.Fatal("transaction from the dead leader was committed")
	}

	// The old leader comes back and catches up.
	c.start(old)
	deadline := time.Now().Add(10 * time.Second)
	want := c.procs[c.leader()].Node().Status().Commit
	for c.procs[old].Node().Status().Applied < want {
		if time.Now().After(deadline) {
			t.Fatal("restarted node did not catch up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
