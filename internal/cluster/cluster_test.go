package cluster

import (
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raftdb/internal/engine"
	"raftdb/internal/storage"
)

// testCluster is a cluster of nodes on an in-memory network.
type testCluster struct {
	t     testing.TB
	net   *MemNetwork
	peers []uint64
	dirs  map[uint64]string
	nodes map[uint64]*Node // nil while crashed
	dbs   map[uint64]*engine.DB
}

func newTestCluster(t testing.TB, n int) *testCluster {
	c := &testCluster{t: t, net: NewMemNetwork(), dirs: map[uint64]string{}, nodes: map[uint64]*Node{}, dbs: map[uint64]*engine.DB{}}
	for i := 1; i <= n; i++ {
		c.peers = append(c.peers, uint64(i))
		c.dirs[uint64(i)] = filepath.Join(t.TempDir(), fmt.Sprint(i))
	}
	for _, id := range c.peers {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			if nd != nil {
				nd.Close()
			}
		}
	})
	return c
}

func (c *testCluster) start(id uint64) {
	nd, err := Open(Config{
		ID: id, Peers: c.peers, Dir: c.dirs[id], Transport: c.net.Transport(id),
		TickInterval: 5 * time.Millisecond, RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = nd
	c.dbs[id] = engine.New(nd, 100)
	c.net.Attach(nd)
}

// crash kills a node without flushing its store.
func (c *testCluster) crash(id uint64) {
	c.net.Detach(id)
	c.nodes[id].Crash()
	c.nodes[id] = nil
	c.dbs[id] = nil
}

// leader waits for a leader that a majority follows, other than the given
// nodes, and returns it.
func (c *testCluster) leader(except ...uint64) uint64 {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.peers {
			nd := c.nodes[id]
			if nd == nil || contains(except, id) {
				continue
			}
			st := nd.Status()
			if st.Leader != id {
				continue
			}
			followers := 0
			for _, other := range c.nodes {
				if other != nil {
					if s := other.Status(); s.Leader == id && s.Term == st.Term {
						followers++
					}
				}
			}
			if followers > len(c.peers)/2 {
				return id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return 0
}

func contains(ids []uint64, id uint64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// exec runs SQL on the current leader, retrying while leadership settles.
func (c *testCluster) exec(sql string) *engine.Result {
	c.t.Helper()
	var err error
	for range 50 {
		var res []*engine.Result
		res, err = c.dbs[c.leader()].Exec(sql)
		if err == nil {
			return res[len(res)-1]
		}
		if !retryable(err) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%s: %v", sql, err)
	return nil
}

func retryable(err error) bool {
	var nl *NotLeaderError
	return errors.As(err, &nl) || errors.Is(err, ErrLeaderChanged) || errors.Is(err, ErrTimeout) || errors.Is(err, engine.ErrConflict)
}

func (c *testCluster) query(sql string) string {
	c.t.Helper()
	res := c.exec(sql)
	var rows []string
	for _, r := range res.Rows {
		var cols []string
		for _, v := range r {
			cols = append(cols, fmt.Sprint(v))
		}
		rows = append(rows, strings.Join(cols, ","))
	}
	return strings.Join(rows, ";")
}

// converge waits until every running node has applied the same index, and
// checks that their stores are byte-for-byte identical.
func (c *testCluster) converge() {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var applied []uint64
		for _, nd := range c.nodes {
			if nd != nil {
				applied = append(applied, nd.Status().Applied)
			}
		}
		same := true
		for _, a := range applied {
			same = same && a == applied[0] && a > 0
		}
		if same {
			break
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("nodes did not converge: applied %v", applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var first map[string]string
	var firstID uint64
	for _, id := range c.peers {
		nd := c.nodes[id]
		if nd == nil {
			continue
		}
		d := map[string]string{}
		if err := nd.tree.Scan(nil, nil, func(k, v []byte) bool {
			d[string(k)] = string(v)
			return true
		}); err != nil {
			c.t.Fatal(err)
		}
		if first == nil {
			first, firstID = d, id
		} else if !maps.Equal(first, d) {
			c.t.Fatalf("replicas %d and %d differ (%d vs %d keys)", firstID, id, len(first), len(d))
		}
	}
}

func TestReplicatedSQL(t *testing.T) {
	c := newTestCluster(t, 3)
	c.exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, city TEXT)")
	c.exec("CREATE INDEX users_city ON users (city)")
	c.exec("INSERT INTO users VALUES (1, 'asha', 'pune'), (2, 'ravi', 'delhi'), (3, 'meera', 'pune')")
	c.exec("BEGIN; UPDATE users SET city = 'goa' WHERE id = 2; INSERT INTO users VALUES (4, 'kabir', 'goa'); COMMIT")
	if got := c.query("SELECT name FROM users WHERE city = 'goa' ORDER BY id"); got != "ravi;kabir" {
		t.Fatalf("got %q", got)
	}
	c.converge()

	// A follower refuses to serve reads: it might be behind.
	lead := c.leader()
	for _, id := range c.peers {
		if id != lead {
			_, err := c.dbs[id].Exec("SELECT * FROM users")
			var nl *NotLeaderError
			if !errors.As(err, &nl) || nl.Leader != lead {
				t.Fatalf("follower %d: %v", id, err)
			}
			break
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)
	c.exec("CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)")
	for i := range 20 {
		c.exec(fmt.Sprintf("INSERT INTO kv VALUES (%d, 'v%d')", i, i))
	}
	old := c.leader()
	c.crash(old)
	lead := c.leader(old)
	if lead == old {
		t.Fatal("crashed node still leads")
	}
	if got := c.query("SELECT COUNT(*) FROM kv"); got != "20" {
		t.Fatalf("after failover: %s rows", got)
	}
	for i := 20; i < 40; i++ {
		c.exec(fmt.Sprintf("INSERT INTO kv VALUES (%d, 'v%d')", i, i))
	}
	// The old leader restarts from its log (its store lost unflushed
	// commands) and catches up.
	c.start(old)
	c.converge()
	if got := c.query("SELECT COUNT(*), MIN(k), MAX(k) FROM kv"); got != "40,0,39" {
		t.Fatalf("got %s", got)
	}
}

// TestPartitionedLeader: a leader cut off from the others can neither
// commit writes nor serve reads; the majority elects a new leader and
// carries on, and the old leader's uncommitted write is discarded.
func TestPartitionedLeader(t *testing.T) {
	c := newTestCluster(t, 5)
	c.exec("CREATE TABLE t (id INT PRIMARY KEY)")
	c.exec("INSERT INTO t VALUES (1)")
	old := c.leader()
	c.net.Isolate(old)

	if _, err := c.dbs[old].Exec("INSERT INTO t VALUES (2)"); err == nil {
		t.Fatal("an isolated leader committed a write")
	}
	if _, err := c.dbs[old].Exec("SELECT * FROM t"); err == nil {
		t.Fatal("an isolated leader served a read")
	}
	lead := c.leader(old)
	c.exec("INSERT INTO t VALUES (3)")

	c.net.Heal()
	c.converge()
	if st := c.nodes[old].Status(); st.Leader != lead && st.Leader != c.leader() {
		t.Fatalf("old leader follows %d", st.Leader)
	}
	if got := c.query("SELECT id FROM t ORDER BY id"); got != "1;3" {
		t.Fatalf("got %s", got)
	}
}

// TestBankUnderChaos runs concurrent transfers while nodes crash and
// restart, links are cut and messages are lost. Transfers keep the total
// constant whether or not an ambiguous (timed-out) commit took effect, so
// every replica must end with the same total and identical contents.
func TestBankUnderChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newTestCluster(t, 5)
	const accounts, initial = 10, 100
	c.exec("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)")
	for i := range accounts {
		c.exec(fmt.Sprintf("INSERT INTO accounts VALUES (%d, %d)", i, initial))
	}

	var mu sync.Mutex // guards c's maps against the chaos goroutine
	leaderDB := func() *engine.DB {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range c.peers {
			if nd := c.nodes[id]; nd != nil && nd.Status().Leader == id {
				return c.dbs[id]
			}
		}
		return nil
	}

	var committed, failed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				db := leaderDB()
				if db == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				a, b, amt := rng.Intn(accounts), rng.Intn(accounts), 1+rng.Intn(10)
				s := db.Session()
				_, err := s.Exec(fmt.Sprintf("BEGIN; UPDATE accounts SET balance = balance - %d WHERE id = %d; "+
					"UPDATE accounts SET balance = balance + %d WHERE id = %d; COMMIT", amt, a, amt, b))
				s.Close()
				if err == nil {
					committed.Add(1)
				} else if retryable(err) || errors.Is(err, ErrStopped) || errors.Is(err, storage.ErrClosed) {
					// ErrClosed: the node crashed under the transaction.
					failed.Add(1)
				} else {
					t.Errorf("transfer: %v", err)
					return
				}
			}
		}()
	}

	// Chaos: crash and restart nodes, isolate them, lose messages.
	rng := rand.New(rand.NewSource(42))
	crashes := 0
	for round := range 12 {
		time.Sleep(150 * time.Millisecond)
		mu.Lock()
		victim := c.peers[rng.Intn(len(c.peers))]
		if round%3 == 0 {
			// Prefer the leader: failover is the interesting case.
			for _, id := range c.peers {
				if nd := c.nodes[id]; nd != nil && nd.Status().Leader == id {
					victim = id
				}
			}
		}
		switch rng.Intn(3) {
		case 0:
			if c.nodes[victim] != nil {
				c.crash(victim)
				crashes++
			}
		case 1:
			c.net.Isolate(victim)
		case 2:
			c.net.SetLoss(0.2)
		}
		time.Sleep(150 * time.Millisecond)
		c.net.Heal()
		c.net.SetLoss(0)
		for _, id := range c.peers {
			if c.nodes[id] == nil {
				c.start(id)
			}
		}
		mu.Unlock()
	}
	close(stop)
	wg.Wait()

	c.converge()
	got := c.query("SELECT SUM(balance), COUNT(*) FROM accounts")
	if want := fmt.Sprintf("%d,%d", accounts*initial, accounts); got != want {
		t.Fatalf("total %s, want %s", got, want)
	}
	t.Logf("%d transfers committed, %d failed or retried, %d crashes", committed.Load(), failed.Load(), crashes)
	if committed.Load() < 20 {
		t.Fatalf("only %d transfers committed", committed.Load())
	}
}

// TestGRPC runs a three-node cluster over real gRPC connections on
// localhost.
func TestGRPC(t *testing.T) {
	addrs := map[uint64]string{}
	lis := map[uint64]net.Listener{}
	for id := uint64(1); id <= 3; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[id], addrs[id] = l, l.Addr().String()
	}
	nodes := map[uint64]*Node{}
	for id := uint64(1); id <= 3; id++ {
		tr, err := NewGRPCTransport(id, addrs)
		if err != nil {
			t.Fatal(err)
		}
		nd, err := Open(Config{ID: id, Peers: []uint64{1, 2, 3}, Dir: t.TempDir(), Transport: tr,
			TickInterval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		srv := Serve(lis[id], nd)
		nodes[id] = nd
		t.Cleanup(func() { srv.Stop(); tr.Close(); nd.Close() })
	}

	var lead uint64
	deadline := time.Now().Add(10 * time.Second)
	for lead == 0 && time.Now().Before(deadline) {
		for id, nd := range nodes {
			if nd.Status().Leader == id {
				lead = id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lead == 0 {
		t.Fatal("no leader over gRPC")
	}
	db := engine.New(nodes[lead], 100)
	for _, sql := range []string{
		"CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)",
		"INSERT INTO notes VALUES (1, 'replicated over gRPC'), (2, 'with a hand-written codec')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatal(err)
		}
	}
	res, err := db.Query("SELECT COUNT(*) FROM notes")
	if err != nil || fmt.Sprint(res.Rows[0][0]) != "2" {
		t.Fatalf("%v %v", res, err)
	}
	// Every follower applies the commits.
	want := nodes[lead].Status().Commit
	for id, nd := range nodes {
		for nd.Status().Applied < want {
			if time.Now().After(deadline) {
				t.Fatalf("node %d applied %d of %d", id, nd.Status().Applied, want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
