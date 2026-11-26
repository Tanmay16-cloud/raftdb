// Command raftbench measures a raftdb cluster: it starts nodes on
// localhost (real gRPC, real disks), runs concurrent clients through the
// client library, and reports throughput and latency. With -failover it
// kills the leader halfway through and measures how long writes stall.
//
//	go run ./cmd/raftbench -workload insert -clients 16 -duration 10s
//	go run ./cmd/raftbench -workload insert -failover
package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"raftdb/internal/client"
	"raftdb/internal/server"
)

var (
	nodes    = flag.Int("nodes", 3, "cluster size")
	clients  = flag.Int("clients", 16, "concurrent client sessions")
	duration = flag.Duration("duration", 10*time.Second, "how long to run")
	workload = flag.String("workload", "insert", "insert, point (reads by primary key), transfer (read-write transactions), or mixed (90% reads)")
	failover = flag.Bool("failover", false, "kill the leader halfway through")
	tick     = flag.Duration("tick", 10*time.Millisecond, "Raft tick (election timeout is 10-20 ticks)")
	dir      = flag.String("dir", "", "data directory (default: a temporary one, removed afterwards)")
)

const (
	preload  = 10000 // rows in kv for reads
	accounts = 1000
)

func main() {
	flag.Parse()
	if *dir == "" {
		d, err := os.MkdirTemp("", "raftbench")
		check(err)
		defer os.RemoveAll(d)
		*dir = d
	}

	addrs := map[uint64]string{}
	for id := uint64(1); id <= uint64(*nodes); id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		check(err)
		addrs[id] = l.Addr().String()
		l.Close()
	}
	procs := map[uint64]*server.Process{}
	var procsMu sync.Mutex
	for id := range addrs {
		p, err := server.Start(server.NodeConfig{ID: id, Addrs: addrs, Dir: filepath.Join(*dir, fmt.Sprint(id)), TickInterval: *tick})
		check(err)
		procs[id] = p
	}
	defer func() {
		procsMu.Lock()
		defer procsMu.Unlock()
		for _, p := range procs {
			p.Stop()
		}
	}()
	leader := func() uint64 {
		procsMu.Lock()
		defer procsMu.Unlock()
		for id, p := range procs {
			if p.Node().Status().Leader == id {
				return id
			}
		}
		return 0
	}
	for leader() == 0 {
		time.Sleep(5 * time.Millisecond)
	}

	var list []string
	for _, a := range addrs {
		list = append(list, a)
	}
	c, err := client.Dial(list, client.Options{})
	check(err)
	defer c.Close()
	setup(c)

	fmt.Printf("raftdb bench: %d nodes, %d clients, %s, workload %q, tick %s\n", *nodes, *clients, *duration, *workload, *tick)

	var (
		keys       atomic.Int64
		mu         sync.Mutex
		lat        []time.Duration
		completed  []span // successful writes, for failover
		errs       = map[string]int{}
		conflicts  atomic.Int64
		stop       = make(chan struct{})
		wg         sync.WaitGroup
		killedAt   time.Time
		electedAt  time.Time
		startClock = time.Now()
	)
	keys.Store(preload)
	for w := range *clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			s := c.Session()
			defer s.Close()
			var mine []time.Duration
			var done []span
			for {
				select {
				case <-stop:
					mu.Lock()
					lat = append(lat, mine...)
					completed = append(completed, done...)
					mu.Unlock()
					return
				default:
				}
				write, sql := op(rng, &keys)
				start := time.Now()
				_, err := s.Exec(sql)
				end := time.Now()
				if err == nil {
					mine = append(mine, end.Sub(start))
					if write {
						done = append(done, span{start, end})
					}
					continue
				}
				if client.IsConflict(err) {
					conflicts.Add(1)
					continue
				}
				mu.Lock()
				errs[errKind(err)]++
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	if *failover {
		time.Sleep(*duration / 2)
		old := leader()
		procsMu.Lock()
		killedAt = time.Now()
		procs[old].Stop()
		delete(procs, old)
		procsMu.Unlock()
		for {
			if l := leader(); l != 0 && l != old {
				electedAt = time.Now()
				break
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(time.Until(startClock.Add(*duration)))
	} else {
		time.Sleep(*duration)
	}
	close(stop)
	wg.Wait()
	elapsed := time.Since(startClock)

	slices.Sort(lat)
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(p*float64(len(lat))))]
	}
	fmt.Printf("  %d ops in %.1fs: %.0f ops/s\n", len(lat), elapsed.Seconds(), float64(len(lat))/elapsed.Seconds())
	fmt.Printf("  latency p50 %s, p90 %s, p99 %s, max %s\n", round(pct(0.5)), round(pct(0.9)), round(pct(0.99)), round(pct(1)))
	if n := conflicts.Load(); n > 0 {
		fmt.Printf("  %d serialization conflicts (rolled back, not counted)\n", n)
	}
	if len(errs) > 0 {
		var parts []string
		for k, v := range errs {
			parts = append(parts, fmt.Sprintf("%s %d", k, v))
		}
		sort.Strings(parts)
		fmt.Printf("  errors: %s\n", strings.Join(parts, ", "))
	}
	if *failover {
		// The write stall: from the kill to the end of the first
		// successful write that began after it.
		resumed := time.Duration(-1)
		for _, sp := range completed {
			if !sp.start.Before(killedAt) {
				if d := sp.end.Sub(killedAt); resumed < 0 || d < resumed {
					resumed = d
				}
			}
		}
		fmt.Printf("  failover: new leader after %s", round(electedAt.Sub(killedAt)))
		if resumed >= 0 {
			fmt.Printf("; the first write begun after the kill succeeded after %s\n", round(resumed))
		} else {
			fmt.Println("; writes did not resume")
		}
	}
	if *workload == "transfer" {
		res, err := c.Session().Query("SELECT SUM(balance) FROM accounts")
		check(err)
		fmt.Printf("  total balance %v (want %d)\n", res.Rows[0][0], accounts*100)
	}
}

type span struct{ start, end time.Time }

func setup(c *client.Client) {
	s := c.Session()
	defer s.Close()
	_, err := s.Exec("CREATE TABLE kv (id INT PRIMARY KEY, v TEXT); CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)")
	check(err)
	for i := 0; i < preload; i += 500 {
		var vals []string
		for j := i; j < i+500; j++ {
			vals = append(vals, fmt.Sprintf("(%d, 'value-%d')", j, j))
		}
		_, err := s.Exec("INSERT INTO kv VALUES " + strings.Join(vals, ", "))
		check(err)
	}
	var vals []string
	for i := range accounts {
		vals = append(vals, fmt.Sprintf("(%d, 100)", i))
	}
	_, err = s.Exec("INSERT INTO accounts VALUES " + strings.Join(vals, ", "))
	check(err)
}

// op returns the next operation's SQL and whether it writes.
func op(rng *rand.Rand, keys *atomic.Int64) (bool, string) {
	w := *workload
	if w == "mixed" {
		if rng.Intn(10) == 0 {
			w = "insert"
		} else {
			w = "point"
		}
	}
	switch w {
	case "insert":
		k := keys.Add(1)
		return true, fmt.Sprintf("INSERT INTO kv VALUES (%d, 'value-%d')", k, k)
	case "point":
		return false, fmt.Sprintf("SELECT v FROM kv WHERE id = %d", rng.Intn(preload))
	case "transfer":
		a, b := rng.Intn(accounts), rng.Intn(accounts)
		return true, fmt.Sprintf("BEGIN; UPDATE accounts SET balance = balance - 1 WHERE id = %d; "+
			"UPDATE accounts SET balance = balance + 1 WHERE id = %d; COMMIT", a, b)
	}
	fmt.Fprintln(os.Stderr, "unknown workload", *workload)
	os.Exit(2)
	return false, ""
}

func errKind(err error) string {
	var e *client.Error
	switch {
	case errors.Is(err, client.ErrConnectionLost):
		return "connection lost (outcome unknown)"
	case errors.As(err, &e):
		return fmt.Sprintf("code %d", e.Code)
	}
	return "unreachable"
}

func round(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		return d.Round(10 * time.Millisecond)
	case d > time.Millisecond:
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "raftbench:", err)
		os.Exit(1)
	}
}
