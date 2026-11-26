package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"raftdb/internal/storage"
)

func newTx(t *testing.T, s *Store) *Tx {
	t.Helper()
	snap, _, err := s.Snapshot()
	must(t, err)
	return NewTx(snap, s)
}

func txGet(t *testing.T, tx *Tx, k string) string {
	t.Helper()
	v, ok, err := tx.Get([]byte(k))
	must(t, err)
	if !ok {
		return "<none>"
	}
	return string(v)
}

func TestOptimisticFirstCommitterWins(t *testing.T) {
	s := newStore(t)
	a, b := newTx(t, s), newTx(t, s)
	must(t, a.Put([]byte("k"), []byte("a")))
	must(t, b.Put([]byte("k"), []byte("b"))) // no conflict yet: nothing is committed
	if txGet(t, b, "k") != "b" || txGet(t, a, "k") != "a" {
		t.Fatal("each transaction must read its own buffered write")
	}
	must(t, a.Commit())
	if err := b.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("second committer: %v", err)
	}
	if got := txGet(t, newTx(t, s), "k"); got != "a" {
		t.Fatalf("k = %s", got)
	}

	// Disjoint keys commit; a read-only transaction always commits.
	c, d, ro := newTx(t, s), newTx(t, s), newTx(t, s)
	must(t, c.Put([]byte("x"), []byte("c")))
	must(t, d.Put([]byte("y"), []byte("d")))
	txGet(t, ro, "x")
	must(t, c.Commit())
	must(t, d.Commit())
	must(t, ro.Commit())
}

func TestOptimisticScanMergesBufferedWrites(t *testing.T) {
	s := newStore(t)
	seed := newTx(t, s)
	for _, k := range []string{"a", "c", "e", "g"} {
		must(t, seed.Put([]byte(k), []byte("old")))
	}
	must(t, seed.Commit())

	tx := newTx(t, s)
	must(t, tx.Put([]byte("b"), []byte("new"))) // between existing keys
	must(t, tx.Put([]byte("c"), []byte("new"))) // overwrite
	_, err := tx.Delete([]byte("e"))            // delete
	must(t, err)
	must(t, tx.Put([]byte("h"), []byte("new")))  // after the last key
	must(t, tx.Put([]byte("zz"), []byte("new"))) // outside the range
	it := tx.Scan([]byte("a"), []byte("z"))
	var got []string
	for {
		k, v, ok, err := it.Next()
		must(t, err)
		if !ok {
			break
		}
		got = append(got, string(k)+"="+string(v))
	}
	want := []string{"a=old", "b=new", "c=new", "g=old", "h=new"}
	if !slices.Equal(got, want) {
		t.Fatalf("scan = %v, want %v", got, want)
	}
}

func TestSchemaRule(t *testing.T) {
	s := newStore(t)
	// A schema change commits first: a concurrent writer must fail.
	ddl, dml := newTx(t, s), newTx(t, s)
	ddl.MarkSchema()
	must(t, ddl.Put([]byte("catalog"), []byte("v1")))
	must(t, dml.Put([]byte("row"), []byte("1")))
	must(t, ddl.Commit())
	if err := dml.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("write concurrent with a committed schema change: %v", err)
	}
	// A writer commits first: a concurrent schema change must fail.
	ddl, dml = newTx(t, s), newTx(t, s)
	ddl.MarkSchema()
	must(t, ddl.Put([]byte("catalog"), []byte("v2")))
	must(t, dml.Put([]byte("row"), []byte("2")))
	must(t, dml.Commit())
	if err := ddl.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("schema change concurrent with a committed write: %v", err)
	}
}

func TestSnapshotTooOld(t *testing.T) {
	s := newStore(t)
	old := newTx(t, s)
	w := newTx(t, s)
	must(t, w.Put([]byte("k"), []byte("1")))
	must(t, w.Commit())
	// A replica's vacuum horizon comes from the leader, which may not know
	// about this snapshot; the commit must then be refused.
	_, err := s.Execute(&Command{Op: OpVacuum, Horizon: old.Snapshot() + 5})
	must(t, err)
	must(t, old.Put([]byte("other"), []byte("x")))
	if err := old.Commit(); !errors.Is(err, ErrSnapshotTooOld) {
		t.Fatalf("commit from a snapshot older than the vacuum horizon: %v", err)
	}
}

func TestApplyIsIdempotentByIndex(t *testing.T) {
	s := newStore(t)
	c := &Command{Op: OpSequence, Name: []byte("ids"), N: 10}
	r1, err := s.Apply(1, c)
	must(t, err)
	r2, err := s.Apply(1, c) // replayed after a restart: ignored
	must(t, err)
	r3, err := s.Apply(2, c)
	must(t, err)
	if r1.First != 1 || r2.First != 0 || r3.First != 11 || s.Applied() != 2 {
		t.Fatalf("results %d %d %d, applied %d", r1.First, r2.First, r3.First, s.Applied())
	}
}

func TestCommandEncoding(t *testing.T) {
	cmds := []*Command{
		{Op: OpNoop},
		{Op: OpCommit, Snapshot: 1 << 40, Schema: true, Writes: []Write{
			{Key: []byte("a"), Value: []byte("1")}, {Key: []byte{0, 1}, Delete: true}, {Key: []byte("e"), Value: []byte{}},
		}},
		{Op: OpSequence, Name: []byte("next-id"), N: 100},
		{Op: OpVacuum, Horizon: 77},
	}
	for _, c := range cmds {
		got, err := DecodeCommand(c.Encode())
		must(t, err)
		// Normalise nil and empty slices, which encode alike.
		norm := func(c *Command) *Command {
			c2 := *c
			c2.Name = bytes.Clone(append([]byte{}, c.Name...))
			c2.Writes = nil
			for _, w := range c.Writes {
				c2.Writes = append(c2.Writes, Write{Key: append([]byte{}, w.Key...), Value: append([]byte{}, w.Value...), Delete: w.Delete})
			}
			return &c2
		}
		if !reflect.DeepEqual(norm(got), norm(c)) {
			t.Fatalf("round trip of %+v gave %+v", c, got)
		}
		enc := c.Encode()
		for n := 0; n < len(enc); n++ {
			if _, err := DecodeCommand(enc[:n]); err == nil {
				t.Fatalf("truncated command (%d of %d bytes) decoded without error", n, len(enc))
			}
		}
	}
}

// TestOptimisticModel checks optimistic transactions against a reference
// model of snapshot isolation with first-committer-wins: a transaction
// reads the committed state as of its start overlaid with its own writes,
// and its commit fails exactly when a transaction that committed after it
// started changed one of the keys it writes. The run includes vacuums and
// crashes, with a tiny buffer pool.
func TestOptimisticModel(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runOptimisticModel(t, seed) })
	}
}

type modelTx struct {
	tx       *Tx
	beginSeq int
	writes   map[string]*string
}

func runOptimisticModel(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	path := filepath.Join(t.TempDir(), "o.db")
	opts := storage.Options{PoolPages: 4, CheckpointFrames: 30}
	s, tree := openStore(t, path, opts)
	defer func() { tree.Close() }()

	keyspace := []string{"", "a", "a\x00", "a\x00b", "ab", "b", "\x00", "zz"}
	commits := []modelCommit{{state: map[string]string{}}}
	var open []*modelTx

	view := func(m *modelTx) map[string]string {
		v := maps.Clone(commits[m.beginSeq].state)
		for k, val := range m.writes {
			if val == nil {
				delete(v, k)
			} else {
				v[k] = *val
			}
		}
		return v
	}

	var committed, conflicts int
	defer func() { t.Logf("%d commits, %d conflicts", committed, conflicts) }()
	for step := 0; step < 3000; step++ {
		if len(open) == 0 || rng.Intn(6) == 0 {
			open = append(open, &modelTx{tx: newTx(t, s), beginSeq: len(commits) - 1, writes: map[string]*string{}})
			continue
		}
		i := rng.Intn(len(open))
		m := open[i]
		k := keyspace[rng.Intn(len(keyspace))]
		finish := func() { open = slices.Delete(open, i, i+1) }

		switch r := rng.Intn(20); {
		case r < 5:
			want, ok := view(m)[k]
			got, gotOK, err := m.tx.Get([]byte(k))
			must(t, err)
			if gotOK != ok || string(got) != want {
				t.Fatalf("step %d: get %q = %q,%v; model %q,%v", step, k, got, gotOK, want, ok)
			}
		case r < 10:
			v := fmt.Sprint(step)
			must(t, m.tx.Put([]byte(k), []byte(v)))
			m.writes[k] = &v
		case r < 12:
			_, present := view(m)[k]
			existed, err := m.tx.Delete([]byte(k))
			must(t, err)
			if existed != present {
				t.Fatalf("step %d: delete %q existed=%v, model %v", step, k, existed, present)
			}
			if present {
				m.writes[k] = nil
			}
		case r < 14:
			lo, hi := keyspace[rng.Intn(len(keyspace))], keyspace[rng.Intn(len(keyspace))]
			if lo > hi {
				lo, hi = hi, lo
			}
			want := map[string]string{}
			for kk, vv := range view(m) {
				if kk >= lo && kk < hi {
					want[kk] = vv
				}
			}
			got := map[string]string{}
			it := m.tx.Scan([]byte(lo), []byte(hi))
			var prev []byte
			for {
				kk, vv, ok, err := it.Next()
				must(t, err)
				if !ok {
					break
				}
				if prev != nil && bytes.Compare(prev, kk) >= 0 {
					t.Fatalf("step %d: scan out of order", step)
				}
				prev = bytes.Clone(kk)
				got[string(kk)] = string(vv)
			}
			if !maps.Equal(got, want) {
				t.Fatalf("step %d: scan [%q,%q) = %v, model %v", step, lo, hi, got, want)
			}
		case r < 17:
			// Which keys would this commit really change, applied to the
			// latest state? Deleting a key that is already gone changes
			// nothing, and so conflicts with nothing later.
			latest := commits[len(commits)-1].state
			conflict := false
			for kk := range m.writes {
				for _, c := range commits[m.beginSeq+1:] {
					if c.keys[kk] {
						conflict = true
					}
				}
			}
			err := m.tx.Commit()
			if conflict != errors.Is(err, ErrConflict) || (err != nil && !errors.Is(err, ErrConflict)) {
				t.Fatalf("step %d: commit err %v, model conflict %v", step, err, conflict)
			}
			if conflict {
				conflicts++
			} else {
				committed++
			}
			if !conflict {
				c := modelCommit{keys: map[string]bool{}, state: maps.Clone(latest)}
				for kk, val := range m.writes {
					_, present := latest[kk]
					if val == nil {
						if present {
							c.keys[kk] = true
						}
						delete(c.state, kk)
					} else {
						c.keys[kk] = true
						c.state[kk] = *val
					}
				}
				if len(c.keys) > 0 {
					commits = append(commits, c)
				}
			}
			finish()
		case r < 19:
			must(t, m.tx.Rollback())
			finish()
		case rng.Intn(3) == 0:
			// Crash: open transactions vanish; committed state survives.
			if rng.Intn(2) == 0 {
				must(t, tree.Rollback())
			}
			must(t, tree.Close())
			s, tree = openStore(t, path, opts)
			open = nil
			got := map[string]string{}
			it := newTx(t, s).Scan(nil, nil)
			for {
				kk, vv, ok, err := it.Next()
				must(t, err)
				if !ok {
					break
				}
				got[string(kk)] = string(vv)
			}
			if want := commits[len(commits)-1].state; !maps.Equal(got, want) {
				t.Fatalf("step %d: after crash %v, want %v", step, got, want)
			}
		default:
			_, err := s.Execute(&Command{Op: OpVacuum, Horizon: s.Horizon()})
			must(t, err)
		}
	}
}

// recorder executes commands on a store as entries of a replicated log,
// and keeps the log.
type recorder struct {
	s   *Store
	log []*Command
}

func (r *recorder) Execute(c *Command) (Result, error) {
	r.log = append(r.log, c)
	res, err := r.s.Apply(uint64(len(r.log)), c)
	if err == nil {
		err = r.s.Flush()
	}
	return res, err
}

func dump(t *testing.T, tree *storage.BTree) map[string]string {
	t.Helper()
	m := map[string]string{}
	must(t, tree.Scan(nil, nil, func(k, v []byte) bool {
		m[string(k)] = string(v)
		return true
	}))
	return m
}

// TestReplicasConverge is the property replication relies on: a replica
// that applies the same commands ends up byte-for-byte identical, even when
// it crashes and replays commands after its last flush.
func TestReplicasConverge(t *testing.T) {
	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			opts := storage.Options{PoolPages: 4, CheckpointFrames: 20}
			a, treeA := openStore(t, filepath.Join(t.TempDir(), "a.db"), opts)
			defer treeA.Close()
			pathB := filepath.Join(t.TempDir(), "b.db")
			b, treeB := openStore(t, pathB, opts)
			defer func() { treeB.Close() }()
			rec := &recorder{s: a}

			keys := []string{"a", "b", "c", "d", "e"}
			var open []*Tx
			for step := 0; step < 1500; step++ {
				switch r := rng.Intn(10); {
				case r < 2 || len(open) == 0:
					snap, _, err := a.Snapshot()
					must(t, err)
					open = append(open, NewTx(snap, rec))
				case r < 6:
					tx := open[rng.Intn(len(open))]
					k := []byte(keys[rng.Intn(len(keys))])
					if rng.Intn(3) == 0 {
						_, err := tx.Delete(k)
						must(t, err)
					} else {
						must(t, tx.Put(k, []byte(fmt.Sprint(step))))
					}
				case r < 8:
					i := rng.Intn(len(open))
					if rng.Intn(4) == 0 {
						open[i].MarkSchema()
					}
					if err := open[i].Commit(); err != nil && !errors.Is(err, ErrConflict) {
						t.Fatal(err)
					}
					open = slices.Delete(open, i, i+1)
				case r == 8:
					_, err := rec.Execute(&Command{Op: OpSequence, Name: []byte("q"), N: uint64(1 + rng.Intn(5))})
					must(t, err)
					if rng.Intn(4) == 0 {
						_, err = rec.Execute(&Command{Op: OpVacuum, Horizon: a.Horizon()})
						must(t, err)
					}
				default:
					// The replica catches up, flushing now and then and
					// sometimes crashing, losing what it hadn't flushed.
					for i := b.Applied() + 1; i <= uint64(len(rec.log)); i++ {
						if _, err := b.Apply(i, rec.log[i-1]); err != nil {
							t.Fatal(err)
						}
						if rng.Intn(5) == 0 {
							must(t, b.Flush())
						}
					}
					if rng.Intn(3) == 0 {
						must(t, treeB.Rollback())
						must(t, treeB.Close())
						b, treeB = openStore(t, pathB, opts)
					}
				}
			}
			for _, tx := range open {
				tx.Rollback()
			}
			for i := b.Applied() + 1; i <= uint64(len(rec.log)); i++ {
				_, err := b.Apply(i, rec.log[i-1])
				must(t, err)
			}
			if da, db := dump(t, treeA), dump(t, treeB); !maps.Equal(da, db) {
				t.Fatalf("replica diverged: %d keys vs %d", len(da), len(db))
			}
			t.Logf("%d commands", len(rec.log))
		})
	}
}
