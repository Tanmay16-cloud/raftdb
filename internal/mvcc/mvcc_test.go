package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"raftdb/internal/storage"
)

func openStore(t *testing.T, path string, opts storage.Options) (*Store, *storage.BTree) {
	t.Helper()
	tree, err := storage.OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(tree)
	if err != nil {
		t.Fatal(err)
	}
	return s, tree
}

func newStore(t *testing.T) *Store {
	t.Helper()
	s, tree := openStore(t, filepath.Join(t.TempDir(), "m.db"), storage.Options{})
	t.Cleanup(func() { tree.Close() })
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func begin(t *testing.T, s *Store) *Txn {
	t.Helper()
	tx, err := s.Begin()
	must(t, err)
	return tx
}

func get(t *testing.T, tx *Txn, k string) string {
	t.Helper()
	v, ok, err := tx.Get([]byte(k))
	must(t, err)
	if !ok {
		return "<none>"
	}
	return string(v)
}

func put(t *testing.T, tx *Txn, k, v string) {
	t.Helper()
	must(t, tx.Put([]byte(k), []byte(v)))
}

func scanAll(t *testing.T, tx *Txn, start, end []byte) map[string]string {
	t.Helper()
	got := map[string]string{}
	it := tx.Scan(start, end)
	var prev []byte
	for {
		k, v, ok, err := it.Next()
		must(t, err)
		if !ok {
			return got
		}
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("scan out of order: %q then %q", prev, k)
		}
		prev = bytes.Clone(k)
		got[string(k)] = string(v)
	}
}

func TestSnapshotVisibility(t *testing.T) {
	s := newStore(t)
	t0 := begin(t, s)
	put(t, t0, "a", "0")
	must(t, t0.Commit())

	t1 := begin(t, s)
	put(t, t1, "a", "1")
	put(t, t1, "b", "1")
	if got := get(t, t1, "a"); got != "1" {
		t.Fatalf("own write: %s", got)
	}

	t2 := begin(t, s) // t1 is still active: none of its writes are visible
	ro := s.BeginReadOnly()
	for _, tx := range []*Txn{t2, ro} {
		if got := get(t, tx, "a"); got != "0" {
			t.Fatalf("uncommitted write visible (dirty read): a = %s", got)
		}
	}
	must(t, t1.Commit())
	for _, tx := range []*Txn{t2, ro} {
		if got := get(t, tx, "a"); got != "0" {
			t.Fatalf("commit after snapshot visible (non-repeatable read): a = %s", got)
		}
		if got := get(t, tx, "b"); got != "<none>" {
			t.Fatalf("insert after snapshot visible (phantom): b = %s", got)
		}
	}
	t3 := begin(t, s)
	if got := get(t, t3, "a") + get(t, t3, "b"); got != "11" {
		t.Fatalf("committed writes not visible to a later transaction: %s", got)
	}
	// A read-only transaction takes the next version without consuming it;
	// the read-write transaction that does get it must stay invisible.
	ro2 := s.BeginReadOnly()
	t4 := begin(t, s)
	if t4.Version() != ro2.Version() {
		t.Fatalf("versions: read-only %d, next read-write %d", ro2.Version(), t4.Version())
	}
	put(t, t4, "a", "4")
	must(t, t4.Commit())
	if got := get(t, ro2, "a"); got != "1" {
		t.Fatalf("read-only snapshot saw a later transaction: %s", got)
	}
	if err := ro2.Put([]byte("x"), nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("write in read-only transaction: %v", err)
	}
}

func TestWriteConflicts(t *testing.T) {
	s := newStore(t)
	t1, t2 := begin(t, s), begin(t, s)
	put(t, t1, "k", "1")
	if err := t2.Put([]byte("k"), []byte("2")); !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent write to an uncommitted key: %v", err)
	}
	must(t, t1.Commit())
	if err := t2.Put([]byte("k"), []byte("2")); !errors.Is(err, ErrConflict) {
		t.Fatalf("write to a key committed after the snapshot (lost update): %v", err)
	}
	if _, err := t2.Delete([]byte("k")); err != nil {
		t.Fatalf("delete of a key t2 cannot see should be a no-op: %v", err)
	}
	put(t, t2, "other", "x") // other keys are fine
	must(t, t2.Commit())

	// A rolled-back writer leaves nothing to conflict with.
	t3, t4 := begin(t, s), begin(t, s)
	put(t, t3, "k", "3")
	must(t, t3.Rollback())
	put(t, t4, "k", "4")
	must(t, t4.Commit())
	if got := get(t, s.BeginReadOnly(), "k"); got != "4" {
		t.Fatalf("k = %s", got)
	}
}

// Snapshot isolation allows write skew: two transactions each check a
// condition over both keys and then write a different key, so they never
// conflict, and together they break the condition.
func TestWriteSkewIsAllowed(t *testing.T) {
	s := newStore(t)
	t0 := begin(t, s)
	put(t, t0, "alice", "on call")
	put(t, t0, "bob", "on call")
	must(t, t0.Commit())

	t1, t2 := begin(t, s), begin(t, s)
	if get(t, t1, "bob") == "on call" {
		put(t, t1, "alice", "off")
	}
	if get(t, t2, "alice") == "on call" {
		put(t, t2, "bob", "off")
	}
	must(t, t1.Commit())
	must(t, t2.Commit())
	ro := s.BeginReadOnly()
	if get(t, ro, "alice") != "off" || get(t, ro, "bob") != "off" {
		t.Fatal("expected the write-skew anomaly")
	}
}

func TestDoneTransactions(t *testing.T) {
	s := newStore(t)
	tx := begin(t, s)
	must(t, tx.Commit())
	if err := tx.Commit(); !errors.Is(err, ErrDone) {
		t.Fatal(err)
	}
	if _, _, err := tx.Get([]byte("a")); !errors.Is(err, ErrDone) {
		t.Fatal(err)
	}
	if err := tx.Put([]byte("a"), nil); !errors.Is(err, ErrDone) {
		t.Fatal(err)
	}
}

func TestSequencesAreNotTransactional(t *testing.T) {
	s := newStore(t)
	for want := uint64(1); want <= 3; want++ {
		n, err := s.Sequence([]byte("ids"), 1)
		must(t, err)
		if n != want {
			t.Fatalf("sequence = %d, want %d", n, want)
		}
	}
	n, err := s.Sequence([]byte("other"), 1)
	must(t, err)
	if n != 1 {
		t.Fatalf("second sequence = %d", n)
	}
}

func TestKeyEncoding(t *testing.T) {
	keys := [][]byte{{}, {0}, {0, 0}, {0, 1}, {0, 0xFF}, {1}, []byte("a"), {'a', 0}, {'a', 0, 'b'}, []byte("ab"), {0xFF}}
	for i, a := range keys {
		got, v, ok := splitVersionKey(versionKey(a, uint64(i)))
		if !ok || !bytes.Equal(got, a) || v != uint64(i) {
			t.Fatalf("round trip of %x: %x, %d, %v", a, got, v, ok)
		}
		for _, b := range keys {
			pa, pb := versionPrefix(a), versionPrefix(b)
			if c := bytes.Compare(pa, pb); c != bytes.Compare(a, b) {
				t.Fatalf("order of %x and %x not preserved", a, b)
			}
			if !bytes.Equal(a, b) && bytes.HasPrefix(pa, pb) {
				t.Fatalf("encoding of %x is a prefix of that of %x", b, a)
			}
		}
	}
}

// A scan whose keys have many versions crosses many batches, and one key's
// versions may span a batch boundary.
func TestScanAcrossBatches(t *testing.T) {
	s := newStore(t)
	want := map[string]string{}
	for round := 0; round < 5; round++ {
		tx := begin(t, s)
		for i := 0; i < 300; i++ {
			k := fmt.Sprintf("k%03d", i)
			switch {
			case i%7 == round:
				_, err := tx.Delete([]byte(k))
				must(t, err)
				delete(want, k)
			default:
				v := fmt.Sprintf("%d.%d", round, i)
				put(t, tx, k, v)
				want[k] = v
			}
		}
		must(t, tx.Commit())
	}
	if got := scanAll(t, s.BeginReadOnly(), nil, nil); !maps.Equal(got, want) {
		t.Fatalf("scan returned %d keys, want %d", len(got), len(want))
	}
	got := scanAll(t, s.BeginReadOnly(), []byte("k100"), []byte("k200"))
	for k := range want {
		if _, ok := got[k]; ok != (k >= "k100" && k < "k200") {
			t.Fatalf("bounded scan: key %s present=%v", k, ok)
		}
	}
}

func TestVacuum(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 5; i++ {
		tx := begin(t, s)
		put(t, tx, "a", fmt.Sprint(i))
		put(t, tx, "gone", "x")
		must(t, tx.Commit())
	}
	old := s.BeginReadOnly() // pins the current versions
	tx := begin(t, s)
	put(t, tx, "a", "new")
	_, err := tx.Delete([]byte("gone"))
	must(t, err)
	must(t, tx.Commit())

	n, err := s.Vacuum()
	must(t, err)
	if n != 8 { // the 4 older versions of each key; old still needs version 5 of both
		t.Fatalf("vacuum removed %d versions, want 8", n)
	}
	if get(t, old, "a") != "4" || get(t, old, "gone") != "x" {
		t.Fatal("vacuum removed a version an open snapshot needs")
	}
	must(t, old.Commit())
	n, err = s.Vacuum()
	must(t, err)
	if n != 3 { // a's version 5, and both versions of the deleted key
		t.Fatalf("second vacuum removed %d versions, want 3", n)
	}
	ro := s.BeginReadOnly()
	if get(t, ro, "a") != "new" || get(t, ro, "gone") != "<none>" {
		t.Fatal("vacuum changed visible data")
	}
	versions, _, err := ro.versions([]byte("a"))
	must(t, err)
	if len(versions) != 1 {
		t.Fatalf("a has %d versions left", len(versions))
	}
}

// TestModel runs random interleavings of transactions against a reference
// model of snapshot isolation that knows nothing about versions: each
// transaction reads the committed state as of when it began, overlaid with
// its own writes, and a write conflicts exactly when another transaction
// that is still open, or that committed after this one began, wrote the
// same key. The store must agree on every read, scan and conflict. The run
// includes vacuums and simulated crashes, with a tiny buffer pool.
func TestModel(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runModel(t, seed) })
	}
}

type modelTxn struct {
	tx        *Txn
	beginSeq  int                // commits that had happened when it began
	writes    map[string]*string // nil value: deleted
	committed bool
}

type modelCommit struct {
	keys  map[string]bool
	state map[string]string // committed state after this commit
}

func runModel(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	path := filepath.Join(t.TempDir(), "m.db")
	opts := storage.Options{PoolPages: 4, CheckpointFrames: 30}
	s, tree := openStore(t, path, opts)
	defer func() { tree.Close() }()

	keyspace := []string{"", "a", "a\x00", "a\x00b", "ab", "b", "\x00", "zz"}
	commits := []modelCommit{{state: map[string]string{}}}
	var open []*modelTxn

	view := func(m *modelTxn) map[string]string {
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
	conflicts := func(m *modelTxn, k string) bool {
		for _, o := range open {
			if o != m && o.tx != nil && !o.tx.ReadOnly() {
				if _, ok := o.writes[k]; ok {
					return true
				}
			}
		}
		for _, c := range commits[m.beginSeq+1:] {
			if c.keys[k] {
				return true
			}
		}
		return false
	}

	for step := 0; step < 3000; step++ {
		if len(open) == 0 || rng.Intn(8) == 0 {
			m := &modelTxn{beginSeq: len(commits) - 1, writes: map[string]*string{}}
			if rng.Intn(4) == 0 {
				m.tx = s.BeginReadOnly()
			} else {
				m.tx = begin(t, s)
			}
			open = append(open, m)
			continue
		}
		i := rng.Intn(len(open))
		m := open[i]
		k := keyspace[rng.Intn(len(keyspace))]
		finish := func() { open = slices.Delete(open, i, i+1) }

		switch r := rng.Intn(20); {
		case r < 6:
			want, ok := view(m)[k]
			got, gotOK, err := m.tx.Get([]byte(k))
			must(t, err)
			if gotOK != ok || string(got) != want {
				t.Fatalf("step %d: txn %d get %q = %q,%v; model %q,%v", step, m.tx.Version(), k, got, gotOK, want, ok)
			}
		case r < 11 && !m.tx.ReadOnly():
			v := fmt.Sprintf("%d", step)
			err := m.tx.Put([]byte(k), []byte(v))
			if want := conflicts(m, k); want != errors.Is(err, ErrConflict) {
				t.Fatalf("step %d: put %q: err %v, model conflict=%v", step, k, err, want)
			}
			if err == nil {
				m.writes[k] = &v
			}
		case r < 13 && !m.tx.ReadOnly():
			_, present := view(m)[k]
			existed, err := m.tx.Delete([]byte(k))
			wantConflict := present && conflicts(m, k)
			if wantConflict != errors.Is(err, ErrConflict) || (err == nil && existed != present) {
				t.Fatalf("step %d: delete %q: existed %v err %v; model present %v conflict %v", step, k, existed, err, present, wantConflict)
			}
			if err == nil && present {
				m.writes[k] = nil
			}
		case r < 15:
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
			if got := scanAll(t, m.tx, []byte(lo), []byte(hi)); !maps.Equal(got, want) {
				t.Fatalf("step %d: scan [%q, %q) = %v, model %v", step, lo, hi, got, want)
			}
		case r < 17:
			must(t, m.tx.Commit())
			if len(m.writes) > 0 {
				// The writes apply on top of the latest committed state, which
				// may include commits made since this transaction began.
				c := modelCommit{keys: map[string]bool{}, state: maps.Clone(commits[len(commits)-1].state)}
				for kk, val := range m.writes {
					c.keys[kk] = true
					if val == nil {
						delete(c.state, kk)
					} else {
						c.state[kk] = *val
					}
				}
				commits = append(commits, c)
			}
			finish()
		case r < 19:
			must(t, m.tx.Rollback())
			finish()
		case rng.Intn(3) == 0:
			// Crash. Sometimes the power fails, losing everything since the
			// last fsync; sometimes the process dies after the storage
			// engine flushed every transaction's uncommitted versions.
			// Either way, reopening must leave exactly the committed state.
			if rng.Intn(2) == 0 {
				must(t, tree.Rollback())
			}
			must(t, tree.Close())
			s, tree = openStore(t, path, opts)
			open = nil
			want := commits[len(commits)-1].state
			if got := scanAll(t, s.BeginReadOnly(), nil, nil); !maps.Equal(got, want) {
				t.Fatalf("step %d: after crash: %v, want %v", step, got, want)
			}
			for _, kk := range keyspace {
				vs, _, err := s.BeginReadOnly().versions([]byte(kk))
				must(t, err)
				for _, v := range vs {
					if v >= s.next {
						t.Fatalf("version %d survived recovery but next is %d", v, s.next)
					}
				}
			}
		default:
			_, err := s.Vacuum()
			must(t, err)
		}
	}
	// No write records or active markers may outlive their transactions.
	for _, m := range open {
		must(t, m.tx.Rollback())
	}
	var leftovers []string
	must(t, tree.Scan([]byte("a"), []byte("b"), func(k, _ []byte) bool {
		leftovers = append(leftovers, fmt.Sprintf("%q", k))
		return true
	}))
	must(t, tree.Scan([]byte("w"), []byte("x"), func(k, _ []byte) bool {
		leftovers = append(leftovers, fmt.Sprintf("%q", k))
		return true
	}))
	if len(leftovers) > 0 {
		t.Fatalf("bookkeeping keys left behind: %s", strings.Join(leftovers, ", "))
	}
}
