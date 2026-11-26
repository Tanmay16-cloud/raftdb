package storage

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"strings"
	"testing"
)

func openMem(t *testing.T, fs *memFS, opts Options) *BTree {
	t.Helper()
	opts.fs = fs
	tree, err := OpenWithOptions("db", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return tree
}

func dump(t *testing.T, tree *BTree) map[string]string {
	t.Helper()
	got := map[string]string{}
	if err := tree.Scan(nil, nil, func(k, v []byte) bool {
		got[string(k)] = string(v)
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return got
}

func TestUnsyncedChangesAreLostOnCrash(t *testing.T) {
	fs := newMemFS()
	tree := openMem(t, fs, Options{})
	mustPut(t, tree, "kept", "1")
	if err := tree.Sync(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, tree, "lost", "2")

	tree = openMem(t, fs.crash(rand.New(rand.NewSource(1))), Options{})
	defer tree.Close()
	checkGet(t, tree, "kept", "1", true)
	checkGet(t, tree, "lost", "", false)
}

// With checkpoints effectively disabled, every commit lives only in the log.
// Recovery must rebuild the whole database from it.
func TestRecoveryReplaysLogIntoEmptyDataFile(t *testing.T) {
	fs := newMemFS()
	tree := openMem(t, fs, Options{CheckpointFrames: 1 << 30})
	model := map[string]string{}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key%05d", i)
		mustPut(t, tree, k, k)
		model[k] = k
		if i%100 == 99 {
			if err := tree.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := tree.Stats().Checkpoints; n != 0 {
		t.Fatalf("expected no checkpoints, got %d", n)
	}

	after := fs.crash(rand.New(rand.NewSource(1)))
	if n := len(after.files["db"].data); n != 0 {
		t.Fatalf("data file has %d bytes; expected everything to still be in the log", n)
	}
	tree = openMem(t, after, Options{})
	defer tree.Close()
	if got := dump(t, tree); !maps.Equal(got, model) {
		t.Fatalf("recovered %d keys, want %d", len(got), len(model))
	}
}

// Damage to the last transaction in the log must discard that transaction
// and nothing before it.
func TestDamagedLastCommitIsDiscarded(t *testing.T) {
	damage := map[string]func([]byte) []byte{
		"torn final frame": func(b []byte) []byte { return b[:len(b)-frameSize/2] },
		"flipped bit":      func(b []byte) []byte { b[len(b)-100] ^= 1; return b },
	}
	for name, corrupt := range damage {
		t.Run(name, func(t *testing.T) {
			fs := newMemFS()
			tree := openMem(t, fs, Options{CheckpointFrames: 1 << 30})
			mustPut(t, tree, "first", "1")
			if err := tree.Sync(); err != nil {
				t.Fatal(err)
			}
			mustPut(t, tree, "second", "2")
			if err := tree.Sync(); err != nil {
				t.Fatal(err)
			}

			after := fs.crash(rand.New(rand.NewSource(1))) // all synced, so nothing random
			wal := after.files["db-wal"]
			wal.data = corrupt(wal.data)
			wal.durable = bytes.Clone(wal.data)

			tree = openMem(t, after, Options{})
			defer tree.Close()
			checkGet(t, tree, "first", "1", true)
			checkGet(t, tree, "second", "", false)
		})
	}
}

// TestCrashRecovery is the main durability test. Each trial runs random
// writes and commits against the simulated disk, cuts the power at a random
// moment, and checks that recovery restores exactly the last committed state
// (or the commit that was in progress, if the crash hit during it).
func TestCrashRecovery(t *testing.T) {
	trials := 300
	if testing.Short() {
		trials = 30
	}
	for seed := int64(1); seed <= int64(trials); seed++ {
		runCrashTrial(t, seed)
	}
}

func runCrashTrial(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	// A tiny pool and short log force evictions and checkpoints, so crashes
	// land in every part of the write path.
	opts := Options{PoolPages: 8, CheckpointFrames: 30}

	fs := newMemFS()
	tree := openMem(t, fs, opts)
	committed := map[string]string{} // state as of the last successful Sync
	current := map[string]string{}   // state including uncommitted changes
	var inFlight map[string]string   // state of a Sync the crash interrupted

	fs.crashAfter(rng.Intn(300))
	for step := 0; ; step++ {
		if step > 1_000_000 {
			t.Fatalf("seed %d: the injected crash never happened", seed)
		}
		k := fmt.Sprintf("k%03d", rng.Intn(400))
		var err error
		if rng.Intn(4) == 0 {
			if _, err = tree.Delete([]byte(k)); err == nil {
				delete(current, k)
			}
		} else {
			v := strings.Repeat(string(rune('a'+rng.Intn(26))), rng.Intn(120))
			if err = tree.Put([]byte(k), []byte(v)); err == nil {
				current[k] = v
			}
		}
		if err == nil && rng.Intn(20) == 0 {
			if err = tree.Rollback(); err == nil {
				current = maps.Clone(committed)
			}
		}
		if err == nil && rng.Intn(8) == 0 {
			if err = tree.Sync(); err == nil {
				committed = maps.Clone(current)
			} else {
				inFlight = maps.Clone(current)
			}
		}
		if err != nil {
			if !errors.Is(err, errCrash) {
				t.Fatalf("seed %d, step %d: unexpected error: %v", seed, step, err)
			}
			break
		}
	}
	fs = fs.crash(rng)

	// Sometimes lose power again partway through recovery. Recovery must be
	// safe to interrupt and repeat.
	if rng.Intn(3) == 0 {
		fs.crashAfter(rng.Intn(10))
		o := opts
		o.fs = fs
		if tree, err := OpenWithOptions("db", o); err == nil {
			tree.Close()
		}
		fs = fs.crash(rng)
	}

	tree = openMem(t, fs, opts)
	defer tree.Close()
	got := dump(t, tree)
	if !maps.Equal(got, committed) && (inFlight == nil || !maps.Equal(got, inFlight)) {
		t.Fatalf("seed %d: recovered %d keys, but the last committed state had %d (commit in flight: %v)",
			seed, len(got), len(committed), inFlight != nil)
	}

	// The recovered database must keep working.
	mustPut(t, tree, "after-crash", "ok")
	if err := tree.Sync(); err != nil {
		t.Fatalf("seed %d: sync after recovery: %v", seed, err)
	}
	checkGet(t, tree, "after-crash", "ok", true)
}

func TestBufferPoolEvictsLeastRecentlyUsed(t *testing.T) {
	b := newBufferPool(2)
	b.put(1, nil, false)
	b.put(2, nil, true)
	b.get(1) // page 2 is now the least recently used
	v := b.put(3, nil, false)
	if v == nil || v.id != 2 || !v.dirty {
		t.Fatalf("evicted %+v, want dirty page 2", v)
	}
	if _, ok := b.get(2); ok {
		t.Fatal("page 2 still cached after eviction")
	}
	if d := b.dirtyPages(); len(d) != 0 {
		t.Fatalf("evicted page still listed as dirty: %v", d)
	}
}

// After a rollback, the next transaction overwrites the discarded frames in
// place. If a crash keeps the new commit frame but loses one of the new
// frames before it, the discarded frame underneath must not be replayed as
// part of the commit. The chained checksums are what prevent that.
func TestDiscardedFrameIsNotReplayedAfterRollback(t *testing.T) {
	fs := newMemFS()
	f, _ := fs.OpenFile("wal")
	w, _, err := openWAL(f)
	if err != nil {
		t.Fatal(err)
	}
	page := func(c byte) []byte { return bytes.Repeat([]byte{c}, PageSize) }
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(w.reset())
	must(w.append(1, page('a'), &commitInfo{root: 1, numPages: 2}))
	must(w.sync())

	// A transaction evicts three pages into the log, then rolls back.
	for id := uint32(10); id < 13; id++ {
		must(w.append(id, page('s'), nil))
	}
	w.rollback()

	// The next transaction writes three frames over the same slots, the last
	// one a commit. Power fails before the fsync; the middle write is lost.
	must(w.append(20, page('n'), nil))
	must(w.append(21, page('n'), nil))
	must(w.append(22, page('n'), &commitInfo{root: 22, numPages: 23}))
	mf := f.(*memFile)
	data := bytes.Clone(mf.durable)
	for i, wr := range mf.pending {
		if i != 4 { // writes: 3 discarded frames, then 20, 21 (lost), 22
			data = wr.apply(data)
		}
	}

	after := newMemFS()
	after.files["wal"] = &memFile{fs: after, data: data, durable: bytes.Clone(data)}
	f2, _ := after.OpenFile("wal")
	w2, last, err := openWAL(f2)
	if err != nil {
		t.Fatal(err)
	}
	if last == nil || last.root != 1 {
		t.Fatalf("recovered commit %+v, want the first commit (root 1)", last)
	}
	if _, ok := w2.index[11]; ok || len(w2.index) != 1 {
		t.Fatalf("recovery replayed frames it should have ignored: index = %v", w2.index)
	}
}
