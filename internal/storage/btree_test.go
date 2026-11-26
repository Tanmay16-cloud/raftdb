package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func openTemp(t *testing.T) (*BTree, string) {
	t.Helper()
	return openTempOpts(t, Options{})
}

func openTempOpts(t *testing.T, opts Options) (*BTree, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	tree, err := OpenWithOptions(path, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { tree.Close() })
	return tree, path
}

func reopen(t *testing.T, tree *BTree, path string) *BTree {
	t.Helper()
	if err := tree.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	nt, err := OpenWithOptions(path, tree.pager.opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { nt.Close() })
	return nt
}

func mustPut(t *testing.T, tree *BTree, k, v string) {
	t.Helper()
	if err := tree.Put([]byte(k), []byte(v)); err != nil {
		t.Fatalf("put %q: %v", k, err)
	}
}

func checkGet(t *testing.T, tree *BTree, k, want string, wantFound bool) {
	t.Helper()
	got, found, err := tree.Get([]byte(k))
	if err != nil {
		t.Fatalf("get %q: %v", k, err)
	}
	if found != wantFound || string(got) != want {
		t.Fatalf("get %q = (%q, %v), want (%q, %v)", k, got, found, want, wantFound)
	}
}

// checkMatches verifies that a full scan of the tree returns exactly the
// contents of model, in sorted order.
func checkMatches(t *testing.T, tree *BTree, model map[string]string) {
	t.Helper()
	var got []string
	err := tree.Scan(nil, nil, func(k, v []byte) bool {
		if want, ok := model[string(k)]; !ok || want != string(v) {
			t.Errorf("scan yielded %q=%q, model has %q (present=%v)", k, v, want, ok)
		}
		got = append(got, string(k))
		return true
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := make([]string, 0, len(model))
	for k := range model {
		want = append(want, k)
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("scan returned %d keys, want %d (or wrong order)", len(got), len(want))
	}
}

func TestPutGetOverwrite(t *testing.T) {
	tree, _ := openTemp(t)
	mustPut(t, tree, "apple", "1")
	mustPut(t, tree, "banana", "2")
	checkGet(t, tree, "apple", "1", true)
	checkGet(t, tree, "banana", "2", true)
	checkGet(t, tree, "cherry", "", false)

	mustPut(t, tree, "apple", "a much longer replacement value")
	checkGet(t, tree, "apple", "a much longer replacement value", true)

	mustPut(t, tree, "empty", "")
	checkGet(t, tree, "empty", "", true)
}

func TestLimits(t *testing.T) {
	tree, _ := openTemp(t)
	maxKey := bytes.Repeat([]byte("k"), MaxKeySize)
	maxVal := bytes.Repeat([]byte("v"), MaxValueSize)

	cases := []struct {
		name string
		key  []byte
		val  []byte
		want error
	}{
		{"empty key", nil, []byte("x"), ErrEmptyKey},
		{"key too large", append(maxKey, 'k'), nil, ErrKeyTooLarge},
		{"value too large", []byte("k"), append(maxVal, 'v'), ErrValueTooLarge},
		{"exactly max sizes", maxKey, maxVal, nil},
	}
	for _, c := range cases {
		if err := tree.Put(c.key, c.val); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestManyKeysRandomOrder(t *testing.T) {
	tree, _ := openTemp(t)
	rng := rand.New(rand.NewSource(1))
	model := map[string]string{}
	for _, i := range rng.Perm(20000) {
		k := fmt.Sprintf("key-%08d", i)
		v := string(bytes.Repeat([]byte{byte('a' + i%26)}, rng.Intn(200)))
		mustPut(t, tree, k, v)
		model[k] = v
	}
	for k, v := range model {
		checkGet(t, tree, k, v, true)
	}
	checkMatches(t, tree, model)

	h, err := tree.height()
	if err != nil {
		t.Fatal(err)
	}
	if h < 3 {
		t.Fatalf("height = %d; expected the tree to have split into at least 3 levels", h)
	}
}

// Maximum-size entries leave room for only a few per leaf and ~15 per internal
// node, which stresses both kinds of split.
func TestMaxSizeEntries(t *testing.T) {
	tree, path := openTemp(t)
	rng := rand.New(rand.NewSource(2))
	model := map[string]string{}
	for _, i := range rng.Perm(2000) {
		k := fmt.Sprintf("%08d", i) + string(bytes.Repeat([]byte("k"), MaxKeySize-8))
		v := string(bytes.Repeat([]byte{byte('a' + i%26)}, MaxValueSize))
		mustPut(t, tree, k, v)
		model[k] = v
	}
	checkMatches(t, tree, model)
	tree = reopen(t, tree, path)
	checkMatches(t, tree, model)
}

func TestScanRange(t *testing.T) {
	tree, _ := openTemp(t)
	for i := 0; i < 1000; i++ {
		mustPut(t, tree, fmt.Sprintf("%03d", i), fmt.Sprint(i))
	}

	collect := func(start, end []byte, limit int) []string {
		var keys []string
		if err := tree.Scan(start, end, func(k, _ []byte) bool {
			keys = append(keys, string(k))
			return len(keys) < limit
		}); err != nil {
			t.Fatal(err)
		}
		return keys
	}

	got := collect([]byte("100"), []byte("200"), 1<<30)
	if len(got) != 100 || got[0] != "100" || got[99] != "199" {
		t.Fatalf("scan [100,200) gave %d keys from %v to %v", len(got), got[0], got[len(got)-1])
	}
	if got := collect([]byte("5"), nil, 3); !slices.Equal(got, []string{"500", "501", "502"}) {
		t.Fatalf("scan from absent key 5 with limit 3 = %v", got)
	}
	if got := collect([]byte("999z"), nil, 1<<30); len(got) != 0 {
		t.Fatalf("scan past the last key = %v", got)
	}
}

func TestDelete(t *testing.T) {
	tree, _ := openTemp(t)
	model := map[string]string{}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("k%05d", i)
		mustPut(t, tree, k, k)
		model[k] = k
	}
	for i := 0; i < 5000; i += 2 {
		k := fmt.Sprintf("k%05d", i)
		if ok, err := tree.Delete([]byte(k)); err != nil || !ok {
			t.Fatalf("delete %q = %v, %v", k, ok, err)
		}
		delete(model, k)
	}
	checkGet(t, tree, "k00000", "", false)
	checkGet(t, tree, "k00001", "k00001", true)
	checkMatches(t, tree, model)

	if ok, err := tree.Delete([]byte("k00000")); err != nil || ok {
		t.Fatalf("deleting an absent key = %v, %v", ok, err)
	}

	// Emptying the tree leaves empty leaves behind; scans must skip them.
	for k := range model {
		if _, err := tree.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	checkMatches(t, tree, map[string]string{})
	mustPut(t, tree, "k00042", "back")
	checkMatches(t, tree, map[string]string{"k00042": "back"})
}

func TestPersistence(t *testing.T) {
	tree, path := openTemp(t)
	model := map[string]string{}
	for i := 0; i < 10000; i++ {
		k, v := fmt.Sprintf("user:%d", i), fmt.Sprintf("balance=%d", i*7)
		mustPut(t, tree, k, v)
		model[k] = v
	}
	tree = reopen(t, tree, path)
	checkMatches(t, tree, model)
	checkGet(t, tree, "user:4242", "balance=29694", true)
}

// TestRandomOpsMatchMap runs a long random mix of puts, deletes, commits and
// reopens, checking the tree against a plain map after every batch. The
// tiny-pool configuration keeps only 4 pages in memory, so nearly every
// operation evicts pages and reads them back from the log or data file.
func TestRandomOpsMatchMap(t *testing.T) {
	configs := map[string]Options{
		"defaults":  {},
		"tiny pool": {PoolPages: 4, CheckpointFrames: 20},
	}
	for name, opts := range configs {
		t.Run(name, func(t *testing.T) { runRandomOps(t, opts) })
	}
}

func runRandomOps(t *testing.T, opts Options) {
	tree, path := openTempOpts(t, opts)
	rng := rand.New(rand.NewSource(3))
	model := map[string]string{}
	for op := 1; op <= 30000; op++ {
		k := fmt.Sprintf("key%d", rng.Intn(3000))
		switch r := rng.Intn(10); {
		case r < 6:
			v := string(bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, rng.Intn(300)))
			mustPut(t, tree, k, v)
			model[k] = v
		case r < 9:
			ok, err := tree.Delete([]byte(k))
			if err != nil {
				t.Fatal(err)
			}
			if _, want := model[k]; ok != want {
				t.Fatalf("op %d: delete %q returned %v, want %v", op, k, ok, want)
			}
			delete(model, k)
		default:
			want, wantFound := model[k]
			checkGet(t, tree, k, want, wantFound)
		}
		if op%500 == 0 {
			if err := tree.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if op%5000 == 0 {
			checkMatches(t, tree, model)
			if opts.PoolPages > 0 {
				if s := tree.Stats(); s.Evictions == 0 || s.Checkpoints == 0 {
					t.Fatalf("tiny pool should evict and checkpoint; stats = %+v", s)
				}
			}
			tree = reopen(t, tree, path)
		}
	}
	checkMatches(t, tree, model)
}

func TestClosedTree(t *testing.T) {
	tree, _ := openTemp(t)
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Fatalf("put after close: %v", err)
	}
	if err := tree.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestRejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junk.db")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0xAB}, PageSize*2), 0o644); err != nil {
		t.Fatal(err)
	}
	if tree, err := Open(path); !errors.Is(err, ErrCorrupt) {
		if tree != nil {
			tree.Close()
		}
		t.Fatalf("open corrupt file: err = %v, want ErrCorrupt", err)
	}
}

func BenchmarkPut(b *testing.B) {
	tree, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()
	rng := rand.New(rand.NewSource(1))
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tree.Put([]byte(fmt.Sprintf("key-%010d", rng.Int63())), val); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutSync commits after every Put, so each operation pays for an
// fsync. Compare it with BenchmarkPut to see why databases batch commits.
func BenchmarkPutSync(b *testing.B) {
	tree, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tree.Put([]byte(fmt.Sprintf("key-%010d", i)), val); err != nil {
			b.Fatal(err)
		}
		if err := tree.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	tree, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer tree.Close()
	const n = 100000
	val := bytes.Repeat([]byte("v"), 100)
	for i := 0; i < n; i++ {
		if err := tree.Put([]byte(fmt.Sprintf("key-%010d", i)), val); err != nil {
			b.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := tree.Get([]byte(fmt.Sprintf("key-%010d", rng.Intn(n)))); err != nil {
			b.Fatal(err)
		}
	}
}
