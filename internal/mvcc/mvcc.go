// Package mvcc adds transactions with snapshot isolation on top of the
// storage engine, using multi-version concurrency control.
//
// Every write creates a new version of a key instead of overwriting it. A
// transaction reads from a snapshot: the newest version of each key written
// by a transaction that committed before it began, plus its own writes.
// Readers therefore never block writers and writers never block readers,
// and a transaction sees one consistent state of the database however long
// it runs.
//
// # Versions and visibility
//
// Each read-write transaction gets a version number when it begins, from a
// counter that only increases, and stamps every version it writes with it.
// It also records the set of transactions that were active at that moment.
// A version written by transaction v is visible to transaction t when
//
//	v == t.version                          (t's own write), or
//	v <  t.version and v was not active when t began (committed before t).
//
// Versions of aborted transactions are deleted when they roll back, so they
// are never seen. Committing a transaction changes no data at all: it only
// removes the transaction from the active set, so later snapshots include it.
//
// # Conflicts
//
// Two concurrent transactions may not both write the same key. Before
// writing, a transaction looks at the key's newest version; if that version
// is not visible to it, another transaction either committed the key after
// this one began or is still writing it, and the write fails with
// ErrConflict ("first writer wins"). This prevents lost updates. As with any
// snapshot isolation, write skew is still possible: two transactions that
// read overlapping data and write disjoint keys can both commit.
//
// # Layout in the storage engine
//
//	'v' + escaped key + version (8 bytes)   -> 1 + value, or 0 for a deletion
//	'a' + version                           -> marks a transaction that has written
//	'n'                                     -> the next version number
//	's' + name                              -> a sequence (see Store.Sequence)
//
// Keys are escaped so that no escaped key is a prefix of another; all the
// versions of one key are then adjacent, ordered oldest first, and the
// versions of keys in [start, end) form one contiguous range.
//
// # Durability and recovery
//
// A transaction touches storage only when it first writes: it then records
// the version counter and an active marker, before its first version. Only
// Commit fsyncs, and only if the transaction wrote something.
//
// Each transaction remembers the keys it wrote, in memory, so Rollback can
// delete its versions. But other transactions' commits flush the whole
// tree, uncommitted versions included, so a crash can leave versions of
// unfinished transactions on disk. Open therefore deletes every version
// whose transaction is still marked active. That costs a scan of all the
// versions after a crash, in exchange for writing nothing but the version
// itself during normal operation.
package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"raftdb/internal/storage"
)

var (
	// ErrConflict means another transaction wrote the same key concurrently.
	// The transaction should be rolled back and retried.
	ErrConflict = errors.New("could not serialize access due to a concurrent update")
	ErrReadOnly = errors.New("mvcc: cannot write in a read-only transaction")
	ErrDone     = errors.New("mvcc: transaction already committed or rolled back")
)

// MaxValueSize is the largest value that can be stored.
const MaxValueSize = storage.MaxValueSize - 1

var nextVersionKey = []byte("n")

// Store is a transactional key/value store over a B+ tree. It is safe for
// concurrent use; each Txn must be used by one goroutine at a time.
type Store struct {
	tree *storage.BTree

	mu        sync.Mutex
	next      uint64            // the next version to hand out
	persisted uint64            // the version counter as last written to storage
	active    map[uint64]bool   // read-write transactions in progress
	open      map[*Txn]struct{} // every transaction in progress, for Vacuum
}

// Open wraps tree. It rolls back any transaction that a crash left active.
func Open(tree *storage.BTree) (*Store, error) {
	s := &Store{tree: tree, next: 1, active: make(map[uint64]bool), open: make(map[*Txn]struct{})}
	v, ok, err := tree.Get(nextVersionKey)
	if err != nil {
		return nil, err
	}
	if ok {
		s.next = binary.BigEndian.Uint64(v)
	}
	s.persisted = s.next
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// recover deletes every version written by a transaction that is still
// marked active, then the markers.
func (s *Store) recover() error {
	orphans := map[uint64]bool{}
	var markers [][]byte
	err := s.tree.Scan([]byte("a"), []byte("b"), func(k, _ []byte) bool {
		orphans[binary.BigEndian.Uint64(k[1:])] = true
		markers = append(markers, bytes.Clone(k))
		return true
	})
	if err != nil || len(orphans) == 0 {
		return err
	}
	cursor := []byte("v")
	for {
		var doomed [][]byte
		n := 0
		var last []byte
		err := s.tree.Scan(cursor, []byte("w"), func(k, _ []byte) bool {
			n++
			last = k
			if _, v, ok := splitVersionKey(k); ok && orphans[v] {
				doomed = append(doomed, bytes.Clone(k))
			}
			return n < scanBatch
		})
		if err != nil {
			return err
		}
		for _, k := range doomed {
			if _, err := s.tree.Delete(k); err != nil {
				return err
			}
		}
		if n < scanBatch {
			break
		}
		cursor = append(bytes.Clone(last), 0)
	}
	for _, k := range markers {
		if _, err := s.tree.Delete(k); err != nil {
			return err
		}
	}
	return s.tree.Sync()
}

// Begin starts a read-write transaction. Nothing is written to storage
// until the transaction's first write.
func (s *Store) Begin() (*Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &Txn{s: s, version: s.next, snapshot: s.snapshot(), writes: map[string]struct{}{}}
	s.next++
	s.active[t.version] = true
	s.open[t] = struct{}{}
	return t, nil
}

// BeginReadOnly starts a read-only transaction. It writes nothing to
// storage: it takes the next version number without consuming it, so it
// sees exactly what a read-write transaction beginning now would see.
func (s *Store) BeginReadOnly() *Txn {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &Txn{s: s, version: s.next, snapshot: s.snapshot(), readOnly: true}
	s.open[t] = struct{}{}
	return t
}

func (s *Store) snapshot() map[uint64]bool {
	m := make(map[uint64]bool, len(s.active))
	for v := range s.active {
		m[v] = true
	}
	return m
}

// Sequence increments the counter called name and returns its new value,
// starting at 1. Sequences are not transactional: a rolled-back
// transaction does not give its numbers back, so concurrent transactions
// never conflict over them (like sequences in PostgreSQL).
func (s *Store) Sequence(name []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := append([]byte("s"), name...)
	n := uint64(0)
	v, ok, err := s.tree.Get(key)
	if err != nil {
		return 0, err
	}
	if ok {
		n = binary.BigEndian.Uint64(v)
	}
	n++
	return n, s.tree.Put(key, binary.BigEndian.AppendUint64(nil, n))
}

// CheckKey reports whether key is small enough to store.
func CheckKey(key []byte) error {
	if n := len(versionKey(key, 0)); n > storage.MaxKeySize {
		return fmt.Errorf("key too large (%d bytes with versioning; the limit is %d)", n, storage.MaxKeySize)
	}
	return nil
}

// Txn is a transaction. Its methods must not be called concurrently.
type Txn struct {
	s        *Store
	version  uint64
	snapshot map[uint64]bool // transactions active when this one began
	readOnly bool
	writes   map[string]struct{} // keys written, for Rollback
	done     bool
}

// Version returns the transaction's version number.
func (t *Txn) Version() uint64 { return t.version }

// ReadOnly reports whether the transaction was started with BeginReadOnly.
func (t *Txn) ReadOnly() bool { return t.readOnly }

// Wrote reports whether the transaction has written anything.
func (t *Txn) Wrote() bool { return len(t.writes) > 0 }

func (t *Txn) visible(v uint64) bool {
	if v == t.version {
		return !t.readOnly
	}
	return v < t.version && !t.snapshot[v]
}

// versions returns every version of key, oldest first, with its raw value.
func (t *Txn) versions(key []byte) (vs []uint64, vals [][]byte, err error) {
	p := versionPrefix(key)
	err = t.s.tree.Scan(p, prefixEnd(p), func(k, v []byte) bool {
		vs = append(vs, binary.BigEndian.Uint64(k[len(k)-8:]))
		vals = append(vals, v)
		return true
	})
	return vs, vals, err
}

// Get returns the value of key in the transaction's snapshot.
func (t *Txn) Get(key []byte) ([]byte, bool, error) {
	if t.done {
		return nil, false, ErrDone
	}
	vs, vals, err := t.versions(key)
	if err != nil {
		return nil, false, err
	}
	for i := len(vs) - 1; i >= 0; i-- {
		if t.visible(vs[i]) {
			return decodeValue(vals[i])
		}
	}
	return nil, false, nil
}

// Put sets key to value.
func (t *Txn) Put(key, value []byte) error {
	if len(value) > MaxValueSize {
		return fmt.Errorf("value too large (%d bytes; the limit is %d)", len(value), MaxValueSize)
	}
	return t.write(key, append([]byte{1}, value...))
}

// Delete removes key, reporting whether it was present.
func (t *Txn) Delete(key []byte) (bool, error) {
	_, ok, err := t.Get(key)
	if err != nil || !ok {
		return false, err
	}
	return true, t.write(key, []byte{0})
}

func (t *Txn) write(key, raw []byte) error {
	if t.done {
		return ErrDone
	}
	if t.readOnly {
		return ErrReadOnly
	}
	if err := CheckKey(key); err != nil {
		return err
	}
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	vs, _, err := t.versions(key)
	if err != nil {
		return err
	}
	if n := len(vs); n > 0 && !t.visible(vs[n-1]) {
		return ErrConflict
	}
	if len(t.writes) == 0 {
		// The first write. Record the transaction as active, and make sure
		// its version number can't be handed out again after a crash,
		// before any of its versions can reach the disk.
		if t.s.persisted <= t.version {
			if err := t.s.tree.Put(nextVersionKey, binary.BigEndian.AppendUint64(nil, t.s.next)); err != nil {
				return err
			}
			t.s.persisted = t.s.next
		}
		if err := t.s.tree.Put(activeKey(t.version), nil); err != nil {
			return err
		}
	}
	if err := t.s.tree.Put(versionKey(key, t.version), raw); err != nil {
		return err
	}
	t.writes[string(key)] = struct{}{}
	return nil
}

// Commit makes the transaction's writes durable and visible to
// transactions that begin afterwards.
func (t *Txn) Commit() error {
	if t.done {
		return ErrDone
	}
	t.done = true
	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.open, t)
	if t.readOnly {
		return nil
	}
	if len(t.writes) > 0 {
		if _, err := s.tree.Delete(activeKey(t.version)); err != nil {
			return err
		}
		// The commit point. Syncing before leaving the active set means no
		// other transaction can see these writes until they are durable.
		if err := s.tree.Sync(); err != nil {
			return err
		}
	}
	delete(s.active, t.version)
	return nil
}

// Rollback discards the transaction's writes.
func (t *Txn) Rollback() error {
	if t.done {
		return ErrDone
	}
	t.done = true
	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.open, t)
	if t.readOnly {
		return nil
	}
	defer delete(s.active, t.version)
	if len(t.writes) == 0 {
		return nil
	}
	for k := range t.writes {
		if _, err := s.tree.Delete(versionKey([]byte(k), t.version)); err != nil {
			return err
		}
	}
	_, err := s.tree.Delete(activeKey(t.version))
	return err
}

// Vacuum deletes versions that no transaction, present or future, can see,
// and returns how many it removed.
//
// The horizon is the oldest version any open transaction might still need:
// the smallest of each open transaction's own version and the versions in
// its snapshot. Every version below the horizon is committed and, for any
// open or future transaction, either visible or shadowed by a newer visible
// one. So for each key, only the newest version below the horizon matters;
// older ones are removed, and so is that newest one if it is a deletion.
func (s *Store) Vacuum() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	horizon := s.next
	for t := range s.open {
		horizon = min(horizon, t.version)
		for v := range t.snapshot {
			horizon = min(horizon, v)
		}
	}

	var doomed [][]byte
	var group []kvPair // versions below the horizon of the current key
	var groupKey []byte
	flush := func() {
		n := len(group)
		if n == 0 {
			return
		}
		for _, e := range group[:n-1] {
			doomed = append(doomed, e.k)
		}
		if group[n-1].v[0] == 0 { // the newest is a deletion
			doomed = append(doomed, group[n-1].k)
		}
		group = group[:0]
	}
	err := s.tree.Scan([]byte("v"), []byte("w"), func(k, v []byte) bool {
		key, ver, ok := splitVersionKey(k)
		if !ok {
			return true
		}
		if !bytes.Equal(key, groupKey) {
			flush()
			groupKey = key
		}
		if ver < horizon {
			group = append(group, kvPair{bytes.Clone(k), bytes.Clone(v)})
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	flush()
	for _, k := range doomed {
		if _, err := s.tree.Delete(k); err != nil {
			return 0, err
		}
	}
	if len(doomed) > 0 {
		if err := s.tree.Sync(); err != nil {
			return 0, err
		}
	}
	return len(doomed), nil
}

type kvPair struct{ k, v []byte }

// Scan returns an iterator over the keys in [start, end) visible to the
// transaction, in order. A nil end means no upper bound.
func (t *Txn) Scan(start, end []byte) *Iterator {
	hi := []byte("w")
	if end != nil {
		hi = versionPrefix(end)
	}
	return &Iterator{t: t, cursor: versionPrefix(start), end: hi}
}

const scanBatch = 256

// Iterator walks a transaction's view of a key range. The storage engine's
// Scan holds the tree's lock while it calls back, so the iterator reads raw
// versions a batch at a time and resumes after the last one; between calls
// the caller may run other operations, including writes in the same
// transaction.
type Iterator struct {
	t           *Txn
	cursor, end []byte
	done        bool
	err         error

	out []kvPair // results ready to return
	i   int

	// The key whose versions are being read, which may span batches, and
	// its newest visible version so far.
	key     []byte
	best    []byte
	hasBest bool
}

// Next returns the next key and value. ok is false at the end of the range.
func (it *Iterator) Next() (key, value []byte, ok bool, err error) {
	for it.i == len(it.out) {
		if it.err != nil {
			return nil, nil, false, it.err
		}
		if it.done {
			return nil, nil, false, nil
		}
		if it.t.done {
			return nil, nil, false, ErrDone
		}
		it.fill()
	}
	e := it.out[it.i]
	it.i++
	return e.k, e.v, true, nil
}

// fill reads the next batch of raw versions into it.out.
func (it *Iterator) fill() {
	it.out, it.i = it.out[:0], 0
	n := 0
	var last []byte
	err := it.t.s.tree.Scan(it.cursor, it.end, func(k, v []byte) bool {
		n++
		last = k
		key, ver, ok := splitVersionKey(k)
		if !ok {
			return true
		}
		if !bytes.Equal(key, it.key) {
			it.emit()
			it.key = bytes.Clone(key)
		}
		if it.t.visible(ver) {
			it.best, it.hasBest = bytes.Clone(v), true
		}
		return n < scanBatch
	})
	if err != nil {
		it.err = err
		return
	}
	if n < scanBatch {
		it.emit()
		it.done = true
		return
	}
	it.cursor = append(bytes.Clone(last), 0)
}

// emit queues the current key if its visible version is not a deletion.
func (it *Iterator) emit() {
	if it.hasBest && it.best[0] == 1 {
		it.out = append(it.out, kvPair{it.key, it.best[1:]})
	}
	it.hasBest = false
}

func decodeValue(raw []byte) ([]byte, bool, error) {
	if len(raw) == 0 {
		return nil, false, errors.New("mvcc: corrupt version")
	}
	if raw[0] == 0 {
		return nil, false, nil
	}
	return raw[1:], true, nil
}

// ---------------------------------------------------------------------------
// Key encoding

// versionPrefix is 'v' followed by key with each 0x00 escaped as 0x00 0xFF
// and the terminator 0x00 0x01. The terminator sorts below every escaped
// byte, so the encoding preserves order and no encoding is a prefix of
// another.
func versionPrefix(key []byte) []byte {
	b := make([]byte, 0, len(key)+3)
	b = append(b, 'v')
	for _, c := range key {
		if c == 0 {
			b = append(b, 0, 0xFF)
		} else {
			b = append(b, c)
		}
	}
	return append(b, 0, 1)
}

func versionKey(key []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(versionPrefix(key), v)
}

// splitVersionKey decodes a 'v' key into the user key and version.
func splitVersionKey(k []byte) (key []byte, v uint64, ok bool) {
	if len(k) < 1+2+8 || k[0] != 'v' {
		return nil, 0, false
	}
	body, ver := k[1:len(k)-8], k[len(k)-8:]
	key = make([]byte, 0, len(body))
	for i := 0; i < len(body); i++ {
		if body[i] != 0 {
			key = append(key, body[i])
			continue
		}
		if i+1 >= len(body) {
			return nil, 0, false
		}
		switch body[i+1] {
		case 0xFF:
			key = append(key, 0)
			i++
		case 1:
			if i+2 != len(body) {
				return nil, 0, false
			}
			return key, binary.BigEndian.Uint64(ver), true
		default:
			return nil, 0, false
		}
	}
	return nil, 0, false
}

func activeKey(v uint64) []byte { return binary.BigEndian.AppendUint64([]byte{'a'}, v) }

// prefixEnd returns the smallest key greater than every key with the
// given prefix, or nil (no upper bound) if there is none.
func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
