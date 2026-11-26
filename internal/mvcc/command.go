package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Commands are how transactions change a Store when it is replicated.
//
// A replicated database can't let each node run transactions against its
// own copy: every node must make the same changes in the same order. So
// transactions execute optimistically (see Tx): they read a snapshot and
// buffer their writes, and at commit time send one self-contained Command
// with the snapshot's version and every write. Each node applies commands
// in the same order, and Apply is deterministic: the conflict check
// depends only on the store's state and the command, so every node makes
// the same decision and ends up with the same versions.
//
// A local, unreplicated store runs the same commands directly (Execute).

// Op is the kind of a Command.
type Op uint8

const (
	OpNoop     Op = iota // does nothing (Raft commits one when a leader is elected)
	OpCommit             // commit a transaction's writes
	OpSequence           // reserve values of a sequence
	OpVacuum             // remove old versions below a horizon
)

// Write is one key a transaction wrote.
type Write struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Command is one change to the store.
type Command struct {
	Op Op

	// OpCommit: the version the transaction read at, whether it changed
	// the schema, and its writes in key order.
	Snapshot uint64
	Schema   bool
	Writes   []Write

	// OpSequence: reserve N values of the sequence Name.
	Name []byte
	N    uint64

	// OpVacuum: remove versions no snapshot at or after Horizon can see.
	Horizon uint64
}

// Result is the outcome of applying a command. Err is a deterministic
// outcome, such as a conflict, that every replica reaches alike; failures
// of the node itself are returned separately by Apply.
type Result struct {
	Version uint64 // OpCommit: the commit's version
	First   uint64 // OpSequence: the first reserved value
	Removed int    // OpVacuum: versions removed
	Err     error
}

// ErrSnapshotTooOld means a transaction's snapshot predates a vacuum that
// may have removed versions it read; it must be retried.
var ErrSnapshotTooOld = fmt.Errorf("%w: snapshot too old", ErrConflict)

// storeMeta is the command layer's state, stored under key 'm' and updated
// in the same storage commit as each command's effects.
type storeMeta struct {
	applied    uint64 // index of the last applied command (replicated stores)
	lastWrite  uint64 // version of the last commit that wrote anything
	lastSchema uint64 // version of the last commit that changed the schema
	floor      uint64 // the highest vacuum horizon so far
	next       uint64 // the version counter, so replicas number commits alike
}

var metaKey = []byte("m")

func (s *Store) loadMeta() error {
	v, ok, err := s.tree.Get(metaKey)
	if err != nil || !ok {
		return err
	}
	if len(v) != 40 {
		return errors.New("mvcc: corrupt metadata")
	}
	s.meta = storeMeta{
		applied:    binary.BigEndian.Uint64(v[0:]),
		lastWrite:  binary.BigEndian.Uint64(v[8:]),
		lastSchema: binary.BigEndian.Uint64(v[16:]),
		floor:      binary.BigEndian.Uint64(v[24:]),
		next:       binary.BigEndian.Uint64(v[32:]),
	}
	// The counter as of the last applied command, even if a commit that
	// ended up writing nothing advanced it without storing it.
	s.mu.Lock()
	s.next = max(s.next, s.meta.next)
	s.persisted = s.next
	s.mu.Unlock()
	return nil
}

func (s *Store) saveMeta() error {
	s.mu.Lock()
	s.meta.next = s.next
	s.mu.Unlock()
	var b []byte
	for _, x := range []uint64{s.meta.applied, s.meta.lastWrite, s.meta.lastSchema, s.meta.floor, s.meta.next} {
		b = binary.BigEndian.AppendUint64(b, x)
	}
	return s.tree.Put(metaKey, b)
}

// Snapshot starts a read-only transaction for an optimistic Tx, and returns
// the version of the last schema change it can see (for caching the
// catalog). Snapshots are taken between commands, never during one.
func (s *Store) Snapshot() (*Txn, uint64, error) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	return s.BeginReadOnly(), s.meta.lastSchema, nil
}

// Execute applies a command to an unreplicated store and makes it durable.
func (s *Store) Execute(c *Command) (Result, error) {
	return s.apply(0, c, true)
}

// Apply applies the command at the given index of a replicated log. It does
// not fsync: the log is durable, and after a crash the commands after
// Applied are applied again. Commands at or below Applied are ignored.
func (s *Store) Apply(index uint64, c *Command) (Result, error) {
	return s.apply(index, c, false)
}

// Applied returns the index of the last command applied, as of the last
// Flush if the process has restarted since.
func (s *Store) Applied() uint64 {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	return s.meta.applied
}

// Flush makes every applied command durable.
func (s *Store) Flush() error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	return s.tree.Sync()
}

func (s *Store) apply(index uint64, c *Command, sync bool) (Result, error) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	if index != 0 && index <= s.meta.applied {
		return Result{}, nil
	}
	var r Result
	var err error
	switch c.Op {
	case OpNoop:
	case OpCommit:
		r, err = s.applyCommit(c)
	case OpSequence:
		r.First, err = s.Sequence(c.Name, c.N)
	case OpVacuum:
		// The horizon only ever rises, so that a snapshot older than any
		// vacuum is reliably rejected (see applyCommit).
		s.meta.floor = max(s.meta.floor, c.Horizon)
		r.Removed, err = s.vacuumBelow(s.meta.floor, false)
	default:
		err = fmt.Errorf("mvcc: unknown command %d", c.Op)
	}
	if err != nil {
		return Result{}, err
	}
	if index != 0 {
		s.meta.applied = index
	}
	if err := s.saveMeta(); err != nil {
		return Result{}, err
	}
	if sync {
		if err := s.tree.Sync(); err != nil {
			return Result{}, err
		}
	}
	return r, nil
}

// applyCommit commits an optimistic transaction's writes unless they
// conflict. The rules, all decided from committed state alone:
//
//   - First committer wins: if any key it writes has a version newer than
//     its snapshot, another transaction committed that key after this one
//     read, and this one fails.
//   - A snapshot older than the last vacuum's horizon may be missing
//     versions it should see, so it fails too.
//   - Schema changes are serialized against writes: a commit that changes
//     the schema fails if anything was committed since its snapshot, and a
//     commit that writes fails if the schema changed since its snapshot.
//     (CREATE INDEX fills the index from its snapshot, so it would miss a
//     row written concurrently; neither collides on a key.)
func (s *Store) applyCommit(c *Command) (Result, error) {
	conflict := func(format string, args ...any) (Result, error) {
		return Result{Err: fmt.Errorf("%w"+format, append([]any{ErrConflict}, args...)...)}, nil
	}
	if len(c.Writes) == 0 {
		return Result{}, nil
	}
	if c.Snapshot < s.meta.floor {
		return Result{Err: ErrSnapshotTooOld}, nil
	}
	if c.Schema && s.meta.lastWrite >= c.Snapshot {
		return conflict(": other transactions wrote data while this one changed the schema")
	}
	if !c.Schema && s.meta.lastSchema >= c.Snapshot {
		return conflict(": another transaction changed the schema")
	}
	for _, w := range c.Writes {
		if err := CheckKey(w.Key); err != nil {
			return Result{Err: err}, nil
		}
		if len(w.Value) > MaxValueSize {
			return Result{Err: fmt.Errorf("value too large (%d bytes; the limit is %d)", len(w.Value), MaxValueSize)}, nil
		}
		newest, err := s.newestVersion(w.Key)
		if err != nil {
			return Result{}, err
		}
		if newest >= c.Snapshot {
			return conflict("")
		}
	}

	// No other transaction is active between commands, so this one sees
	// every committed version and its writes can't conflict.
	t, err := s.Begin()
	if err != nil {
		return Result{}, err
	}
	for _, w := range c.Writes {
		if w.Delete {
			_, err = t.Delete(w.Key)
		} else {
			err = t.Put(w.Key, w.Value)
		}
		if err != nil {
			t.Rollback()
			return Result{}, err
		}
	}
	wrote := t.Wrote() // deleting keys that don't exist writes nothing
	if err := t.commit(false); err != nil {
		return Result{}, err
	}
	if wrote {
		s.meta.lastWrite = t.version
		if c.Schema {
			s.meta.lastSchema = t.version
		}
	}
	return Result{Version: t.version}, nil
}

// newestVersion returns the newest version of key, or 0 if it has none.
func (s *Store) newestVersion(key []byte) (uint64, error) {
	var newest uint64
	p := versionPrefix(key)
	err := s.tree.Scan(p, prefixEnd(p), func(k, _ []byte) bool {
		newest = binary.BigEndian.Uint64(k[len(k)-8:])
		return true
	})
	return newest, err
}

// ---------------------------------------------------------------------------
// Encoding, for the Raft log

// Encode returns the command's binary encoding.
func (c *Command) Encode() []byte {
	b := []byte{byte(c.Op)}
	b = binary.AppendUvarint(b, c.Snapshot)
	b = append(b, boolByte(c.Schema))
	b = binary.AppendUvarint(b, uint64(len(c.Writes)))
	for _, w := range c.Writes {
		b = appendBytes(b, w.Key)
		b = appendBytes(b, w.Value)
		b = append(b, boolByte(w.Delete))
	}
	b = appendBytes(b, c.Name)
	b = binary.AppendUvarint(b, c.N)
	return binary.AppendUvarint(b, c.Horizon)
}

// DecodeCommand decodes the output of Encode.
func DecodeCommand(b []byte) (*Command, error) {
	d := decoder{b: b}
	c := &Command{Op: Op(d.byte())}
	c.Snapshot = d.uvarint()
	c.Schema = d.byte() == 1
	n := d.uvarint()
	if n > uint64(len(b)) {
		return nil, errors.New("mvcc: corrupt command")
	}
	for i := uint64(0); i < n && d.err == nil; i++ {
		w := Write{Key: d.bytes(), Value: d.bytes()}
		w.Delete = d.byte() == 1
		c.Writes = append(c.Writes, w)
	}
	c.Name = d.bytes()
	c.N = d.uvarint()
	c.Horizon = d.uvarint()
	if d.err != nil || len(d.b) != 0 {
		return nil, errors.New("mvcc: corrupt command")
	}
	return c, nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func appendBytes(b, s []byte) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() { d.err, d.b = errors.New("corrupt"), nil }

func (d *decoder) byte() byte {
	if len(d.b) == 0 {
		d.fail()
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if n > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	s := bytes.Clone(d.b[:n])
	d.b = d.b[n:]
	return s
}

// sortedWrites returns writes sorted by key.
func sortedWrites(m map[string]Write) []Write {
	ws := make([]Write, 0, len(m))
	for _, w := range m {
		ws = append(ws, w)
	}
	slices.SortFunc(ws, func(a, b Write) int { return bytes.Compare(a.Key, b.Key) })
	return ws
}
