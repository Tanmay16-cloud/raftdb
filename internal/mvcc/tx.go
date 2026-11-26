package mvcc

import (
	"bytes"
	"fmt"
	"slices"
)

// Executor runs commands: a local Store, or a Raft cluster that replicates
// them to every node's Store.
type Executor interface {
	Execute(*Command) (Result, error)
}

// Tx is an optimistic transaction. It reads from a snapshot and buffers its
// writes in memory; reads see the snapshot overlaid with the buffer. Commit
// sends the writes as one OpCommit command, which fails if another
// transaction committed any of the same keys after the snapshot was taken
// (snapshot isolation, first committer wins). Nothing reaches storage
// before Commit, so Rollback just forgets the buffer.
type Tx struct {
	snap   *Txn
	exec   Executor
	writes map[string]Write
	schema bool
	done   bool
}

// NewTx starts an optimistic transaction reading from snap, a read-only
// transaction from Store.Snapshot, whose commit is run by exec.
func NewTx(snap *Txn, exec Executor) *Tx {
	return &Tx{snap: snap, exec: exec, writes: map[string]Write{}}
}

// Snapshot returns the version the transaction reads at.
func (tx *Tx) Snapshot() uint64 { return tx.snap.version }

// Wrote reports whether the transaction has buffered any writes.
func (tx *Tx) Wrote() bool { return len(tx.writes) > 0 }

// MarkSchema records that the transaction changes the schema, which
// serializes it against concurrent writes (see applyCommit).
func (tx *Tx) MarkSchema() { tx.schema = true }

// Get returns the value of key as the transaction sees it.
func (tx *Tx) Get(key []byte) ([]byte, bool, error) {
	if tx.done {
		return nil, false, ErrDone
	}
	if w, ok := tx.writes[string(key)]; ok {
		return w.Value, !w.Delete, nil
	}
	return tx.snap.Get(key)
}

// Put sets key to value.
func (tx *Tx) Put(key, value []byte) error {
	if tx.done {
		return ErrDone
	}
	if err := CheckKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return fmt.Errorf("value too large (%d bytes; the limit is %d)", len(value), MaxValueSize)
	}
	tx.writes[string(key)] = Write{Key: bytes.Clone(key), Value: bytes.Clone(value)}
	return nil
}

// Delete removes key, reporting whether it was present.
func (tx *Tx) Delete(key []byte) (bool, error) {
	_, ok, err := tx.Get(key)
	if err != nil || !ok {
		return false, err
	}
	tx.writes[string(key)] = Write{Key: bytes.Clone(key), Delete: true}
	return true, nil
}

// Commit commits the buffered writes. On a conflict the transaction is
// rolled back and the error wraps ErrConflict.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrDone
	}
	tx.done = true
	defer tx.snap.Rollback()
	if len(tx.writes) == 0 {
		return nil
	}
	res, err := tx.exec.Execute(&Command{
		Op:       OpCommit,
		Snapshot: tx.snap.version,
		Schema:   tx.schema,
		Writes:   sortedWrites(tx.writes),
	})
	if err != nil {
		return err
	}
	return res.Err
}

// Rollback discards the transaction.
func (tx *Tx) Rollback() error {
	if tx.done {
		return ErrDone
	}
	tx.done = true
	return tx.snap.Rollback()
}

// Scan returns an iterator over the keys in [start, end) as the transaction
// sees them. It reflects writes made before the call, not after.
func (tx *Tx) Scan(start, end []byte) *TxIterator {
	var pending []Write
	for _, w := range tx.writes {
		if bytes.Compare(w.Key, start) >= 0 && (end == nil || bytes.Compare(w.Key, end) < 0) {
			pending = append(pending, w)
		}
	}
	slices.SortFunc(pending, func(a, b Write) int { return bytes.Compare(a.Key, b.Key) })
	return &TxIterator{base: tx.snap.Scan(start, end), pending: pending}
}

// TxIterator merges the snapshot's keys with the transaction's buffered
// writes, which take precedence.
type TxIterator struct {
	base    *Iterator
	pending []Write

	// The base iterator's next entry, read ahead for the merge.
	k, v      []byte
	have, end bool
}

// Next returns the next key and value. ok is false at the end.
func (it *TxIterator) Next() (key, value []byte, ok bool, err error) {
	for {
		if !it.have && !it.end {
			it.k, it.v, it.have, err = it.base.Next()
			if err != nil {
				return nil, nil, false, err
			}
			it.end = !it.have
		}
		switch {
		case len(it.pending) > 0 && (!it.have || bytes.Compare(it.pending[0].Key, it.k) <= 0):
			w := it.pending[0]
			it.pending = it.pending[1:]
			if it.have && bytes.Equal(w.Key, it.k) {
				it.have = false // the buffered write replaces the snapshot's version
			}
			if !w.Delete {
				return w.Key, w.Value, true, nil
			}
		case it.have:
			it.have = false
			return it.k, it.v, true, nil
		default:
			return nil, nil, false, nil
		}
	}
}
