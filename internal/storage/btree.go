// Package storage implements raftdb's on-disk storage engine: a B+ tree of
// byte-string keys and values stored in fixed-size pages of a single file.
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Size limits keep every node splittable into two halves that each fit in a
// page (see node.split). Larger values will be supported with overflow pages
// later.
const (
	MaxKeySize   = 256
	MaxValueSize = 1024
)

var (
	ErrEmptyKey      = errors.New("storage: key must not be empty")
	ErrKeyTooLarge   = fmt.Errorf("storage: key larger than %d bytes", MaxKeySize)
	ErrValueTooLarge = fmt.Errorf("storage: value larger than %d bytes", MaxValueSize)
	ErrClosed        = errors.New("storage: tree is closed")
)

// BTree is a disk-backed B+ tree mapping keys to values, ordered by
// bytes.Compare. Values live only in leaves, and leaves are linked left to
// right so range scans walk sideways instead of revisiting internal nodes.
//
// Changes become durable when Sync (or Close) commits them, and each commit
// is atomic: after a crash the tree holds everything up to some commit and
// nothing after it. A single mutex serialises all operations; finer-grained
// latching comes later.
type BTree struct {
	mu     sync.Mutex
	pager  *Pager
	closed bool
}

// Open opens the tree stored at path with default options, creating an empty
// one if needed and recovering committed changes after a crash.
func Open(path string) (*BTree, error) {
	return OpenWithOptions(path, Options{})
}

// OpenWithOptions is like Open but lets the caller tune the storage engine.
func OpenWithOptions(path string, opts Options) (*BTree, error) {
	p, err := OpenPager(path, opts)
	if err != nil {
		return nil, err
	}
	if p.Root() == 0 {
		id := p.Allocate()
		err := p.Write(id, (&node{typ: leafNode}).encode())
		if err == nil {
			p.SetRoot(id)
			err = p.Commit()
		}
		if err != nil {
			p.Close()
			return nil, err
		}
	}
	return &BTree{pager: p}, nil
}

func checkKey(key []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	}
	return nil
}

func (t *BTree) readNode(id uint32) (*node, error) {
	buf, err := t.pager.Read(id)
	if err != nil {
		return nil, err
	}
	n, err := decodeNode(buf)
	if err != nil {
		return nil, fmt.Errorf("page %d: %w", id, err)
	}
	return n, nil
}

func (t *BTree) writeNode(id uint32, n *node) error {
	return t.pager.Write(id, n.encode())
}

// findLeaf descends from the root to the leaf whose key range covers key.
func (t *BTree) findLeaf(key []byte) (uint32, *node, error) {
	id := t.pager.Root()
	for {
		n, err := t.readNode(id)
		if err != nil {
			return 0, nil, err
		}
		if n.isLeaf() {
			return id, n, nil
		}
		id = n.children[n.childIndex(key)]
	}
}

// Get returns the value stored under key and whether it was found.
func (t *BTree) Get(key []byte) ([]byte, bool, error) {
	if err := checkKey(key); err != nil {
		return nil, false, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, false, ErrClosed
	}
	_, leaf, err := t.findLeaf(key)
	if err != nil {
		return nil, false, err
	}
	i, found := leaf.search(key)
	if !found {
		return nil, false, nil
	}
	return leaf.vals[i], true, nil // decodeNode gave us a private copy
}

// Put stores value under key, replacing any existing value.
func (t *BTree) Put(key, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}

	root := t.pager.Root()
	sep, right, err := t.insert(root, key, value)
	if err != nil {
		return err
	}
	if right != 0 {
		// The root split, so the tree grows one level taller.
		newRoot := &node{
			typ:      internalNode,
			keys:     [][]byte{sep},
			children: []uint32{root, right},
		}
		id := t.pager.Allocate()
		if err := t.writeNode(id, newRoot); err != nil {
			return err
		}
		t.pager.SetRoot(id)
	}
	return nil
}

// insert puts key into the subtree rooted at page id. If that page had to
// split, it returns the separator key and the new right sibling's page ID for
// the caller to add to the parent; otherwise right is 0.
func (t *BTree) insert(id uint32, key, value []byte) (sep []byte, right uint32, err error) {
	n, err := t.readNode(id)
	if err != nil {
		return nil, 0, err
	}

	if n.isLeaf() {
		i, found := n.search(key)
		if found {
			n.vals[i] = value
		} else {
			n.keys = slices.Insert(n.keys, i, key)
			n.vals = slices.Insert(n.vals, i, value)
		}
	} else {
		ci := n.childIndex(key)
		childSep, childRight, err := t.insert(n.children[ci], key, value)
		if err != nil {
			return nil, 0, err
		}
		if childRight == 0 {
			return nil, 0, nil // nothing changed at this level
		}
		n.keys = slices.Insert(n.keys, ci, childSep)
		n.children = slices.Insert(n.children, ci+1, childRight)
	}

	if n.size() <= PageSize {
		return nil, 0, t.writeNode(id, n)
	}

	rightNode, sep := n.split()
	right = t.pager.Allocate()
	if n.isLeaf() {
		n.next = right // rightNode.next already points at n's old sibling
	}
	if err := t.writeNode(id, n); err != nil {
		return nil, 0, err
	}
	if err := t.writeNode(right, rightNode); err != nil {
		return nil, 0, err
	}
	return sep, right, nil
}

// Delete removes key and reports whether it was present.
//
// Leaves are not merged or rebalanced when they shrink, so a tree that has
// many keys deleted keeps its size until it is rebuilt. Rebalancing is left
// as a later improvement; correctness does not depend on it.
func (t *BTree) Delete(key []byte) (bool, error) {
	if err := checkKey(key); err != nil {
		return false, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false, ErrClosed
	}
	id, leaf, err := t.findLeaf(key)
	if err != nil {
		return false, err
	}
	i, found := leaf.search(key)
	if !found {
		return false, nil
	}
	leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
	leaf.vals = append(leaf.vals[:i], leaf.vals[i+1:]...)
	if err := t.writeNode(id, leaf); err != nil {
		return false, err
	}
	return true, nil
}

// Scan calls fn for each key in [start, end) in ascending order and stops
// early if fn returns false. A nil start begins at the first key; a nil end
// means no upper bound.
//
// fn runs while the tree is locked, so it must not call methods on t.
func (t *BTree) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	_, n, err := t.findLeaf(start)
	if err != nil {
		return err
	}
	i, _ := n.search(start)
	for {
		for ; i < len(n.keys); i++ {
			if end != nil && bytes.Compare(n.keys[i], end) >= 0 {
				return nil
			}
			if !fn(n.keys[i], n.vals[i]) {
				return nil
			}
		}
		if n.next == 0 {
			return nil
		}
		if n, err = t.readNode(n.next); err != nil {
			return err
		}
		i = 0
	}
}

// Sync commits every change made since the previous Sync as one atomic,
// durable unit.
func (t *BTree) Sync() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	return t.pager.Commit()
}

// Stats returns storage activity counters since the tree was opened.
func (t *BTree) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pager.Stats()
}

// Close syncs and closes the tree. Closing an already-closed tree is a no-op.
func (t *BTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	return t.pager.Close()
}

// height returns the number of levels in the tree (1 for a single leaf).
func (t *BTree) height() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h, id := 1, t.pager.Root()
	for {
		n, err := t.readNode(id)
		if err != nil {
			return 0, err
		}
		if n.isLeaf() {
			return h, nil
		}
		id = n.children[0]
		h++
	}
}
