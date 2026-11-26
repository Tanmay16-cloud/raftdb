package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

type nodeType uint8

const (
	leafNode     nodeType = 1
	internalNode nodeType = 2
)

// headerSize is the same for both node types: type u8 | key count u16 | u32,
// where the u32 is the next-leaf pointer in a leaf and the first child
// pointer in an internal node.
const headerSize = 1 + 2 + 4

// node is the decoded, in-memory form of one B+ tree page.
//
// Leaf page:
//
//	header | count × (key len u16 | value len u16 | key | value)
//
// Internal page:
//
//	header | count × (key len u16 | key | child u32)
//
// In an internal node, children[i] holds keys < keys[i] and children[i+1]
// holds keys >= keys[i], so len(children) == len(keys)+1.
type node struct {
	typ      nodeType
	keys     [][]byte
	vals     [][]byte // leaf only
	children []uint32 // internal only
	next     uint32   // leaf only: page ID of the right sibling, 0 if none
}

func (n *node) isLeaf() bool { return n.typ == leafNode }

// entrySize is the number of bytes entry i takes up on the page.
func (n *node) entrySize(i int) int {
	if n.isLeaf() {
		return 2 + 2 + len(n.keys[i]) + len(n.vals[i])
	}
	return 2 + len(n.keys[i]) + 4
}

// size is the number of bytes the node needs when encoded.
func (n *node) size() int {
	s := headerSize
	for i := range n.keys {
		s += n.entrySize(i)
	}
	return s
}

// search returns the position of key in the node and whether it is present.
// If it is absent, the position is where it would be inserted.
func (n *node) search(key []byte) (int, bool) {
	i := sort.Search(len(n.keys), func(i int) bool {
		return bytes.Compare(n.keys[i], key) >= 0
	})
	return i, i < len(n.keys) && bytes.Equal(n.keys[i], key)
}

// childIndex returns which child of an internal node may contain key.
func (n *node) childIndex(key []byte) int {
	return sort.Search(len(n.keys), func(i int) bool {
		return bytes.Compare(n.keys[i], key) > 0
	})
}

// split turns an overfull n into its left half and returns the new right half
// plus the separator key the parent should use to tell them apart.
//
// The split point balances the halves by bytes, not by key count. With
// variable-length keys and values, a count-based split can leave one half
// still too big for a page; the byte-balanced split cannot, given the
// MaxKeySize and MaxValueSize limits.
func (n *node) split() (right *node, sep []byte) {
	s := n.splitPoint()
	if n.isLeaf() {
		right = &node{
			typ:  leafNode,
			keys: append([][]byte(nil), n.keys[s:]...),
			vals: append([][]byte(nil), n.vals[s:]...),
			next: n.next,
		}
		n.keys, n.vals = n.keys[:s], n.vals[:s]
		return right, bytes.Clone(right.keys[0])
	}
	// keys[s] moves up to the parent and appears in neither half.
	sep = n.keys[s]
	right = &node{
		typ:      internalNode,
		keys:     append([][]byte(nil), n.keys[s+1:]...),
		children: append([]uint32(nil), n.children[s+1:]...),
	}
	n.keys, n.children = n.keys[:s], n.children[:s+1]
	return right, sep
}

// splitPoint picks the split index that minimises the larger half's size.
func (n *node) splitPoint() int {
	total := 0
	for i := range n.keys {
		total += n.entrySize(i)
	}
	lo, hi := 1, len(n.keys)-1
	if !n.isLeaf() {
		hi = len(n.keys) - 2 // keep at least one key on each side
	}
	best, bestMax := lo, -1
	left := n.entrySize(0) // bytes of entries [0, s)
	for s := lo; s <= hi; s++ {
		right := total - left
		if !n.isLeaf() {
			right -= n.entrySize(s) // the promoted key leaves the node
		}
		if m := max(left, right); bestMax < 0 || m < bestMax {
			best, bestMax = s, m
		}
		left += n.entrySize(s)
	}
	return best
}

// encode serialises the node into a page-sized buffer.
func (n *node) encode() []byte {
	if sz := n.size(); sz > PageSize {
		panic(fmt.Sprintf("storage: node of %d bytes does not fit in a page", sz))
	}
	buf := make([]byte, PageSize)
	buf[0] = byte(n.typ)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(n.keys)))
	if n.isLeaf() {
		binary.LittleEndian.PutUint32(buf[3:], n.next)
	} else {
		binary.LittleEndian.PutUint32(buf[3:], n.children[0])
	}
	off := headerSize
	for i, k := range n.keys {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(k)))
		off += 2
		if n.isLeaf() {
			binary.LittleEndian.PutUint16(buf[off:], uint16(len(n.vals[i])))
			off += 2
			off += copy(buf[off:], k)
			off += copy(buf[off:], n.vals[i])
		} else {
			off += copy(buf[off:], k)
			binary.LittleEndian.PutUint32(buf[off:], n.children[i+1])
			off += 4
		}
	}
	return buf
}

// decodeNode parses a page. The returned node owns its memory and does not
// alias buf.
func decodeNode(buf []byte) (*node, error) {
	if len(buf) != PageSize {
		return nil, fmt.Errorf("%w: page has %d bytes", ErrCorrupt, len(buf))
	}
	n := &node{typ: nodeType(buf[0])}
	if n.typ != leafNode && n.typ != internalNode {
		return nil, fmt.Errorf("%w: unknown node type %d", ErrCorrupt, buf[0])
	}
	count := int(binary.LittleEndian.Uint16(buf[1:]))
	ptr := binary.LittleEndian.Uint32(buf[3:])
	n.keys = make([][]byte, 0, count)
	if n.isLeaf() {
		n.next = ptr
		n.vals = make([][]byte, 0, count)
	} else {
		n.children = make([]uint32, 1, count+1)
		n.children[0] = ptr
	}

	off := headerSize
	take := func(m int) []byte {
		if off+m > len(buf) {
			return nil
		}
		b := buf[off : off+m]
		off += m
		return b
	}
	truncated := fmt.Errorf("%w: node entries run past the end of the page", ErrCorrupt)
	for i := 0; i < count; i++ {
		if n.isLeaf() {
			h := take(4)
			if h == nil {
				return nil, truncated
			}
			k := take(int(binary.LittleEndian.Uint16(h[0:])))
			if k == nil {
				return nil, truncated
			}
			v := take(int(binary.LittleEndian.Uint16(h[2:])))
			if v == nil {
				return nil, truncated
			}
			n.keys = append(n.keys, bytes.Clone(k))
			n.vals = append(n.vals, append([]byte{}, v...))
		} else {
			h := take(2)
			if h == nil {
				return nil, truncated
			}
			k := take(int(binary.LittleEndian.Uint16(h)))
			if k == nil {
				return nil, truncated
			}
			c := take(4)
			if c == nil {
				return nil, truncated
			}
			n.keys = append(n.keys, bytes.Clone(k))
			n.children = append(n.children, binary.LittleEndian.Uint32(c))
		}
	}
	return n, nil
}
