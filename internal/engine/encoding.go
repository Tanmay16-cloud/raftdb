package engine

import (
	"encoding/binary"
	"errors"
	"math"
)

// The SQL layer stores everything in the storage engine's single ordered
// key space:
//
//	c/<table name>                       -> table definition (JSON)
//	s/next-id                            -> next table or index ID
//	q<table ID>                          -> next hidden row ID, for tables without a primary key
//	r<table ID><primary key>             -> row
//	x<index ID><indexed value><primary key> -> primary key
//
// IDs are 4-byte big-endian integers, so all rows of a table, and all
// entries of an index, sit next to each other in key order.
//
// Primary keys and indexed values use an order-preserving encoding:
// comparing two encoded values byte by byte gives the same answer as
// comparing the values themselves. That is what lets a B+ tree range scan
// answer "WHERE age BETWEEN 18 AND 30". Including the primary key in each
// index key keeps the keys of duplicate values distinct.

const (
	tagNull  byte = 0x01
	tagFalse byte = 0x02
	tagTrue  byte = 0x03
	tagInt   byte = 0x04
	tagFloat byte = 0x05
	tagText  byte = 0x06
)

// appendKey appends the order-preserving encoding of v:
//
//   - NULL, FALSE and TRUE are a single tag byte.
//   - Integers are 8 bytes big-endian with the sign bit flipped, so
//     negative numbers sort before positive ones.
//   - Floats are their IEEE 754 bits, with every bit flipped for negative
//     numbers and just the sign bit flipped for positive ones.
//   - Text is its bytes with each 0x00 escaped as 0x00 0xFF, then the
//     terminator 0x00 0x01. The terminator sorts below every escaped byte,
//     so "ab" < "abc", and no encoding is a prefix of another.
func appendKey(b []byte, v Value) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, tagNull)
	case bool:
		if x {
			return append(b, tagTrue)
		}
		return append(b, tagFalse)
	case int64:
		b = append(b, tagInt)
		return binary.BigEndian.AppendUint64(b, uint64(x)^(1<<63))
	case float64:
		if x == 0 {
			x = 0 // -0.0 and 0.0 are equal, so they must encode the same
		}
		bits := math.Float64bits(x)
		if bits>>63 == 1 {
			bits = ^bits
		} else {
			bits |= 1 << 63
		}
		b = append(b, tagFloat)
		return binary.BigEndian.AppendUint64(b, bits)
	case string:
		b = append(b, tagText)
		for i := 0; i < len(x); i++ {
			if x[i] == 0 {
				b = append(b, 0x00, 0xFF)
			} else {
				b = append(b, x[i])
			}
		}
		return append(b, 0x00, 0x01)
	}
	panic("engine: cannot encode " + typeName(v))
}

// prefixEnd returns the smallest key greater than every key that starts with
// prefix, or nil if there is none.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func withID(tag byte, id uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{tag}, id)
}

func catalogKey(name string) []byte { return append([]byte("c/"), name...) }

var nextIDKey = []byte("s/next-id")

func rowIDKey(tableID uint32) []byte    { return withID('q', tableID) }
func rowPrefix(tableID uint32) []byte   { return withID('r', tableID) }
func indexPrefix(indexID uint32) []byte { return withID('x', indexID) }

func rowKey(tableID uint32, pk []byte) []byte { return append(rowPrefix(tableID), pk...) }

func indexKey(indexID uint32, v Value, pk []byte) []byte {
	return append(appendKey(indexPrefix(indexID), v), pk...)
}

// Rows are stored in a compact format that need not preserve order: for
// each column, a tag byte followed by the value (a varint for integers,
// 8 bytes for floats, a length-prefixed string for text).

func encodeRow(r Row) []byte {
	var b []byte
	for _, v := range r {
		switch x := v.(type) {
		case nil:
			b = append(b, tagNull)
		case bool:
			if x {
				b = append(b, tagTrue)
			} else {
				b = append(b, tagFalse)
			}
		case int64:
			b = binary.AppendVarint(append(b, tagInt), x)
		case float64:
			b = binary.LittleEndian.AppendUint64(append(b, tagFloat), math.Float64bits(x))
		case string:
			b = binary.AppendUvarint(append(b, tagText), uint64(len(x)))
			b = append(b, x...)
		default:
			panic("engine: cannot encode " + typeName(v))
		}
	}
	return b
}

var errBadRow = errors.New("engine: stored row is corrupt")

func decodeRow(b []byte, ncols int) (Row, error) {
	r := make(Row, 0, ncols)
	for len(b) > 0 {
		tag := b[0]
		b = b[1:]
		switch tag {
		case tagNull:
			r = append(r, nil)
		case tagFalse, tagTrue:
			r = append(r, tag == tagTrue)
		case tagInt:
			v, n := binary.Varint(b)
			if n <= 0 {
				return nil, errBadRow
			}
			r = append(r, v)
			b = b[n:]
		case tagFloat:
			if len(b) < 8 {
				return nil, errBadRow
			}
			r = append(r, math.Float64frombits(binary.LittleEndian.Uint64(b)))
			b = b[8:]
		case tagText:
			l, n := binary.Uvarint(b)
			if n <= 0 || uint64(len(b)-n) < l {
				return nil, errBadRow
			}
			r = append(r, string(b[n:n+int(l)]))
			b = b[n+int(l):]
		default:
			return nil, errBadRow
		}
	}
	if len(r) != ncols {
		return nil, errBadRow
	}
	return r, nil
}
