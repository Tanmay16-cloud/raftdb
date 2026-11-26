// Package wire is the gRPC codec shared by raftdb's services. gRPC allows
// one codec per server, and raftdb serves both Raft traffic and SQL
// sessions on one port, so the codec just asks each message to encode
// itself. There are no .proto files and no generated code.
package wire

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Codec marshals messages that implement encoding.BinaryMarshaler and
// encoding.BinaryUnmarshaler.
type Codec struct{}

// Name implements grpc's encoding.Codec.
func (Codec) Name() string { return "raftdb" }

// Marshal implements grpc's encoding.Codec.
func (Codec) Marshal(v any) ([]byte, error) {
	m, ok := v.(encoding.BinaryMarshaler)
	if !ok {
		return nil, fmt.Errorf("wire: cannot marshal %T", v)
	}
	return m.MarshalBinary()
}

// Unmarshal implements grpc's encoding.Codec.
func (Codec) Unmarshal(data []byte, v any) error {
	u, ok := v.(encoding.BinaryUnmarshaler)
	if !ok {
		return fmt.Errorf("wire: cannot unmarshal into %T", v)
	}
	return u.UnmarshalBinary(data)
}

// Empty is a message with no content.
type Empty struct{}

func (*Empty) MarshalBinary() ([]byte, error) { return nil, nil }
func (*Empty) UnmarshalBinary([]byte) error   { return nil }

// ---------------------------------------------------------------------------
// Helpers for hand-written encodings

// ErrCorrupt is returned for malformed input.
var ErrCorrupt = errors.New("wire: corrupt message")

// AppendUvarint, AppendBytes and AppendString append encoded fields.
func AppendUvarint(b []byte, v uint64) []byte { return binary.AppendUvarint(b, v) }

func AppendBytes(b, s []byte) []byte { return append(binary.AppendUvarint(b, uint64(len(s))), s...) }

func AppendString(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

func AppendFloat(b []byte, f float64) []byte {
	return binary.BigEndian.AppendUint64(b, math.Float64bits(f))
}

// Reader decodes fields in order. After a failure every read returns a
// zero value and Err reports the failure.
type Reader struct {
	b   []byte
	err error
}

func NewReader(b []byte) *Reader { return &Reader{b: b} }

func (r *Reader) fail() { r.err, r.b = ErrCorrupt, nil }

// Err returns ErrCorrupt if any read failed or input is left over.
func (r *Reader) Err() error {
	if r.err == nil && len(r.b) > 0 {
		return ErrCorrupt
	}
	return r.err
}

func (r *Reader) Byte() byte {
	if len(r.b) == 0 {
		r.fail()
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *Reader) Uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *Reader) Bytes() []byte {
	n := r.Uvarint()
	if n > uint64(len(r.b)) {
		r.fail()
		return nil
	}
	s := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return s
}

func (r *Reader) String() string { return string(r.Bytes()) }

func (r *Reader) Float() float64 {
	if len(r.b) < 8 {
		r.fail()
		return 0
	}
	f := math.Float64frombits(binary.BigEndian.Uint64(r.b))
	r.b = r.b[8:]
	return f
}

// Count reads a length prefix for a list whose elements take at least one
// byte each, rejecting lengths the remaining input can't hold.
func (r *Reader) Count() int {
	n := r.Uvarint()
	if n > uint64(len(r.b)) {
		r.fail()
		return 0
	}
	return int(n)
}
