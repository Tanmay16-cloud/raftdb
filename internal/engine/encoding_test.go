package engine

import (
	"bytes"
	"math"
	"math/rand"
	"slices"
	"testing"
)

// randomValue returns a value drawn to hit the encoding's edge cases:
// NULL, both booleans, extreme and nearby integers, signed zeros,
// infinities, and strings containing NUL bytes or sharing prefixes.
func randomValue(rng *rand.Rand) Value {
	switch rng.Intn(5) {
	case 0:
		return []Value{nil, true, false}[rng.Intn(3)]
	case 1:
		ints := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 255, 256, -256}
		if rng.Intn(2) == 0 {
			return ints[rng.Intn(len(ints))]
		}
		return rng.Int63n(2000) - 1000
	case 2:
		floats := []float64{0, math.Copysign(0, -1), 1.5, -1.5, math.Inf(1), math.Inf(-1),
			math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.MaxFloat64}
		if rng.Intn(2) == 0 {
			return floats[rng.Intn(len(floats))]
		}
		return rng.NormFloat64() * 100
	default:
		alphabet := []byte{0x00, 0x01, 'a', 'b', 0xFF}
		b := make([]byte, rng.Intn(5))
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}
}

// encodedOrder is the order the key encoding must reproduce: NULL, FALSE,
// TRUE, then integers, floats and text, each type ordered by value.
func encodedOrder(a, b Value) int {
	rank := func(v Value) int {
		switch x := v.(type) {
		case nil:
			return 0
		case bool:
			if x {
				return 2
			}
			return 1
		case int64:
			return 3
		case float64:
			return 4
		}
		return 5
	}
	if ra, rb := rank(a), rank(b); ra != rb || ra < 3 {
		return ra - rb
	}
	c, err := compareValues(a, b)
	if err != nil {
		panic(err)
	}
	return c
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	}
	return 0
}

func TestKeyEncodingPreservesOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 200000; i++ {
		a, b := randomValue(rng), randomValue(rng)
		want := sign(encodedOrder(a, b))
		got := sign(bytes.Compare(appendKey(nil, a), appendKey(nil, b)))
		if got != want {
			t.Fatalf("order of %#v and %#v: encoded %d, want %d", a, b, got, want)
		}
	}
}

// Index keys append the primary key to the indexed value, so no encoding may
// be a prefix of a different one: otherwise ("a", pk "b...") could collide
// with ("ab", ...) and sort wrongly.
func TestKeyEncodingIsPrefixFree(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 100000; i++ {
		a, b := randomValue(rng), randomValue(rng)
		ka, kb := appendKey(nil, a), appendKey(nil, b)
		if !bytes.Equal(ka, kb) && (bytes.HasPrefix(ka, kb) || bytes.HasPrefix(kb, ka)) {
			t.Fatalf("encoding of %#v (%x) and %#v (%x): one is a prefix of the other", a, ka, b, kb)
		}
	}
}

func TestNegativeZeroEncodesAsZero(t *testing.T) {
	if !bytes.Equal(appendKey(nil, math.Copysign(0, -1)), appendKey(nil, 0.0)) {
		t.Fatal("-0.0 and 0.0 encode differently")
	}
}

func TestRowEncodingRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 20000; i++ {
		row := make(Row, rng.Intn(6))
		for j := range row {
			row[j] = randomValue(rng)
		}
		got, err := decodeRow(encodeRow(row), len(row))
		if err != nil {
			t.Fatalf("decode %#v: %v", row, err)
		}
		if !slices.EqualFunc(row, got, sameValue) {
			t.Fatalf("round trip: got %#v, want %#v", got, row)
		}
	}
}

// sameValue is exact equality, including the sign of zero.
func sameValue(a, b Value) bool {
	if fa, ok := a.(float64); ok {
		fb, ok := b.(float64)
		return ok && math.Float64bits(fa) == math.Float64bits(fb)
	}
	return a == b
}

func TestDecodeRowRejectsCorruptData(t *testing.T) {
	good := encodeRow(Row{int64(5), "hello", 2.5})
	for n := 0; n < len(good); n++ {
		if _, err := decodeRow(good[:n], 3); err == nil {
			t.Fatalf("truncated to %d bytes: no error", n)
		}
	}
	if _, err := decodeRow(good, 4); err == nil {
		t.Fatal("wrong column count: no error")
	}
	if _, err := decodeRow([]byte{0x7F}, 1); err == nil {
		t.Fatal("unknown tag: no error")
	}
}

func TestPrefixEnd(t *testing.T) {
	cases := []struct{ in, want []byte }{
		{[]byte("ab"), []byte("ac")},
		{[]byte{'a', 0xFF}, []byte{'b'}},
		{[]byte{0xFF, 0xFF}, nil},
		{[]byte{}, nil},
	}
	for _, c := range cases {
		if got := prefixEnd(c.in); !bytes.Equal(got, c.want) {
			t.Errorf("prefixEnd(%x) = %x, want %x", c.in, got, c.want)
		}
	}
}
