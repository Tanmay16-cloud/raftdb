package raft

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLogStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")
	s, err := OpenLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("0123456789"), 500) // spans several chunks
	ents := []Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2, Data: []byte("a")},
		{Term: 2, Index: 3, Data: big},
		{Term: 2, Index: 4, Data: []byte("b")},
	}
	if err := s.Save(HardState{Term: 2, Vote: 3, Commit: 2}, ents); err != nil {
		t.Fatal(err)
	}
	// A new leader overwrites the log from index 3 with a shorter entry:
	// the old entry's extra chunks and index 4 must go.
	repl := []Entry{{Term: 3, Index: 3, Data: []byte("c")}}
	if err := s.Save(HardState{Term: 3, Vote: 0, Commit: 3}, repl); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	hs, got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := append(ents[:2:2], repl...)
	norm := func(es []Entry) []Entry {
		for i := range es {
			es[i].Data = append([]byte{}, es[i].Data...)
		}
		return es
	}
	if hs != (HardState{3, 0, 3}) || !reflect.DeepEqual(norm(got), norm(want)) {
		t.Fatalf("loaded %+v %v, want %v", hs, got, want)
	}

	// Large entries survive a round trip intact.
	if err := s.Save(hs, []Entry{{Term: 3, Index: 4, Data: big}}); err != nil {
		t.Fatal(err)
	}
	_, got, err = s.Load()
	if err != nil || len(got) != 4 || !bytes.Equal(got[3].Data, big) {
		t.Fatalf("big entry: %v, %d entries", err, len(got))
	}
}

func TestMessageEncoding(t *testing.T) {
	msgs := []Message{
		{Type: MsgVote, From: 1, To: 2, Term: 7, LogTerm: 6, Index: 100},
		{Type: MsgApp, From: 1, To: 3, Term: 1 << 40, LogTerm: 5, Index: 9, Commit: 8,
			Entries: []Entry{{Term: 5, Index: 10, Data: []byte("x")}, {Term: 5, Index: 11}}},
		{Type: MsgAppResp, From: 3, To: 1, Term: 5, Index: 9, Reject: true, Hint: 4},
		{Type: MsgHeartbeatResp, From: 2, To: 1, Term: 5, Context: 99},
	}
	for _, m := range msgs {
		enc := EncodeMessage(m)
		got, err := DecodeMessage(enc)
		if err != nil {
			t.Fatal(err)
		}
		for i := range got.Entries {
			if len(got.Entries[i].Data) == 0 {
				got.Entries[i].Data = nil
			}
		}
		if !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip of %+v gave %+v", m, got)
		}
		for n := range len(enc) {
			if _, err := DecodeMessage(enc[:n]); err == nil {
				t.Fatalf("truncated message (%d of %d bytes) decoded", n, len(enc))
			}
		}
	}
}
