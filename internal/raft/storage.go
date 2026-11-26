package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"raftdb/internal/storage"
)

// LogStore persists a node's HardState and log in a B+ tree of its own.
//
// Keys: "h" holds the HardState; "e" + index (8 bytes) + chunk (2 bytes)
// holds an entry. The tree limits values to storage.MaxValueSize, so an
// entry is split into chunks: chunk 0 holds the term and the start of the
// data, and further chunks the rest.
type LogStore struct {
	tree *storage.BTree
}

const chunkSize = storage.MaxValueSize - 8

// OpenLogStore opens the log at path, creating it if needed.
func OpenLogStore(path string) (*LogStore, error) {
	tree, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	return &LogStore{tree: tree}, nil
}

// Close closes the log.
func (s *LogStore) Close() error { return s.tree.Close() }

func entryKey(index uint64, chunk uint16) []byte {
	k := binary.BigEndian.AppendUint64([]byte{'e'}, index)
	return binary.BigEndian.AppendUint16(k, chunk)
}

// Load returns the persisted HardState and log.
func (s *LogStore) Load() (HardState, []Entry, error) {
	var hs HardState
	v, ok, err := s.tree.Get([]byte("h"))
	if err != nil {
		return hs, nil, err
	}
	if ok {
		if len(v) != 24 {
			return hs, nil, errors.New("raft: corrupt hard state")
		}
		hs = HardState{binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:]), binary.BigEndian.Uint64(v[16:])}
	}
	var ents []Entry
	err = s.tree.Scan([]byte("e"), []byte("f"), func(k, v []byte) bool {
		if len(k) != 11 {
			err = errors.New("raft: corrupt log key")
			return false
		}
		index, chunk := binary.BigEndian.Uint64(k[1:]), binary.BigEndian.Uint16(k[9:])
		if chunk == 0 {
			if len(v) < 8 || index != uint64(len(ents))+1 {
				err = fmt.Errorf("raft: corrupt log at index %d", index)
				return false
			}
			ents = append(ents, Entry{Index: index, Term: binary.BigEndian.Uint64(v), Data: bytes.Clone(v[8:])})
			return true
		}
		if len(ents) == 0 || ents[len(ents)-1].Index != index {
			err = fmt.Errorf("raft: orphan log chunk at index %d", index)
			return false
		}
		e := &ents[len(ents)-1]
		e.Data = append(e.Data, v...)
		return true
	})
	return hs, ents, err
}

// Save persists a Ready's HardState and entries durably. The entries
// replace any stored at Entries[0].Index and beyond.
func (s *LogStore) Save(hs HardState, ents []Entry) error {
	if len(ents) > 0 {
		var doomed [][]byte
		err := s.tree.Scan(entryKey(ents[0].Index, 0), []byte("f"), func(k, _ []byte) bool {
			doomed = append(doomed, bytes.Clone(k))
			return true
		})
		if err != nil {
			return err
		}
		for _, k := range doomed {
			if _, err := s.tree.Delete(k); err != nil {
				return err
			}
		}
		for _, e := range ents {
			data := e.Data
			first := min(len(data), chunkSize)
			v := binary.BigEndian.AppendUint64(nil, e.Term)
			if err := s.tree.Put(entryKey(e.Index, 0), append(v, data[:first]...)); err != nil {
				return err
			}
			data = data[first:]
			for c := uint16(1); len(data) > 0; c++ {
				n := min(len(data), storage.MaxValueSize)
				if err := s.tree.Put(entryKey(e.Index, c), data[:n]); err != nil {
					return err
				}
				data = data[n:]
			}
		}
	}
	v := binary.BigEndian.AppendUint64(nil, hs.Term)
	v = binary.BigEndian.AppendUint64(v, hs.Vote)
	v = binary.BigEndian.AppendUint64(v, hs.Commit)
	if err := s.tree.Put([]byte("h"), v); err != nil {
		return err
	}
	return s.tree.Sync()
}

// ---------------------------------------------------------------------------
// Message encoding, for the network

// EncodeMessage returns m's binary encoding.
func EncodeMessage(m Message) []byte {
	b := []byte{byte(m.Type)}
	for _, x := range []uint64{m.From, m.To, m.Term, m.LogTerm, m.Index, m.Commit, m.Hint, m.Context} {
		b = binary.AppendUvarint(b, x)
	}
	if m.Reject {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.AppendUvarint(b, uint64(len(m.Entries)))
	for _, e := range m.Entries {
		b = binary.AppendUvarint(b, e.Term)
		b = binary.AppendUvarint(b, e.Index)
		b = binary.AppendUvarint(b, uint64(len(e.Data)))
		b = append(b, e.Data...)
	}
	return b
}

var errCorruptMessage = errors.New("raft: corrupt message")

// DecodeMessage decodes the output of EncodeMessage.
func DecodeMessage(b []byte) (Message, error) {
	var m Message
	if len(b) == 0 {
		return m, errCorruptMessage
	}
	m.Type, b = MsgType(b[0]), b[1:]
	uv := func() uint64 {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			b = nil
			return 0
		}
		b = b[n:]
		return v
	}
	fields := []*uint64{&m.From, &m.To, &m.Term, &m.LogTerm, &m.Index, &m.Commit, &m.Hint, &m.Context}
	for _, f := range fields {
		if b == nil {
			return m, errCorruptMessage
		}
		*f = uv()
	}
	if len(b) == 0 {
		return m, errCorruptMessage
	}
	m.Reject, b = b[0] == 1, b[1:]
	n := uv()
	if b == nil || n > uint64(len(b)) {
		return m, errCorruptMessage
	}
	for range n {
		e := Entry{Term: uv(), Index: uv()}
		size := uv()
		if b == nil || size > uint64(len(b)) {
			return m, errCorruptMessage
		}
		e.Data, b = bytes.Clone(b[:size]), b[size:]
		m.Entries = append(m.Entries, e)
	}
	if len(b) != 0 {
		return m, errCorruptMessage
	}
	return m, nil
}
