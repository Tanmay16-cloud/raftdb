package server

import (
	"encoding/json"
	"fmt"

	"raftdb/internal/engine"
	"raftdb/internal/wire"
)

// The SQL protocol: a client opens a raftdb.SQL/Session stream, which is
// one SQL session (so BEGIN ... COMMIT spans requests), and sends Requests;
// the server answers each with a Response.

// Kind is what a Request asks for.
type Kind uint8

const (
	KindSQL    Kind = iota + 1 // run SQL statements
	KindTables                 // describe the tables
	KindStatus                 // describe the node
)

// Request is one client request.
type Request struct {
	Kind Kind
	SQL  string
}

// Code classifies an error, so clients can decide whether to retry.
type Code uint8

const (
	CodeOK Code = iota
	// CodeNotLeader: the node isn't the leader; LeaderAddr says where to
	// go. Nothing was executed.
	CodeNotLeader
	// CodeLeaderChanged: a commit was lost to a leadership change and did
	// not happen. Safe to retry.
	CodeLeaderChanged
	// CodeConflict: a serialization conflict; the transaction was rolled
	// back and may be retried.
	CodeConflict
	// CodeUnavailable: no quorum answered in time. A commit may or may
	// not have happened.
	CodeUnavailable
	// CodeError: any other error, such as a syntax error.
	CodeError
)

// Response answers a Request.
type Response struct {
	// KindSQL: the results of the statements that ran, in order. On an
	// error, the statements before the failing one ran and their results
	// are here.
	Results []*engine.Result
	Code    Code
	Err     string
	InTx    bool // a transaction is open in this session

	// CodeNotLeader, and KindStatus: the leader, if known.
	Leader     uint64
	LeaderAddr string

	Tables []*engine.Table // KindTables
	Status *NodeStatus     // KindStatus
}

// NodeStatus describes a node.
type NodeStatus struct {
	ID      uint64
	Role    string
	Term    uint64
	Commit  uint64
	Applied uint64
}

func (q *Request) MarshalBinary() ([]byte, error) {
	return wire.AppendString([]byte{byte(q.Kind)}, q.SQL), nil
}

func (q *Request) UnmarshalBinary(b []byte) error {
	r := wire.NewReader(b)
	q.Kind = Kind(r.Byte())
	q.SQL = r.String()
	return r.Err()
}

// Value tags.
const (
	tagNull byte = iota
	tagFalse
	tagTrue
	tagInt
	tagFloat
	tagText
)

func appendValue(b []byte, v engine.Value) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(b, tagNull), nil
	case bool:
		if v {
			return append(b, tagTrue), nil
		}
		return append(b, tagFalse), nil
	case int64:
		return wire.AppendUvarint(append(b, tagInt), uint64(v)), nil
	case float64:
		return wire.AppendFloat(append(b, tagFloat), v), nil
	case string:
		return wire.AppendString(append(b, tagText), v), nil
	}
	return nil, fmt.Errorf("server: cannot encode %T", v)
}

func readValue(r *wire.Reader) engine.Value {
	switch r.Byte() {
	case tagNull:
		return nil
	case tagFalse:
		return false
	case tagTrue:
		return true
	case tagInt:
		return int64(r.Uvarint())
	case tagFloat:
		return r.Float()
	case tagText:
		return r.String()
	}
	r.Bytes() // force a failure: unknown tag
	return nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func (p *Response) MarshalBinary() ([]byte, error) {
	b := wire.AppendUvarint(nil, uint64(len(p.Results)))
	var err error
	for _, res := range p.Results {
		b = wire.AppendUvarint(b, uint64(len(res.Columns)))
		for _, c := range res.Columns {
			b = wire.AppendString(b, c)
		}
		b = wire.AppendUvarint(b, uint64(len(res.Rows)))
		for _, row := range res.Rows {
			b = wire.AppendUvarint(b, uint64(len(row)))
			for _, v := range row {
				if b, err = appendValue(b, v); err != nil {
					return nil, err
				}
			}
		}
		b = wire.AppendString(b, res.Message)
	}
	b = append(b, byte(p.Code), boolByte(p.InTx))
	b = wire.AppendString(b, p.Err)
	b = wire.AppendUvarint(b, p.Leader)
	b = wire.AppendString(b, p.LeaderAddr)
	// Table definitions already have a JSON form (the catalog's).
	var tables []byte
	if p.Tables != nil {
		if tables, err = json.Marshal(p.Tables); err != nil {
			return nil, err
		}
	}
	b = wire.AppendBytes(b, tables)
	if p.Status == nil {
		return append(b, 0), nil
	}
	b = append(b, 1)
	b = wire.AppendUvarint(b, p.Status.ID)
	b = wire.AppendString(b, p.Status.Role)
	b = wire.AppendUvarint(b, p.Status.Term)
	b = wire.AppendUvarint(b, p.Status.Commit)
	return wire.AppendUvarint(b, p.Status.Applied), nil
}

func (p *Response) UnmarshalBinary(b []byte) error {
	r := wire.NewReader(b)
	for range r.Count() {
		res := &engine.Result{}
		for range r.Count() {
			res.Columns = append(res.Columns, r.String())
		}
		for range r.Count() {
			row := engine.Row{}
			for range r.Count() {
				row = append(row, readValue(r))
			}
			res.Rows = append(res.Rows, row)
		}
		res.Message = r.String()
		p.Results = append(p.Results, res)
	}
	p.Code = Code(r.Byte())
	p.InTx = r.Byte() == 1
	p.Err = r.String()
	p.Leader = r.Uvarint()
	p.LeaderAddr = r.String()
	if tables := r.Bytes(); len(tables) > 0 {
		if err := json.Unmarshal(tables, &p.Tables); err != nil {
			return wire.ErrCorrupt
		}
	}
	if r.Byte() == 1 {
		p.Status = &NodeStatus{ID: r.Uvarint(), Role: r.String(), Term: r.Uvarint(), Commit: r.Uvarint(), Applied: r.Uvarint()}
	}
	return r.Err()
}
