// Package server serves SQL sessions over gRPC from a cluster node, and
// package client (in this module) talks to it.
package server

import (
	"errors"
	"io"

	"google.golang.org/grpc"

	"raftdb/internal/cluster"
	"raftdb/internal/engine"
	"raftdb/internal/storage"
)

// Server serves SQL from one node.
type Server struct {
	node  *cluster.Node
	db    *engine.DB
	addrs map[uint64]string // every node's address, to redirect clients
}

// New returns a server for node. addrs maps node IDs to the addresses
// clients reach them at.
func New(node *cluster.Node, addrs map[uint64]string) *Server {
	// Table and row IDs are reserved 100 at a time, so most inserts need
	// one round of consensus, not two.
	return &Server{node: node, db: engine.New(node, 100), addrs: addrs}
}

// Register adds the SQL service to s, which must use raftdb's codec (see
// cluster.NewGRPCServer).
func (srv *Server) Register(s *grpc.Server) {
	s.RegisterService(&serviceDesc, srv)
}

// sqlService is what gRPC checks the registered value against.
type sqlService interface {
	session(grpc.ServerStream) error
}

const sessionMethod = "/raftdb.SQL/Session"

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "raftdb.SQL",
	HandlerType: (*sqlService)(nil),
	Streams: []grpc.StreamDesc{{
		StreamName:    "Session",
		ServerStreams: true,
		ClientStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(sqlService).session(stream)
		},
	}},
}

// SessionStreamDesc describes the session stream, for clients.
var SessionStreamDesc = &serviceDesc.Streams[0]

// SessionMethod is the full method name of the session stream.
const SessionMethod = sessionMethod

func (srv *Server) session(stream grpc.ServerStream) error {
	sess := srv.db.Session()
	defer sess.Close() // rolls back a transaction left open
	for {
		var req Request
		if err := stream.RecvMsg(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		resp := srv.handle(sess, &req)
		if err := stream.SendMsg(resp); err != nil {
			return err
		}
	}
}

func (srv *Server) handle(sess *engine.Session, req *Request) *Response {
	st := srv.node.Status()
	resp := &Response{}
	switch req.Kind {
	case KindSQL:
		results, err := sess.Exec(req.SQL)
		resp.Results = results
		if err != nil {
			resp.Code, resp.Err = classify(err), err.Error()
		}
	case KindTables:
		if tables, err := srv.tables(sess); err != nil {
			resp.Code, resp.Err = classify(err), err.Error()
		} else {
			resp.Tables = tables
		}
	case KindStatus:
		resp.Status = &NodeStatus{ID: st.ID, Role: st.State.String(), Term: st.Term, Commit: st.Commit, Applied: st.Applied}
	default:
		resp.Code, resp.Err = CodeError, "unknown request"
	}
	resp.InTx = sess.InTransaction()
	resp.Leader = st.Leader
	resp.LeaderAddr = srv.addrs[st.Leader]
	return resp
}

// tables returns the schema. Outside a transaction this takes a
// linearizable snapshot, like any read.
func (srv *Server) tables(sess *engine.Session) ([]*engine.Table, error) {
	return sess.ListTables()
}

func classify(err error) Code {
	var nl *cluster.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return CodeNotLeader
	case errors.Is(err, cluster.ErrLeaderChanged):
		return CodeLeaderChanged
	case errors.Is(err, engine.ErrConflict):
		return CodeConflict
	case errors.Is(err, cluster.ErrTimeout), errors.Is(err, cluster.ErrStopped), errors.Is(err, storage.ErrClosed):
		// ErrClosed: the node shut down under a running statement.
		return CodeUnavailable
	}
	return CodeError
}
