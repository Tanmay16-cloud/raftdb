// Package client connects to a raftdb cluster over gRPC.
//
// A Client knows every node's address and remembers which one leads. A
// Session is one SQL session on the leader. Requests that reach a
// follower, or a leader that just lost its job, are redirected and retried,
// but only when the server guarantees nothing ran: a request that might
// have committed is never sent twice.
package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftdb/internal/engine"
	"raftdb/internal/server"
	"raftdb/internal/wire"
)

// Error is an error reported by the server.
type Error struct {
	Code server.Code
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// IsConflict reports whether err is a serialization conflict: the
// transaction was rolled back and the application may retry it.
func IsConflict(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == server.CodeConflict
}

// ErrConnectionLost means the connection failed while a request was in
// flight or a transaction was open. The request may or may not have taken
// effect; an open transaction was rolled back.
var ErrConnectionLost = errors.New("client: connection lost; the outcome is unknown")

// Options tunes a client.
type Options struct {
	// How long to keep retrying while no leader is known (an election
	// takes a few hundred milliseconds). Default 10s.
	RetryFor time.Duration
}

// Client is a connection to a cluster. It is safe for concurrent use.
type Client struct {
	addrs []string
	opts  Options

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	leader string // the last known leader
	next   int    // round-robin position for when no leader is known
}

// Dial returns a client for the cluster whose nodes are at addrs.
// Connections are made lazily.
func Dial(addrs []string, opts Options) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	if opts.RetryFor == 0 {
		opts.RetryFor = 10 * time.Second
	}
	return &Client{addrs: slices.Clone(addrs), opts: opts, conns: map[string]*grpc.ClientConn{}}, nil
}

// Close closes every connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	clear(c.conns)
	return nil
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(wire.Codec{})))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// target returns the address to try: the known leader, or the next node.
func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	addr := c.addrs[c.next%len(c.addrs)]
	c.next++
	return addr
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = addr
}

// forget drops addr as the known leader, if it is.
func (c *Client) forget(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader == addr {
		c.leader = ""
	}
}

// Session starts a session. Its stream is opened on first use.
func (c *Client) Session() *Session { return &Session{c: c} }

// Exec runs SQL in a fresh session, for one-off statements.
func (c *Client) Exec(sql string) ([]*engine.Result, error) {
	s := c.Session()
	defer s.Close()
	return s.Exec(sql)
}

// Session is one SQL session on the cluster's leader. Use it from one
// goroutine at a time.
type Session struct {
	c      *Client
	addr   string
	stream grpc.ClientStream
	cancel context.CancelFunc
	inTx   bool
}

// InTransaction reports whether a transaction is open.
func (s *Session) InTransaction() bool { return s.inTx }

// Close ends the session; an open transaction is rolled back.
func (s *Session) Close() error {
	s.drop()
	return nil
}

func (s *Session) drop() {
	if s.stream != nil {
		s.stream.CloseSend()
		s.cancel()
		s.stream, s.cancel = nil, nil
	}
	s.inTx = false
}

// open makes sure the session has a stream, to the leader if known.
func (s *Session) open() error {
	if s.stream != nil {
		return nil
	}
	addr := s.c.target()
	conn, err := s.c.conn(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := conn.NewStream(ctx, server.SessionStreamDesc, server.SessionMethod)
	if err != nil {
		cancel()
		s.c.forget(addr)
		return err
	}
	s.addr, s.stream, s.cancel = addr, stream, cancel
	return nil
}

// roundTrip sends one request. sent reports whether the request may have
// reached the server, which makes a failure ambiguous.
func (s *Session) roundTrip(req *server.Request) (resp *server.Response, sent bool, err error) {
	if err := s.open(); err != nil {
		return nil, false, err
	}
	if err := s.stream.SendMsg(req); err != nil {
		s.c.forget(s.addr)
		s.drop()
		return nil, false, err
	}
	resp = &server.Response{}
	if err := s.stream.RecvMsg(resp); err != nil {
		s.c.forget(s.addr)
		s.drop()
		return nil, true, err
	}
	return resp, true, nil
}

// do sends a request, following redirects to the leader and retrying
// while it is safe.
func (s *Session) do(req *server.Request, readOnly bool) (*server.Response, error) {
	deadline := time.Now().Add(s.c.opts.RetryFor)
	backoff := 10 * time.Millisecond
	for {
		wasInTx := s.inTx
		resp, sent, err := s.roundTrip(req)
		if err != nil {
			// The node is unreachable. Before anything was sent, or for a
			// read, trying another node is safe; otherwise the outcome is
			// unknown.
			if wasInTx || (sent && !readOnly) {
				return nil, fmt.Errorf("%w: %v", ErrConnectionLost, err)
			}
		} else {
			s.inTx = resp.InTx
			if resp.Code == server.CodeOK {
				if resp.LeaderAddr != "" && resp.LeaderAddr == s.addr {
					s.c.setLeader(s.addr)
				}
				return resp, nil
			}
			// Safe to retry only if nothing ran: no statement completed,
			// and no transaction was open (it lived on the old leader).
			nothingRan := len(resp.Results) == 0 && !wasInTx
			switch {
			case resp.Code == server.CodeNotLeader && nothingRan:
				s.c.setLeader(resp.LeaderAddr) // "" if unknown: round-robin
				s.drop()
			case resp.Code == server.CodeLeaderChanged && nothingRan:
				s.c.forget(s.addr)
				s.drop()
			case resp.Code == server.CodeUnavailable && nothingRan && readOnly:
				s.c.forget(s.addr)
				s.drop()
			default:
				if resp.Code == server.CodeNotLeader || resp.Code == server.CodeLeaderChanged {
					s.drop() // the transaction is gone with the old leader
				}
				return resp, &Error{Code: resp.Code, Msg: resp.Err}
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return nil, err
			}
			return resp, &Error{Code: resp.Code, Msg: resp.Err}
		}
		time.Sleep(backoff)
		backoff = min(backoff*2, 200*time.Millisecond)
	}
}

// Exec runs one or more SQL statements and returns their results. On an
// error, the results of the statements that ran before it are returned
// too.
func (s *Session) Exec(sql string) ([]*engine.Result, error) {
	resp, err := s.do(&server.Request{Kind: server.KindSQL, SQL: sql}, false)
	if resp == nil {
		return nil, err
	}
	return resp.Results, err
}

// Query runs SQL and returns the last statement's result.
func (s *Session) Query(sql string) (*engine.Result, error) {
	results, err := s.Exec(sql)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("client: no statements")
	}
	return results[len(results)-1], nil
}

// Tables describes the tables.
func (s *Session) Tables() ([]*engine.Table, error) {
	resp, err := s.do(&server.Request{Kind: server.KindTables}, true)
	if err != nil {
		return nil, err
	}
	return resp.Tables, nil
}

// Status describes the node the session is connected to, and its leader.
func (s *Session) Status() (*server.NodeStatus, string, error) {
	resp, _, err := s.roundTrip(&server.Request{Kind: server.KindStatus})
	if err != nil {
		return nil, "", err
	}
	return resp.Status, resp.LeaderAddr, nil
}
