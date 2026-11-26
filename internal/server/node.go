package server

import (
	"net"
	"slices"
	"time"

	"google.golang.org/grpc"

	"raftdb/internal/cluster"
)

// NodeConfig configures a full node: Raft, the replicated store and the
// SQL service, all served by gRPC on one address.
type NodeConfig struct {
	ID    uint64
	Addrs map[uint64]string // every node's address, including this one's
	Dir   string

	TickInterval time.Duration // default 10ms
}

// Process is a running node.
type Process struct {
	ID   uint64
	Addr string

	node *cluster.Node
	grpc *grpc.Server
	tr   *cluster.GRPCTransport
	done chan struct{}
}

// Start starts a node, listening on its address in cfg.Addrs.
func Start(cfg NodeConfig) (*Process, error) {
	lis, err := net.Listen("tcp", cfg.Addrs[cfg.ID])
	if err != nil {
		return nil, err
	}
	tr, err := cluster.NewGRPCTransport(cfg.ID, cfg.Addrs)
	if err != nil {
		lis.Close()
		return nil, err
	}
	var peers []uint64
	for id := range cfg.Addrs {
		peers = append(peers, id)
	}
	slices.Sort(peers)
	node, err := cluster.Open(cluster.Config{
		ID: cfg.ID, Peers: peers, Dir: cfg.Dir, Transport: tr, TickInterval: cfg.TickInterval,
	})
	if err != nil {
		tr.Close()
		lis.Close()
		return nil, err
	}
	s := cluster.NewGRPCServer()
	cluster.Register(s, node)
	New(node, cfg.Addrs).Register(s)
	p := &Process{ID: cfg.ID, Addr: cfg.Addrs[cfg.ID], node: node, grpc: s, tr: tr, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		s.Serve(lis)
	}()
	return p, nil
}

// Node returns the node's Raft member.
func (p *Process) Node() *cluster.Node { return p.node }

// Stop shuts the node down: open sessions are cut off and their
// transactions rolled back.
func (p *Process) Stop() error {
	p.grpc.Stop()
	<-p.done
	p.tr.Close()
	return p.node.Close()
}

// Wait blocks until the node stops serving.
func (p *Process) Wait() { <-p.done }
