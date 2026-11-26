package cluster

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raftdb/internal/raft"
	"raftdb/internal/wire"
)

// ---------------------------------------------------------------------------
// In-memory network, for tests

// MemNetwork connects nodes in one process, with injectable faults.
type MemNetwork struct {
	mu    sync.Mutex
	nodes map[uint64]*Node
	cut   map[[2]uint64]bool
	loss  float64 // probability of dropping each message
	rng   *rand.Rand
}

// NewMemNetwork returns an empty network.
func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: map[uint64]*Node{}, cut: map[[2]uint64]bool{}, rng: rand.New(rand.NewSource(1))}
}

// Attach connects node to the network (replacing an earlier incarnation).
func (net *MemNetwork) Attach(n *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[n.id] = n
}

// Detach disconnects a node, as when it crashes.
func (net *MemNetwork) Detach(id uint64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.nodes, id)
}

// Isolate cuts id off from every other node, both ways.
func (net *MemNetwork) Isolate(id uint64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	for other := range net.nodes {
		if other != id {
			net.cut[[2]uint64{id, other}] = true
			net.cut[[2]uint64{other, id}] = true
		}
	}
}

// CutOneWay drops every message from one node to another.
func (net *MemNetwork) CutOneWay(from, to uint64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.cut[[2]uint64{from, to}] = true
}

// Heal restores every link.
func (net *MemNetwork) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	clear(net.cut)
}

// SetLoss makes the network drop each message with probability p.
func (net *MemNetwork) SetLoss(p float64) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.loss = p
}

// Transport returns the transport for node id.
func (net *MemNetwork) Transport(id uint64) Transport { return memTransport{net, id} }

type memTransport struct {
	net *MemNetwork
	id  uint64
}

func (t memTransport) Send(msgs []raft.Message) {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	for _, m := range msgs {
		if t.net.cut[[2]uint64{m.From, m.To}] || (t.net.loss > 0 && t.net.rng.Float64() < t.net.loss) {
			continue
		}
		if n := t.net.nodes[m.To]; n != nil {
			n.Step(m)
		}
	}
}

// ---------------------------------------------------------------------------
// gRPC
//
// Nodes talk over a single gRPC method, raftdb.Raft/Send, which carries a
// batch of Raft messages. There is no .proto file: the service descriptor
// is written by hand and messages encode themselves (package wire), so the
// build needs no code generator.

const sendMethod = "/raftdb.Raft/Send"

// batch is a list of Raft messages, the request of raftdb.Raft/Send.
type batch struct{ msgs []raft.Message }

func (b *batch) MarshalBinary() ([]byte, error) {
	out := wire.AppendUvarint(nil, uint64(len(b.msgs)))
	for _, m := range b.msgs {
		out = wire.AppendBytes(out, raft.EncodeMessage(m))
	}
	return out, nil
}

func (b *batch) UnmarshalBinary(data []byte) error {
	r := wire.NewReader(data)
	for range r.Count() {
		m, err := raft.DecodeMessage(r.Bytes())
		if err != nil {
			return err
		}
		b.msgs = append(b.msgs, m)
	}
	return r.Err()
}

// raftService is implemented by *Node; gRPC checks the registered value
// against it.
type raftService interface {
	Step(raft.Message)
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "raftdb.Raft",
	HandlerType: (*raftService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Send",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			var b batch
			if err := dec(&b); err != nil {
				return nil, err
			}
			for _, m := range b.msgs {
				srv.(raftService).Step(m)
			}
			return &wire.Empty{}, nil
		},
	}},
}

// NewGRPCServer returns a gRPC server using raftdb's codec, ready for
// Register and other raftdb services.
func NewGRPCServer() *grpc.Server {
	return grpc.NewServer(grpc.ForceServerCodec(wire.Codec{}))
}

// Register adds the Raft service for node to s.
func Register(s *grpc.Server, node *Node) {
	s.RegisterService(&serviceDesc, node)
}

// Serve accepts Raft traffic for node on lis until the returned server is
// stopped.
func Serve(lis net.Listener, node *Node) *grpc.Server {
	s := NewGRPCServer()
	Register(s, node)
	go s.Serve(lis)
	return s
}

// GRPCTransport sends messages to peers over gRPC. Each peer has a queue
// and a sender goroutine that batches whatever has queued up into one
// call. A full queue or a failed call drops messages; Raft retries.
type GRPCTransport struct {
	peers map[uint64]*grpcPeer
	stop  chan struct{}
	wg    sync.WaitGroup
}

type grpcPeer struct {
	conn  *grpc.ClientConn
	queue chan raft.Message
}

// NewGRPCTransport connects to the peers at addrs (by node ID), skipping
// self. Connections are made lazily, so peers needn't be up yet.
func NewGRPCTransport(self uint64, addrs map[uint64]string) (*GRPCTransport, error) {
	t := &GRPCTransport{peers: map[uint64]*grpcPeer{}, stop: make(chan struct{})}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(wire.Codec{})))
		if err != nil {
			t.Close()
			return nil, err
		}
		p := &grpcPeer{conn: conn, queue: make(chan raft.Message, 4096)}
		t.peers[id] = p
		t.wg.Add(1)
		go t.sender(p)
	}
	return t, nil
}

func (t *GRPCTransport) sender(p *grpcPeer) {
	defer t.wg.Done()
	for {
		var b batch
		select {
		case m := <-p.queue:
			b.msgs = append(b.msgs, m)
		case <-t.stop:
			return
		}
	drain:
		for len(b.msgs) < 256 {
			select {
			case m := <-p.queue:
				b.msgs = append(b.msgs, m)
			default:
				break drain
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		p.conn.Invoke(ctx, sendMethod, &b, &wire.Empty{}) // errors mean lost messages
		cancel()
	}
}

// Send queues messages for their peers.
func (t *GRPCTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if p := t.peers[m.To]; p != nil {
			select {
			case p.queue <- m:
			default: // queue full: drop
			}
		}
	}
}

// Close stops the senders and closes the connections.
func (t *GRPCTransport) Close() error {
	select {
	case <-t.stop:
	default:
		close(t.stop)
	}
	t.wg.Wait()
	for _, p := range t.peers {
		p.conn.Close()
	}
	return nil
}
