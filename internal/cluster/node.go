// Package cluster runs a replicated raftdb node: the Raft core driven by a
// goroutine, a persistent Raft log, the replicated MVCC store, and a
// transport to the other nodes.
//
// A Node is an engine.Backend, so a SQL database runs on top of it with
// engine.New. Transactions read from the leader's local store and commit
// by proposing their command through Raft; every node applies committed
// commands to its own store in log order.
package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"raftdb/internal/mvcc"
	"raftdb/internal/raft"
	"raftdb/internal/storage"
)

// Transport carries messages to other nodes. Send must not block; it may
// drop messages, which Raft tolerates.
type Transport interface {
	Send(msgs []raft.Message)
}

// Config configures a node.
type Config struct {
	ID        uint64
	Peers     []uint64 // every member, including ID
	Dir       string   // holds raft.log and data.db
	Transport Transport

	TickInterval   time.Duration // default 10ms
	ElectionTicks  int           // default 10
	HeartbeatTicks int           // default 2
	RequestTimeout time.Duration // default 5s
	Storage        storage.Options
}

var (
	// ErrTimeout means a request got no answer in time, for example
	// because the node is cut off from a quorum. A commit that times out
	// may or may not have happened.
	ErrTimeout = errors.New("cluster: request timed out")
	// ErrLeaderChanged means leadership changed before a proposal was
	// committed, and it was lost; it is safe to retry.
	ErrLeaderChanged = errors.New("cluster: leadership changed; the request was not applied")
	// ErrStopped means the node is shutting down.
	ErrStopped = errors.New("cluster: node stopped")
)

// NotLeaderError means a request reached a node that isn't the leader.
type NotLeaderError struct {
	Leader uint64 // the leader this node knows of, or 0
}

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "cluster: not the leader, and no leader is known"
	}
	return fmt.Sprintf("cluster: not the leader; the leader is node %d", e.Leader)
}

func (e *NotLeaderError) Unwrap() error { return raft.ErrNotLeader }

// Node is one member of a cluster.
type Node struct {
	id    uint64
	cfg   Config
	log   *raft.LogStore
	tree  *storage.BTree
	store *mvcc.Store

	inbox chan raft.Message
	props chan *proposal
	reads chan *readRequest
	stop  chan struct{}
	done  chan struct{}

	mu     sync.Mutex
	status Status
	err    error // the fatal error that stopped the node, if any
}

// Status describes a node's view of the cluster.
type Status struct {
	ID      uint64
	State   raft.State
	Term    uint64
	Leader  uint64
	Commit  uint64
	Applied uint64
}

type proposal struct {
	data  []byte
	term  uint64 // set when proposed
	reply chan proposalResult
}

type proposalResult struct {
	res mvcc.Result
	err error
}

type readRequest struct {
	index uint64 // set when confirmed
	reply chan error
}

// Open starts the node, recovering its state from cfg.Dir.
func Open(cfg Config) (*Node, error) {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 10 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 2
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	log, err := raft.OpenLogStore(filepath.Join(cfg.Dir, "raft.log"))
	if err != nil {
		return nil, err
	}
	hs, ents, err := log.Load()
	if err != nil {
		log.Close()
		return nil, err
	}
	tree, err := storage.OpenWithOptions(filepath.Join(cfg.Dir, "data.db"), cfg.Storage)
	if err != nil {
		log.Close()
		return nil, err
	}
	store, err := mvcc.Open(tree)
	if err != nil {
		tree.Close()
		log.Close()
		return nil, err
	}
	r := raft.New(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers,
		ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks, MaxBatch: 64,
		HardState: hs, Entries: ents, Applied: store.Applied(),
	})
	n := &Node{
		id: cfg.ID, cfg: cfg, log: log, tree: tree, store: store,
		inbox: make(chan raft.Message, 1024),
		props: make(chan *proposal, 256),
		reads: make(chan *readRequest, 256),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	n.status = Status{ID: cfg.ID, Applied: store.Applied()}
	go n.run(r)
	return n, nil
}

// Step delivers a message from another node. It never blocks: if the
// node is overwhelmed, the message is dropped.
func (n *Node) Step(m raft.Message) {
	select {
	case n.inbox <- m:
	default:
	}
}

// Status returns the node's current view of the cluster.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status
}

// Store returns the node's local replica, for inspection.
func (n *Node) Store() *mvcc.Store { return n.store }

// Close stops the node and closes its storage. Applied commands are
// flushed; a later Open resumes from where it left off.
func (n *Node) Close() error {
	select {
	case <-n.stop:
	default:
		close(n.stop)
	}
	<-n.done
	err := n.store.Flush()
	if e := n.tree.Close(); err == nil {
		err = e
	}
	if e := n.log.Close(); err == nil {
		err = e
	}
	return err
}

// Crash stops the node without flushing applied commands, as if the
// process died: on restart, it replays its log. For tests.
func (n *Node) Crash() {
	select {
	case <-n.stop:
	default:
		close(n.stop)
	}
	<-n.done
	n.tree.Rollback() // lose everything not yet flushed
	n.tree.Close()
	n.log.Close()
}

// ---------------------------------------------------------------------------
// The engine.Backend interface

// Snapshot starts a snapshot for a transaction. It is linearizable: the
// node first confirms with a quorum that it is still the leader (Raft's
// ReadIndex), and waits until it has applied everything committed before
// the request. A stale leader can't serve a snapshot missing recent
// commits.
func (n *Node) Snapshot() (*mvcc.Txn, uint64, error) {
	deadline := time.Now().Add(n.cfg.RequestTimeout)
	for {
		rq := &readRequest{reply: make(chan error, 1)}
		select {
		case n.reads <- rq:
		case <-n.done:
			return nil, 0, n.stoppedErr()
		}
		err := n.wait(rq.reply, deadline)
		if errors.Is(err, raft.ErrNotReady) && time.Now().Before(deadline) {
			// A new leader that hasn't committed an entry of its term
			// yet; that takes one round trip.
			time.Sleep(n.cfg.TickInterval)
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		return n.store.Snapshot()
	}
}

// Execute proposes a command and waits until it is committed and applied,
// returning its result.
func (n *Node) Execute(c *mvcc.Command) (mvcc.Result, error) {
	p := &proposal{data: c.Encode(), reply: make(chan proposalResult, 1)}
	select {
	case n.props <- p:
	case <-n.done:
		return mvcc.Result{}, n.stoppedErr()
	}
	select {
	case r := <-p.reply:
		return r.res, r.err
	case <-time.After(n.cfg.RequestTimeout):
		return mvcc.Result{}, ErrTimeout
	case <-n.done:
		return mvcc.Result{}, n.stoppedErr()
	}
}

// Horizon returns the oldest version a snapshot open on this node may
// need. Only the leader serves snapshots, so its horizon covers them all.
func (n *Node) Horizon() uint64 { return n.store.Horizon() }

func (n *Node) wait(ch chan error, deadline time.Time) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Until(deadline)):
		return ErrTimeout
	case <-n.done:
		return n.stoppedErr()
	}
}

func (n *Node) stoppedErr() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return n.err
	}
	return ErrStopped
}

// ---------------------------------------------------------------------------
// The event loop

// flushEvery is how many applied commands may be lost from the store on a
// crash (they are replayed from the log) before the node flushes it.
const flushEvery = 256

func (n *Node) run(r *raft.Raft) {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	waiters := map[uint64]*proposal{}    // by log index
	pending := map[uint64]*readRequest{} // by ReadIndex context
	var confirmed []*readRequest         // awaiting apply
	var ctx uint64
	applied := n.store.Applied()
	unflushed := 0
	var saved raft.HardState

	fail := func(err error) {
		n.mu.Lock()
		n.err = fmt.Errorf("cluster: node %d failed: %w", n.id, err)
		n.mu.Unlock()
	}
	notLeader := func() error {
		_, _, lead := r.State()
		return &NotLeaderError{Leader: lead}
	}

	propose := func(p *proposal) {
		index, term, err := r.Propose(p.data)
		if err != nil {
			p.reply <- proposalResult{err: notLeader()}
			return
		}
		p.term = term
		waiters[index] = p
	}
	read := func(rq *readRequest) {
		ctx++
		if err := r.ReadIndex(ctx); err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				err = notLeader()
			}
			rq.reply <- err
			return
		}
		pending[ctx] = rq
	}

	for {
		// Wait for something to do, unless the last round left work behind
		// (Persisted can advance the commit index by itself).
		if r.HasReady() {
			select {
			case <-n.stop:
				return
			default:
			}
		} else {
			select {
			case <-n.stop:
				return
			case <-ticker.C:
				r.Tick()
			case m := <-n.inbox:
				r.Step(m)
			case p := <-n.props:
				propose(p)
			case rq := <-n.reads:
				read(rq)
			}
		}
		// Group commit: take everything else already waiting before
		// handling the Ready, so that concurrent transactions share one
		// log append, one fsync and one round of messages.
	drain:
		for range 1024 {
			select {
			case m := <-n.inbox:
				r.Step(m)
			case p := <-n.props:
				propose(p)
			case rq := <-n.reads:
				read(rq)
			default:
				break drain
			}
		}

		if !r.HasReady() {
			continue
		}
		rd := r.Ready()
		// A leader's appends go out before its own disk write, so the
		// leader and its followers fsync in parallel (thesis section
		// 10.2.1); the leader counts its copy only once Persisted says
		// so. Everything else waits for the write: a vote or an
		// acknowledgement must never outrun the state it vouches for.
		var later []raft.Message
		var early []raft.Message
		for _, m := range rd.Messages {
			if raft.EarlySend(m) {
				early = append(early, m)
			} else {
				later = append(later, m)
			}
		}
		if len(early) > 0 {
			n.cfg.Transport.Send(early)
		}
		// The commit index is part of HardState but needn't be durable
		// (after a restart, committed entries are learned again and
		// replayed), so a change to it alone costs no fsync: it rides
		// along with the next write of entries, term or vote.
		if rd.HardState.Term != saved.Term || rd.HardState.Vote != saved.Vote || len(rd.Entries) > 0 {
			if err := n.log.Save(rd.HardState, rd.Entries); err != nil {
				fail(err)
				return
			}
			saved = rd.HardState
		}
		if len(rd.Entries) > 0 {
			last := rd.Entries[len(rd.Entries)-1]
			r.Persisted(last.Index, last.Term)
		}
		if len(later) > 0 {
			n.cfg.Transport.Send(later)
		}
		for _, e := range rd.Committed {
			c := &mvcc.Command{Op: mvcc.OpNoop}
			if len(e.Data) > 0 {
				var err error
				if c, err = mvcc.DecodeCommand(e.Data); err != nil {
					fail(fmt.Errorf("entry %d: %w", e.Index, err))
					return
				}
			}
			res, err := n.store.Apply(e.Index, c)
			if err != nil {
				fail(fmt.Errorf("applying entry %d: %w", e.Index, err))
				return
			}
			applied = e.Index
			unflushed++
			if p, ok := waiters[e.Index]; ok {
				delete(waiters, e.Index)
				if p.term == e.Term {
					p.reply <- proposalResult{res: res}
				} else {
					p.reply <- proposalResult{err: ErrLeaderChanged}
				}
			}
		}
		if unflushed >= flushEvery {
			if err := n.store.Flush(); err != nil {
				fail(err)
				return
			}
			unflushed = 0
		}
		for _, rs := range rd.Reads {
			if rq, ok := pending[rs.Context]; ok {
				delete(pending, rs.Context)
				rq.index = rs.Index
				confirmed = append(confirmed, rq)
			}
		}
		for _, c := range rd.DroppedReads {
			if rq, ok := pending[c]; ok {
				delete(pending, c)
				rq.reply <- notLeader()
			}
		}
		kept := confirmed[:0]
		for _, rq := range confirmed {
			if rq.index <= applied {
				rq.reply <- nil
			} else {
				kept = append(kept, rq)
			}
		}
		confirmed = kept

		state, term, lead := r.State()
		n.mu.Lock()
		n.status = Status{ID: n.id, State: state, Term: term, Leader: lead, Commit: r.Commit(), Applied: applied}
		n.mu.Unlock()
	}
}
