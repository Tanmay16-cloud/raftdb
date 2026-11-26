package raft

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// simNode is one node of a simulated cluster: its consensus state, what it
// has persisted (which survives crashes) and its state machine, a list of
// applied entries rebuilt from the log after a crash.
type simNode struct {
	id      uint64
	r       *Raft
	up      bool
	hs      HardState
	disk    []Entry // persisted log
	applied []Entry
	pending []pendingReady // Readys whose disk writes are in flight, oldest first
}

// pendingReady is a Ready still being written to disk; its early messages
// have been sent.
type pendingReady struct {
	rd    Ready
	later []Message
}

type sim struct {
	t     *testing.T
	rng   *rand.Rand
	peers []uint64
	nodes map[uint64]*simNode
	net   []Message
	cut   map[[2]uint64]bool // links that drop every message

	// Invariant state, across the whole run.
	committed map[uint64]Entry  // index -> the entry first applied there
	leaders   map[uint64]uint64 // term -> its leader
	proposals int
	reads     map[uint64]uint64 // pending read ctx -> committed length when issued
	nextCtx   uint64
	confirmed int
	crashes   int
	maxBatch  int
	faults    bool           // whether the current step may inject faults
	rate      map[uint64]int // each node's clock speed, in percent
}

func newSim(t *testing.T, seed int64, n int) *sim {
	s := &sim{
		t: t, rng: rand.New(rand.NewSource(seed)), nodes: map[uint64]*simNode{},
		cut: map[[2]uint64]bool{}, committed: map[uint64]Entry{}, leaders: map[uint64]uint64{},
		reads: map[uint64]uint64{}, rate: map[uint64]int{},
	}
	for i := 1; i <= n; i++ {
		s.peers = append(s.peers, uint64(i))
		// Clocks drift: some nodes' clocks run at half speed. Raft's safety
		// must not depend on timing.
		s.rate[uint64(i)] = 50 + s.rng.Intn(51)
	}
	for _, id := range s.peers {
		nd := &simNode{id: id}
		s.nodes[id] = nd
		s.start(nd)
	}
	return s
}

// start boots a node from its persisted state. Its state machine starts
// empty, so every committed entry is applied again.
func (s *sim) start(nd *simNode) {
	nd.r = New(Config{
		ID: nd.id, Peers: s.peers, ElectionTicks: 10, HeartbeatTicks: 2, MaxBatch: cmp.Or(s.maxBatch, 4),
		Rand:      rand.New(rand.NewSource(s.rng.Int63())),
		HardState: nd.hs, Entries: slices.Clone(nd.disk),
	})
	nd.applied = nil
	nd.pending = nil
	nd.up = true
}

// handleReady does what a real node does with Ready: persist, send, apply.
// A leader sends its appends and heartbeats first, so its disk write
// overlaps the followers'. Under faults, writes are also asynchronous and
// pipelined: several Readys may be in flight, finishing in order, while
// the node keeps stepping messages and handing out new Readys (whose early
// messages may announce a commit index); and the node may crash with
// writes in flight.
func (s *sim) handleReady(nd *simNode) {
	if len(nd.pending) > 0 && (!s.faults || s.rng.Intn(2) == 0) {
		p := nd.pending[0]
		nd.pending = nd.pending[1:]
		s.finishReady(nd, p.rd, p.later)
		return
	}
	if !nd.r.HasReady() {
		return
	}
	rd := nd.r.Ready()
	var later []Message
	for _, m := range rd.Messages {
		if EarlySend(m) {
			s.net = append(s.net, m)
		} else {
			later = append(later, m)
		}
	}
	if len(nd.pending) > 0 || (s.faults && s.rng.Intn(3) == 0) {
		nd.pending = append(nd.pending, pendingReady{rd, later})
		return
	}
	s.finishReady(nd, rd, later)
}

func (s *sim) finishReady(nd *simNode, rd Ready, later []Message) {
	if rd.HardState.Term < nd.hs.Term || (rd.HardState.Term == nd.hs.Term && nd.hs.Vote != 0 && rd.HardState.Vote != nd.hs.Vote) {
		s.t.Fatalf("node %d: hard state went backwards: %+v -> %+v", nd.id, nd.hs, rd.HardState)
	}
	nd.hs = rd.HardState
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].Index
		if first > uint64(len(nd.disk))+1 {
			s.t.Fatalf("node %d: gap in persisted log", nd.id)
		}
		for _, e := range nd.disk[first-1:] {
			if c, ok := s.committed[e.Index]; ok && c.Term == e.Term && e.Index < first+uint64(len(rd.Entries)) && rd.Entries[e.Index-first].Term != e.Term {
				s.t.Fatalf("node %d: overwrote committed entry %d", nd.id, e.Index)
			}
		}
		nd.disk = append(nd.disk[:first-1], rd.Entries...)
		last := rd.Entries[len(rd.Entries)-1]
		nd.r.Persisted(last.Index, last.Term)
	}
	s.net = append(s.net, later...)
	for _, e := range rd.Committed {
		if e.Index != uint64(len(nd.applied))+1 {
			s.t.Fatalf("node %d: applied %d after %d", nd.id, e.Index, len(nd.applied))
		}
		// State machine safety: every node applies the same entry at each
		// index.
		if c, ok := s.committed[e.Index]; ok {
			if c.Term != e.Term || !bytes.Equal(c.Data, e.Data) {
				s.t.Fatalf("node %d applied %+v at index %d, but %+v was committed there", nd.id, e, e.Index, c)
			}
		} else {
			s.committed[e.Index] = e
		}
		nd.applied = append(nd.applied, e)
	}
	for _, rs := range rd.Reads {
		// Linearizable reads: the read index covers every entry that was
		// committed anywhere when the read was requested.
		if want, ok := s.reads[rs.Context]; ok {
			if rs.Index < want {
				s.t.Fatalf("node %d: read %d confirmed at index %d, but %d entries were committed when it began", nd.id, rs.Context, rs.Index, want)
			}
			delete(s.reads, rs.Context)
			s.confirmed++
		}
	}
	for _, ctx := range rd.DroppedReads {
		delete(s.reads, ctx)
	}
}

// check verifies the invariants that must hold after every step.
func (s *sim) check() {
	for _, nd := range s.nodes {
		if !nd.up {
			continue
		}
		state, term, _ := nd.r.State()
		if state != Leader {
			continue
		}
		// Election safety: at most one leader per term.
		if l, ok := s.leaders[term]; ok && l != nd.id {
			s.t.Fatalf("two leaders in term %d: %d and %d", term, l, nd.id)
		}
		if _, ok := s.leaders[term]; !ok {
			s.leaders[term] = nd.id
			// Leader completeness: a new leader holds every entry
			// committed before its election.
			for i, c := range s.committed {
				if i > nd.r.LastIndex() || nd.r.log[i].Term != c.Term {
					s.t.Fatalf("leader %d of term %d lacks committed entry %d", nd.id, term, i)
				}
			}
		}
	}
	// Log matching: if two logs have an entry with the same index and term,
	// they are identical up to it. Checking the last common such entry
	// suffices.
	for _, a := range s.nodes {
		for _, b := range s.nodes {
			if a.id >= b.id || !a.up || !b.up {
				continue
			}
			la, lb := a.r.log, b.r.log
			for i := min(len(la), len(lb)) - 1; i > 0; i-- {
				if la[i].Term == lb[i].Term {
					for j := 1; j <= i; j++ {
						if la[j].Term != lb[j].Term || !bytes.Equal(la[j].Data, lb[j].Data) {
							s.t.Fatalf("logs of %d and %d match at %d but differ at %d", a.id, b.id, i, j)
						}
					}
					break
				}
			}
		}
	}
}

func (s *sim) leader() *simNode {
	var best *simNode
	for _, nd := range s.nodes {
		if st, term, _ := nd.r.State(); nd.up && st == Leader {
			if best == nil {
				best = nd
			} else if _, bt, _ := best.r.State(); term > bt {
				best = nd
			}
		}
	}
	return best
}

// anyLeader returns a random node that believes it is the leader. It may
// be a stale leader cut off in a minority, which must not commit
// anything or confirm reads.
func (s *sim) anyLeader() *simNode {
	var ls []*simNode
	for _, id := range s.peers {
		if nd := s.nodes[id]; nd.up {
			if st, _, _ := nd.r.State(); st == Leader {
				ls = append(ls, nd)
			}
		}
	}
	if len(ls) == 0 {
		return nil
	}
	return ls[s.rng.Intn(len(ls))]
}

func (s *sim) deliver(i int, keep bool) {
	m := s.net[i]
	if !keep {
		s.net = slices.Delete(s.net, i, i+1)
	}
	nd := s.nodes[m.To]
	if nd.up && !s.cut[[2]uint64{m.From, m.To}] {
		m.Entries = slices.Clone(m.Entries)
		nd.r.Step(m)
	}
}

// step performs one random action.
func (s *sim) step(faults bool) {
	s.faults = faults
	ids := s.peers
	nd := s.nodes[ids[s.rng.Intn(len(ids))]]
	switch r := s.rng.Intn(100); {
	case r < 8:
		// Time passes for every node at once, at each node's own rate.
		for _, id := range ids {
			if n := s.nodes[id]; n.up {
				if s.rng.Intn(100) < s.rate[id] {
					n.r.Tick()
				}
			} else if s.rng.Intn(10) == 0 {
				s.start(n) // a crashed node restarts after a while
			}
		}
	case r < 55:
		// Every link carries messages at once, so a bigger cluster moves
		// more of them per step.
		for range max(1, len(ids)-1) {
			if len(s.net) > 0 {
				i := s.rng.Intn(len(s.net)) // any message: reordering
				s.deliver(i, faults && s.rng.Intn(20) == 0)
			}
		}
	case r < 75:
		if nd.up {
			s.handleReady(nd)
		}
	case r < 82:
		if l := s.anyLeader(); l != nil {
			s.proposals++
			l.r.Propose([]byte(fmt.Sprintf("p%d", s.proposals)))
		}
	case r < 85:
		if l := s.anyLeader(); l != nil {
			s.nextCtx++
			if l.r.ReadIndex(s.nextCtx) == nil {
				s.reads[s.nextCtx] = uint64(len(s.committed))
			}
		}
	case !faults:
	case r < 90:
		if len(s.net) > 0 {
			i := s.rng.Intn(len(s.net))
			s.net = slices.Delete(s.net, i, i+1) // drop
		}
	case r < 99:
		// Network faults come and go. A partition lasts a few hundred
		// steps on average, longer than an election, so the cut-off side
		// may keep a stale leader while the rest elect a new one.
		switch f := s.rng.Intn(27); {
		case f < 2:
			// Partition: cut a random node off from the rest, both ways.
			for _, p := range ids {
				if p != nd.id {
					s.cut[[2]uint64{p, nd.id}] = true
					s.cut[[2]uint64{nd.id, p}] = true
				}
			}
		case f == 2:
			// Cut one link in one direction.
			s.cut[[2]uint64{nd.id, ids[s.rng.Intn(len(ids))]}] = true
		case f == 3:
			clear(s.cut)
		}
	default:
		// Crash, about once every 600 steps: lose everything not
		// persisted, including unsent messages and unhandled Ready output.
		if nd.up && s.rng.Intn(6) == 0 {
			if l := s.leader(); l != nil && s.rng.Intn(2) == 0 {
				nd = l // crash leaders often: that is where the trouble is
			}
			nd.up = false
			nd.applied = nil
			s.crashes++
		}
	}
	s.check()
}

// heal removes all faults and runs the cluster until it settles.
func (s *sim) heal() {
	clear(s.cut)
	for _, nd := range s.nodes {
		if !nd.up {
			s.start(nd)
		}
	}
	for i := 0; i < 20000; i++ {
		s.step(false)
	}
	// Drain: deliver everything and handle every Ready.
	for range 200 {
		for _, nd := range s.nodes {
			nd.r.Tick()
			s.handleReady(nd)
		}
		for len(s.net) > 0 {
			s.deliver(0, false)
			for _, nd := range s.nodes {
				s.handleReady(nd)
			}
		}
	}
	s.check()
}

func TestSimulatedCluster(t *testing.T) {
	for _, size := range []int{1, 3, 5} {
		for seed := int64(1); seed <= 8; seed++ {
			t.Run(fmt.Sprintf("n%d/seed%d", size, seed), func(t *testing.T) {
				s := newSim(t, seed*100+int64(size), size)
				leaderSteps := 0
				for range 30000 {
					s.step(true)
					if s.leader() != nil {
						leaderSteps++
					}
				}
				faultyCommits, faultyTerms := len(s.committed), len(s.leaders)
				// The faults must leave the cluster working part of the
				// time, or the run tests nothing.
				if size > 1 && (faultyCommits < 50 || faultyTerms < 10 || s.crashes < 20) {
					t.Fatalf("faulty phase too quiet: leader %d%% of steps, %d commits, %d terms, %d crashes", leaderSteps*100/30000, faultyCommits, faultyTerms, s.crashes)
				}
				s.heal()

				// Liveness after healing: one leader, every node has
				// applied the same, complete committed log, and a fresh
				// proposal commits everywhere.
				l := s.leader()
				if l == nil {
					t.Fatal("no leader after healing")
				}
				before := len(s.committed)
				l.r.Propose([]byte("final"))
				s.heal()
				if len(s.committed) <= before {
					t.Fatal("a proposal after healing did not commit")
				}
				for _, nd := range s.nodes {
					if len(nd.applied) != len(s.committed) {
						t.Fatalf("node %d applied %d of %d committed entries", nd.id, len(nd.applied), len(s.committed))
					}
				}
				t.Logf("under faults: leader %d%% of steps, %d commits, %d terms with a leader, %d crashes; total %d commits, %d reads confirmed",
					leaderSteps*100/30000, faultyCommits, faultyTerms, s.crashes, len(s.committed), s.confirmed)
			})
		}
	}
}

// flush delivers, in order, the messages allow accepts, handling every
// node's Ready as it goes, until none are left. Other messages are lost.
func (s *sim) flush(allow func(Message) bool) {
	for range 100000 {
		for _, id := range s.peers {
			if nd := s.nodes[id]; nd.up {
				s.handleReady(nd)
			}
		}
		i := slices.IndexFunc(s.net, allow)
		if i < 0 {
			s.net = nil
			return
		}
		m := s.net[i]
		s.net = slices.Delete(s.net, 0, i+1)
		if nd := s.nodes[m.To]; nd.up {
			nd.r.Step(m)
		}
		s.check()
	}
	s.t.Fatal("flush did not settle")
}

func (s *sim) crash(id uint64) {
	s.nodes[id].up = false
	s.nodes[id].applied = nil
}

func (s *sim) elect(id uint64, allow func(Message) bool) {
	for range 5 {
		s.nodes[id].r.Campaign()
		s.flush(allow)
		if st, _, _ := s.nodes[id].r.State(); st == Leader {
			return
		}
	}
	for _, n := range s.peers {
		st, term, lead := s.nodes[n].r.State()
		s.t.Logf("node %d: up %v %v term %d lead %d vote %d last %d/%d", n, s.nodes[n].up, st, term, lead, s.nodes[n].r.vote, s.nodes[n].r.lastIndex(), s.nodes[n].r.lastTerm())
	}
	s.t.Fatalf("node %d could not win an election", id)
}

func among(ids ...uint64) func(Message) bool {
	return func(m Message) bool { return slices.Contains(ids, m.From) && slices.Contains(ids, m.To) }
}

// TestFigure8 replays the scenario of Figure 8 in the Raft paper, where a
// leader that counted replicas to commit an entry from an earlier term
// would lose a committed entry. Raft forbids it: a leader commits only
// entries of its own term by counting, and earlier ones with them.
func TestFigure8(t *testing.T) {
	s := newSim(t, 1, 5)
	s.maxBatch = 1
	for _, nd := range s.nodes {
		s.start(nd)
	}
	all := func(Message) bool { return true }

	// S1 leads term 1 and replicates entry 2 to S2 only.
	s.elect(1, all)
	s.nodes[1].r.Propose([]byte("x"))
	s.flush(among(1, 2))

	// S1 crashes. S5 wins term 2 with votes from S3 and S4, and appends
	// its no-op at index 2, but replicates nothing.
	s.crash(1)
	s.elect(5, func(m Message) bool { return m.Type == MsgVote || m.Type == MsgVoteResp })

	// S5 crashes. S1 comes back, wins a later term, and replicates its
	// entry 2 (from term 1) to S3. It is now on a majority (S1, S2, S3),
	// but S1's own no-op at index 3 must not reach them.
	s.crash(5)
	s.start(s.nodes[1])
	inGroup := among(1, 2, 3)
	withoutIndex3 := func(m Message) bool {
		if !inGroup(m) {
			return false
		}
		for _, e := range m.Entries {
			if e.Index >= 3 {
				return false
			}
		}
		return true
	}
	s.elect(1, withoutIndex3)
	for range 4 {
		s.nodes[1].r.Tick()
	}
	s.flush(withoutIndex3)

	// S1 crashes before replicating index 3. S5 comes back and wins: its
	// last entry has term 2, newer than anyone else's. It overwrites
	// index 2 everywhere, which is only safe if index 2 was never
	// committed.
	s.crash(1)
	s.start(s.nodes[5])
	s.elect(5, among(2, 3, 4, 5))
	for range 4 {
		s.nodes[5].r.Tick()
		s.flush(among(2, 3, 4, 5))
	}
	if c, ok := s.committed[2]; !ok || c.Term != 2 {
		t.Fatalf("index 2 committed as %+v (present %v), want S5's entry from term 2", c, ok)
	}
}

// TestStaleLeaderCannotConfirmReads: a leader's clock stops (a long GC
// pause, a suspended VM) while the rest of the cluster elects a new leader
// and commits more. When the old leader resumes, it still believes it
// leads. A read it serves from its own state would be stale, so ReadIndex
// must not confirm without hearing from a quorum, which now answers with
// a higher term and deposes it.
func TestStaleLeaderCannotConfirmReads(t *testing.T) {
	s := newSim(t, 1, 3)
	all := func(Message) bool { return true }
	s.elect(1, all)
	s.nodes[1].r.Propose([]byte("x"))
	s.flush(all)

	// Node 1 freezes. Nodes 2 and 3 elect a leader and commit an entry.
	var lead *simNode
	for range 200 {
		for _, id := range []uint64{2, 3} {
			s.nodes[id].r.Tick()
		}
		s.flush(among(2, 3))
		if l := s.leader(); l != nil && l.id != 1 {
			lead = l
			break
		}
	}
	if lead == nil {
		t.Fatal("nodes 2 and 3 did not elect a leader")
	}
	lead.r.Propose([]byte("y"))
	s.flush(among(2, 3))
	if st, _, _ := s.nodes[1].r.State(); st != Leader {
		t.Fatal("node 1 should still believe it leads")
	}

	// Node 1 resumes and is asked for a linearizable read.
	s.nextCtx++
	ctx := s.nextCtx
	if err := s.nodes[1].r.ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
	s.reads[ctx] = uint64(len(s.committed))
	confirmed := s.confirmed
	s.flush(all) // handleReady checks any confirmation against what was committed
	if s.confirmed != confirmed || s.reads[ctx] != 0 {
		t.Fatalf("stale leader's read: confirmed %v, still pending %v", s.confirmed != confirmed, s.reads[ctx] != 0)
	}
	if st, _, _ := s.nodes[1].r.State(); st == Leader {
		t.Fatal("the stale leader did not step down")
	}
}
