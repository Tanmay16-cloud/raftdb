// Package raft implements the Raft consensus algorithm (Ongaro and
// Ousterhout, "In Search of an Understandable Consensus Algorithm", 2014).
//
// The core is a deterministic state machine with no I/O, goroutines or
// clocks, in the style of etcd's raft library. The caller feeds it time
// (Tick), incoming messages (Step) and client requests (Propose,
// ReadIndex), and collects its output with Ready: state and log entries to
// persist, messages to send, and committed entries to apply. Because the
// core does nothing on its own, tests can drive whole clusters through a
// simulated network, step by step and reproducibly.
//
// The caller must handle each Ready in order: persist HardState and
// Entries durably, call Persisted, then send Messages and apply Committed.
// One exception speeds up the leader (thesis section 10.2.1): messages
// for which EarlySend is true may be sent before persisting, so that the
// leader's disk write overlaps its followers'. The leader counts its own
// copy of an entry towards a quorum only once Persisted says it is on
// disk.
package raft

import (
	"errors"
	"math/rand"
	"slices"
)

// State is a node's role.
type State uint8

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

// Entry is one entry of the replicated log.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte // nil for the no-op a new leader appends
}

// MsgType is the kind of a Message.
type MsgType uint8

const (
	MsgVote          MsgType = iota // request a vote
	MsgVoteResp                     // grant or refuse a vote
	MsgApp                          // append entries
	MsgAppResp                      // accept or reject appended entries
	MsgHeartbeat                    // assert leadership; confirm reads
	MsgHeartbeatResp                // acknowledge a heartbeat
	MsgPreVote                      // ask whether a vote would be granted
	MsgPreVoteResp                  // answer a pre-vote
)

func (t MsgType) String() string {
	return [...]string{"Vote", "VoteResp", "App", "AppResp", "Heartbeat", "HeartbeatResp", "PreVote", "PreVoteResp"}[t]
}

// Message is a message between nodes.
type Message struct {
	Type     MsgType
	From, To uint64
	Term     uint64

	// MsgVote: the candidate's last log entry. MsgApp: the entry before
	// Entries. MsgAppResp: on success, the index of the last entry known
	// to match the leader's log; on rejection, the rejected Index.
	LogTerm, Index uint64
	Entries        []Entry
	Commit         uint64 // MsgApp, MsgHeartbeat: the leader's commit index

	Reject bool
	Hint   uint64 // MsgAppResp rejection: where the leader should retry from

	Context uint64 // MsgHeartbeat(Resp): the ReadIndex request it confirms
}

// HardState is the state a node must persist before sending messages.
type HardState struct {
	Term   uint64 // the latest term the node has seen
	Vote   uint64 // whom it voted for in Term, or 0
	Commit uint64 // the commit index (persisting it is optional but saves work)
}

// ReadState says that a ReadIndex request is confirmed: once the node has
// applied every entry up to Index, a read reflects every write committed
// before the request.
type ReadState struct {
	Context uint64
	Index   uint64
}

// Ready is a batch of work for the caller.
type Ready struct {
	HardState HardState
	// Entries to persist. They replace any persisted entries at
	// Entries[0].Index and beyond.
	Entries   []Entry
	Messages  []Message
	Committed []Entry // to apply, in order
	Reads     []ReadState
	// ReadIndex requests that can no longer be confirmed, because the
	// node lost leadership.
	DroppedReads []uint64
}

// Config configures a node.
type Config struct {
	ID    uint64
	Peers []uint64 // every member of the cluster, including ID

	// Ticks without hearing from a leader before starting an election
	// (randomized up to twice this), and ticks between heartbeats.
	ElectionTicks  int
	HeartbeatTicks int
	MaxBatch       int // most entries per append message

	Rand *rand.Rand // randomizes election timeouts; nil for a default

	// State restored from storage after a restart.
	HardState HardState
	Entries   []Entry // the whole persisted log, starting at index 1
	Applied   uint64  // last entry the state machine has applied
}

var (
	// ErrNotLeader means only the leader can serve the request.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrNotReady means a new leader has not yet committed an entry of
	// its own term, so it cannot confirm reads yet.
	ErrNotReady = errors.New("raft: leader not ready")
)

// Raft is one node's consensus state.
type Raft struct {
	id    uint64
	peers []uint64

	state State
	term  uint64
	vote  uint64
	lead  uint64

	log       []Entry // log[0] is a sentinel with index 0
	persisted uint64  // the last log index the caller has made durable
	commit    uint64
	applied   uint64 // committed entries handed out by Ready

	electionTicks, heartbeatTicks, maxBatch int
	rng                                     *rand.Rand
	// Ticks since the node last heard from a leader (or, as leader, since
	// it last checked for a quorum), and since its last heartbeat.
	elapsed, timeout, hbElapsed int

	votes       map[uint64]bool   // (pre-)candidate: votes received
	next, match map[uint64]uint64 // leader: per-follower replication state
	active      map[uint64]bool   // leader: peers heard from this election timeout
	hbMatch     map[uint64]uint64 // leader: match as of each peer's last heartbeat response
	reads       []*readRequest    // leader: unconfirmed ReadIndex requests

	// Output not yet collected by Ready.
	msgs         []Message
	unstable     uint64 // first log index not yet handed out for persisting
	prevHS       HardState
	readStates   []ReadState
	droppedReads []uint64
}

type readRequest struct {
	ctx   uint64
	index uint64
	acks  map[uint64]bool
}

// New creates a node from cfg.
func New(cfg Config) *Raft {
	r := &Raft{
		id:             cfg.ID,
		peers:          slices.Clone(cfg.Peers),
		electionTicks:  max(cfg.ElectionTicks, 2),
		heartbeatTicks: max(cfg.HeartbeatTicks, 1),
		maxBatch:       max(cfg.MaxBatch, 1),
		rng:            cfg.Rand,
		term:           cfg.HardState.Term,
		vote:           cfg.HardState.Vote,
		commit:         cfg.HardState.Commit,
		applied:        cfg.Applied,
		log:            append([]Entry{{}}, cfg.Entries...),
	}
	if r.rng == nil {
		r.rng = rand.New(rand.NewSource(int64(cfg.ID)))
	}
	for i, e := range r.log {
		if e.Index != uint64(i) {
			panic("raft: restored log is not contiguous from index 1")
		}
	}
	r.commit = min(max(r.commit, r.applied), r.lastIndex())
	r.unstable = r.lastIndex() + 1
	r.persisted = r.lastIndex()
	r.prevHS = r.hardState()
	r.becomeFollower(r.term, 0)
	return r
}

// ID returns the node's ID.
func (r *Raft) ID() uint64 { return r.id }

// State returns the node's role, term, and the leader it knows of (0 if
// none).
func (r *Raft) State() (state State, term, lead uint64) { return r.state, r.term, r.lead }

// Commit returns the node's commit index.
func (r *Raft) Commit() uint64 { return r.commit }

// LastIndex returns the index of the node's last log entry.
func (r *Raft) LastIndex() uint64 { return r.lastIndex() }

func (r *Raft) lastIndex() uint64 { return uint64(len(r.log) - 1) }

func (r *Raft) lastTerm() uint64 { return r.log[len(r.log)-1].Term }

// termAt returns the term of the entry at index i, or 0 if there is none.
func (r *Raft) termAt(i uint64) uint64 {
	if i > r.lastIndex() {
		return 0
	}
	return r.log[i].Term
}

func (r *Raft) hardState() HardState { return HardState{r.term, r.vote, r.commit} }

func (r *Raft) quorum() int { return len(r.peers)/2 + 1 }

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) resetTimer() {
	r.elapsed = 0
	r.timeout = r.electionTicks + r.rng.Intn(r.electionTicks)
}

// ---------------------------------------------------------------------------
// Roles

func (r *Raft) becomeFollower(term, lead uint64) {
	if term > r.term {
		r.term, r.vote = term, 0
	}
	r.state, r.lead = Follower, lead
	r.votes, r.next, r.match, r.active, r.hbMatch = nil, nil, nil, nil, nil
	for _, rq := range r.reads {
		r.droppedReads = append(r.droppedReads, rq.ctx)
	}
	r.reads = nil
	r.resetTimer()
}

// preCampaign starts a pre-vote (thesis section 9.6): the node asks
// whether it would win an election, without raising its term. Peers that
// have heard from a leader recently say no. Without it, a node that
// can't hear the leader (cut off, or with a one-way network failure)
// keeps starting elections at ever higher terms, and each one deposes a
// working leader when the node's messages get through.
func (r *Raft) preCampaign() {
	r.becomeFollower(r.term, 0)
	r.state = PreCandidate
	r.votes = map[uint64]bool{r.id: true}
	if len(r.votes) >= r.quorum() {
		r.campaign()
		return
	}
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: MsgPreVote, To: p, Term: r.term + 1, LogTerm: r.lastTerm(), Index: r.lastIndex()})
		}
	}
}

func (r *Raft) campaign() {
	r.becomeFollower(r.term+1, 0)
	r.state, r.vote = Candidate, r.id
	r.votes = map[uint64]bool{r.id: true}
	if len(r.votes) >= r.quorum() {
		r.becomeLeader()
		return
	}
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: MsgVote, To: p, LogTerm: r.lastTerm(), Index: r.lastIndex()})
		}
	}
}

func (r *Raft) becomeLeader() {
	r.state, r.lead = Leader, r.id
	r.votes = nil
	r.next, r.match, r.active = map[uint64]uint64{}, map[uint64]uint64{}, map[uint64]bool{}
	r.hbMatch = map[uint64]uint64{}
	r.elapsed, r.hbElapsed = 0, 0
	for _, p := range r.peers {
		r.next[p] = r.lastIndex() + 1
	}
	r.match[r.id] = r.persisted
	// A leader may only count replicas to commit entries of its own term
	// (section 5.4.2), so it commits a no-op right away. That commits
	// everything before it, and lets the leader serve reads.
	r.appendEntry(nil)
	r.broadcastAppend()
}

// ---------------------------------------------------------------------------
// Input

// Tick advances the node's clock by one tick.
func (r *Raft) Tick() {
	r.elapsed++
	switch r.state {
	case Leader:
		// Check quorum (thesis section 6.2): a leader that hasn't heard
		// from a quorum for an election timeout steps down, instead of
		// lingering in a minority partition.
		if r.elapsed >= r.electionTicks {
			r.elapsed = 0
			r.active[r.id] = true
			if len(r.active) < r.quorum() {
				r.becomeFollower(r.term, 0)
				return
			}
			clear(r.active)
		}
		r.hbElapsed++
		if r.hbElapsed >= r.heartbeatTicks {
			r.hbElapsed = 0
			r.broadcastHeartbeat(0)
		}
	default:
		if r.elapsed >= r.timeout {
			r.preCampaign()
		}
	}
}

// Campaign starts an election now, skipping the pre-vote.
func (r *Raft) Campaign() {
	if r.state != Leader {
		r.campaign()
	}
}

// Propose appends data to the log if the node is the leader, and returns
// the new entry's index and term. The entry is committed if and when an
// entry with that index and term comes out of Ready.Committed; if a
// different term comes out at that index, the proposal was lost.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	if data == nil {
		data = []byte{} // nil marks the leader's no-op
	}
	r.appendEntry(data)
	r.broadcastAppend()
	return r.lastIndex(), r.term, nil
}

// ReadIndex asks to confirm that this node is still the leader, for a
// linearizable read (section 6.4). When a quorum confirms it, a ReadState
// with ctx comes out of Ready; if the node loses leadership first, ctx
// comes out in Ready.DroppedReads.
func (r *Raft) ReadIndex(ctx uint64) error {
	if r.state != Leader {
		return ErrNotLeader
	}
	if r.termAt(r.commit) != r.term {
		return ErrNotReady
	}
	rq := &readRequest{ctx: ctx, index: r.commit, acks: map[uint64]bool{r.id: true}}
	r.reads = append(r.reads, rq)
	if len(rq.acks) >= r.quorum() {
		r.confirmReads(rq)
		return nil
	}
	r.broadcastHeartbeat(ctx)
	return nil
}

// Step processes a message from another node.
func (r *Raft) Step(m Message) {
	switch {
	case m.Term > r.term && (m.Type == MsgPreVote || (m.Type == MsgPreVoteResp && !m.Reject)):
		// A pre-vote carries the term the sender would campaign in; it
		// changes nobody's term.
	case m.Term > r.term:
		lead := uint64(0)
		if m.Type == MsgApp || m.Type == MsgHeartbeat {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.term:
		// A stale leader or candidate: tell it the current term, so it
		// steps down. Stale responses are ignored.
		switch m.Type {
		case MsgApp, MsgHeartbeat:
			r.send(Message{Type: MsgAppResp, To: m.From, Reject: true, Index: m.Index})
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return
	}
	if r.state == Leader && m.Type != MsgPreVote {
		r.active[m.From] = true
	}

	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgPreVote:
		// Grant only if we'd grant the vote itself, and haven't heard
		// from a leader within the minimum election timeout.
		inLease := r.lead != 0 && r.elapsed < r.electionTicks
		if m.Term > r.term && r.upToDate(m) && !inLease {
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term})
		} else {
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
	case MsgVoteResp:
		if r.state == Candidate && r.countVote(m) {
			r.becomeLeader()
		}
	case MsgPreVoteResp:
		if r.state == PreCandidate && (m.Reject || m.Term == r.term+1) && r.countVote(m) {
			r.campaign()
		}
	case MsgApp:
		r.acceptLeader(m.From)
		r.handleAppend(m)
	case MsgHeartbeat:
		r.acceptLeader(m.From)
		r.commitTo(min(m.Commit, r.lastIndex()))
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
	case MsgAppResp:
		if r.state == Leader {
			r.handleAppendResp(m)
		}
	case MsgHeartbeatResp:
		if r.state == Leader {
			r.handleHeartbeatResp(m)
		}
	}
}

// acceptLeader handles a message from the leader of the current term.
func (r *Raft) acceptLeader(from uint64) {
	if r.state != Follower || r.lead != from {
		// A candidate that hears from a leader of its own term has lost.
		r.becomeFollower(r.term, from)
	}
	r.resetTimer()
}

// countVote records a (pre-)vote and reports whether a quorum granted.
func (r *Raft) countVote(m Message) bool {
	r.votes[m.From] = !m.Reject
	granted := 0
	for _, v := range r.votes {
		if v {
			granted++
		}
	}
	return granted >= r.quorum()
}

// upToDate reports whether a candidate's log, ending at m.LogTerm and
// m.Index, is at least as up to date as ours (section 5.4.1).
func (r *Raft) upToDate(m Message) bool {
	return m.LogTerm > r.lastTerm() || (m.LogTerm == r.lastTerm() && m.Index >= r.lastIndex())
}

func (r *Raft) handleVote(m Message) {
	// Grant at most one vote per term, and only to a candidate whose log
	// is at least as up to date as ours, so a leader always holds every
	// committed entry.
	if (r.vote == 0 || r.vote == m.From) && r.upToDate(m) && r.state == Follower {
		r.vote = m.From
		r.resetTimer()
		r.send(Message{Type: MsgVoteResp, To: m.From})
		return
	}
	r.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
}

// ---------------------------------------------------------------------------
// Log replication

func (r *Raft) appendEntry(data []byte) {
	r.log = append(r.log, Entry{Term: r.term, Index: r.lastIndex() + 1, Data: data})
	// The leader's own copy counts once it is persisted (see Persisted).
	r.next[r.id] = r.lastIndex() + 1
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.peers {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

func (r *Raft) sendAppend(to uint64) {
	next := r.next[to]
	end := min(r.lastIndex()+1, next+uint64(r.maxBatch))
	ents := slices.Clone(r.log[next:end])
	r.send(Message{
		Type: MsgApp, To: to, LogTerm: r.termAt(next - 1), Index: next - 1,
		Entries: ents, Commit: r.commit,
	})
	// Optimistically assume the entries arrive; a rejection or a lagging
	// heartbeat response resets next.
	r.next[to] = end
}

func (r *Raft) broadcastHeartbeat(ctx uint64) {
	for _, p := range r.peers {
		if p != r.id {
			// Only up to what the follower is known to hold: its log may
			// not match ours beyond that.
			r.send(Message{Type: MsgHeartbeat, To: p, Commit: min(r.commit, r.match[p]), Context: ctx})
		}
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index > r.lastIndex() || r.termAt(m.Index) != m.LogTerm {
		// The logs diverge at or before m.Index. Suggest retrying from
		// before the whole conflicting term, rather than one entry at a
		// time (section 5.3).
		hint := min(m.Index, r.lastIndex()+1) - 1
		if m.Index <= r.lastIndex() {
			t := r.termAt(m.Index)
			for hint > r.commit && r.termAt(hint) == t {
				hint--
			}
		}
		r.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, Hint: hint})
		return
	}
	for i, e := range m.Entries {
		if e.Index <= r.lastIndex() {
			if r.termAt(e.Index) == e.Term {
				continue // already have it
			}
			if e.Index <= r.commit {
				panic("raft: leader overwrote a committed entry")
			}
			r.log = r.log[:e.Index] // conflict: drop it and everything after
			r.persisted = min(r.persisted, e.Index-1)
		}
		r.log = append(r.log, m.Entries[i:]...)
		r.unstable = min(r.unstable, e.Index)
		break
	}
	last := m.Index + uint64(len(m.Entries))
	// Entries after last may be stale leftovers, so commit no further.
	r.commitTo(min(m.Commit, last))
	r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
}

func (r *Raft) handleAppendResp(m Message) {
	p := m.From
	if m.Reject {
		// A rejection of an index the follower is known to hold is a
		// stale, reordered reply.
		if m.Index > r.match[p] {
			r.next[p] = max(r.match[p]+1, min(m.Hint, m.Index)+1)
			r.sendAppend(p)
		}
		return
	}
	if m.Index > r.match[p] {
		r.match[p] = m.Index
		r.next[p] = max(r.next[p], m.Index+1)
		r.maybeCommit()
	}
	// Send more only in reply to the furthest append sent. Otherwise every
	// resend (after a heartbeat or rejection) would start another chain of
	// append, ack, append that never ends while the follower lags, and the
	// chains would flood the network.
	if m.Index+1 == r.next[p] && r.next[p] <= r.lastIndex() {
		r.sendAppend(p)
	}
}

func (r *Raft) handleHeartbeatResp(m Message) {
	p := m.From
	if r.match[p] < r.lastIndex() && r.match[p] == r.hbMatch[p] {
		// The follower is behind and made no progress since its last
		// heartbeat response, so an append or its reply was probably
		// lost: resend from what it is known to hold.
		r.next[p] = r.match[p] + 1
		r.sendAppend(p)
	}
	r.hbMatch[p] = r.match[p]
	if m.Context == 0 {
		return
	}
	for _, rq := range r.reads {
		if rq.ctx == m.Context {
			rq.acks[p] = true
			if len(rq.acks) >= r.quorum() {
				r.confirmReads(rq)
			}
			return
		}
	}
}

// confirmReads confirms rq and every request before it: a quorum that
// confirms leadership now also confirms it for requests made earlier.
func (r *Raft) confirmReads(rq *readRequest) {
	i := slices.Index(r.reads, rq)
	for _, q := range r.reads[:i+1] {
		r.readStates = append(r.readStates, ReadState{Context: q.ctx, Index: q.index})
	}
	r.reads = r.reads[i+1:]
}

// maybeCommit advances the commit index to the highest entry of the current
// term stored on a quorum.
func (r *Raft) maybeCommit() {
	ms := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		ms = append(ms, r.match[p])
	}
	slices.Sort(ms)
	n := ms[len(ms)-r.quorum()]
	if n > r.commit && r.termAt(n) == r.term {
		r.commit = n
	}
}

func (r *Raft) commitTo(i uint64) {
	if i > r.commit {
		r.commit = i
	}
}

// ---------------------------------------------------------------------------
// Output

// Persisted tells the node that the caller has durably stored the entries
// of a Ready, the last of which has the given index and term. The write
// may finish after later Steps (asynchronous persistence): if the entry
// has been replaced since, the call is ignored, and a later Ready hands
// out the replacement. Entries are persisted in order, so if the entry
// still has that term, so does everything before it (log matching).
func (r *Raft) Persisted(index, term uint64) {
	if index <= r.persisted || r.termAt(index) != term {
		return
	}
	r.persisted = index
	if r.state == Leader {
		r.match[r.id] = index
		r.maybeCommit()
	}
}

// EarlySend reports whether m may be sent before the Ready it came in is
// persisted. Only a leader's appends and heartbeats qualify: they vouch
// for nothing on the leader's disk, since the leader doesn't count its own
// unpersisted entries, and its term and vote were persisted when it
// became a candidate.
func EarlySend(m Message) bool { return m.Type == MsgApp || m.Type == MsgHeartbeat }

// HasReady reports whether Ready has anything to return.
func (r *Raft) HasReady() bool {
	return len(r.msgs) > 0 || r.unstable <= r.lastIndex() || r.applied < r.commit ||
		r.hardState() != r.prevHS || len(r.readStates) > 0 || len(r.droppedReads) > 0
}

// Ready returns the work accumulated since the last call.
func (r *Raft) Ready() Ready {
	rd := Ready{
		HardState:    r.hardState(),
		Messages:     r.msgs,
		Reads:        r.readStates,
		DroppedReads: r.droppedReads,
	}
	if r.unstable <= r.lastIndex() {
		rd.Entries = slices.Clone(r.log[r.unstable:])
	}
	if r.applied < r.commit {
		rd.Committed = slices.Clone(r.log[r.applied+1 : r.commit+1])
	}
	r.msgs, r.readStates, r.droppedReads = nil, nil, nil
	r.unstable = r.lastIndex() + 1
	r.applied = r.commit
	r.prevHS = rd.HardState
	return rd
}
