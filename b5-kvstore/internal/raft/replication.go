package raft

import (
	"context"
	"errors"
	"log"
	"time"

	"b5-kvstore/internal/raft/persistence"
	"b5-kvstore/pkg/pb"
)

// ErrNotLeader is returned by ProposeSync when this node is not currently
// Leader, so it cannot accept the write at all.
var ErrNotLeader = errors.New("raft: not leader")

// AppendEntries is the follower-side handler (§4 "Follower side"): checks
// log consistency via prevLogIndex/prevLogTerm before appending anything,
// persists+fsyncs before acknowledging, and resets the election timer on
// every valid AppendEntries from the current leader.
func (n *Node) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() > n.currentTerm {
		if _, err := n.observeTermLocked(req.GetTerm(), "AppendEntries"); err != nil {
			return nil, err
		}
	}

	if req.GetTerm() < n.currentTerm {
		return &pb.AppendEntriesReply{Term: n.currentTerm, Success: false}, nil
	}

	// req.Term == n.currentTerm: this is a legitimate leader for our
	// current term. A Candidate that sees this must stand down to
	// Follower; a Follower stays Follower.
	n.setRoleLocked(Follower)
	n.currentLeader = req.GetLeaderId()
	n.resetElectionDeadlineLocked()

	// Log consistency check (§4 point 1-2), before appending anything.
	// prevLogIndex == 0 means "no previous entry to check" (first-ever
	// entry); prevLogIndex <= logOffset means it refers to an entry this
	// node has already compacted away (§5.1/§5.3) — already-committed data
	// that Leader Completeness guarantees is consistent, so there's nothing
	// left to verify against a term we no longer have on hand either way.
	if req.GetPrevLogIndex() > n.logOffset {
		entry := n.entryAtLocked(req.GetPrevLogIndex())
		if entry == nil {
			// Our log doesn't even reach prevLogIndex.
			return &pb.AppendEntriesReply{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: n.lastLogIndexLocked() + 1,
				ConflictTerm:  0,
			}, nil
		}
		if entry.Term != req.GetPrevLogTerm() {
			conflictTerm := entry.Term
			return &pb.AppendEntriesReply{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: n.firstIndexOfTermLocked(conflictTerm),
				ConflictTerm:  conflictTerm,
			}, nil
		}
	}

	// Request has passed every validity check (term correct, prevLogIndex/
	// prevLogTerm consistent per §3.3) — a heartbeat or replication call the
	// follower is actually accepting, not one it's about to reject. Recorded
	// (not logged) so a future ELECTION_START can report how long it's been.
	n.lastAppendEntriesAt = time.Now()

	// Consistent: merge in the new entries (truncating any conflicting
	// suffix first), then persist+fsync before acknowledging.
	if len(req.GetEntries()) > 0 {
		if err := n.mergeEntriesLocked(req.GetPrevLogIndex(), req.GetEntries()); err != nil {
			return nil, err
		}
	}

	// Apply newly-committed entries using leaderCommit, never speculatively
	// ahead of what the leader has confirmed.
	if req.GetLeaderCommit() > n.commitIndex {
		n.commitIndex = min64(req.GetLeaderCommit(), n.lastLogIndexLocked())
		n.applyCommittedLocked()
	}

	return &pb.AppendEntriesReply{Term: n.currentTerm, Success: true}, nil
}

// firstIndexOfTermLocked scans backward from the end of the log for the
// first entry with the given term (used to build conflictIndex for a
// term-mismatch rejection). Must be called with n.mu held.
func (n *Node) firstIndexOfTermLocked(term uint64) uint64 {
	idx := len(n.log)
	for idx > 0 && n.log[idx-1].Term == term {
		idx--
	}
	return n.logOffset + uint64(idx) + 1
}

// lastIndexOfTermLocked returns the index of the last entry with the given
// term in our own log, or 0 if we have none. Used by the leader's fast
// log-repair path (§5).
func (n *Node) lastIndexOfTermLocked(term uint64) uint64 {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Term == term {
			return n.logOffset + uint64(i) + 1
		}
		if n.log[i].Term < term {
			break
		}
	}
	return 0
}

// mergeEntriesLocked implements the standard AppendEntries receiver rule:
// walk the new entries against the existing log starting at
// prevLogIndex+1; on the first index where an existing entry's term
// differs from the incoming one, truncate the log from there (on disk too)
// and append everything from that point on. Existing entries that already
// match are left untouched (they are not re-appended/re-fsynced).
func (n *Node) mergeEntriesLocked(prevLogIndex uint64, newEntries []*pb.LogEntry) error {
	insertAt := prevLogIndex + 1
	i := 0
	// Entries at or below logOffset are already compacted away (§5.1/§5.3):
	// their effect is already captured by the snapshot baseline the anchor
	// stands in for, so skip them rather than trying to look them up.
	for i < len(newEntries) && insertAt+uint64(i) <= n.logOffset {
		i++
	}
	for ; i < len(newEntries); i++ {
		idx := insertAt + uint64(i)
		existing := n.entryAtLocked(idx)
		if existing == nil {
			break // nothing more on disk; everything from here is new
		}
		if existing.Term != newEntries[i].Term {
			if err := persistence.TruncateLogFrom(n.dataDir, idx); err != nil {
				return err
			}
			n.log = n.log[:idx-n.logOffset-1]
			break
		}
		// Already have this entry with a matching term; skip it.
	}
	if i >= len(newEntries) {
		return nil
	}

	toAppend := make([]persistence.LogEntry, 0, len(newEntries)-i)
	for _, e := range newEntries[i:] {
		toAppend = append(toAppend, persistence.LogEntry{
			Term:    e.GetTerm(),
			Index:   e.GetIndex(),
			Command: e.GetCommand(),
		})
	}
	if err := persistence.AppendLogEntries(n.dataDir, toAppend); err != nil {
		return err
	}
	n.log = append(n.log, toAppend...)
	return nil
}

// applyCommittedLocked applies every entry between lastApplied+1 and
// commitIndex, in order, via the caller-supplied ApplyFn. Entries are only
// ever applied after commit, never speculatively (§4 point 4). Any progress
// wakes up goroutines blocked in waitForApplied (md-week4 §4 follower step
// 3), via applyWake.
func (n *Node) applyCommittedLocked() {
	advanced := false
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		advanced = true
		entry := n.entryAtLocked(n.lastApplied)
		if entry == nil {
			break
		}
		if n.applyFn != nil {
			n.applyFn(ApplyMsg{Index: entry.Index, Term: entry.Term, Command: entry.Command})
		}
		if ch, ok := n.waiters[entry.Index]; ok {
			ch <- nil
			delete(n.waiters, entry.Index)
		}
	}
	if advanced {
		close(n.applyWake)
		n.applyWake = make(chan struct{})
	}
}

// waitForApplied blocks (no busy-wait: it parks on applyWake) until
// lastApplied reaches at least target, or ctx is done. Used by
// FollowerLinearizableRead (readindex.go, md-week4 §4 follower step 3).
func (n *Node) waitForApplied(ctx context.Context, target uint64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= target {
			n.mu.Unlock()
			return nil
		}
		ch := n.applyWake
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// appendLocalLocked writes command to the local log — write-ahead: on disk
// (fsync'd) before it becomes observable in memory or to any RPC caller.
// Must be called with n.mu held and n confirmed Leader.
func (n *Node) appendLocalLocked(command []byte) (index, term uint64, err error) {
	index = n.lastLogIndexLocked() + 1
	term = n.currentTerm
	entry := persistence.LogEntry{Term: term, Index: index, Command: command}
	if err := persistence.AppendLogEntries(n.dataDir, []persistence.LogEntry{entry}); err != nil {
		return 0, term, err
	}
	n.log = append(n.log, entry)
	return index, term, nil
}

// Propose appends a new command to the leader's log and immediately begins
// replicating it, returning as soon as the entry is durable on this node's
// own log — it does not wait for the entry to be committed. This is the
// client-write entry point used by the in-process test harness in this
// phase (§4 point 1, §6) — the Client Proxy doesn't exist until Week 4.
func (n *Node) Propose(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	if n.role != Leader {
		term = n.currentTerm
		n.mu.Unlock()
		return 0, term, false
	}
	index, term, err := n.appendLocalLocked(command)
	n.mu.Unlock()
	if err != nil {
		return 0, term, false
	}

	n.broadcastAppendEntries()
	return index, term, true
}

// ProposeSync behaves like Propose but blocks until the entry has been
// committed (replicated to and applied by a quorum) or ctx is done,
// whichever comes first. If this node stops being Leader for the term that
// accepted the entry before it commits, err is errLeadershipLost: the entry
// may or may not end up committed under a different leader, so this is
// reported as "unknown outcome", not as a definite failure.
func (n *Node) ProposeSync(ctx context.Context, command []byte) (index uint64, term uint64, err error) {
	n.mu.Lock()
	if n.role != Leader {
		term = n.currentTerm
		n.mu.Unlock()
		return 0, term, ErrNotLeader
	}
	index, term, err = n.appendLocalLocked(command)
	if err != nil {
		n.mu.Unlock()
		return 0, term, err
	}
	done := make(chan error, 1)
	n.waiters[index] = done
	n.mu.Unlock()

	n.broadcastAppendEntries()

	select {
	case err := <-done:
		return index, term, err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
		return index, term, ctx.Err()
	}
}

// outgoingAppendEntries is one leader->follower AppendEntries call, built by
// buildAppendEntriesRequestsLocked and shared between the periodic heartbeat
// broadcast and the Read-Index quorum confirmation (readindex.go, md-week4
// §4 leader step 2) — both reuse the exact same request-construction and
// reply-handling path, per that section's "do not build a second heartbeat
// mechanism just for this."
type outgoingAppendEntries struct {
	peer       string
	req        *pb.AppendEntriesRequest
	prevIndex  uint64
	numEntries int
}

// buildAppendEntriesRequestsLocked builds one AppendEntries request per peer
// from the leader's current nextIndex/log state (heartbeat-only if a peer is
// fully caught up, replication otherwise). Must be called with n.mu held and
// n confirmed Leader.
func (n *Node) buildAppendEntriesRequestsLocked() (term uint64, reqs []outgoingAppendEntries) {
	term = n.currentTerm
	leaderAddr := n.address
	leaderCommit := n.commitIndex
	reqs = make([]outgoingAppendEntries, 0, len(n.peers))
	for _, p := range n.peers {
		next := n.nextIndex[p]
		if next <= n.logOffset {
			// Either genuinely unset (0) or fell behind our own compaction
			// floor (§5.1/§5.3, e.g. via repeated fast-backoff): we no
			// longer have anything before logOffset+1 to offer this peer —
			// entries below that only exist in the Snapshot & Backup
			// service's catalog now. Clamp to the oldest entry we still
			// have; a peer that's genuinely behind logOffset will keep
			// rejecting until its own periodic catch-up loop (not this
			// replication path) pulls it forward via the catalog — see
			// §5.3's documented bounded-lag window.
			next = n.logOffset + 1
		}
		prevIndex := next - 1
		var prevTerm uint64
		if prevIndex > 0 {
			if e := n.entryAtLocked(prevIndex); e != nil {
				prevTerm = e.Term
			}
		}
		var entries []*pb.LogEntry
		for idx := next; idx <= n.lastLogIndexLocked(); idx++ {
			e := n.entryAtLocked(idx)
			entries = append(entries, &pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command})
		}
		reqs = append(reqs, outgoingAppendEntries{
			peer: p,
			req: &pb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderAddr,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			},
			prevIndex:  prevIndex,
			numEntries: len(entries),
		})
	}
	return term, reqs
}

// sendAppendEntriesAndHandle sends one outgoing AppendEntries call, applies
// the standard reply handling (nextIndex/matchIndex advancement, fast log
// repair, higher-term stepdown), and reports whether it was acknowledged
// successfully by a still-current leader for term. Must be called without
// n.mu held.
func (n *Node) sendAppendEntriesAndHandle(term uint64, r outgoingAppendEntries) (success bool) {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()
	reply, err := n.transport.SendAppendEntries(ctx, r.peer, r.req)
	if err != nil {
		log.Printf("consensus-node[%s]: APPENDENTRIES_SEND_FAILED to=%s term=%d error=%q", n.id, r.peer, term, err)
		return false
	}
	if reply == nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return false // stale: no longer leader for the term that sent this
	}
	n.handleAppendEntriesReplyLocked(r.peer, r.prevIndex, r.numEntries, reply)
	return reply.GetTerm() == term && reply.GetSuccess()
}

// broadcastAppendEntries sends AppendEntries (heartbeat if a follower is
// fully caught up, replication otherwise) to every peer in parallel. It is
// called periodically by the heartbeat ticker and eagerly after Propose.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term, reqs := n.buildAppendEntriesRequestsLocked()
	n.mu.Unlock()

	for _, r := range reqs {
		r := r
		go func() { n.sendAppendEntriesAndHandle(term, r) }()
	}
}

// handleAppendEntriesReplyLocked processes one follower's reply: advances
// nextIndex/matchIndex on success (§5), applies the higher-term rule, or
// backs nextIndex off using the fast conflictIndex/conflictTerm path on
// rejection. Must be called with n.mu held, with n confirmed still Leader
// for the term that sent the original request.
func (n *Node) handleAppendEntriesReplyLocked(peer string, sentPrevLogIndex uint64, numEntriesSent int, reply *pb.AppendEntriesReply) {
	if reply.GetTerm() > n.currentTerm {
		_, _ = n.observeTermLocked(reply.GetTerm(), "AppendEntriesReply")
		return
	}

	if reply.GetSuccess() {
		newMatch := sentPrevLogIndex + uint64(numEntriesSent)
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		if newMatch+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = newMatch + 1
		}
		n.maybeAdvanceCommitIndexLocked()
		return
	}

	// Rejected: fast-backoff using conflictIndex/conflictTerm (§5) instead
	// of decrementing nextIndex by one and retrying.
	var next uint64
	if reply.GetConflictTerm() != 0 {
		if lastIdx := n.lastIndexOfTermLocked(reply.GetConflictTerm()); lastIdx > 0 {
			next = lastIdx + 1
		} else {
			next = reply.GetConflictIndex()
		}
	} else {
		next = reply.GetConflictIndex()
	}
	if next < 1 {
		next = 1
	}
	n.nextIndex[peer] = next
}

// maybeAdvanceCommitIndexLocked implements the term-boundary commit rule
// (§4 point 3 / Raft paper "Figure 8"): a quorum of matchIndex[peer] >= N
// is necessary but not sufficient — N is only committed if the entry at N
// was appended during the leader's own current term. Older-term entries
// are committed only indirectly, once covered by a same-term N. Must be
// called with n.mu held, n confirmed Leader.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {
		entry := n.entryAtLocked(N)
		if entry == nil || entry.Term != n.currentTerm {
			continue
		}
		count := 1 // leader counts itself
		for _, m := range n.matchIndex {
			if m >= N {
				count++
			}
		}
		if count >= quorumSize(len(n.peers)+1) {
			n.commitIndex = N
			n.applyCommittedLocked()
			return
		}
	}
}

// InstallSnapshot satisfies pb.ConsensusServer so the node can be
// registered as a real gRPC server (§6 Step C), but snapshotting itself is
// an explicit non-goal of this phase (§7) — no log compaction and no
// Snapshot & Backup integration happen yet.
func (n *Node) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.GetTerm() > n.currentTerm {
		if _, err := n.observeTermLocked(req.GetTerm(), "InstallSnapshot"); err != nil {
			return nil, err
		}
	}
	return &pb.InstallSnapshotReply{Term: n.currentTerm}, nil
}

// GetStatus is a cheap, non-blocking status read (§0). Not consumed by
// anything until Week 4's Service Discovery, but the wire representation
// exists from the start.
func (n *Node) GetStatus(_ context.Context, _ *pb.GetStatusRequest) (*pb.GetStatusReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &pb.GetStatusReply{
		Role:    n.role.toProto(),
		Term:    n.currentTerm,
		Address: n.address,
		NodeId:  n.id,
	}, nil
}
