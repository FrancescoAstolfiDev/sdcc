package raft

import (
	"context"
	"errors"
	"log"
	"time"

	"b5-kvstore/pkg/pb"
)

// ErrReadIndexUnavailable is returned by FollowerLinearizableRead whenever
// the follower-side Read-Index handshake cannot be completed right now —
// either because the leader couldn't confirm its own leadership
// (ReadIndexReply.ok == false), the RequestReadIndex RPC itself
// failed/timed out, or the wait for lastApplied to catch up ran out of
// time. In every case the caller's documented recovery is the same: fall
// back to a single, direct read against the cached Leader — never bounce to
// a second follower (the handshake's "runtime fallback rule").
var ErrReadIndexUnavailable = errors.New("raft: read-index unavailable; caller must fall back to the leader")

// RequestReadIndex is the leader-side Read-Index handler ("Leader side").
// If this node isn't currently leader it replies ok=false
// with the best-known leader address, if any. Otherwise it captures the
// current commitIndex, confirms leadership is still valid via a fresh round
// of heartbeats acknowledged by a quorum (reusing the same AppendEntries
// machinery as the periodic heartbeat broadcast — see
// confirmLeadershipQuorum), and only then replies ok=true with the
// captured index: anything already committed at capture time is still
// committed once quorum-reconfirmed, so it's safe to hand out even though
// the confirmation round happens afterward.
func (n *Node) RequestReadIndex(ctx context.Context, req *pb.ReadIndexRequest) (*pb.ReadIndexReply, error) {
	n.mu.Lock()
	if n.role != Leader {
		leaderAddr := n.currentLeader
		n.mu.Unlock()
		return &pb.ReadIndexReply{Ok: false, RedirectLeader: leaderAddr}, nil
	}
	term := n.currentTerm
	readIndex := n.commitIndex
	n.mu.Unlock()

	// Logged only here, once per genuine read-index attempt this node
	// actually processes as leader — not on every GET (those served
	// directly by a Leader never reach this RPC at all, §3's default
	// path), and not on the immediate not-leader bounce above (that's a
	// rejection, not a request being handled).
	log.Printf("consensus-node[%s]: READ_INDEX_REQUESTED term=%d follower=%s", n.id, term, req.GetFollowerId())
	quorumStart := time.Now()

	if err := n.confirmLeadershipQuorum(ctx, term); err != nil {
		n.mu.Lock()
		stillLeader := n.role == Leader && n.currentTerm == term
		leaderAddr := n.currentLeader
		n.mu.Unlock()
		if !stillLeader {
			// Lost (or never had, for this term) leadership while
			// confirming: this is exactly the "stale leader" case §4
			// exists to prevent handing out a read index for.
			return &pb.ReadIndexReply{Ok: false, RedirectLeader: leaderAddr}, nil
		}
		// Still leader, but couldn't reach a quorum in time (e.g. network
		// partition): surface as an RPC error so the caller's transport
		// failure path (not the ok=false path) handles it — both paths
		// converge on the same single-hop-to-leader fallback regardless.
		return nil, err
	}

	log.Printf("consensus-node[%s]: READ_INDEX_GRANTED term=%d follower=%s readIndex=%d durationMs=%d",
		n.id, term, req.GetFollowerId(), readIndex, time.Since(quorumStart).Milliseconds())
	return &pb.ReadIndexReply{Ok: true, ReadIndex: readIndex}, nil
}

// confirmLeadershipQuorum sends one fresh round of AppendEntries to every
// peer (via the same request-building/reply-handling path as the periodic
// heartbeat broadcast — reusing the existing heartbeat/AppendEntries
// machinery instead of building a second one just for this) and blocks
// until a quorum has acknowledged it
// for term, ctx is done, or every peer has replied without reaching quorum.
func (n *Node) confirmLeadershipQuorum(ctx context.Context, term uint64) error {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return ErrNotLeader
	}
	_, reqs := n.buildAppendEntriesRequestsLocked()
	numPeers := len(n.peers)
	n.mu.Unlock()

	if numPeers == 0 {
		return nil // single-node cluster: the leader's own vote is already a quorum.
	}

	results := make(chan bool, numPeers)
	for _, r := range reqs {
		r := r
		go func() { results <- n.sendAppendEntriesAndHandle(term, r) }()
	}

	acked := 1 // leader counts itself
	quorum := quorumSize(numPeers + 1)
	for i := 0; i < numPeers; i++ {
		select {
		case ok := <-results:
			if ok {
				acked++
				if acked >= quorum {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("raft: could not reconfirm leadership via quorum heartbeat")
}

// FollowerLinearizableRead performs the follower side of the Read-Index
// handshake ("Follower side", steps 1-4): if this node is currently Leader
// it returns immediately (nothing to confirm — the caller reads the local
// state machine directly, per §3's leader-default path).
// Otherwise it asks the currently-known leader to confirm a read index and
// blocks until this node's own lastApplied has caught up, at which point
// it's safe for the caller to read the local state machine. Any failure
// (stale leader knowledge, transport error/timeout, or the wait itself
// timing out) is reported as ErrReadIndexUnavailable — ctx should carry a
// reasonable deadline so a partitioned/lagging follower doesn't block a
// caller forever.
func (n *Node) FollowerLinearizableRead(ctx context.Context) error {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return nil
	}
	leaderAddr := n.currentLeader
	selfID := n.id
	n.mu.Unlock()

	if leaderAddr == "" {
		// No leader observed yet (e.g. mid-election): nothing to ask.
		return ErrReadIndexUnavailable
	}

	reply, err := n.transport.SendReadIndex(ctx, leaderAddr, &pb.ReadIndexRequest{FollowerId: selfID})
	if err != nil || reply == nil {
		// Transport failure/timeout: the handshake's dedicated runtime
		// fallback rule (distinct from ok == false, but same outcome for
		// the caller).
		return ErrReadIndexUnavailable
	}
	if !reply.GetOk() {
		// The leader we knew about is stale. Do NOT retry against another
		// follower — remember the redirect (if any) for next time, and
		// hand the single-hop-to-leader fallback back to the caller.
		if addr := reply.GetRedirectLeader(); addr != "" {
			n.mu.Lock()
			n.currentLeader = addr
			n.mu.Unlock()
		}
		return ErrReadIndexUnavailable
	}

	waitStart := time.Now()
	if err := n.waitForApplied(ctx, reply.GetReadIndex()); err != nil {
		// Bounded by ctx: this can time out if a follower is lagging
		// further behind than the caller's deadline allows, or if the
		// target index has already been compacted away.
		return ErrReadIndexUnavailable
	}
	// waitedMs is ~0 when this node's lastApplied already met readIndex
	// (waitForApplied returns immediately, no blocking) and reflects the
	// real wait otherwise — the local-catch-up half of the handshake's
	// latency, distinct from READ_INDEX_GRANTED's durationMs (the leader's
	// quorum-confirmation round).
	log.Printf("consensus-node[%s]: READ_INDEX_WAIT_COMPLETE readIndex=%d waitedMs=%d", n.id, reply.GetReadIndex(), time.Since(waitStart).Milliseconds())
	return nil
}
