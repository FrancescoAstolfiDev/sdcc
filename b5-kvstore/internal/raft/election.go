package raft

import (
	"context"
	"log"
	"time"

	"b5-kvstore/pkg/pb"
)

// RequestVote implements the vote-granting rule from §3: both conditions
// below must hold, in this order, for a vote to be granted.
func (n *Node) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() > n.currentTerm {
		if _, err := n.observeTermLocked(req.GetTerm(), "RequestVote"); err != nil {
			return nil, err
		}
	}

	if req.GetTerm() < n.currentTerm {
		// Stale candidate: reject and report our term so it can discover
		// it's behind and step down.
		return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}, nil
	}

	// Condition 1: not already voted for a different candidate this term.
	notAlreadyVoted := n.votedFor == "" || n.votedFor == req.GetCandidateId()

	// Condition 2: candidate's log is at least as up-to-date as ours —
	// compare lastLogTerm first (higher wins outright), and only if equal
	// compare lastLogIndex (higher-or-equal wins).
	candLogUpToDate := isLogAtLeastAsUpToDate(
		req.GetLastLogTerm(), req.GetLastLogIndex(),
		n.lastLogTermLocked(), n.lastLogIndexLocked(),
	)

	if !notAlreadyVoted || !candLogUpToDate {
		reason := "stale_log"
		if !notAlreadyVoted {
			reason = "already_voted"
		}
		log.Printf("consensus-node[%s]: VOTE_REJECTED term=%d candidate=%s reason=%s", n.id, n.currentTerm, req.GetCandidateId(), reason)
		return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}, nil
	}

	n.votedFor = req.GetCandidateId()
	if err := n.persistStateLocked(); err != nil {
		return nil, err
	}
	// Reset our own election timer the moment we grant a vote — otherwise
	// we might time out moments later and start a redundant competing
	// election, needlessly increasing the split-vote rate.
	n.resetElectionDeadlineLocked()

	log.Printf("consensus-node[%s]: VOTE_GRANTED term=%d candidate=%s", n.id, n.currentTerm, req.GetCandidateId())
	return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: true}, nil
}

func isLogAtLeastAsUpToDate(candTerm, candIndex, voterTerm, voterIndex uint64) bool {
	if candTerm != voterTerm {
		return candTerm > voterTerm
	}
	return candIndex >= voterIndex
}

// startElection is invoked by the election ticker on timeout. It increments
// the term, votes for self, persists both, resets the timer with a freshly
// randomized value, and fans RequestVote out to all peers in parallel. A
// split-vote (no quorum before the next timeout) surfaces naturally: the
// ticker will call startElection again with a new term and a new random
// timeout, since nothing reset the deadline in the meantime.
func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	oldTerm := n.currentTerm
	n.setRoleLocked(Candidate)
	n.currentTerm++
	n.votedFor = n.id
	if err := n.persistStateLocked(); err != nil {
		n.mu.Unlock()
		return
	}
	n.resetElectionDeadlineLocked()

	term := n.currentTerm
	candidateID := n.id
	timeSinceLastHeartbeat := "never"
	if !n.lastAppendEntriesAt.IsZero() {
		timeSinceLastHeartbeat = time.Since(n.lastAppendEntriesAt).String()
	}
	log.Printf("consensus-node[%s]: ELECTION_START term=%d (previous_term=%d) time_since_last_heartbeat=%s",
		n.id, term, oldTerm, timeSinceLastHeartbeat)
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	peers := append([]string(nil), n.peers...)
	totalNodes := len(peers) + 1
	quorum := quorumSize(totalNodes)
	votes := 1 // self-vote
	n.mu.Unlock()

	if votes >= quorum {
		n.mu.Lock()
		if n.role == Candidate && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, peer, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil || reply == nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.GetTerm() > n.currentTerm {
				_, _ = n.observeTermLocked(reply.GetTerm(), "RequestVoteReply")
				return
			}
			// Stale reply: election has moved on since we sent this RPC.
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.GetVoteGranted() {
				votes++
			}
			log.Printf("consensus-node[%s]: VOTE_RECEIVED term=%d from=%s granted=%v votes_so_far=%d/%d", n.id, term, peer, reply.GetVoteGranted(), votes, totalNodes)
			if reply.GetVoteGranted() && votes >= quorum {
				n.becomeLeaderLocked()
			}
		}()
	}
}

// becomeLeaderLocked transitions a winning Candidate to Leader and resets
// the per-follower nextIndex/matchIndex tracking (§5): nextIndex starts at
// the leader's last log index + 1 for every follower, matchIndex at 0.
// Must be called with n.mu held.
func (n *Node) becomeLeaderLocked() {
	if n.role != Candidate {
		return
	}
	n.role = Leader
	n.currentLeader = n.address
	log.Printf("consensus-node[%s]: BECAME_LEADER term=%d", n.id, n.currentTerm)
	lastIdx := n.lastLogIndexLocked()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	// Assert leadership immediately rather than waiting for the next
	// heartbeat tick, so followers stop their election timers as soon as
	// possible.
	go n.broadcastAppendEntries()
}
