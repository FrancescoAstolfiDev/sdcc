package raft_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"b5-kvstore/internal/raft"
	"b5-kvstore/pkg/pb"
)

func fastTiming() raft.TimingConfig {
	return raft.TimingConfig{
		ElectionTimeoutMin: 40 * time.Millisecond,
		ElectionTimeoutMax: 80 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// alwaysGrantTransport grants every RequestVote instantly and lets tests
// script the AppendEntries reply via a callback, so a single Node can be
// driven to Leader without a real peer or the harness package.
type scriptedTransport struct {
	mu               sync.Mutex
	appendEntriesFn  func(req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error)
	requestVoteReply func(req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error)
}

func (s *scriptedTransport) SendRequestVote(_ context.Context, _ string, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	s.mu.Lock()
	fn := s.requestVoteReply
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &pb.RequestVoteReply{Term: req.GetTerm(), VoteGranted: true}, nil
}

func (s *scriptedTransport) SendAppendEntries(_ context.Context, _ string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	s.mu.Lock()
	fn := s.appendEntriesFn
	s.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &pb.AppendEntriesReply{Term: req.GetTerm(), Success: true}, nil
}

func (s *scriptedTransport) SendReadIndex(_ context.Context, _ string, req *pb.ReadIndexRequest) (*pb.ReadIndexReply, error) {
	return &pb.ReadIndexReply{Ok: false}, nil
}

// TestLeaderStepsDownOnHigherTermInAppendEntriesReply is the dedicated test
// required by §3: a Leader receives an AppendEntries reply carrying a
// higher term than its own, and must be observed reverting to Follower —
// the failure mode this guards against is a stale Leader that keeps
// sending heartbeats and accepting writes after a newer election has
// already happened elsewhere.
func TestLeaderStepsDownOnHigherTermInAppendEntriesReply(t *testing.T) {
	const higherTerm = uint64(999)
	transport := &scriptedTransport{}

	n, err := raft.NewNode(raft.Config{
		ID:        "A",
		DataDir:   t.TempDir(),
		Peers:     []string{"B"}, // one peer, quorum = 2 (self + peer vote)
		Timing:    fastTiming(),
		Tick:      2 * time.Millisecond,
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	waitFor(t, time.Second, func() bool {
		role, _ := n.Status()
		return role == raft.Leader
	})

	// Now that A believes it's Leader, make every subsequent AppendEntries
	// reply report a much higher term than A's own.
	transport.mu.Lock()
	transport.appendEntriesFn = func(req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
		return &pb.AppendEntriesReply{Term: higherTerm, Success: false}, nil
	}
	transport.mu.Unlock()

	waitFor(t, time.Second, func() bool {
		role, term := n.Status()
		return role == raft.Follower && term == higherTerm
	})
}

// TestFollowerStepsDownOnHigherTermInIncomingRequest covers the "easy"
// half of the universal higher-term rule explicitly for completeness: a
// node in any role observes a higher term in an incoming RPC request (not
// just a reply) and must update immediately.
func TestFollowerStepsDownOnHigherTermInIncomingRequest(t *testing.T) {
	n, err := raft.NewNode(raft.Config{
		ID:      "A",
		DataDir: t.TempDir(),
		Timing:  fastTiming(),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	reply, err := n.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:     50,
		LeaderId: "B",
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.GetSuccess() {
		t.Fatalf("expected success on empty log with prevLogIndex 0, got %+v", reply)
	}
	role, term := n.Status()
	if role != raft.Follower || term != 50 {
		t.Fatalf("got role=%v term=%d, want Follower/50", role, term)
	}
}

// TestRequestVote_RejectsCandidateWithStaleLog verifies condition 2 of the
// vote-granting rule (§3): a candidate whose log is less up-to-date than
// the voter's own must be rejected, even if the candidate hasn't been
// voted for yet.
func TestRequestVote_RejectsCandidateWithStaleLog(t *testing.T) {
	transport := &scriptedTransport{}
	n, err := raft.NewNode(raft.Config{
		ID:        "A",
		DataDir:   t.TempDir(),
		Timing:    fastTiming(),
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	// Single-node cluster (no peers) wins its own election immediately.
	waitFor(t, time.Second, func() bool {
		role, _ := n.Status()
		return role == raft.Leader
	})
	if _, _, ok := n.Propose([]byte("x")); !ok {
		t.Fatalf("Propose failed while Leader")
	}
	if _, _, ok := n.Propose([]byte("y")); !ok {
		t.Fatalf("Propose failed while Leader")
	}
	_, leaderTerm := n.Status()

	reply, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term:         leaderTerm + 1,
		CandidateId:  "stale-candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if reply.GetVoteGranted() {
		t.Fatalf("expected vote to be rejected for a candidate with an empty/stale log against a 2-entry log")
	}
}

// TestRequestVote_RejectsSecondCandidateSameTerm verifies condition 1: a
// voter that already granted its vote to one candidate in a term must
// reject a different candidate in that same term, but must still grant a
// duplicate request from the same candidate it already voted for.
func TestRequestVote_RejectsSecondCandidateSameTerm(t *testing.T) {
	n, err := raft.NewNode(raft.Config{
		ID:      "A",
		DataDir: t.TempDir(),
		Timing:  fastTiming(),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	first, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{Term: 5, CandidateId: "X"})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !first.GetVoteGranted() {
		t.Fatalf("expected first vote in term 5 to be granted, got %+v", first)
	}

	second, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{Term: 5, CandidateId: "Y"})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if second.GetVoteGranted() {
		t.Fatalf("expected second candidate in the same term to be rejected, got %+v", second)
	}

	repeat, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{Term: 5, CandidateId: "X"})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !repeat.GetVoteGranted() {
		t.Fatalf("expected a repeat request from the already-voted-for candidate to still be granted, got %+v", repeat)
	}
}
