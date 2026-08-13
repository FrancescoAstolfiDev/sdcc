package discovery

import (
	"context"
	"testing"
	"time"

	"b5-kvstore/pkg/pb"
)

// fakeProber is a StatusProber controlled entirely by the test: each
// address maps to either a canned reply or a "never responds" sentinel
// (simulated via a context that's always already expired), letting tests
// exercise the "peer doesn't respond within RPCTimeout" path deterministically.
type fakeProber struct {
	replies map[string]*pb.GetStatusReply
	timeout map[string]bool // addresses that should look like they never reply
}

func (f *fakeProber) GetStatus(ctx context.Context, addr string) (*pb.GetStatusReply, error) {
	if f.timeout[addr] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if r, ok := f.replies[addr]; ok {
		return r, nil
	}
	return nil, context.DeadlineExceeded
}

func TestPollOnce_BuildsClusterView(t *testing.T) {
	prober := &fakeProber{replies: map[string]*pb.GetStatusReply{
		"node-1:9001": {NodeId: "node-1", Role: pb.Role_LEADER, Term: 3},
		"node-2:9002": {NodeId: "node-2", Role: pb.Role_FOLLOWER, Term: 3},
		"node-3:9003": {NodeId: "node-3", Role: pb.Role_FOLLOWER, Term: 3},
	}}
	reg := NewRegistry([]string{"node-1:9001", "node-2:9002", "node-3:9003"}, prober, 50*time.Millisecond)
	reg.PollOnce(context.Background())

	view := reg.ClusterView()
	if view.GetLeaderAddress() != "node-1:9001" {
		t.Fatalf("leader address = %q, want node-1:9001", view.GetLeaderAddress())
	}
	if len(view.GetFollowers()) != 2 {
		t.Fatalf("followers = %d, want 2", len(view.GetFollowers()))
	}
	if len(view.GetAllNodes()) != 3 {
		t.Fatalf("allNodes = %d, want 3", len(view.GetAllNodes()))
	}
}

func TestPollOnce_UnresponsiveNodeDroppedNotStickyDown(t *testing.T) {
	prober := &fakeProber{
		replies: map[string]*pb.GetStatusReply{
			"node-1:9001": {NodeId: "node-1", Role: pb.Role_LEADER, Term: 1},
		},
		timeout: map[string]bool{"node-2:9002": true},
	}
	reg := NewRegistry([]string{"node-1:9001", "node-2:9002"}, prober, 30*time.Millisecond)

	reg.PollOnce(context.Background())
	view := reg.ClusterView()
	if len(view.GetAllNodes()) != 1 {
		t.Fatalf("round 1: allNodes = %d, want 1 (node-2 should be dropped this round)", len(view.GetAllNodes()))
	}

	// A node that comes back on the next round must be re-added automatically
	// — no sticky "down" state carried over.
	prober.timeout = nil
	prober.replies["node-2:9002"] = &pb.GetStatusReply{NodeId: "node-2", Role: pb.Role_FOLLOWER, Term: 1}
	reg.PollOnce(context.Background())
	view = reg.ClusterView()
	if len(view.GetAllNodes()) != 2 {
		t.Fatalf("round 2: allNodes = %d, want 2 (node-2 should have rejoined)", len(view.GetAllNodes()))
	}
}

func TestPollOnce_NoLeaderYieldsEmptyLeaderAddress(t *testing.T) {
	prober := &fakeProber{replies: map[string]*pb.GetStatusReply{
		"node-1:9001": {NodeId: "node-1", Role: pb.Role_CANDIDATE, Term: 4},
	}}
	reg := NewRegistry([]string{"node-1:9001"}, prober, 30*time.Millisecond)
	reg.PollOnce(context.Background())
	if got := reg.ClusterView().GetLeaderAddress(); got != "" {
		t.Fatalf("leader address = %q, want empty (no node reported Leader)", got)
	}
}

func TestRun_PollsImmediatelyThenOnInterval(t *testing.T) {
	prober := &fakeProber{replies: map[string]*pb.GetStatusReply{
		"node-1:9001": {NodeId: "node-1", Role: pb.Role_LEADER, Term: 1},
	}}
	reg := NewRegistry([]string{"node-1:9001"}, prober, 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		reg.Run(ctx, 25*time.Millisecond)
		close(done)
	}()

	// The initial poll (before the ticker fires) should already be visible
	// almost immediately, not after a full interval.
	time.Sleep(10 * time.Millisecond)
	if reg.ClusterView().GetLeaderAddress() != "node-1:9001" {
		t.Fatal("expected the immediate initial poll to have populated the view")
	}
	<-done
}
