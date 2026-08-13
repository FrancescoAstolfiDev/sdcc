package harness

import (
	"context"
	"testing"
	"time"

	"b5-kvstore/internal/raft"
	"b5-kvstore/pkg/pb"
)

func TestSmokeReadIndex(t *testing.T) {
	dirs := map[string]string{}
	c, err := NewCluster(3, func(id string) string {
		if dirs[id] == "" {
			dirs[id] = t.TempDir()
		}
		return dirs[id]
	}, FastTiming())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leaderID, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	leader := c.Nodes[leaderID]
	idx, _, err := leader.ProposeSync(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("ProposeSync: %v", err)
	}
	t.Logf("committed at index %d", idx)

	var followerID string
	for id := range c.Nodes {
		if id != leaderID {
			followerID = id
			break
		}
	}
	follower := c.Nodes[followerID]

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := follower.FollowerLinearizableRead(ctx); err != nil {
		t.Fatalf("FollowerLinearizableRead: %v", err)
	}
	t.Log("follower linearizable read succeeded")

	// Direct RequestReadIndex against leader.
	reply, err := c.Net.Transport(followerID).SendReadIndex(context.Background(), leaderID, &pb.ReadIndexRequest{FollowerId: followerID})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.GetOk() || reply.GetReadIndex() < idx {
		t.Fatalf("unexpected reply: %+v", reply)
	}

	// Non-leader RequestReadIndex should say ok=false with a redirect.
	reply2, err := c.Net.Transport(leaderID).SendReadIndex(context.Background(), followerID, &pb.ReadIndexRequest{FollowerId: leaderID})
	if err != nil {
		t.Fatal(err)
	}
	if reply2.GetOk() {
		t.Fatalf("expected ok=false from a follower's RequestReadIndex, got %+v", reply2)
	}
}

// TestReadIndex_FailsWhenQuorumUnreachable covers spec §4.2 point 13's
// safety property directly: RequestReadIndex must never hand back a
// readIndex unless confirmLeadershipQuorum actually got a fresh
// quorum-acked heartbeat round — not just because this node still believes
// itself Leader. Every AppendEntries this node sends is dropped (both
// peers, so acked can never exceed 1/3, well under quorum), while nothing
// causes the node to observe a higher term or otherwise step down — it
// stays Leader in its own state throughout, isolating the one property
// under test (quorum confirmation) from role/term correctness, which is
// already covered elsewhere.
func TestReadIndex_FailsWhenQuorumUnreachable(t *testing.T) {
	dirs := map[string]string{}
	c, err := NewCluster(3, func(id string) string {
		if dirs[id] == "" {
			dirs[id] = t.TempDir()
		}
		return dirs[id]
	}, FastTiming())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leaderID, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	leader := c.Nodes[leaderID]

	idx, _, err := leader.ProposeSync(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("ProposeSync: %v", err)
	}

	// Drop every AppendEntries the leader sends, to every peer, from now
	// on. RequestVote/ReadIndex traffic is left alone — irrelevant here and
	// left working means a spurious election isn't what makes this test
	// pass or fail.
	c.Net.SetFilter(func(from, _ string, kind MessageKind) (dropRequest, dropReply bool, delay time.Duration) {
		if kind == KindAppendEntries && from == leaderID {
			return true, false, 0
		}
		return false, false, 0
	})
	defer c.Net.SetFilter(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, rpcErr := leader.RequestReadIndex(ctx, &pb.ReadIndexRequest{FollowerId: "someone"})

	if rpcErr == nil && reply.GetOk() {
		t.Fatalf("RequestReadIndex must not succeed without a quorum-acked heartbeat round: got ok=true readIndex=%d (leader believes itself Leader the whole time; committed index was %d)", reply.GetReadIndex(), idx)
	}
	// Whichever shape the failure takes (RPC error while still Leader, or
	// ok=false), a caller must never be able to extract a usable readIndex
	// from it.
	if reply != nil && reply.GetOk() {
		t.Fatalf("reply must never report ok=true when quorum was unreachable, got %+v", reply)
	}
	if role, _ := leader.Status(); role != raft.Leader {
		t.Fatalf("test setup invariant broken: leader should still believe itself Leader (isolating the quorum-confirmation failure from a role/term change), got role=%s", role)
	}
}

// TestReadIndex_FollowerWaitsForLaggingApply covers spec §4.2 point 14
// directly: a follower whose lastApplied is genuinely behind the readIndex
// the leader hands back must actually block inside waitForApplied, not
// return early with a stale answer. AppendEntries to the follower under
// test is dropped from the start (so it never learns about the entry the
// leader commits), while ReadIndex traffic is left alone, so its
// FollowerLinearizableRead call reaches the leader, gets back a readIndex
// past what it has applied, and must wait. A goroutine + channel proves the
// block is real: the call must not complete within a bounded window while
// still blocked, and must complete only once lastApplied is explicitly
// advanced by a direct AppendEntries call this test controls (standing in
// for "the block lifts and the leader's next heartbeat delivers it") — not
// inferred from a bare sleep.
func TestReadIndex_FollowerWaitsForLaggingApply(t *testing.T) {
	dirs := map[string]string{}
	c, err := NewCluster(3, func(id string) string {
		if dirs[id] == "" {
			dirs[id] = t.TempDir()
		}
		return dirs[id]
	}, FastTiming())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leaderID, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	leader := c.Nodes[leaderID]

	var laggingID string
	for id := range c.Nodes {
		if id != leaderID {
			laggingID = id
			break
		}
	}
	lagging := c.Nodes[laggingID]

	// WaitForLeader only confirms leaderID's own Status() reports Leader —
	// it says nothing about whether followers have yet processed that
	// leader's first heartbeat (Node.currentLeader is only set inside the
	// AppendEntries handler, and becomeLeaderLocked's initial broadcast is
	// asynchronous). Wait for that explicitly and before installing the
	// filter below: once it's installed, lagging can never receive an
	// AppendEntries again, so it must already know the leader by then, or
	// FollowerLinearizableRead has nothing to ask and bails out immediately
	// with an empty leaderAddr — a test-setup race, not anything the
	// Read-Index protocol itself got wrong.
	deadline := time.Now().Add(2 * time.Second)
	for lagging.LeaderAddress() != leaderID && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := lagging.LeaderAddress(); got != leaderID {
		t.Fatalf("lagging follower never recorded the elected leader: got %q, want %q", got, leaderID)
	}

	// Block AppendEntries specifically to the lagging follower from now on
	// — the other follower stays fully connected, so the leader can still
	// reach quorum (self + the other follower) for both the ProposeSync
	// below and the read-index confirmation.
	c.Net.SetFilter(func(_, to string, kind MessageKind) (dropRequest, dropReply bool, delay time.Duration) {
		if kind == KindAppendEntries && to == laggingID {
			return true, false, 0
		}
		return false, false, 0
	})
	defer c.Net.SetFilter(nil)

	idx, _, err := leader.ProposeSync(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("ProposeSync: %v", err)
	}
	if got := lagging.LastApplied(); got != 0 {
		t.Fatalf("test setup invariant broken: lagging follower should still be at lastApplied=0 (blocked from the entry), got %d", got)
	}

	resultCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		resultCh <- lagging.FollowerLinearizableRead(ctx)
	}()

	// Must NOT return within a bounded window: waitForApplied should be
	// genuinely parked (lastApplied 0 < readIndex idx), not a premature
	// success.
	select {
	case err := <-resultCh:
		t.Fatalf("FollowerLinearizableRead returned early (err=%v) before lastApplied was advanced — waitForApplied did not actually block", err)
	case <-time.After(200 * time.Millisecond):
		// still blocked, as expected
	}

	// Being cut off from every AppendEntries also cuts the lagging follower
	// off from heartbeats, so — correctly, matching the documented
	// isolated-node behavior (week4-mid.md §4) — its election timer keeps
	// firing and climbing terms for as long as the block stands. Explicitly
	// lifting it here (rather than crafting a manual AppendEntries with a
	// term captured before that climb, which the climb would make stale)
	// lets the leader's regular heartbeat retry deliver the entry itself,
	// at whatever term is current by then: nextIndex for this follower
	// never advanced (every prior attempt was dropped before ever counting
	// as a reply), so the very next heartbeat resends exactly the missing
	// entry.
	c.Net.SetFilter(nil)

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("FollowerLinearizableRead failed after lastApplied was explicitly advanced: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("FollowerLinearizableRead never returned after lastApplied was explicitly advanced")
	}

	if got := lagging.LastApplied(); got < idx {
		t.Fatalf("expected lastApplied >= %d after unblocking, got %d", idx, got)
	}
}
