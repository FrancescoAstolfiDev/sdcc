package harness_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/raft/harness"
)

func newTestCluster(t *testing.T, n int) *harness.Cluster {
	t.Helper()
	base := t.TempDir()
	c, err := harness.NewCluster(n, func(id string) string {
		return fmt.Sprintf("%s/%s", base, id)
	}, harness.FastTiming())
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	t.Cleanup(c.Stop)
	return c
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func otherIDs(all []string, exclude ...string) []string {
	excluded := make(map[string]bool, len(exclude))
	for _, e := range exclude {
		excluded[e] = true
	}
	var out []string
	for _, id := range all {
		if !excluded[id] {
			out = append(out, id)
		}
	}
	return out
}

// TestElection_NoFaults: normal leader election with no faults.
func TestElection_NoFaults(t *testing.T) {
	c := newTestCluster(t, 3)

	leader, term, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if term == 0 {
		t.Fatalf("expected a positive election term, got 0")
	}
	for _, id := range c.IDs {
		role, nodeTerm := c.Nodes[id].Status()
		if id == leader {
			if role != raft.Leader {
				t.Fatalf("elected leader %s reports role %v", id, role)
			}
			continue
		}
		if role != raft.Follower {
			t.Fatalf("non-leader %s reports role %v, want Follower", id, role)
		}
		if nodeTerm != term {
			t.Fatalf("follower %s at term %d, want %d", id, nodeTerm, term)
		}
	}
}

// TestHeartbeats_SuppressFollowerElections: heartbeats must keep followers
// from timing out and starting spurious elections under normal operation.
func TestHeartbeats_SuppressFollowerElections(t *testing.T) {
	c := newTestCluster(t, 3)

	_, term, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Wait several multiples of the max election timeout; with heartbeats
	// working, the term must never change and exactly one leader must
	// remain throughout.
	deadline := time.Now().Add(8 * harness.FastTiming().ElectionTimeoutMax)
	for time.Now().Before(deadline) {
		leaders := c.Leaders()
		if len(leaders) != 1 {
			t.Fatalf("expected exactly one leader while heartbeats are flowing, got %v", leaders)
		}
		for _, lterm := range leaders {
			if lterm != term {
				t.Fatalf("leader term changed from %d to %d without any fault", term, lterm)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestReplication_QuorumCommitWithUnresponsiveFollower: log replication and
// quorum commit must succeed even with one follower unresponsive, since
// 2 of 3 is already a quorum.
func TestReplication_QuorumCommitWithUnresponsiveFollower(t *testing.T) {
	c := newTestCluster(t, 3)

	leader, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	unresponsive := otherIDs(c.IDs, leader)[0]
	healthy := otherIDs(c.IDs, leader, unresponsive)[0]

	c.Net.SetFilter(harness.Disconnected(unresponsive))

	index, _, ok := c.Nodes[leader].Propose([]byte("hello"))
	if !ok {
		t.Fatalf("Propose failed on leader %s", leader)
	}

	waitUntil(t, 2*time.Second, func() bool {
		applied := c.Applied(leader)
		return len(applied) >= 1 && applied[0].Index == index
	})
	waitUntil(t, 2*time.Second, func() bool {
		applied := c.Applied(healthy)
		return len(applied) >= 1 && applied[0].Index == index
	})
	if got := c.Applied(unresponsive); len(got) != 0 {
		t.Fatalf("unresponsive follower %s should not have applied anything, got %+v", unresponsive, got)
	}
}

// TestLeaderCrash_ReelectionAndCommitSafety reproduces the exact scenario
// from §4 point 3 / §6 Step B bullet 4: a leader replicates an entry to
// followers (durably, on disk) but crashes before ever learning the writes
// succeeded, so it never commits. A new leader is elected from the
// followers that do have the entry. The entry must NOT be silently exposed
// as committed on the strength of quorum replication alone (it was
// appended in the old leader's term) until the new leader commits an entry
// of its own current term, which then indirectly commits it too.
func TestLeaderCrash_ReelectionAndCommitSafety(t *testing.T) {
	c := newTestCluster(t, 3)

	leader, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	followers := otherIDs(c.IDs, leader)

	// Let the request through to both followers (so they durably persist
	// it) but black-hole every reply the old leader would see, so it can
	// never reach quorum acknowledgment and never commits.
	c.Net.SetFilter(harness.DropReplyFrom(leader))

	index, term, ok := c.Nodes[leader].Propose([]byte("figure-8-entry"))
	if !ok {
		t.Fatalf("Propose failed on leader %s", leader)
	}

	// Give both followers time to durably persist the entry via the (reply
	// silently dropped) AppendEntries call.
	time.Sleep(200 * time.Millisecond)

	// "Crash" the old leader: fully partition it from here on.
	c.Net.SetFilter(harness.Disconnected(leader))

	// A new leader must be elected from the two remaining followers.
	var newLeader string
	waitUntil(t, 3*time.Second, func() bool {
		for _, id := range followers {
			role, nodeTerm := c.Nodes[id].Status()
			if role == raft.Leader && nodeTerm > term {
				newLeader = id
				return true
			}
		}
		return false
	})
	other := otherIDs(followers, newLeader)[0]

	// Give the new leader a chance to send at least one heartbeat/replication
	// round to the remaining follower, so matchIndex reaches a numeric
	// quorum for the old-term entry — this is the exact moment the unsafe
	// implementation would incorrectly commit it.
	time.Sleep(200 * time.Millisecond)

	if applied := c.Applied(newLeader); len(applied) != 0 {
		t.Fatalf("old-term entry %d must not be committed on quorum count alone; new leader %s already applied %+v", index, newLeader, applied)
	}
	if applied := c.Applied(other); len(applied) != 0 {
		t.Fatalf("old-term entry %d must not be committed on quorum count alone; follower %s already applied %+v", index, other, applied)
	}

	// Now the new leader commits an entry of its OWN current term. This
	// must indirectly commit the earlier old-term entry too.
	newIndex, _, ok := c.Nodes[newLeader].Propose([]byte("new-term-entry"))
	if !ok {
		t.Fatalf("Propose failed on new leader %s", newLeader)
	}

	waitUntil(t, 2*time.Second, func() bool {
		applied := c.Applied(newLeader)
		return len(applied) >= 2 && applied[len(applied)-1].Index == newIndex
	})
	applied := c.Applied(newLeader)
	if len(applied) != 2 || applied[0].Index != index || applied[1].Index != newIndex {
		t.Fatalf("expected old entry %d then new entry %d applied in order on %s, got %+v", index, newIndex, newLeader, applied)
	}

	waitUntil(t, 2*time.Second, func() bool {
		applied := c.Applied(other)
		return len(applied) >= 2 && applied[len(applied)-1].Index == newIndex
	})
}

// TestLogRepair_ConvergesWithinBoundedRoundTrips forces a follower to miss
// a whole batch of entries while partitioned and confirms the leader's fast
// conflictIndex/conflictTerm backoff converges the follower's log in a
// small, bounded number of AppendEntries round trips once reconnected —
// not one round trip per conflicting entry, which is what a naive
// decrement-nextIndex-by-one-and-retry implementation would need (§5).
func TestLogRepair_ConvergesWithinBoundedRoundTrips(t *testing.T) {
	c := newTestCluster(t, 3)

	leader, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	behind := otherIDs(c.IDs, leader)[0]

	// Partition the follower away, then replicate several entries across
	// several distinct terms while it's gone.
	c.Net.SetFilter(harness.Disconnected(behind))

	const entriesPerRound = 3
	const rounds = 4
	var lastIndex uint64
	for r := 0; r < rounds; r++ {
		for e := 0; e < entriesPerRound; e++ {
			idx, _, ok := c.Nodes[leader].Propose([]byte(fmt.Sprintf("r%d-e%d", r, e)))
			if !ok {
				t.Fatalf("Propose failed mid-run (round %d, entry %d)", r, e)
			}
			lastIndex = idx
		}
		waitUntil(t, 2*time.Second, func() bool {
			applied := c.Applied(leader)
			return len(applied) > 0 && applied[len(applied)-1].Index == lastIndex
		})
	}

	// Reconnect the follower and measure how many AppendEntries round
	// trips it takes to fully catch up.
	var roundTrips int
	c.Net.SetFilter(func(from, to string, kind harness.MessageKind) (bool, bool, time.Duration) {
		if kind == harness.KindAppendEntries && to == behind {
			roundTrips++
		}
		return false, false, 0
	})

	waitUntil(t, 3*time.Second, func() bool {
		applied := c.Applied(behind)
		return len(applied) > 0 && applied[len(applied)-1].Index == lastIndex
	})

	// A naive one-decrement-per-rejection repair would need close to
	// rounds*entriesPerRound (12) round trips for a follower this far
	// behind; the fast conflictIndex/conflictTerm jump should need only a
	// handful regardless of how many entries are missing.
	const maxAcceptableRoundTrips = 6
	if roundTrips > maxAcceptableRoundTrips {
		t.Fatalf("log repair took %d AppendEntries round trips to converge %d entries; expected a small bounded number (<=%d), not ~one per conflicting entry", roundTrips, rounds*entriesPerRound, maxAcceptableRoundTrips)
	}
	t.Logf("log repair converged %d entries in %d AppendEntries round trips", rounds*entriesPerRound, roundTrips)
}

// TestProposeSync_CommitsSuccessfully: the blocking ProposeSync variant
// must not return until the entry is actually committed (replicated to and
// applied by a quorum) — not merely durable on the leader's own disk.
func TestProposeSync_CommitsSuccessfully(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	index, _, err := c.Nodes[leader].ProposeSync(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("ProposeSync: %v", err)
	}

	applied := c.Applied(leader)
	if len(applied) == 0 || applied[len(applied)-1].Index != index {
		t.Fatalf("ProposeSync returned before the entry was actually applied: applied=%+v index=%d", applied, index)
	}
}

// TestProposeSync_AbortsOnLeadershipLost: if the leader that accepted a
// ProposeSync call loses leadership — a new leader wins election at a
// higher term — before the entry commits, the call must return
// ErrLeadershipLost promptly instead of hanging until the context deadline.
// This is the exact hazard discussed for the crash case (§ Scenario 3/5 of
// SIMULAZIONE_SCENARI.md): the entry may or may not survive under the new
// leader, so the old leader can only report "I can no longer promise",
// never a definite success or failure.
func TestProposeSync_AbortsOnLeadershipLost(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, term, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	followers := otherIDs(c.IDs, leader)

	// Drop every message the old leader tries to SEND (so its heartbeats
	// never reach the followers and they time out into a new election), but
	// let messages sent TO it through — so it does learn about the new term
	// once the new leader asserts itself, instead of being fully
	// partitioned and never finding out at all.
	c.Net.SetFilter(func(from, to string, _ harness.MessageKind) (bool, bool, time.Duration) {
		return from == leader, false, 0
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, _, err := c.Nodes[leader].ProposeSync(ctx, []byte("orphaned"))
		errCh <- err
	}()

	// Wait for the followers to elect a new leader at a higher term — this
	// is what will eventually reach the old leader and make it step down.
	waitUntil(t, 3*time.Second, func() bool {
		for _, id := range followers {
			role, nodeTerm := c.Nodes[id].Status()
			if role == raft.Leader && nodeTerm > term {
				return true
			}
		}
		return false
	})

	select {
	case err := <-errCh:
		if !errors.Is(err, raft.ErrLeadershipLost) {
			t.Fatalf("ProposeSync error = %v, want ErrLeadershipLost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ProposeSync did not return after leadership was lost; it should not block until the context deadline")
	}
}
