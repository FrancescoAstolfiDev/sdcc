package harness_test

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/raft/harness"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/internal/statemachine"
	"b5-kvstore/pkg/pb"
)

// safeLogBuffer is a thread-safe io.Writer for capturing log.Printf output
// in tests. log.SetOutput's writer is invoked from whatever goroutine calls
// log.Printf — here, the node's background snapshotPollLoop
// (started by Node.Start()) — while the test reads the captured content
// from the main test goroutine via String(). A plain bytes.Buffer is not
// safe for that combination (concurrent Write from one goroutine, String's
// underlying read from another); this wraps one behind a mutex so both
// sides serialize on the same lock.
type safeLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSnapshotCatchup_AnchorsAndResumesReplication exercises the §3
// catch-up state machine end to end at unit-test speed: a node
// starting with lastApplied=0 against a fake catalog reporting a snapshot
// at index 50 should anchor correctly (adopt the state, never apply the
// anchor itself) and resume normal AppendEntries-based replication from
// there — no containers, no real gRPC.
func TestSnapshotCatchup_AnchorsAndResumesReplication(t *testing.T) {
	dir := t.TempDir()
	catalog := &harness.FakeSnapshotCatalog{}
	catalog.SetSnapshot(snapshotfile.File{
		State:             map[string]string{"k": "v"},
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
	})
	kv := statemachine.New()

	// A long election timeout keeps this single, peer-less node a Follower
	// for the whole test (no self-election noise to race against): the
	// catch-up loop is independent of role/leadership by design (§5.3).
	longTiming := raft.TimingConfig{
		ElectionTimeoutMin: 10 * time.Minute,
		ElectionTimeoutMax: 20 * time.Minute,
		HeartbeatInterval:  5 * time.Millisecond,
	}

	node, err := raft.NewNode(raft.Config{
		ID:              "node-1",
		Address:         "node-1",
		DataDir:         dir,
		Timing:          longTiming,
		Tick:            5 * time.Millisecond,
		Transport:       harness.NewNetwork().Transport("node-1"),
		Snapshot:        raft.SnapshotConfig{PollInterval: 20 * time.Millisecond},
		SnapshotCatalog: catalog,
		StateMachine:    kv,
		ApplyFn:         func(raft.ApplyMsg) {},
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.Start()
	defer node.Stop()

	waitUntil(t, 2*time.Second, func() bool { return node.LastApplied() >= 50 })

	if got := kv.Snapshot()["k"]; got != "v" {
		t.Fatalf("state machine not restored from fetched snapshot: got %q, want %q", got, "v")
	}
	if role, _ := node.Status(); role != raft.Follower {
		t.Fatalf("expected node to remain Follower, got %s", role)
	}

	// Resume replication from the anchor: a leader sending prevLogIndex/
	// prevLogTerm matching the anchor must be accepted, and the new entry
	// applied — confirming the anchor genuinely satisfies the
	// prevLogIndex/prevLogTerm consistency check per §3 point 2.
	reply, err := node.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader-x",
		PrevLogIndex: 50,
		PrevLogTerm:  3,
		Entries:      []*pb.LogEntry{{Term: 1, Index: 51, Command: []byte("cmd")}},
		LeaderCommit: 51,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.GetSuccess() {
		t.Fatalf("expected AppendEntries past the anchor to succeed, got %+v", reply)
	}
	waitUntil(t, time.Second, func() bool { return node.LastApplied() >= 51 })
}

// TestSnapshotCatchup_FetchFailureKeepsExistingState covers §3 point 4's
// failure handling: a transient FetchSnapshot error must not discard
// whatever state the node already has.
func TestSnapshotCatchup_FetchFailureKeepsExistingState(t *testing.T) {
	dir := t.TempDir()
	catalog := &harness.FakeSnapshotCatalog{}
	catalog.SetSnapshot(snapshotfile.File{State: map[string]string{"k": "v"}, LastIncludedIndex: 50, LastIncludedTerm: 3})
	catalog.SetFetchErr(context.DeadlineExceeded)
	kv := statemachine.New()

	longTiming := raft.TimingConfig{
		ElectionTimeoutMin: 10 * time.Minute,
		ElectionTimeoutMax: 20 * time.Minute,
		HeartbeatInterval:  5 * time.Millisecond,
	}
	node, err := raft.NewNode(raft.Config{
		ID:              "node-1",
		Address:         "node-1",
		DataDir:         dir,
		Timing:          longTiming,
		Tick:            5 * time.Millisecond,
		Transport:       harness.NewNetwork().Transport("node-1"),
		Snapshot:        raft.SnapshotConfig{PollInterval: 15 * time.Millisecond},
		SnapshotCatalog: catalog,
		StateMachine:    kv,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.Start()
	defer node.Stop()

	// Give the loop several chances to (fail to) catch up.
	time.Sleep(150 * time.Millisecond)

	if node.LastApplied() != 0 {
		t.Fatalf("expected lastApplied to stay 0 after repeated fetch failures, got %d", node.LastApplied())
	}
	if len(kv.Snapshot()) != 0 {
		t.Fatalf("expected state machine untouched after repeated fetch failures, got %+v", kv.Snapshot())
	}
}

// TestLocalCompaction_DoesNotRepeatAcrossPollTicks is a regression test for
// an off-by-one in the local-compaction trigger condition (§5.3 point 5):
// once a node has compacted up to target, logOffset == target-1, which the
// original `target > logOffset` check still satisfied — so every
// subsequent poll tick re-fetched the snapshot and re-ran the whole
// compaction, re-logging LOCAL_LOG_COMPACTED forever. This must fire
// exactly once per target, not once per tick.
func TestLocalCompaction_DoesNotRepeatAcrossPollTicks(t *testing.T) {
	net := harness.NewNetwork()
	kv1 := statemachine.New()
	catalog := &harness.FakeSnapshotCatalog{}
	timing := harness.FastTiming()

	applyFn := func(kv *statemachine.KV) func(raft.ApplyMsg) {
		return func(msg raft.ApplyMsg) {
			var cmd pb.KVCommand
			if err := proto.Unmarshal(msg.Command, &cmd); err == nil {
				kv.Apply(&cmd)
			}
		}
	}

	node1, err := raft.NewNode(raft.Config{
		ID: "node-1", Address: "node-1", DataDir: t.TempDir(),
		Peers: []string{"node-2"}, Timing: timing, Tick: 5 * time.Millisecond,
		Transport:       net.Transport("node-1"),
		Snapshot:        raft.SnapshotConfig{PollInterval: 25 * time.Millisecond},
		SnapshotCatalog: catalog,
		StateMachine:    kv1,
		ApplyFn:         applyFn(kv1),
	})
	if err != nil {
		t.Fatalf("NewNode node-1: %v", err)
	}
	net.Register("node-1", node1)

	node2, err := raft.NewNode(raft.Config{
		ID: "node-2", Address: "node-2", DataDir: t.TempDir(),
		Peers: []string{"node-1"}, Timing: timing, Tick: 5 * time.Millisecond,
		Transport: net.Transport("node-2"),
	})
	if err != nil {
		t.Fatalf("NewNode node-2: %v", err)
	}
	net.Register("node-2", node2)

	node1.Start()
	node2.Start()
	defer node1.Stop()
	defer node2.Stop()

	var leader *raft.Node
	waitUntil(t, time.Second, func() bool {
		if r, _ := node1.Status(); r == raft.Leader {
			leader = node1
			return true
		}
		if r, _ := node2.Status(); r == raft.Leader {
			leader = node2
			return true
		}
		return false
	})

	for i := 0; i < 3; i++ {
		cmd, err := proto.Marshal(&pb.KVCommand{Op: pb.KVCommand_PUT, Key: "k", Value: "v"})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, _, err := leader.ProposeSync(context.Background(), cmd); err != nil {
			t.Fatalf("ProposeSync #%d: %v", i, err)
		}
	}
	waitUntil(t, time.Second, func() bool { return node1.LastApplied() >= 3 })

	var logBuf safeLogBuffer
	orig := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(orig)

	catalog.SetSnapshot(snapshotfile.File{State: map[string]string{"k": "v"}, LastIncludedIndex: 2, LastIncludedTerm: 1})

	waitUntil(t, time.Second, func() bool {
		return strings.Contains(logBuf.String(), "LOCAL_LOG_COMPACTED truncatedTo=2")
	})

	// Let several more poll intervals elapse and confirm it did not fire again.
	time.Sleep(200 * time.Millisecond)

	count := strings.Count(logBuf.String(), "LOCAL_LOG_COMPACTED truncatedTo=2")
	if count != 1 {
		t.Fatalf("expected LOCAL_LOG_COMPACTED to fire exactly once for target=2, fired %d times:\n%s", count, logBuf.String())
	}
}
