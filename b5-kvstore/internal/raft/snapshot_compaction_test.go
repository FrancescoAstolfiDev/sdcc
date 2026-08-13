package raft_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/internal/statemachine"
	"b5-kvstore/pkg/pb"
)

func putCmd(t *testing.T, key, value string) []byte {
	t.Helper()
	b, err := proto.Marshal(&pb.KVCommand{Op: pb.KVCommand_PUT, Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal KVCommand: %v", err)
	}
	return b
}

func applyFnFor(kv *statemachine.KV) func(raft.ApplyMsg) {
	return func(msg raft.ApplyMsg) {
		var cmd pb.KVCommand
		if err := proto.Unmarshal(msg.Command, &cmd); err == nil {
			kv.Apply(&cmd)
		}
	}
}

// TestLocalCompaction_ShrinksLogAndSurvivesRestart covers ConfirmCompaction
// (§1's leader-side trigger) and §3.6's restart-recovery startup sequence
// together: compacting must shrink the physical log to the anchor plus
// whatever came after it, persist a local snapshot-<index>.dat built from
// the *authoritative* (Snapshot & Backup service-provided) state rather
// than this node's own live state machine — which has already moved past
// the compaction point by the time this runs, since entries 4/5 were
// committed before compaction — and a fresh Node constructed against the
// same DataDir afterward must reconstruct lastApplied/log exactly from
// that snapshot, not from 0.
func TestLocalCompaction_ShrinksLogAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	kv := statemachine.New()

	node, err := raft.NewNode(raft.Config{
		ID:      "node-1",
		Address: "node-1",
		DataDir: dir,
		// A phantom peer, not zero: ProposeSync only ever commits via
		// maybeAdvanceCommitIndexLocked, which only runs off an
		// AppendEntries *reply* — with zero peers that path never fires,
		// so nothing would ever commit. scriptedTransport's default
		// AppendEntries handler auto-acks, giving a real (if fake) quorum
		// partner instead of relying on a self-only quorum path that
		// doesn't exist in this codebase.
		Peers:     []string{"peer-b"},
		Timing:    fastTiming(),
		Tick:      5 * time.Millisecond,
		Transport: &scriptedTransport{},
		ApplyFn:   applyFnFor(kv),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.Start()

	waitFor(t, time.Second, func() bool {
		role, _ := node.Status()
		return role == raft.Leader
	})

	for i := 0; i < 5; i++ {
		if _, _, err := node.ProposeSync(context.Background(), putCmd(t, fmt.Sprintf("k%d", i), "v")); err != nil {
			t.Fatalf("ProposeSync #%d: %v", i, err)
		}
	}
	waitFor(t, time.Second, func() bool { return node.LastApplied() >= 5 })

	// The authoritative snapshot content as of index 3 specifically (k0-k2
	// only) — deliberately NOT everything kv currently holds (k0-k4), which
	// is exactly the trap compactLocked's design avoids (see its doc).
	stateAt3 := map[string]string{"k0": "v", "k1": "v", "k2": "v"}
	file := snapshotfile.File{State: stateAt3, LastIncludedIndex: 3, LastIncludedTerm: 1}
	var buf bytes.Buffer
	if err := snapshotfile.Encode(&buf, file); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if _, err := node.ConfirmCompaction(context.Background(), &pb.CompactionAck{
		TruncatedUpToIndex:   3,
		StateMachineSnapshot: buf.Bytes(),
	}); err != nil {
		t.Fatalf("ConfirmCompaction: %v", err)
	}

	status, err := node.GetLogStatus(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLogStatus: %v", err)
	}
	if status.GetTotalEntries() != 3 {
		t.Fatalf("got TotalEntries=%d, want 3 (anchor + 2 remaining entries)", status.GetTotalEntries())
	}
	if status.GetFirstIndex() != 3 || status.GetLastIndex() != 5 {
		t.Fatalf("got firstIndex=%d lastIndex=%d, want 3/5", status.GetFirstIndex(), status.GetLastIndex())
	}

	node.Stop()

	// Restart: §3.6's startup sequence.
	loaded, _, found, err := snapshotfile.Latest(dir)
	if err != nil || !found {
		t.Fatalf("expected a local snapshot file after compaction, found=%v err=%v", found, err)
	}
	if loaded.LastIncludedIndex != 3 {
		t.Fatalf("got local snapshot lastIncludedIndex=%d, want 3", loaded.LastIncludedIndex)
	}

	kv2 := statemachine.New()
	kv2.Restore(loaded.State)

	node2, err := raft.NewNode(raft.Config{
		ID:            "node-1",
		Address:       "node-1",
		DataDir:       dir,
		Timing:        fastTiming(),
		Tick:          5 * time.Millisecond,
		Transport:     &scriptedTransport{},
		LocalSnapshot: &loaded,
		ApplyFn:       applyFnFor(kv2),
	})
	if err != nil {
		t.Fatalf("NewNode (restart): %v", err)
	}
	defer node2.Stop()

	if got := node2.LastApplied(); got != 3 {
		t.Fatalf("got lastApplied=%d after restart, want 3", got)
	}
	if got := kv2.Snapshot()["k1"]; got != "v" {
		t.Fatalf("state machine not restored after restart: k1=%q", got)
	}
	if _, ok := kv2.Snapshot()["k4"]; ok {
		t.Fatalf("state machine should not contain k4 (index 5, beyond the anchor) — it must come back only via normal replication")
	}

	status2, err := node2.GetLogStatus(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLogStatus (restart): %v", err)
	}
	if status2.GetFirstIndex() != 3 || status2.GetLastIndex() != 5 {
		t.Fatalf("got firstIndex=%d lastIndex=%d after restart, want 3/5", status2.GetFirstIndex(), status2.GetLastIndex())
	}
}

// TestConfirmCompaction_RejectsMismatchedSnapshotIndex is a light defensive
// check on the RPC boundary: a request whose embedded snapshot disagrees
// with its own truncated_up_to_index must be rejected, not silently
// accepted with inconsistent state.
func TestConfirmCompaction_RejectsMismatchedSnapshotIndex(t *testing.T) {
	dir := t.TempDir()
	kv := statemachine.New()
	node, err := raft.NewNode(raft.Config{
		ID:        "node-1",
		Address:   "node-1",
		DataDir:   dir,
		Peers:     []string{"peer-b"},
		Timing:    fastTiming(),
		Tick:      5 * time.Millisecond,
		Transport: &scriptedTransport{},
		ApplyFn:   applyFnFor(kv),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	node.Start()
	defer node.Stop()
	waitFor(t, time.Second, func() bool { role, _ := node.Status(); return role == raft.Leader })

	// index 2, not 1: target==1 with logOffset==0 is the one degenerate
	// case where "never compacted" and "already compacted to exactly 1"
	// are indistinguishable from logOffset alone (see ConfirmCompaction's
	// idempotency check) — practically unreachable in production (it needs
	// occupancy >= threshold at a single log entry), but worth steering
	// this test clear of so it exercises the mismatch check, not that
	// boundary.
	if _, _, err := node.ProposeSync(context.Background(), putCmd(t, "k0", "v")); err != nil {
		t.Fatalf("ProposeSync #1: %v", err)
	}
	if _, _, err := node.ProposeSync(context.Background(), putCmd(t, "k1", "v")); err != nil {
		t.Fatalf("ProposeSync #2: %v", err)
	}
	waitFor(t, time.Second, func() bool { return node.LastApplied() >= 2 })

	var buf bytes.Buffer
	_ = snapshotfile.Encode(&buf, snapshotfile.File{State: map[string]string{}, LastIncludedIndex: 999, LastIncludedTerm: 1})

	_, err = node.ConfirmCompaction(context.Background(), &pb.CompactionAck{
		TruncatedUpToIndex:   2,
		StateMachineSnapshot: buf.Bytes(),
	})
	if err == nil {
		t.Fatalf("expected ConfirmCompaction to reject a mismatched snapshot index, got nil error")
	}
}
