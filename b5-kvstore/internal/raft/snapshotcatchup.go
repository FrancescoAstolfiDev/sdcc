package raft

import (
	"context"
	"log"
	"time"

	"b5-kvstore/internal/raft/persistence"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// SnapshotCatalogClient is how a Node reaches the Snapshot & Backup
// service's Snapshot Catalog API (§9.5) for its periodic catch-up/local-
// compaction loop (§5.3). Mirrors the same abstraction principle as
// Transport: the real implementation dials real gRPC
// (internal/raft/snapshotcatalogclient), while tests substitute an
// in-process fake (harness) — no real networking needed to exercise the
// catch-up state machine.
type SnapshotCatalogClient interface {
	GetLatestSnapshotInfo(ctx context.Context) (*pb.SnapshotInfo, error)
	// FetchSnapshot returns the decoded contents of the latest snapshot.
	// The wire-level chunk reassembly (§9.5's streamed SnapshotChunk) is
	// the concrete client's concern, not this interface's.
	FetchSnapshot(ctx context.Context) (snapshotfile.File, error)
}

// StateMachineSnapshotter is the state-machine access the catch-up loop
// needs: adopting a fetched map as this node's new baseline. A structural
// interface so this package doesn't need to import internal/statemachine —
// *statemachine.KV satisfies it as-is. (Local compaction, by contrast, gets
// its snapshot content from the Snapshot Catalog rather than this node's
// own live state machine — see compactLocked's doc — so it doesn't need a
// Snapshot()-style read method here.)
type StateMachineSnapshotter interface {
	Restore(state map[string]string)
}

// snapshotPollLoop is the periodic loop backing §5.3: every
// snapshotCfg.PollInterval, check the Snapshot Catalog for a newer snapshot
// than this node has applied (catch up if so) and for a local-compaction
// opportunity (truncate if this node is already at or past what the backup
// service holds). Started from Start() only when a SnapshotCatalogClient
// was configured.
func (n *Node) snapshotPollLoop() {
	t := time.NewTicker(n.snapshotCfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.snapshotPollOnce()
		}
	}
}

func (n *Node) snapshotPollOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()

	info, err := n.snapshotCatalog.GetLatestSnapshotInfo(ctx)
	if err != nil || info == nil || !info.GetAvailable() {
		// Transient failure or nothing backed up yet: try again next tick
		// (§2's GetLatestSnapshotInfo is documented cheap/fail-fast, §0).
		return
	}
	target := info.GetLastIncludedIndex()

	n.mu.Lock()
	lastApplied := n.lastApplied
	logOffset := n.logOffset
	n.mu.Unlock()

	// Catch-up rule (§5.3 point 2/3): covers both a brand-new node
	// (lastApplied == 0) and an already-attached node that fell behind a
	// compaction point, with no separate code path for the two.
	if target > lastApplied {
		n.catchUpFromSnapshot(ctx, target)
		return
	}

	// Local compaction rule (§5.3 point 5): applies to every node
	// including the leader, independent of catch-up — this node is already
	// at or past what the backup service holds, so it's safe to drop the
	// physical prefix in favor of the anchor. The snapshot content comes
	// from the catalog, not this node's own live state machine (see
	// compactLocked's doc: this node has very likely already applied
	// entries past target too, so only the catalog's point-in-time replica
	// is correct for target specifically).
	//
	// "Already compacted at least this far" is logOffset+1 >= target, i.e.
	// anchor.Index already covers target (compactLocked always leaves
	// logOffset == the index it just compacted to, minus one). The
	// condition below is that check's negation — plain `target > logOffset`
	// is off by one and would re-fire (re-fetching the snapshot and
	// re-running compactLocked, hence re-logging LOCAL_LOG_COMPACTED) on
	// every single tick forever after the first successful compaction to
	// target, since logOffset == target-1 satisfies `target > logOffset`
	// just as much as any genuinely-not-yet-compacted value would.
	if target > logOffset+1 {
		file, err := n.snapshotCatalog.FetchSnapshot(ctx)
		if err != nil || file.LastIncludedIndex != target {
			return // transient failure, or a newer snapshot has already superseded target: retry next tick
		}
		n.mu.Lock()
		// Re-check under lock: lastApplied/logOffset may have moved since
		// the unlocked read above (e.g. ConfirmCompaction already did this
		// on the leader, concurrently with this tick).
		if target > n.logOffset+1 && target <= n.lastApplied {
			if err := n.compactLocked(file); err != nil {
				log.Printf("consensus-node[%s]: local compaction to index %d failed: %v", n.id, target, err)
			}
		}
		n.mu.Unlock()
	}
}

// catchUpFromSnapshot fetches and adopts the latest snapshot as this node's
// new baseline: wholesale-replaces the local log with a single anchor entry
// and restores the state machine from the snapshot's map (§5.3 point 2/3;
// no separate path for a brand-new node vs. one that fell behind). On any
// failure it leaves existing state untouched and returns — §5.3 point 4's
// failure handling — so the next tick simply retries.
func (n *Node) catchUpFromSnapshot(ctx context.Context, targetIndex uint64) {
	n.mu.Lock()
	lastApplied := n.lastApplied
	n.mu.Unlock()
	log.Printf("consensus-node[%s]: SNAPSHOT_CATCHUP_START lastApplied=%d targetIndex=%d", n.id, lastApplied, targetIndex)

	file, err := n.snapshotCatalog.FetchSnapshot(ctx)
	if err != nil {
		return
	}

	anchor := persistence.LogEntry{Term: file.LastIncludedTerm, Index: file.LastIncludedIndex}
	if anchor.Index == 0 {
		return // defensive: a zero-index snapshot is not a valid baseline
	}

	// Persist the fetched snapshot locally first (§3.6), then rewrite
	// log.dat — in that order, so a crash between the two still leaves
	// NewNode able to recover correctly (it trusts the snapshot file's
	// lastIncludedIndex over whatever log.dat happens to still contain).
	newPath, err := snapshotfile.WriteFile(n.dataDir, file)
	if err != nil {
		return
	}
	if err := persistence.RewriteLog(n.dataDir, []persistence.LogEntry{anchor}); err != nil {
		return
	}
	if n.stateMachine != nil {
		n.stateMachine.Restore(file.State)
	}

	n.mu.Lock()
	n.replaceSnapshotFileLocked(newPath)
	n.log = []persistence.LogEntry{anchor}
	n.logOffset = anchor.Index - 1
	if anchor.Index > n.commitIndex {
		n.commitIndex = anchor.Index
	}
	if anchor.Index > n.lastApplied {
		n.lastApplied = anchor.Index
	}
	newLastApplied := n.lastApplied
	n.mu.Unlock()

	log.Printf("consensus-node[%s]: SNAPSHOT_CATCHUP_COMPLETE newLastApplied=%d", n.id, newLastApplied)
}
