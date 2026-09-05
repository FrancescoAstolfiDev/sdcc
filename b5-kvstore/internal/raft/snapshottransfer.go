package raft

import (
	"bytes"
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/internal/raft/persistence"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// GetLogStatus, StreamLogRange, and ConfirmCompaction implement
// pb.SnapshotTransferServer (§9.4): the surface the Snapshot & Backup
// service calls against this node. Per the spec, GetLogStatus and
// ConfirmCompaction are only meaningful when this node is the leader (only
// the leader can authoritatively decide what's safe to truncate) — the
// backup service is responsible for targeting the leader and re-verifying
// before ConfirmCompaction (§1 points 1/5); these handlers don't
// themselves reject a non-leader caller, since doing the (harmless,
// idempotent) work anyway is simpler than adding a role gate no correct
// caller should ever need.

// GetLogStatus is a cheap, non-blocking read (matching GetStatus's spirit,
// §0) — polled by the Snapshot & Backup service every
// SNAPSHOT_BACKUP_POLL_INTERVAL_MS (§1 point 2).
func (n *Node) GetLogStatus(_ context.Context, _ *emptypb.Empty) (*pb.LogStatusReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	total := uint64(len(n.log))
	var firstIndex uint64
	if total > 0 {
		firstIndex = n.logOffset + 1
	}
	return &pb.LogStatusReply{
		TotalEntries:     total,
		OccupancyPercent: total * 100 / n.snapshotCfg.LogCapacityEntries,
		FirstIndex:       firstIndex,
		LastIndex:        n.lastLogIndexLocked(),
	}, nil
}

// StreamLogRange serves the bulk historical download (§1 point 3): may be
// called against any node, leader or follower, since it only ever reads
// already-committed entries (no linearizability concern, §9.4). The entries
// are copied out under lock, then streamed without holding it — sending
// over the network must never block AppendEntries/heartbeat handling.
func (n *Node) StreamLogRange(req *pb.LogRangeRequest, stream pb.SnapshotTransfer_StreamLogRangeServer) error {
	n.mu.Lock()
	var entries []persistence.LogEntry
	for idx := req.GetFromIndex(); idx <= req.GetToIndex(); idx++ {
		if e := n.entryAtLocked(idx); e != nil {
			entries = append(entries, *e)
		}
	}
	n.mu.Unlock()

	for _, e := range entries {
		if err := stream.Send(&pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}); err != nil {
			return err
		}
	}
	return nil
}

// ConfirmCompaction is the leader-side trigger for actually removing
// entries from the active log (§1 point 3's "only remove them from the
// leader's active log after the backup write is confirmed") — the backup
// service has already durably stored everything up to
// truncated_up_to_index, so it's now safe to drop the local copy in favor
// of the anchor placeholder (§5.1/§5.3). state_machine_snapshot (§9.4) is
// decoded and used as-is for the local snapshot file's contents, rather
// than recomputed from this node's own live state machine: by the time
// this RPC arrives, the leader has very likely already applied entries
// *past* truncated_up_to_index too (writes keep flowing during a backup
// cycle), so only the backup service's own point-in-time replica — built
// incrementally as it streamed entries — has the value that's actually
// correct for that specific index (see compactLocked's doc for why this
// matters).
func (n *Node) ConfirmCompaction(_ context.Context, req *pb.CompactionAck) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	target := req.GetTruncatedUpToIndex()
	if target <= n.logOffset+1 {
		// Already compacted at least this far: anchor.Index == logOffset+1
		// once compacted, so target <= logOffset+1 means the anchor already
		// covers it. (target <= n.logOffset alone is off by one: right
		// after compacting to target, logOffset == target-1, which fails
		// that check and would re-run the whole compaction — refetching
		// nothing new and re-logging LOCAL_LOG_COMPACTED — on every
		// duplicate/retried ConfirmCompaction for the same index.)
		return &emptypb.Empty{}, nil
	}
	if target > n.lastApplied {
		return nil, fmt.Errorf("raft: cannot confirm compaction up to index %d: not yet applied (lastApplied=%d)", target, n.lastApplied)
	}
	file, err := snapshotfile.Decode(bytes.NewReader(req.GetStateMachineSnapshot()))
	if err != nil {
		return nil, fmt.Errorf("raft: cannot confirm compaction: decode state_machine_snapshot: %w", err)
	}
	if file.LastIncludedIndex != target {
		return nil, fmt.Errorf("raft: state_machine_snapshot lastIncludedIndex=%d does not match truncated_up_to_index=%d", file.LastIncludedIndex, target)
	}
	if err := n.compactLocked(file); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
