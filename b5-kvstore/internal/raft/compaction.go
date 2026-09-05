package raft

import (
	"fmt"
	"log"
	"os"

	"b5-kvstore/internal/raft/persistence"
	"b5-kvstore/internal/snapshotfile"
)

// compactLocked rewrites the local log so every entry at or before
// file.LastIncludedIndex is replaced by a single anchor placeholder entry
// (Term == file.LastIncludedTerm), keeping only entries after it, and
// persists file as the new local snapshot backing that anchor (§3.6). This
// is the shared primitive behind both compaction paths:
//   - the leader's immediate truncation on ConfirmCompaction (§5.1,
//     snapshottransfer.go), where file is decoded from the Snapshot &
//     Backup service's own request;
//   - every node's own periodic local-compaction check against the
//     Snapshot Catalog (§5.3, snapshotcatchup.go), where file is fetched
//     from the catalog for the occasion.
//
// file.State must already represent the state machine as of
// file.LastIncludedIndex specifically. Deliberately NOT reconstructed from
// this node's own live state machine here: by the time compaction runs,
// this node has very likely already applied entries *past*
// file.LastIncludedIndex too (writes keep flowing during a backup cycle),
// so its live map reflects more than the anchor is supposed to — only the
// Snapshot & Backup service, which built the map incrementally by
// replaying entries up to exactly that point, has the correct point-in-time
// value. Baking the live (too-current) map into an anchor tagged with an
// older index would make a later catch-up double-apply the entries between
// the anchor and what was actually live at compaction time.
//
// Must be called with n.mu held, and only once the caller has confirmed
// file.LastIncludedIndex > n.logOffset (there's compaction to do) and
// n.lastApplied >= file.LastIncludedIndex (§5.3's safety precondition:
// never compact away an entry this node hasn't itself already applied).
func (n *Node) compactLocked(file snapshotfile.File) error {
	targetIndex := file.LastIncludedIndex
	if n.entryAtLocked(targetIndex) == nil {
		return fmt.Errorf("raft: cannot compact up to index %d: not present in local log (have %d..%d)", targetIndex, n.logOffset+1, n.lastLogIndexLocked())
	}

	newPath, err := snapshotfile.WriteFile(n.dataDir, file)
	if err != nil {
		return err
	}
	n.replaceSnapshotFileLocked(newPath)

	anchor := persistence.LogEntry{Term: file.LastIncludedTerm, Index: targetIndex}
	pos := targetIndex - n.logOffset // first slice position strictly after targetIndex
	kept := append([]persistence.LogEntry{anchor}, n.log[pos:]...)
	if err := persistence.RewriteLog(n.dataDir, kept); err != nil {
		return err
	}
	n.log = kept
	n.logOffset = targetIndex - 1
	log.Printf("consensus-node[%s]: LOCAL_LOG_COMPACTED truncatedTo=%d", n.id, targetIndex)
	return nil
}

// replaceSnapshotFileLocked records newPath as the current local snapshot
// file and best-effort deletes whichever one preceded it (§3.6: "the
// previous snapshot file... may be deleted"). Must be called with n.mu
// held.
func (n *Node) replaceSnapshotFileLocked(newPath string) {
	old := n.currentSnapshotPath
	n.currentSnapshotPath = newPath
	if old != "" && old != newPath {
		_ = os.Remove(old)
	}
}
