package snapshot

import (
	"os"
	"sync"

	"google.golang.org/protobuf/proto"

	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/internal/statemachine"
	"b5-kvstore/pkg/pb"
)

// Store is the Snapshot & Backup service's own durable state (§5.2): a
// cumulative state-machine replica, built by replaying the same committed
// entries a consensus node would (reusing internal/statemachine.KV.Apply
// directly, rather than re-implementing PUT/UPDATE/DELETE semantics), plus
// the metadata of the most recently completed compaction. Shared between
// Compactor (the writer, §5.1) and CatalogServer (the reader, §9.5's
// SnapshotCatalog API) so there is exactly one place that knows "what's the
// latest snapshot" — matching §3.6's persistence conventions (write-to-
// temp-then-rename, single current file with the previous one cleaned up).
type Store struct {
	dataDir string

	mu                sync.RWMutex
	state             *statemachine.KV
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	available         bool
	currentPath       string
}

// NewStore builds a Store rooted at dataDir, recovering the latest
// previously-written snapshot file (if any) so a restart of this service
// doesn't forget everything it had already backed up.
func NewStore(dataDir string) (*Store, error) {
	s := &Store{dataDir: dataDir, state: statemachine.New()}
	file, path, found, err := snapshotfile.Latest(dataDir)
	if err != nil {
		return nil, err
	}
	if found {
		s.state.Restore(file.State)
		s.lastIncludedIndex = file.LastIncludedIndex
		s.lastIncludedTerm = file.LastIncludedTerm
		s.available = true
		s.currentPath = path
	}
	return s, nil
}

// Latest returns the most recently completed compaction's boundary.
func (s *Store) Latest() (lastIncludedIndex, lastIncludedTerm uint64, available bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIncludedIndex, s.lastIncludedTerm, s.available
}

// File returns the current snapshot's full contents (state machine map +
// boundary), or ok=false if none has been produced yet.
func (s *Store) File() (file snapshotfile.File, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.available {
		return snapshotfile.File{}, false
	}
	return snapshotfile.File{
		State:             s.state.Snapshot(),
		LastIncludedIndex: s.lastIncludedIndex,
		LastIncludedTerm:  s.lastIncludedTerm,
	}, true
}

// Apply feeds newly downloaded, already-committed entries (in index order)
// into the running replica — the same apply logic a consensus node itself
// uses (internal/statemachine.KV.Apply), so the resulting map matches
// exactly what the source node's own state machine holds at that point.
func (s *Store) Apply(entries []*pb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		var cmd pb.KVCommand
		if err := proto.Unmarshal(e.GetCommand(), &cmd); err != nil {
			return err
		}
		s.state.Apply(&cmd)
	}
	return nil
}

// Commit persists the replica's current contents as the new snapshot
// boundary (lastIncludedIndex, lastIncludedTerm), replacing whichever
// snapshot preceded it (§3.6: "the previous snapshot file... may be
// deleted"), and returns the written file's full contents.
func (s *Store) Commit(lastIncludedIndex, lastIncludedTerm uint64) (snapshotfile.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file := snapshotfile.File{
		State:             s.state.Snapshot(),
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	}
	newPath, err := snapshotfile.WriteFile(s.dataDir, file)
	if err != nil {
		return snapshotfile.File{}, err
	}
	old := s.currentPath
	s.currentPath = newPath
	s.lastIncludedIndex = lastIncludedIndex
	s.lastIncludedTerm = lastIncludedTerm
	s.available = true
	if old != "" && old != newPath {
		_ = os.Remove(old)
	}
	return file, nil
}
