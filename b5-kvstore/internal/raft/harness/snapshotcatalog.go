package harness

import (
	"context"
	"sync"

	"b5-kvstore/internal/raft"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// FakeSnapshotCatalog is an in-process, controllable stand-in for
// raft.SnapshotCatalogClient (§5.3), letting catch-up tests exercise a
// Node's periodic poll loop without the real Snapshot & Backup service or
// any gRPC (md-week5.md §5).
type FakeSnapshotCatalog struct {
	mu        sync.Mutex
	available bool
	file      snapshotfile.File
	fetchErr  error
	infoErr   error
}

var _ raft.SnapshotCatalogClient = (*FakeSnapshotCatalog)(nil)

// SetSnapshot makes file the latest available snapshot.
func (f *FakeSnapshotCatalog) SetSnapshot(file snapshotfile.File) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.file = file
	f.available = true
}

// SetFetchErr makes FetchSnapshot fail with err (nil clears it).
func (f *FakeSnapshotCatalog) SetFetchErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetchErr = err
}

func (f *FakeSnapshotCatalog) GetLatestSnapshotInfo(_ context.Context) (*pb.SnapshotInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	return &pb.SnapshotInfo{
		LastIncludedIndex: f.file.LastIncludedIndex,
		LastIncludedTerm:  f.file.LastIncludedTerm,
		Available:         f.available,
	}, nil
}

func (f *FakeSnapshotCatalog) FetchSnapshot(_ context.Context) (snapshotfile.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErr != nil {
		return snapshotfile.File{}, f.fetchErr
	}
	return f.file, nil
}
