package snapshot

import (
	"bytes"
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// chunkSize bounds each SnapshotChunk sent by FetchSnapshot. This project's
// scale doesn't call for streaming straight off disk without buffering
// (md-week5.md §2: "a simple in-memory read-then-chunk implementation is
// acceptable — don't over-engineer this") — the whole encoded snapshot is
// built in memory once, then split into chunkSize pieces just so a single
// SnapshotChunk message doesn't grow unbounded with the state machine.
const chunkSize = 64 * 1024

// CatalogServer implements pb.SnapshotCatalogServer (§9.5) over a Store.
type CatalogServer struct {
	pb.UnimplementedSnapshotCatalogServer

	store *Store
}

// NewCatalogServer wraps store as a pb.SnapshotCatalogServer.
func NewCatalogServer(store *Store) *CatalogServer {
	return &CatalogServer{store: store}
}

// GetLatestSnapshotInfo is cheap and non-blocking (§2): a plain read off
// Store, no I/O.
func (s *CatalogServer) GetLatestSnapshotInfo(_ context.Context, _ *emptypb.Empty) (*pb.SnapshotInfo, error) {
	index, term, available := s.store.Latest()
	return &pb.SnapshotInfo{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Available:         available,
	}, nil
}

// FetchSnapshot streams the current snapshot's encoded contents in
// chunkSize pieces.
func (s *CatalogServer) FetchSnapshot(_ *emptypb.Empty, stream pb.SnapshotCatalog_FetchSnapshotServer) error {
	file, ok := s.store.File()
	if !ok {
		return status.Error(codes.NotFound, "no snapshot available yet")
	}
	var buf bytes.Buffer
	if err := snapshotfile.Encode(&buf, file); err != nil {
		return err
	}
	data := buf.Bytes()
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&pb.SnapshotChunk{Data: data[i:end], IsLast: end == len(data)}); err != nil {
			return err
		}
	}
	return nil
}
