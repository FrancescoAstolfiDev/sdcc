package snapshot

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/internal/grpcutil"
	"b5-kvstore/pkg/pb"
)

// TransferClient is this service's client toward consensus nodes'
// SnapshotTransfer API (§9.4). Interfaced so the compaction cycle's
// threshold/concurrency-guard/leader-re-verification logic (§5, unit
// tests) can run against a fake, deterministic node — no real gRPC needed.
type TransferClient interface {
	// GetLogStatus is deliberately fail-fast (§0): a transient failure just
	// means "try again next poll interval."
	GetLogStatus(ctx context.Context, addr string) (*pb.LogStatusReply, error)
	// StreamLogRange downloads entries [fromIndex, toIndex] from addr (any
	// node, leader or follower, §9.4). WaitForReady(true) (§0).
	StreamLogRange(ctx context.Context, addr string, fromIndex, toIndex uint64) ([]*pb.LogEntry, error)
	// ConfirmCompaction tells addr (must be the current leader, §1 point 5)
	// it's safe to truncate up to truncatedUpToIndex. WaitForReady(true) (§0).
	ConfirmCompaction(ctx context.Context, addr string, truncatedUpToIndex uint64, stateMachineSnapshot []byte) error
}

// GRPCTransferClient is a TransferClient backed by real gRPC, caching one
// connection per address (mirroring internal/raft/grpctransport's pattern)
// since the leader/download target changes over the service's lifetime.
// Dials via grpcutil.DialPassthrough (md-week5.md §0): every reconnection
// attempt re-resolves fresh against Docker's embedded DNS instead of a
// cached gRPC resolver, exactly like the consensus-peer transport fix.
type GRPCTransferClient struct {
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCTransferClient builds a GRPCTransferClient. With no options, it
// dials plaintext (insecure) gRPC, matching every other internal client in
// this project (§11.2: never crosses a public network boundary).
func NewGRPCTransferClient(opts ...grpc.DialOption) *GRPCTransferClient {
	return &GRPCTransferClient{dialOpts: opts, conns: make(map[string]*grpc.ClientConn)}
}

func (t *GRPCTransferClient) clientFor(addr string) (pb.SnapshotTransferClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	conn, ok := t.conns[addr]
	if !ok {
		var err error
		conn, err = grpcutil.DialPassthrough(addr, t.dialOpts...)
		if err != nil {
			return nil, err
		}
		t.conns[addr] = conn
	}
	return pb.NewSnapshotTransferClient(conn), nil
}

func (t *GRPCTransferClient) GetLogStatus(ctx context.Context, addr string) (*pb.LogStatusReply, error) {
	c, err := t.clientFor(addr)
	if err != nil {
		return nil, err
	}
	return c.GetLogStatus(ctx, &emptypb.Empty{})
}

func (t *GRPCTransferClient) StreamLogRange(ctx context.Context, addr string, fromIndex, toIndex uint64) ([]*pb.LogEntry, error) {
	c, err := t.clientFor(addr)
	if err != nil {
		return nil, err
	}
	stream, err := c.StreamLogRange(ctx, &pb.LogRangeRequest{FromIndex: fromIndex, ToIndex: toIndex}, grpc.WaitForReady(true))
	if err != nil {
		return nil, err
	}
	var entries []*pb.LogEntry
	for {
		e, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (t *GRPCTransferClient) ConfirmCompaction(ctx context.Context, addr string, truncatedUpToIndex uint64, stateMachineSnapshot []byte) error {
	c, err := t.clientFor(addr)
	if err != nil {
		return err
	}
	_, err = c.ConfirmCompaction(ctx, &pb.CompactionAck{
		TruncatedUpToIndex:   truncatedUpToIndex,
		StateMachineSnapshot: stateMachineSnapshot,
	}, grpc.WaitForReady(true))
	return err
}

// Close tears down every cached connection.
func (t *GRPCTransferClient) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var firstErr error
	for addr, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(t.conns, addr)
	}
	return firstErr
}
