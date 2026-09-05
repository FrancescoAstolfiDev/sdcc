// Package snapshotcatalogclient is the real gRPC implementation of
// raft.SnapshotCatalogClient: a consensus node's client toward the Snapshot
// & Backup service's Snapshot Catalog API (§9.5), used by the periodic
// catch-up/local-compaction loop (§5.3).
package snapshotcatalogclient

import (
	"bytes"
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/internal/grpcutil"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// Client dials the Snapshot & Backup service via grpcutil.DialPassthrough:
// every reconnection attempt re-resolves fresh instead of reusing a cached
// gRPC resolver, exactly like internal/raft/grpctransport's consensus-peer
// dialing.
type Client struct {
	client pb.SnapshotCatalogClient
	conn   *grpc.ClientConn
}

// New dials addr (the Snapshot & Backup service's address).
func New(addr string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpcutil.DialPassthrough(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{client: pb.NewSnapshotCatalogClient(conn), conn: conn}, nil
}

// GetLatestSnapshotInfo is deliberately fail-fast — no WaitForReady: this
// is a cheap, frequent (every SNAPSHOT_POLL_INTERVAL_MS) poll, so a
// transient failure just means "try again next interval."
func (c *Client) GetLatestSnapshotInfo(ctx context.Context) (*pb.SnapshotInfo, error) {
	return c.client.GetLatestSnapshotInfo(ctx, &emptypb.Empty{})
}

// FetchSnapshot streams the latest snapshot's chunks and reassembles them
// into a decoded snapshotfile.File. WaitForReady(true): a node catching up
// should wait rather than fail-fast if the Snapshot &
// Backup service happens to be mid-restart, since this is the specific RPC
// that gets a lagging node unstuck.
func (c *Client) FetchSnapshot(ctx context.Context) (snapshotfile.File, error) {
	stream, err := c.client.FetchSnapshot(ctx, &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		return snapshotfile.File{}, err
	}
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return snapshotfile.File{}, err
		}
		buf.Write(chunk.GetData())
		if chunk.GetIsLast() {
			break
		}
	}
	return snapshotfile.Decode(&buf)
}

// Close tears down the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
