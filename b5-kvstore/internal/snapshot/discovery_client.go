package snapshot

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/pkg/pb"
)

// DiscoveryClient resolves the current cluster leader (§5.1's "never
// hard-code a leader address"). Interfaced so tests can substitute a fake,
// deterministic view — same principle as internal/proxy's DiscoveryClient.
type DiscoveryClient interface {
	GetClusterView(ctx context.Context) (*pb.ClusterView, error)
}

// GRPCDiscoveryClient is a DiscoveryClient backed by a real gRPC connection
// to the Discovery service. Discovery is a single well-known, long-lived
// address (not a peer that gets killed/restarted as part of any tested
// scenario), so — matching internal/proxy's own discovery client — this
// dials plainly rather than through grpcutil.DialPassthrough; that fix is
// for the two new Week 5 directions specifically (md-week5.md §0), not this
// pre-existing one.
type GRPCDiscoveryClient struct {
	client pb.DiscoveryClient
	conn   *grpc.ClientConn
}

// NewGRPCDiscoveryClient dials addr (Service Discovery's address).
func NewGRPCDiscoveryClient(addr string, opts ...grpc.DialOption) (*GRPCDiscoveryClient, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &GRPCDiscoveryClient{client: pb.NewDiscoveryClient(conn), conn: conn}, nil
}

func (c *GRPCDiscoveryClient) GetClusterView(ctx context.Context) (*pb.ClusterView, error) {
	return c.client.GetClusterView(ctx, &emptypb.Empty{})
}

func (c *GRPCDiscoveryClient) Close() error {
	return c.conn.Close()
}
