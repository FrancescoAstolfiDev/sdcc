package proxy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"b5-kvstore/pkg/pb"
)

// DiscoveryClient fetches the current ClusterView from Service Discovery.
// Interfaced for the same reason as KVClientFactory: so tests can
// substitute a fake, deterministic Discovery view.
type DiscoveryClient interface {
	GetClusterView(ctx context.Context) (*pb.ClusterView, error)
}

// GRPCDiscoveryClient is a DiscoveryClient backed by a real gRPC connection
// to the Discovery service.
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
