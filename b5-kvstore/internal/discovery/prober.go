package discovery

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"b5-kvstore/pkg/pb"
)

// StatusProber calls GetStatus on a single consensus-node address. It's an
// interface (rather than a concrete gRPC dependency baked into Registry) so
// the polling/aggregation logic in registry.go can be unit tested with a
// fake, deterministic prober instead of real network calls — the same
// decoupling principle already used for internal/raft's Transport.
type StatusProber interface {
	GetStatus(ctx context.Context, addr string) (*pb.GetStatusReply, error)
}

// GRPCProber is a StatusProber backed by real gRPC connections to
// pkg/pb's Consensus service, one cached connection per address.
type GRPCProber struct {
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCProber builds a GRPCProber. With no options it dials peers over
// plaintext gRPC — Discovery only ever talks to consensus nodes inside the
// isolated Docker network (§11.2), same as the raft grpctransport.
func NewGRPCProber(opts ...grpc.DialOption) *GRPCProber {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &GRPCProber{dialOpts: opts, conns: make(map[string]*grpc.ClientConn)}
}

func (p *GRPCProber) clientFor(addr string) (pb.ConsensusClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	conn, ok := p.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, p.dialOpts...)
		if err != nil {
			return nil, err
		}
		p.conns[addr] = conn
	}
	return pb.NewConsensusClient(conn), nil
}

func (p *GRPCProber) GetStatus(ctx context.Context, addr string) (*pb.GetStatusReply, error) {
	client, err := p.clientFor(addr)
	if err != nil {
		return nil, err
	}
	return client.GetStatus(ctx, &pb.GetStatusRequest{})
}

// Close tears down every cached connection.
func (p *GRPCProber) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(p.conns, addr)
	}
	return firstErr
}
