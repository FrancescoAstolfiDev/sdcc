// Package proxy implements the Client Proxy (spec §4): REST<->gRPC
// translation, leader/follower routing with redirect-following, the
// Read-Index-aware read path, and Circuit Breaker wiring around every
// outbound call to a consensus node. It is stateless beyond the cached
// ClusterView (§4.1's "MUST NOT participate in consensus").
package proxy

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"b5-kvstore/pkg/pb"
)

// KVClientFactory returns a pb.KVServiceClient for a given node address.
// It's an interface (rather than a concrete gRPC dependency baked into
// Proxy) so §6's routing/redirect/Read-Index tests can run the exact same
// Proxy routing code against the in-process raft harness instead of real
// gRPC — see internal/proxy's test harness adapter.
type KVClientFactory interface {
	KVClientFor(addr string) pb.KVServiceClient
}

// GRPCClientFactory dials peers lazily and caches one connection per
// address, exactly like internal/raft/grpctransport and
// internal/discovery's GRPCProber.
type GRPCClientFactory struct {
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCClientFactory builds a GRPCClientFactory. With no options it dials
// over plaintext gRPC, inside the isolated Docker network (§11.2).
func NewGRPCClientFactory(opts ...grpc.DialOption) *GRPCClientFactory {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &GRPCClientFactory{dialOpts: opts, conns: make(map[string]*grpc.ClientConn)}
}

func (f *GRPCClientFactory) KVClientFor(addr string) pb.KVServiceClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	conn, ok := f.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, f.dialOpts...)
		if err != nil {
			// A construction-time dial error (bad target syntax) is the
			// only failure mode here: grpc.NewClient doesn't actually
			// connect eagerly. Any real connectivity failure surfaces
			// later, from the RPC call itself, where it's handled through
			// the circuit breaker like any other transport error.
			return errClient{err: err}
		}
		f.conns[addr] = conn
	}
	return pb.NewKVServiceClient(conn)
}

// Close tears down every cached connection.
func (f *GRPCClientFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var firstErr error
	for addr, conn := range f.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(f.conns, addr)
	}
	return firstErr
}

// errClient is a pb.KVServiceClient that fails every call with a fixed
// error — used only for the (practically unreachable) grpc.NewClient
// construction-error case above, so KVClientFor never needs to return an
// error itself and callers have one uniform "the call failed" path.
type errClient struct{ err error }

func (e errClient) Get(context.Context, *pb.GetRequest, ...grpc.CallOption) (*pb.GetReply, error) {
	return nil, e.err
}
func (e errClient) Put(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.WriteReply, error) {
	return nil, e.err
}
func (e errClient) Update(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.WriteReply, error) {
	return nil, e.err
}
func (e errClient) Delete(context.Context, *pb.DeleteRequest, ...grpc.CallOption) (*pb.WriteReply, error) {
	return nil, e.err
}
