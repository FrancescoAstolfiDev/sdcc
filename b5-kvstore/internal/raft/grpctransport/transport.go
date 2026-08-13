// Package grpctransport implements raft.Transport over real gRPC
// connections to pkg/pb's Consensus service (§6 Step C: the same Node core
// validated in-process against the fake transport, now wired to real gRPC
// between separate processes/containers).
package grpctransport

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"b5-kvstore/internal/grpcutil"
	"b5-kvstore/pkg/pb"
)

// Transport dials peers lazily and caches one connection per peer address,
// reused across every RequestVote/AppendEntries call to that peer.
type Transport struct {
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// New builds a Transport. With no options, it dials peers over plaintext
// (insecure) gRPC — consensus nodes only ever talk to each other inside the
// isolated Docker network (§11.2), never across a public network boundary.
func New(opts ...grpc.DialOption) *Transport {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &Transport{
		dialOpts: opts,
		conns:    make(map[string]*grpc.ClientConn),
	}
}

func (t *Transport) clientFor(addr string) (pb.ConsensusClient, error) {
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
	return pb.NewConsensusClient(conn), nil
}

func (t *Transport) SendRequestVote(ctx context.Context, peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	client, err := t.clientFor(peerID)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, req)
}

func (t *Transport) SendAppendEntries(ctx context.Context, peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	client, err := t.clientFor(peerID)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, req, grpc.WaitForReady(true))
}

func (t *Transport) SendReadIndex(ctx context.Context, peerID string, req *pb.ReadIndexRequest) (*pb.ReadIndexReply, error) {
	client, err := t.clientFor(peerID)
	if err != nil {
		return nil, err
	}
	return client.RequestReadIndex(ctx, req, grpc.WaitForReady(true))
}

// Close tears down every cached connection.
func (t *Transport) Close() error {
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
