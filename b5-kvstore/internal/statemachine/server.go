package statemachine

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"b5-kvstore/internal/raft"
	"b5-kvstore/pkg/pb"
)

// Server implements pb.KVServiceServer (spec §9.2) on top of a raft.Node and
// its local KV. It is the server-side counterpart the Client Proxy talks to
// (internal/proxy is the client side, Week 4): writes go through
// ProposeSync and only reply once committed; reads either serve directly
// (this node is Leader — §3's default path) or perform the follower-side
// Read-Index handshake first (md-week4 §4) before reading the local map.
type Server struct {
	pb.UnimplementedKVServiceServer

	node *raft.Node
	kv   *KV

	// ProposeTimeout bounds how long a write waits for commit before the
	// caller gets an error back (translated by internal/proxy into a
	// timeout/503, not exposed here). ReadIndexTimeout bounds the
	// follower-side Read-Index handshake (RPC to leader + local catch-up
	// wait) before falling back per md-week4 §4's runtime fallback rule.
	ProposeTimeout   time.Duration
	ReadIndexTimeout time.Duration
}

// NewServer builds a Server. Zero timeouts fall back to sane defaults.
func NewServer(node *raft.Node, kv *KV) *Server {
	return &Server{
		node:             node,
		kv:               kv,
		ProposeTimeout:   5 * time.Second,
		ReadIndexTimeout: 2 * time.Second,
	}
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	role, _ := s.node.Status()
	if role != raft.Leader {
		rctx, cancel := context.WithTimeout(ctx, s.ReadIndexTimeout)
		defer cancel()
		if err := s.node.FollowerLinearizableRead(rctx); err != nil {
			// md-week4 §4: both the ok=false case and the transport-failure
			// case converge here — Ok=false tells the caller found/value
			// are meaningless (as opposed to a genuine "key not found");
			// RedirectLeader carries the best-known leader address, if any,
			// but the proxy's own cached leader is the authoritative
			// fallback target either way (§4's single-hop rule).
			return &pb.GetReply{Ok: false, RedirectLeader: s.node.LeaderAddress()}, nil
		}
	}
	value, found := s.kv.Get(req.GetKey())
	return &pb.GetReply{Ok: true, Found: found, Value: value}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.WriteReply, error) {
	return s.write(ctx, pb.KVCommand_PUT, req.GetKey(), req.GetValue())
}

func (s *Server) Update(ctx context.Context, req *pb.PutRequest) (*pb.WriteReply, error) {
	return s.write(ctx, pb.KVCommand_UPDATE, req.GetKey(), req.GetValue())
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.WriteReply, error) {
	return s.write(ctx, pb.KVCommand_DELETE, req.GetKey(), "")
}

// write proposes cmd through Raft and blocks until it commits (§3.2: "apply
// only after commit"). A non-leader or a leader that loses leadership
// before commit both report back as a redirect, never as a locally-executed
// write — per §3.3's MUST NOT, a follower must never answer a write itself.
func (s *Server) write(ctx context.Context, op pb.KVCommand_Op, key, value string) (*pb.WriteReply, error) {
	payload, err := proto.Marshal(&pb.KVCommand{Op: op, Key: key, Value: value})
	if err != nil {
		return nil, err
	}
	wctx, cancel := context.WithTimeout(ctx, s.ProposeTimeout)
	defer cancel()
	index, _, err := s.node.ProposeSync(wctx, payload)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return &pb.WriteReply{Success: false, RedirectLeader: s.node.LeaderAddress()}, nil
		}
		return nil, err
	}
	return &pb.WriteReply{Success: true, CommitIndex: index}, nil
}
