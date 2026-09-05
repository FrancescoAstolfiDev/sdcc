package raft

import (
	"context"

	"b5-kvstore/pkg/pb"
)

// Transport is how a Node reaches its peers. It is deliberately the same
// message shape as the real pkg/pb gRPC service (RequestVote/AppendEntries),
// so the exact same Node core can run either against an in-process fake
// transport (§6 Step B, for cheap deterministic protocol testing) or a real
// gRPC transport (§6 Step C) without any change to the raft logic itself.
type Transport interface {
	SendRequestVote(ctx context.Context, peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error)
	// SendReadIndex delivers a follower's RequestReadIndex call to peerID.
	// peerID here is always the currently-known leader.
	SendReadIndex(ctx context.Context, peerID string, req *pb.ReadIndexRequest) (*pb.ReadIndexReply, error)
}
