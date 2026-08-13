// Package harness provides an in-process fake transport connecting N
// raft.Node instances via direct function calls (standing in for RPCs)
// with controllable message delivery: individual messages can be paused,
// dropped, or delayed on command. This is what makes leader-crash and
// partition scenarios cheap and deterministic to simulate here (§6 Step B),
// instead of expensive to simulate later via Docker (§6 Step C).
package harness

import (
	"context"
	"errors"
	"sync"
	"time"

	"b5-kvstore/internal/raft"
	"b5-kvstore/pkg/pb"
)

// MessageKind distinguishes RequestVote from AppendEntries traffic for
// filters that only want to affect one RPC kind.
type MessageKind int

const (
	KindRequestVote MessageKind = iota
	KindAppendEntries
	KindReadIndex
)

// Filter decides what happens to a single in-flight message.
//
// dropRequest discards the message before it ever reaches the destination
// node's handler (as if the packet never arrived — the destination sees
// and does nothing). dropReply lets the request through (the destination
// node processes and durably persists it as normal) but discards the
// response on the way back, so the sender sees an RPC error and never
// learns the call succeeded — this is what makes it possible to construct
// "entry replicated to a follower but the leader doesn't know it, then the
// leader crashes" scenarios (§4 point 3 / commit-safety test) instead of
// only whole-node partitions. A positive delay holds the message for that
// long before delivery (or until the caller's context is done, whichever
// comes first).
type Filter func(from, to string, kind MessageKind) (dropRequest, dropReply bool, delay time.Duration)

var errDropped = errors.New("harness: message dropped")

// Network is a shared fake network that in-process Nodes are registered on.
// It is the substrate raft.Transport implementations in this package route
// through.
type Network struct {
	mu     sync.RWMutex
	nodes  map[string]*raft.Node
	filter Filter
}

func NewNetwork() *Network {
	return &Network{nodes: make(map[string]*raft.Node)}
}

// Register makes a Node reachable on the network under the given peer ID.
func (net *Network) Register(id string, n *raft.Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[id] = n
}

// SetFilter installs (or, with nil, clears) the delivery filter consulted
// for every message. This is the fault-injection hook: tests can swap it at
// any point to pause, drop, or delay specific messages or all traffic
// to/from a given node.
func (net *Network) SetFilter(f Filter) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.filter = f
}

// Transport returns a raft.Transport bound to the given sender identity,
// routing all sends through this Network.
func (net *Network) Transport(from string) raft.Transport {
	return &fakeTransport{net: net, from: from}
}

func (net *Network) route(to string) (*raft.Node, Filter) {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return net.nodes[to], net.filter
}

type fakeTransport struct {
	net  *Network
	from string
}

func (t *fakeTransport) SendRequestVote(ctx context.Context, peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	node, dropReq, dropReply, delay := t.check(peerID, KindRequestVote)
	if node == nil || dropReq {
		return nil, errDropped
	}
	if delay > 0 {
		if err := wait(ctx, delay); err != nil {
			return nil, err
		}
	}
	reply, err := node.RequestVote(ctx, req)
	if dropReply {
		return nil, errDropped
	}
	return reply, err
}

func (t *fakeTransport) SendAppendEntries(ctx context.Context, peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	node, dropReq, dropReply, delay := t.check(peerID, KindAppendEntries)
	if node == nil || dropReq {
		return nil, errDropped
	}
	if delay > 0 {
		if err := wait(ctx, delay); err != nil {
			return nil, err
		}
	}
	reply, err := node.AppendEntries(ctx, req)
	if dropReply {
		return nil, errDropped
	}
	return reply, err
}

func (t *fakeTransport) SendReadIndex(ctx context.Context, peerID string, req *pb.ReadIndexRequest) (*pb.ReadIndexReply, error) {
	node, dropReq, dropReply, delay := t.check(peerID, KindReadIndex)
	if node == nil || dropReq {
		return nil, errDropped
	}
	if delay > 0 {
		if err := wait(ctx, delay); err != nil {
			return nil, err
		}
	}
	reply, err := node.RequestReadIndex(ctx, req)
	if dropReply {
		return nil, errDropped
	}
	return reply, err
}

func (t *fakeTransport) check(peerID string, kind MessageKind) (node *raft.Node, dropReq, dropReply bool, delay time.Duration) {
	n, filter := t.net.route(peerID)
	if n == nil {
		return nil, true, false, 0 // unknown peer: treat as dropped, not a hard error
	}
	if filter == nil {
		return n, false, false, 0
	}
	dropReq, dropReply, delay = filter(t.from, peerID, kind)
	return n, dropReq, dropReply, delay
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Disconnected returns a Filter that drops every message to or from the
// given set of peer IDs — modeling those nodes as fully partitioned
// (equivalent to "stop delivering its messages" for a crashed leader, or an
// isolated minority partition).
func Disconnected(ids ...string) Filter {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return func(from, to string, _ MessageKind) (bool, bool, time.Duration) {
		cut := set[from] || set[to]
		return cut, false, 0
	}
}

// DropReplyFrom returns a Filter that lets every request through (so the
// destination processes and durably persists it as normal) but discards
// the reply whenever it originates from the given sender — modeling
// "the leader's writes are reaching followers, but it never finds out",
// which is what makes replicated-but-not-yet-committed states reachable
// without a full partition.
func DropReplyFrom(id string) Filter {
	return func(from, _ string, _ MessageKind) (bool, bool, time.Duration) {
		return false, from == id, 0
	}
}
