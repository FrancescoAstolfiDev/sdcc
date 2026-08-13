package discovery

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"b5-kvstore/pkg/pb"
)

// Registry holds the latest polled ClusterView (md-week4 §1) and knows how
// to rebuild it from scratch every polling round. It has no persistence: if
// Discovery restarts, PollOnce/Run simply rebuilds the view within one
// interval, exactly as specified.
type Registry struct {
	peers   []string
	prober  StatusProber
	timeout time.Duration

	mu        sync.RWMutex
	view      *pb.ClusterView
	reachable map[string]bool // last poll round's per-peer reachability, for NODE_STATUS_CHANGED
}

// NewRegistry builds a Registry over the given static peer list.
func NewRegistry(peers []string, prober StatusProber, rpcTimeout time.Duration) *Registry {
	return &Registry{
		peers:     append([]string(nil), peers...),
		prober:    prober,
		timeout:   rpcTimeout,
		view:      &pb.ClusterView{},
		reachable: make(map[string]bool),
	}
}

// PollOnce runs one polling round: every peer is queried via GetStatus
// concurrently, and the whole ClusterView is rebuilt from scratch from
// whichever replies arrived. A peer that doesn't respond within RPCTimeout
// is simply left out of this round's view — not marked with a sticky "down"
// flag — so the very next successful poll re-adds it automatically (§1).
// Health tracking beyond this is deliberately not Discovery's job; that's
// the Circuit Breaker's concern (md-week4 §5), kept out of this package.
func (r *Registry) PollOnce(ctx context.Context) {
	type result struct {
		addr  string
		reply *pb.GetStatusReply
		ok    bool
	}
	results := make(chan result, len(r.peers))
	var wg sync.WaitGroup
	for _, addr := range r.peers {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()
			reply, err := r.prober.GetStatus(rctx, addr)
			results <- result{addr: addr, reply: reply, ok: err == nil && reply != nil}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	view := &pb.ClusterView{}
	newReachable := make(map[string]bool, len(r.peers))
	var leaderTerm uint64
	haveLeader := false
	for res := range results {
		newReachable[res.addr] = res.ok
		if !res.ok {
			continue
		}
		// The address we successfully dialed is what every consumer of
		// this view needs to be able to dial too — used verbatim, rather
		// than trusting the node's self-reported GetStatusReply.Address,
		// which could drift from what's actually reachable.
		info := &pb.NodeInfo{
			NodeId:  res.reply.GetNodeId(),
			Address: res.addr,
			Role:    res.reply.GetRole(),
			Term:    res.reply.GetTerm(),
		}
		view.AllNodes = append(view.AllNodes, info)
		switch info.Role {
		case pb.Role_LEADER:
			// A stale/partitioned former leader could still answer
			// GetStatus claiming Role_LEADER; prefer the highest term
			// seen, since that's the most recent legitimate leader.
			if !haveLeader || info.Term >= leaderTerm {
				view.LeaderAddress = info.Address
				leaderTerm = info.Term
				haveLeader = true
			}
		case pb.Role_FOLLOWER:
			view.Followers = append(view.Followers, info)
		}
	}

	r.mu.Lock()
	oldView := r.view
	oldReachable := r.reachable
	r.view = view
	r.reachable = newReachable
	r.mu.Unlock()

	logViewChangeIfAny(oldView, view)
	logReachabilityChanges(oldReachable, newReachable)
}

// logViewChangeIfAny logs VIEW_UPDATED when the resolved leader or the
// follower set differs from the previous poll round — must fire even if
// only the leader changed and the follower set didn't (Week 5's
// ConfirmCompaction re-verification cares about leader changes above all).
func logViewChangeIfAny(oldView, newView *pb.ClusterView) {
	oldLeader := oldView.GetLeaderAddress()
	newLeader := newView.GetLeaderAddress()
	oldFollowers := sortedFollowerAddrs(oldView.GetFollowers())
	newFollowers := sortedFollowerAddrs(newView.GetFollowers())
	if oldLeader == newLeader && followersEqual(oldFollowers, newFollowers) {
		return
	}
	log.Printf("discovery: VIEW_UPDATED leader=%s followers=%v (was_leader=%s)", newLeader, newFollowers, oldLeader)
}

func sortedFollowerAddrs(followers []*pb.NodeInfo) []string {
	addrs := make([]string, len(followers))
	for i, f := range followers {
		addrs[i] = f.GetAddress()
	}
	sort.Strings(addrs)
	return addrs
}

func followersEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// logReachabilityChanges logs NODE_STATUS_CHANGED for every peer whose
// reachability flipped since the previous poll round, in either direction —
// dropped-for-one-round nodes are not sticky (§1), so this must fire on
// both directions, not just down->up.
func logReachabilityChanges(oldReachable, newReachable map[string]bool) {
	for addr, reachable := range newReachable {
		if old, ok := oldReachable[addr]; ok && old == reachable {
			continue
		}
		log.Printf("discovery: NODE_STATUS_CHANGED node=%s reachable=%v", addr, reachable)
	}
}

// Run polls every interval until ctx is done. Intended to run as a
// long-lived goroutine from cmd/discovery, polling once immediately before
// entering the periodic loop so the first GetClusterView call after
// startup doesn't have to wait a full interval for a non-empty view.
func (r *Registry) Run(ctx context.Context, interval time.Duration) {
	r.PollOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.PollOnce(ctx)
		}
	}
}

// ClusterView returns the most recently polled view. The returned value is
// replaced wholesale by the next PollOnce, never mutated in place, so
// callers may treat it as an immutable snapshot without copying.
func (r *Registry) ClusterView() *pb.ClusterView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.view
}
