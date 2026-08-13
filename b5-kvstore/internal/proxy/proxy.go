package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sony/gobreaker"

	"b5-kvstore/internal/circuitbreaker"
	"b5-kvstore/pkg/pb"
)

// maxHops caps internal redirect-following (md-week4 §3): at most this many
// attempts (the first hit plus retries against redirected leaders) before
// giving up with 503 unavailable.
const maxHops = 3

// WriteOp identifies which KVService write RPC a write request maps to.
type WriteOp int

const (
	OpPut WriteOp = iota
	OpUpdate
	OpDelete
)

// Proxy is the stateless (beyond its cached ClusterView) Client Proxy core:
// REST handlers (http.go) call into Write/Get, which handle leader/follower
// routing, redirect-following, the Read-Index-aware read path, and circuit
// breaker wrapping of every outbound call.
type Proxy struct {
	disc       DiscoveryClient
	factory    KVClientFactory
	breakers   *circuitbreaker.Registry
	rpcTimeout time.Duration

	mu      sync.RWMutex
	view    *pb.ClusterView
	rrIndex int
}

// NewProxy builds a Proxy. Call Start to begin the scheduled ClusterView
// refresh loop before serving traffic (an initial synchronous refresh
// happens as Start's first action).
func NewProxy(disc DiscoveryClient, factory KVClientFactory, cfg Config) *Proxy {
	p := &Proxy{
		disc:       disc,
		factory:    factory,
		rpcTimeout: cfg.RPCTimeout,
		view:       &pb.ClusterView{},
	}
	// §5: on a breaker trip, trigger an out-of-schedule ClusterView refresh
	// rather than waiting for the next periodic one.
	p.breakers = circuitbreaker.NewRegistry(func(string) { p.asyncRefresh() })
	return p
}

// Start runs the scheduled ClusterView refresh loop (§3: "refreshed on a
// schedule") until ctx is done. Performs one synchronous refresh first so
// the very first request isn't served against an empty cache.
func (p *Proxy) Start(ctx context.Context, interval time.Duration) {
	p.refreshNow(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.refreshNow(ctx)
		}
	}
}

func (p *Proxy) refreshNow(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
	defer cancel()
	view, err := p.disc.GetClusterView(rctx)
	if err != nil || view == nil {
		return // keep the stale cache; the next scheduled/forced refresh retries
	}
	p.mu.Lock()
	oldLeader := p.view.GetLeaderAddress()
	p.view = view
	p.mu.Unlock()

	if newLeader := view.GetLeaderAddress(); newLeader != oldLeader {
		log.Printf("client-proxy: LEADER_UPDATED old=%s new=%s source=discovery_refresh", oldLeader, newLeader)
	}

	live := make(map[string]bool, len(view.GetAllNodes()))
	for _, n := range view.GetAllNodes() {
		live[n.GetAddress()] = true
	}
	p.breakers.Prune(live)
}

// asyncRefresh triggers a refresh without blocking the caller — used from
// request-handling paths (breaker trip, hop-budget exhaustion) that must
// still return an HTTP response promptly.
func (p *Proxy) asyncRefresh() {
	go p.refreshNow(context.Background())
}

func (p *Proxy) currentView() *pb.ClusterView {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.view
}

// updateLeaderCache patches just the cached leader address (§3's "invalidated
// early on... a redirectLeader received from any node"), without waiting for
// a full Discovery round-trip. Copy-on-write: never mutates a ClusterView a
// concurrent reader might already hold a pointer to.
func (p *Proxy) updateLeaderCache(addr string) {
	p.mu.Lock()
	oldLeader := p.view.GetLeaderAddress()
	p.view = &pb.ClusterView{
		LeaderAddress: addr,
		Followers:     p.view.GetFollowers(),
		AllNodes:      p.view.GetAllNodes(),
	}
	p.mu.Unlock()

	if addr != oldLeader {
		log.Printf("client-proxy: LEADER_UPDATED old=%s new=%s source=redirect_hop", oldLeader, addr)
	}
}

// leaderContactFailReason categorizes an error from a breaker-wrapped call
// to what the proxy believed was the leader, for LEADER_CONTACT_FAILED:
// "breaker_open" if the breaker itself skipped the call, "transport_error"
// otherwise (connection refused/timeout from the actual RPC attempt).
func leaderContactFailReason(err error) string {
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return "breaker_open"
	}
	return "transport_error"
}

// allNodesOpen reports whether every currently-known node's breaker is
// open (§5's dedicated 503 condition, checked proactively so a request
// against a fully broken cluster fails fast instead of attempting and
// individually catching N breaker-open errors).
func (p *Proxy) allNodesOpen(view *pb.ClusterView) bool {
	nodes := view.GetAllNodes()
	if len(nodes) == 0 {
		return false
	}
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.GetAddress()
	}
	return p.breakers.AllOpen(addrs)
}

func (p *Proxy) pickFollowerRoundRobin(followers []*pb.NodeInfo) string {
	p.mu.Lock()
	idx := p.rrIndex % len(followers)
	p.rrIndex++
	p.mu.Unlock()
	return followers[idx].GetAddress()
}

func (p *Proxy) callWrite(ctx context.Context, addr string, op WriteOp, key, value string) (*pb.WriteReply, error) {
	result, err := p.breakers.Call(addr, func() (any, error) {
		rctx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
		defer cancel()
		client := p.factory.KVClientFor(addr)
		switch op {
		case OpPut:
			return client.Put(rctx, &pb.PutRequest{Key: key, Value: value})
		case OpUpdate:
			return client.Update(rctx, &pb.PutRequest{Key: key, Value: value})
		case OpDelete:
			return client.Delete(rctx, &pb.DeleteRequest{Key: key})
		default:
			return nil, fmt.Errorf("proxy: unknown write op %d", op)
		}
	})
	if err != nil {
		return nil, err
	}
	return result.(*pb.WriteReply), nil
}

func (p *Proxy) callGet(ctx context.Context, addr, key string) (*pb.GetReply, error) {
	result, err := p.breakers.Call(addr, func() (any, error) {
		rctx, cancel := context.WithTimeout(ctx, p.rpcTimeout)
		defer cancel()
		return p.factory.KVClientFor(addr).Get(rctx, &pb.GetRequest{Key: key})
	})
	if err != nil {
		return nil, err
	}
	return result.(*pb.GetReply), nil
}

// Write executes a Put/Update/Delete: always routed to the cached Leader,
// following redirects internally up to maxHops (md-week4 §2/§3). Never
// surfaced to the external client as an HTTP redirect.
func (p *Proxy) Write(ctx context.Context, op WriteOp, key, value string) (*pb.WriteReply, *apiError) {
	view := p.currentView()
	if p.allNodesOpen(view) {
		return nil, errUnavailable("every known node's circuit breaker is open", circuitbreakerRetryAfter)
	}
	addr := view.GetLeaderAddress()
	if addr == "" {
		p.asyncRefresh()
		return nil, errUnavailable("no known leader", defaultRefreshInterval)
	}

	for attempt := 1; attempt <= maxHops; attempt++ {
		reply, err := p.callWrite(ctx, addr, op, key, value)
		if err != nil {
			reason := leaderContactFailReason(err)
			log.Printf("client-proxy: LEADER_CONTACT_FAILED node=%s reason=%s detail=%q", addr, reason, err.Error())
			p.asyncRefresh()
			return nil, errUnavailable("failed to reach leader: "+err.Error(), circuitbreakerRetryAfter)
		}
		if reply.GetSuccess() {
			return reply, nil
		}
		redirect := reply.GetRedirectLeader()
		log.Printf("client-proxy: LEADER_CONTACT_FAILED node=%s reason=redirect_received detail=%q", addr, redirectDetail(redirect))
		if redirect == "" {
			p.asyncRefresh()
			return nil, errUnavailable("target is not leader and no redirect is known", defaultRefreshInterval)
		}
		log.Printf("client-proxy: REDIRECT_FOLLOWED hop=%d/%d from=%s to=%s", attempt, maxHops, addr, redirect)
		p.updateLeaderCache(redirect)
		addr = redirect
	}
	p.asyncRefresh()
	log.Printf("client-proxy: REQUEST_FAILED_EXHAUSTED key=%s hops_attempted=%d", key, maxHops)
	return nil, errUnavailable("redirect-hop budget exhausted", defaultRefreshInterval)
}

// redirectDetail renders the raw redirect-leader field for
// LEADER_CONTACT_FAILED's detail field, distinguishing "redirected
// elsewhere" from "not leader, and it doesn't know who is" without
// overloading the reason field itself.
func redirectDetail(redirect string) string {
	if redirect == "" {
		return "no redirect known"
	}
	return "redirect_leader=" + redirect
}

// Get executes a read (md-week4 §3/§4): by default round-robins across
// cached followers via the Read-Index protocol; if a follower can't answer
// authoritatively (or can't be reached at all) it falls back once, directly
// to the cached Leader (§4's single-hop fallback — distinct from the
// generic redirect-following used when routing to the Leader by default).
func (p *Proxy) Get(ctx context.Context, key string) (*pb.GetReply, *apiError) {
	view := p.currentView()
	if p.allNodesOpen(view) {
		return nil, errUnavailable("every known node's circuit breaker is open", circuitbreakerRetryAfter)
	}
	followers := view.GetFollowers()
	if len(followers) == 0 {
		return p.leaderPathGet(ctx, view.GetLeaderAddress(), key)
	}

	addr := p.pickFollowerRoundRobin(followers)
	reply, err := p.callGet(ctx, addr, key)
	if err != nil {
		// Breaker open, or a transport failure reaching the follower at
		// all: §4's runtime fallback rule — single hop to the leader, no
		// second follower attempted.
		return p.singleHopLeaderGet(ctx, key)
	}
	if !reply.GetOk() {
		// The follower's Read-Index handshake failed (§4 step 2): fall
		// back to a single hop against the leader — opportunistically
		// remember the follower's redirect hint, but the fallback target
		// is always the proxy's own cached leader either way.
		if redirect := reply.GetRedirectLeader(); redirect != "" {
			p.updateLeaderCache(redirect)
		}
		return p.singleHopLeaderGet(ctx, key)
	}
	if !reply.GetFound() {
		return nil, errNotFound("key not found")
	}
	return reply, nil
}

// leaderPathGet is §3's generic redirect-following applied to a Get routed
// directly at (what the cache believes is) the Leader — used when there are
// no cached followers to round-robin across. Distinct from
// singleHopLeaderGet: this one follows redirects up to maxHops.
func (p *Proxy) leaderPathGet(ctx context.Context, addr, key string) (*pb.GetReply, *apiError) {
	if addr == "" {
		p.asyncRefresh()
		return nil, errUnavailable("no known leader", defaultRefreshInterval)
	}
	for attempt := 1; attempt <= maxHops; attempt++ {
		reply, err := p.callGet(ctx, addr, key)
		if err != nil {
			reason := leaderContactFailReason(err)
			log.Printf("client-proxy: LEADER_CONTACT_FAILED node=%s reason=%s detail=%q", addr, reason, err.Error())
			p.asyncRefresh()
			return nil, errUnavailable("failed to reach leader: "+err.Error(), circuitbreakerRetryAfter)
		}
		if !reply.GetOk() {
			redirect := reply.GetRedirectLeader()
			log.Printf("client-proxy: LEADER_CONTACT_FAILED node=%s reason=redirect_received detail=%q", addr, redirectDetail(redirect))
			if redirect == "" {
				p.asyncRefresh()
				return nil, errUnavailable("target could not answer authoritatively", defaultRefreshInterval)
			}
			log.Printf("client-proxy: REDIRECT_FOLLOWED hop=%d/%d from=%s to=%s", attempt, maxHops, addr, redirect)
			p.updateLeaderCache(redirect)
			addr = redirect
			continue
		}
		if !reply.GetFound() {
			return nil, errNotFound("key not found")
		}
		return reply, nil
	}
	p.asyncRefresh()
	log.Printf("client-proxy: REQUEST_FAILED_EXHAUSTED key=%s hops_attempted=%d", key, maxHops)
	return nil, errUnavailable("redirect-hop budget exhausted", defaultRefreshInterval)
}

// singleHopLeaderGet is md-week4 §4's Read-Index-specific fallback: exactly
// one direct attempt against the cached Leader, no further redirect
// chasing and no second follower.
func (p *Proxy) singleHopLeaderGet(ctx context.Context, key string) (*pb.GetReply, *apiError) {
	addr := p.currentView().GetLeaderAddress()
	if addr == "" {
		p.asyncRefresh()
		return nil, errUnavailable("no known leader", defaultRefreshInterval)
	}
	reply, err := p.callGet(ctx, addr, key)
	if err != nil {
		p.asyncRefresh()
		return nil, errUnavailable("failed to reach leader: "+err.Error(), circuitbreakerRetryAfter)
	}
	if !reply.GetOk() {
		p.asyncRefresh()
		return nil, errUnavailable("leader could not answer authoritatively", defaultRefreshInterval)
	}
	if !reply.GetFound() {
		return nil, errNotFound("key not found")
	}
	return reply, nil
}

// circuitbreakerRetryAfter is used for the Retry-After header (§2) when the
// 503 was caused by a failed/broken-circuit call, matching §5's guidance
// ("roughly the breaker Timeout").
const circuitbreakerRetryAfter = 2 * time.Second
