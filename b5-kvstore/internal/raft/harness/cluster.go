package harness

import (
	"fmt"
	"sync"
	"time"

	"b5-kvstore/internal/raft"
)

// FastTiming is a timing configuration tuned for quick, deterministic-ish
// in-process tests: proportionally identical to the spec defaults
// (heartbeat well below min election timeout) but scaled down so tests
// don't spend real seconds waiting out 150-300ms elections.
func FastTiming() raft.TimingConfig {
	return raft.TimingConfig{
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	}
}

// Cluster is N raft.Node instances wired together over a fake Network, each
// with its own temp on-disk directory (so persistence is exercised for
// real, not mocked out).
type Cluster struct {
	Net   *Network
	IDs   []string
	Nodes map[string]*raft.Node

	applyHook func(id string, msg raft.ApplyMsg)

	mu      sync.Mutex
	applied map[string][]raft.ApplyMsg
}

// ClusterOption configures optional Cluster behavior at construction time.
type ClusterOption func(*Cluster)

// WithApplyHook registers a callback invoked, in addition to the internal
// Applied() bookkeeping, every time any node applies a committed entry.
// This is what lets a package outside internal/raft — e.g. internal/proxy's
// integration tests (md-week4 §6: "extend [the harness]... so the proxy's
// routing code can run against the in-process Node instances instead of
// real gRPC") — feed committed entries into its own state machine, wired to
// the same in-process cluster the proxy under test talks to.
func WithApplyHook(fn func(id string, msg raft.ApplyMsg)) ClusterOption {
	return func(c *Cluster) { c.applyHook = fn }
}

// NewCluster builds and starts an n-node cluster. dataDir(id) must return a
// fresh, empty directory for that node (tests pass t.TempDir()-derived
// paths so restarts-with-fresh-Node can reuse the same directory to
// simulate a crash/restart of the same node).
func NewCluster(n int, dataDir func(id string) string, timing raft.TimingConfig, opts ...ClusterOption) (*Cluster, error) {
	c := &Cluster{
		Net:     NewNetwork(),
		Nodes:   make(map[string]*raft.Node, n),
		applied: make(map[string][]raft.ApplyMsg),
	}
	for _, opt := range opts {
		opt(c)
	}
	for i := 0; i < n; i++ {
		c.IDs = append(c.IDs, fmt.Sprintf("node-%d", i+1))
	}
	for _, id := range c.IDs {
		if err := c.addNode(id, dataDir(id), timing); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) addNode(id, dir string, timing raft.TimingConfig) error {
	node, err := raft.NewNode(raft.Config{
		ID:        id,
		Address:   id,
		DataDir:   dir,
		Peers:     c.peersOf(id),
		Timing:    timing,
		Tick:      5 * time.Millisecond,
		Transport: c.Net.Transport(id),
		ApplyFn: func(msg raft.ApplyMsg) {
			c.mu.Lock()
			c.applied[id] = append(c.applied[id], msg)
			c.mu.Unlock()
			if c.applyHook != nil {
				c.applyHook(id, msg)
			}
		},
	})
	if err != nil {
		return err
	}
	c.Net.Register(id, node)
	c.Nodes[id] = node
	node.Start()
	return nil
}

func (c *Cluster) peersOf(id string) []string {
	peers := make([]string, 0, len(c.IDs)-1)
	for _, other := range c.IDs {
		if other != id {
			peers = append(peers, other)
		}
	}
	return peers
}

// Applied returns a copy of the entries node id has applied so far, in
// commit order.
func (c *Cluster) Applied(id string) []raft.ApplyMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]raft.ApplyMsg, len(c.applied[id]))
	copy(out, c.applied[id])
	return out
}

// Stop terminates every node's background tickers.
func (c *Cluster) Stop() {
	for _, n := range c.Nodes {
		n.Stop()
	}
}

// Leaders returns the IDs of nodes that currently believe themselves to be
// Leader, alongside the term each reported.
func (c *Cluster) Leaders() map[string]uint64 {
	out := make(map[string]uint64)
	for id, n := range c.Nodes {
		role, term := n.Status()
		if role == raft.Leader {
			out[id] = term
		}
	}
	return out
}

// WaitForLeader polls until exactly one node reports itself Leader (and
// every other node's term is <= the leader's term, i.e. no stale split
// state), or the timeout elapses.
func (c *Cluster) WaitForLeader(timeout time.Duration) (id string, term uint64, err error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := c.Leaders()
		if len(leaders) == 1 {
			for lid, lterm := range leaders {
				return lid, lterm, nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "", 0, fmt.Errorf("no single leader elected within %s (leaders=%v)", timeout, c.Leaders())
}
