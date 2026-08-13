// Package discovery implements the pull-based Service Discovery registry
// (md-week4 §1, spec §7/§9.3): a periodic poller over a static peer list,
// exposing an in-memory ClusterView via gRPC to the Client Proxy and (Week
// 5) the Snapshot & Backup service. Nodes never register themselves — this
// is the permanent design (§10.6: consensus-node peer addressing never
// depends on any runtime service), not a temporary simplification.
package discovery

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// defaultPollInterval matches md-week4 §1's default and the spec's timing
// table.
const defaultPollInterval = 2 * time.Second

// defaultRPCTimeout bounds a single GetStatus call so one unresponsive node
// can't stall an entire poll round.
const defaultRPCTimeout = 500 * time.Millisecond

// Config configures a Registry's polling loop.
type Config struct {
	// Peers is the static list of consensus-node addresses to poll,
	// sourced from the same shared config as consensus-node peer
	// addressing (§10.6) — never dynamically registered.
	Peers []string
	// PollInterval is how often every peer in Peers is polled.
	PollInterval time.Duration
	// RPCTimeout bounds each individual GetStatus call within a poll round.
	RPCTimeout time.Duration
}

// LoadConfigFromEnv reads PEERS (comma-separated "host:port" list) and
// DISCOVERY_POLL_INTERVAL_MS (default 2000, per md-week4 §1).
func LoadConfigFromEnv() (Config, error) {
	peersEnv := os.Getenv("PEERS")
	var peers []string
	for _, raw := range strings.Split(peersEnv, ",") {
		addr := strings.TrimSpace(raw)
		if addr != "" {
			peers = append(peers, addr)
		}
	}

	intervalMS := defaultPollInterval.Milliseconds()
	if v := os.Getenv("DISCOVERY_POLL_INTERVAL_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid DISCOVERY_POLL_INTERVAL_MS=%q: %w", v, err)
		}
		if n <= 0 {
			return Config{}, fmt.Errorf("DISCOVERY_POLL_INTERVAL_MS must be > 0, got %d", n)
		}
		intervalMS = int64(n)
	}

	return Config{
		Peers:        peers,
		PollInterval: time.Duration(intervalMS) * time.Millisecond,
		RPCTimeout:   defaultRPCTimeout,
	}, nil
}
