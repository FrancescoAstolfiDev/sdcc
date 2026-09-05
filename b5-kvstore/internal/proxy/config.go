package proxy

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultRefreshInterval = 2 * time.Second
	defaultRPCTimeout      = 2 * time.Second
)

// Config configures a Proxy's cluster-map cache refresh cadence and the
// per-call timeout to a consensus node.
type Config struct {
	// RefreshInterval is how often the cached ClusterView is refreshed on
	// its normal schedule — separate from the early invalidation triggers
	// (breaker trip, redirect) which refresh out-of-schedule.
	RefreshInterval time.Duration
	// RPCTimeout bounds every individual outbound call to a consensus node
	// (KVService or Discovery).
	RPCTimeout time.Duration
}

// LoadConfigFromEnv reads PROXY_REFRESH_INTERVAL_MS and
// PROXY_RPC_TIMEOUT_MS, defaulting both to 2s.
func LoadConfigFromEnv() (Config, error) {
	refresh, err := envDurationMS("PROXY_REFRESH_INTERVAL_MS", defaultRefreshInterval)
	if err != nil {
		return Config{}, err
	}
	rpcTimeout, err := envDurationMS("PROXY_RPC_TIMEOUT_MS", defaultRPCTimeout)
	if err != nil {
		return Config{}, err
	}
	return Config{RefreshInterval: refresh, RPCTimeout: rpcTimeout}, nil
}

func envDurationMS(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s must be > 0, got %d", key, n)
	}
	return time.Duration(n) * time.Millisecond, nil
}
