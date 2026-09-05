package snapshot

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultPollInterval               = 5 * time.Second
	defaultCompactionThresholdPercent = 30
	defaultConcurrencyGuardPercent    = 90
	defaultRPCTimeout                 = 2 * time.Second
)

// Config configures the Snapshot & Backup service's polling/compaction
// cycle (§5.1).
type Config struct {
	// PollInterval is how often the leader's log occupancy is checked
	// (SNAPSHOT_BACKUP_POLL_INTERVAL_MS, default 5000ms; distinct from
	// raft.SnapshotConfig.PollInterval, which is the consensus nodes' own
	// poll of this service, §5.3).
	PollInterval time.Duration
	// CompactionThresholdPercent is the occupancy percentage that starts a
	// compaction cycle (COMPACTION_THRESHOLD_PERCENT, default 30).
	CompactionThresholdPercent uint64
	// ConcurrencyGuardPercent is the second, higher occupancy percentage
	// that means a compaction cycle must already be in flight (this
	// instance's own, or — since this service is stateless, §5 — another
	// replica's) and this tick should wait rather than start a second,
	// overlapping one (COMPACTION_CONCURRENCY_GUARD_PERCENT, default 90).
	ConcurrencyGuardPercent uint64
	// RPCTimeout bounds every individual outbound call to a consensus node
	// or Service Discovery.
	RPCTimeout time.Duration
}

// LoadConfigFromEnv reads SNAPSHOT_BACKUP_POLL_INTERVAL_MS (default 5000),
// COMPACTION_THRESHOLD_PERCENT (default 30),
// COMPACTION_CONCURRENCY_GUARD_PERCENT (default 90), and
// SNAPSHOT_BACKUP_RPC_TIMEOUT_MS (default 2000).
func LoadConfigFromEnv() (Config, error) {
	pollMS, err := envIntDefault("SNAPSHOT_BACKUP_POLL_INTERVAL_MS", int(defaultPollInterval/time.Millisecond))
	if err != nil {
		return Config{}, err
	}
	threshold, err := envIntDefault("COMPACTION_THRESHOLD_PERCENT", defaultCompactionThresholdPercent)
	if err != nil {
		return Config{}, err
	}
	guard, err := envIntDefault("COMPACTION_CONCURRENCY_GUARD_PERCENT", defaultConcurrencyGuardPercent)
	if err != nil {
		return Config{}, err
	}
	if guard <= threshold {
		return Config{}, fmt.Errorf("COMPACTION_CONCURRENCY_GUARD_PERCENT (%d) must be > COMPACTION_THRESHOLD_PERCENT (%d)", guard, threshold)
	}
	rpcTimeoutMS, err := envIntDefault("SNAPSHOT_BACKUP_RPC_TIMEOUT_MS", int(defaultRPCTimeout/time.Millisecond))
	if err != nil {
		return Config{}, err
	}
	return Config{
		PollInterval:               time.Duration(pollMS) * time.Millisecond,
		CompactionThresholdPercent: uint64(threshold),
		ConcurrencyGuardPercent:    uint64(guard),
		RPCTimeout:                 time.Duration(rpcTimeoutMS) * time.Millisecond,
	}, nil
}

func envIntDefault(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
	return n, nil
}
