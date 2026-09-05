package raft

import "time"

// defaultLogCapacityEntries is the denominator GetLogStatus's
// occupancyPercent (§9.4) is computed against. The spec (§5.1/§9.4) defines
// occupancy as a percentage but, unlike every other compaction threshold,
// never specifies what capacity it's a percentage *of* — this project
// fills that gap with an explicit, configurable per-node log capacity
// rather than an implicit or hardcoded one, so the 30%/90% thresholds in
// internal/snapshot have a well-defined denominator to compare against.
const defaultLogCapacityEntries = 1000

// defaultSnapshotPollInterval is §5.3/§10.1's "Snapshot poll interval": how
// often a node checks the Snapshot Catalog for a newer snapshot and for its
// own local-compaction opportunity. Distinct from
// SNAPSHOT_BACKUP_POLL_INTERVAL_MS (internal/snapshot's own poll of the
// leader's log occupancy) — same suffix, different service, different
// question.
const defaultSnapshotPollInterval = 5 * time.Second

// SnapshotConfig configures the compaction/catch-up integration.
type SnapshotConfig struct {
	// LogCapacityEntries is GetLogStatus's occupancyPercent denominator.
	LogCapacityEntries uint64
	// PollInterval is how often the catch-up loop (§5.3) runs.
	PollInterval time.Duration
}

// LoadSnapshotConfigFromEnv reads LOG_CAPACITY_ENTRIES (default 1000) and
// SNAPSHOT_POLL_INTERVAL_MS (default 5000ms, per §10.1).
func LoadSnapshotConfigFromEnv() (SnapshotConfig, error) {
	capacity, err := envIntDefault("LOG_CAPACITY_ENTRIES", defaultLogCapacityEntries)
	if err != nil {
		return SnapshotConfig{}, err
	}
	pollMS, err := envIntDefault("SNAPSHOT_POLL_INTERVAL_MS", int(defaultSnapshotPollInterval/time.Millisecond))
	if err != nil {
		return SnapshotConfig{}, err
	}
	return SnapshotConfig{
		LogCapacityEntries: uint64(capacity),
		PollInterval:       time.Duration(pollMS) * time.Millisecond,
	}, nil
}
