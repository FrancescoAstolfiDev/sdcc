package snapshot

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// Compactor drives the periodic polling/compaction cycle (§5.1).
type Compactor struct {
	discovery DiscoveryClient
	transfer  TransferClient
	store     *Store
	cfg       Config

	mu         sync.Mutex
	compacting bool
}

// NewCompactor builds a Compactor.
func NewCompactor(discovery DiscoveryClient, transfer TransferClient, store *Store, cfg Config) *Compactor {
	return &Compactor{discovery: discovery, transfer: transfer, store: store, cfg: cfg}
}

// Run polls every cfg.PollInterval until ctx is done.
func (c *Compactor) Run(ctx context.Context) {
	t := time.NewTicker(c.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Tick(ctx)
		}
	}
}

// Tick runs one polling round: query the leader's log occupancy and decide
// whether to start, skip, or wait on a compaction cycle. Exported (rather
// than folded into an unexported ticker callback) so unit tests can drive
// it directly and deterministically against a fake DiscoveryClient/
// TransferClient — no real gRPC or timers needed.
func (c *Compactor) Tick(ctx context.Context) {
	view, err := c.discovery.GetClusterView(ctx)
	if err != nil || view.GetLeaderAddress() == "" {
		return
	}
	leader := view.GetLeaderAddress()

	status, err := c.transfer.GetLogStatus(ctx, leader)
	if err != nil {
		return
	}
	occupancy := status.GetOccupancyPercent()

	if occupancy < c.cfg.CompactionThresholdPercent {
		return // below the start threshold (§1 point 2/3): nothing to do
	}
	if occupancy >= c.cfg.ConcurrencyGuardPercent {
		// §1 point 4: occupancy this high implies a compaction cycle —
		// this instance's own, or (this service is stateless, §5) another
		// replica's — is already in flight and hasn't been confirmed yet.
		// Wait for it rather than starting a second, overlapping one.
		return
	}

	c.mu.Lock()
	if c.compacting {
		c.mu.Unlock()
		return // this instance's own cycle is still running
	}
	c.compacting = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.compacting = false
		c.mu.Unlock()
	}()

	c.runCycle(ctx, leader, status)
}

// runCycle downloads the not-yet-backed-up oldest entries, applies them to
// the running replica, re-verifies leadership, and confirms compaction
// (§1 points 3/5/6).
func (c *Compactor) runCycle(ctx context.Context, leader string, status *pb.LogStatusReply) {
	lastBackedUp, _, _ := c.store.Latest()
	from := lastBackedUp + 1
	if status.GetFirstIndex() > from {
		// The leader has already truncated further than we've backed up
		// (shouldn't normally happen — ConfirmCompaction is the only thing
		// that causes that, and it's this service that sends it — but
		// clamp defensively rather than requesting an unavailable range).
		from = status.GetFirstIndex()
	}
	to := status.GetLastIndex()
	if from > to {
		return // nothing new since the last cycle
	}

	log.Printf("snapshot-backup: COMPACTION_START leader=%s occupancy=%d%%", leader, status.GetOccupancyPercent())
	cycleStart := time.Now()

	entries, err := c.transfer.StreamLogRange(ctx, leader, from, to)
	if err != nil || len(entries) == 0 {
		return
	}
	if err := c.store.Apply(entries); err != nil {
		return
	}
	last := entries[len(entries)-1]

	// Re-verify leadership before ConfirmCompaction (§1 point 5): abort
	// without truncating anything if it changed mid-cycle. No data is at
	// risk either way (Leader Completeness, §10.2) — this cycle just
	// restarts against the new leader on its next tick.
	view, err := c.discovery.GetClusterView(ctx)
	if err != nil {
		return
	}
	if view.GetLeaderAddress() != leader {
		log.Printf("snapshot-backup: COMPACTION_ABORTED_LEADER_CHANGED old=%s new=%s", leader, view.GetLeaderAddress())
		return
	}

	file, err := c.store.Commit(last.GetIndex(), last.GetTerm())
	if err != nil {
		return
	}

	var stateBytes bytes.Buffer
	if err := snapshotfile.Encode(&stateBytes, file); err != nil {
		return
	}
	if err := c.transfer.ConfirmCompaction(ctx, leader, last.GetIndex(), stateBytes.Bytes()); err != nil {
		return
	}

	log.Printf("snapshot-backup: COMPACTION_COMPLETE lastIncludedIndex=%d durationMs=%d", last.GetIndex(), time.Since(cycleStart).Milliseconds())
}
