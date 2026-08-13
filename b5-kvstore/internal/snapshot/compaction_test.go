package snapshot_test

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"b5-kvstore/internal/snapshot"
	"b5-kvstore/pkg/pb"
)

// fakeDiscovery is a controllable snapshot.DiscoveryClient: no real gRPC or
// Service Discovery needed to exercise the compaction cycle's threshold/
// concurrency-guard/leader-re-verification logic (md-week5.md §5).
type fakeDiscovery struct {
	mu     sync.Mutex
	leader string
}

func (f *fakeDiscovery) SetLeader(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leader = addr
}

func (f *fakeDiscovery) GetClusterView(_ context.Context) (*pb.ClusterView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &pb.ClusterView{LeaderAddress: f.leader}, nil
}

// fakeTransfer is a controllable snapshot.TransferClient.
type fakeTransfer struct {
	mu               sync.Mutex
	status           *pb.LogStatusReply
	entries          []*pb.LogEntry
	onStream         func() // hook invoked inside StreamLogRange, before returning
	streamCalls      int32
	getLogStatusCall int32
	confirmed        []uint64
}

func (f *fakeTransfer) GetLogStatus(_ context.Context, _ string) (*pb.LogStatusReply, error) {
	atomic.AddInt32(&f.getLogStatusCall, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status, nil
}

func (f *fakeTransfer) StreamLogRange(_ context.Context, _ string, _, _ uint64) ([]*pb.LogEntry, error) {
	atomic.AddInt32(&f.streamCalls, 1)
	f.mu.Lock()
	hook := f.onStream
	entries := f.entries
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return entries, nil
}

func (f *fakeTransfer) ConfirmCompaction(_ context.Context, _ string, truncatedUpToIndex uint64, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = append(f.confirmed, truncatedUpToIndex)
	return nil
}

func (f *fakeTransfer) StreamCalls() int32 { return atomic.LoadInt32(&f.streamCalls) }

func putEntry(t *testing.T, index, term uint64, key, value string) *pb.LogEntry {
	t.Helper()
	cmd, err := proto.Marshal(&pb.KVCommand{Op: pb.KVCommand_PUT, Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal KVCommand: %v", err)
	}
	return &pb.LogEntry{Term: term, Index: index, Command: cmd}
}

func newCompactor(t *testing.T, disc *fakeDiscovery, transfer *fakeTransfer) *snapshot.Compactor {
	t.Helper()
	store, err := snapshot.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cfg := snapshot.Config{
		PollInterval:               time.Second, // Tick is driven directly, not via Run's ticker
		CompactionThresholdPercent: 30,
		ConcurrencyGuardPercent:    90,
		RPCTimeout:                 time.Second,
	}
	return snapshot.NewCompactor(disc, transfer, store, cfg)
}

// TestCompactor_BelowThreshold_NoCycleStarts covers the 30% boundary: below
// it, nothing happens.
func TestCompactor_BelowThreshold_NoCycleStarts(t *testing.T) {
	disc := &fakeDiscovery{leader: "leader-a"}
	transfer := &fakeTransfer{status: &pb.LogStatusReply{OccupancyPercent: 29, FirstIndex: 1, LastIndex: 10}}
	c := newCompactor(t, disc, transfer)

	c.Tick(context.Background())

	if got := transfer.StreamCalls(); got != 0 {
		t.Fatalf("expected no StreamLogRange call below threshold, got %d", got)
	}
}

// TestCompactor_AtThreshold_StartsCycle covers the 30% boundary from the
// other side: reaching it starts a full cycle through to ConfirmCompaction.
func TestCompactor_AtThreshold_StartsCycle(t *testing.T) {
	disc := &fakeDiscovery{leader: "leader-a"}
	transfer := &fakeTransfer{
		status:  &pb.LogStatusReply{OccupancyPercent: 30, FirstIndex: 1, LastIndex: 2},
		entries: []*pb.LogEntry{putEntry(t, 1, 1, "k0", "v"), putEntry(t, 2, 1, "k1", "v")},
	}
	c := newCompactor(t, disc, transfer)

	c.Tick(context.Background())

	if got := transfer.StreamCalls(); got != 1 {
		t.Fatalf("expected exactly one StreamLogRange call at threshold, got %d", got)
	}
	transfer.mu.Lock()
	confirmed := append([]uint64(nil), transfer.confirmed...)
	transfer.mu.Unlock()
	if len(confirmed) != 1 || confirmed[0] != 2 {
		t.Fatalf("expected ConfirmCompaction(2), got %v", confirmed)
	}
}

// TestCompactor_AtConcurrencyGuard_Waits covers the 90% boundary as its own
// distinct check, independent of whether a cycle happens to already be
// running from this instance's own point of view: occupancy this high is
// itself treated as a signal that a cycle must be in flight somewhere
// (md-week5.md §1 point 4), so no new one starts even though nothing here
// is actually running yet.
func TestCompactor_AtConcurrencyGuard_Waits(t *testing.T) {
	disc := &fakeDiscovery{leader: "leader-a"}
	transfer := &fakeTransfer{status: &pb.LogStatusReply{OccupancyPercent: 90, FirstIndex: 1, LastIndex: 10}}
	c := newCompactor(t, disc, transfer)

	c.Tick(context.Background())

	if got := transfer.StreamCalls(); got != 0 {
		t.Fatalf("expected no StreamLogRange call at/above the concurrency-guard threshold, got %d", got)
	}
}

// TestCompactor_JustBelowConcurrencyGuard_StartsCycle is the boundary's
// other side: one point below 90% is still safe to start.
func TestCompactor_JustBelowConcurrencyGuard_StartsCycle(t *testing.T) {
	disc := &fakeDiscovery{leader: "leader-a"}
	transfer := &fakeTransfer{
		status:  &pb.LogStatusReply{OccupancyPercent: 89, FirstIndex: 1, LastIndex: 1},
		entries: []*pb.LogEntry{putEntry(t, 1, 1, "k0", "v")},
	}
	c := newCompactor(t, disc, transfer)

	c.Tick(context.Background())

	if got := transfer.StreamCalls(); got != 1 {
		t.Fatalf("expected one StreamLogRange call just below the concurrency-guard threshold, got %d", got)
	}
}

// TestCompactor_ConcurrencyGuard_SkipsSecondConcurrentCycle is the
// mechanism itself (as opposed to the occupancy-based boundary above):
// while a cycle from this instance is genuinely in flight (blocked mid
// StreamLogRange), a second concurrent Tick must not start a second cycle
// on top of it — "never run two compactions concurrently."
func TestCompactor_ConcurrencyGuard_SkipsSecondConcurrentCycle(t *testing.T) {
	disc := &fakeDiscovery{leader: "leader-a"}
	release := make(chan struct{})
	entered := make(chan struct{})
	transfer := &fakeTransfer{
		status:  &pb.LogStatusReply{OccupancyPercent: 50, FirstIndex: 1, LastIndex: 1},
		entries: []*pb.LogEntry{putEntry(t, 1, 1, "k0", "v")},
	}
	transfer.onStream = func() {
		close(entered)
		<-release
	}
	c := newCompactor(t, disc, transfer)

	done := make(chan struct{})
	go func() {
		c.Tick(context.Background())
		close(done)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first Tick never reached StreamLogRange")
	}

	// A second, concurrent Tick while the first is still blocked inside its
	// own StreamLogRange call must return without starting a second cycle.
	c.Tick(context.Background())
	if got := transfer.StreamCalls(); got != 1 {
		t.Fatalf("expected the concurrent Tick to skip StreamLogRange (guard), got %d total calls", got)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first Tick never finished")
	}
}

// TestCompactor_AbortsOnLeaderChangeMidCycle is the required integration
// test (md-week5.md §5): leadership changes between the download and
// ConfirmCompaction, and the cycle must abort without truncating anything,
// with COMPACTION_ABORTED_LEADER_CHANGED actually firing — not just
// implemented and left unverified (§4's checklist item).
func TestCompactor_AbortsOnLeaderChangeMidCycle(t *testing.T) {
	var logBuf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(orig)

	disc := &fakeDiscovery{leader: "leader-a"}
	transfer := &fakeTransfer{
		status:  &pb.LogStatusReply{OccupancyPercent: 40, FirstIndex: 1, LastIndex: 1},
		entries: []*pb.LogEntry{putEntry(t, 1, 1, "k0", "v")},
	}
	transfer.onStream = func() {
		// Leadership changes while this cycle is mid-flight, after the
		// download but before the pre-ConfirmCompaction re-verification.
		disc.SetLeader("leader-b")
	}
	c := newCompactor(t, disc, transfer)

	c.Tick(context.Background())

	transfer.mu.Lock()
	confirmed := append([]uint64(nil), transfer.confirmed...)
	transfer.mu.Unlock()
	if len(confirmed) != 0 {
		t.Fatalf("expected ConfirmCompaction NOT to be called after a leader change mid-cycle, got %v", confirmed)
	}
	if !strings.Contains(logBuf.String(), "COMPACTION_ABORTED_LEADER_CHANGED old=leader-a new=leader-b") {
		t.Fatalf("expected COMPACTION_ABORTED_LEADER_CHANGED to fire, got log output: %s", logBuf.String())
	}
}

// TestCompactor_NoLeader_NoCycle is a light sanity check: with no leader
// known yet, nothing is attempted.
func TestCompactor_NoLeader_NoCycle(t *testing.T) {
	disc := &fakeDiscovery{}
	transfer := &fakeTransfer{}
	c := newCompactor(t, disc, transfer)

	c.Tick(context.Background())

	if got := transfer.StreamCalls(); got != 0 {
		t.Fatalf("expected no calls with no known leader, got %d", got)
	}
}
