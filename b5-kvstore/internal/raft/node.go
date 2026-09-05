// Package raft implements the Raft-lite consensus core: persistent state,
// randomized-timeout election, AppendEntries replication with the
// term-boundary commit rule, and fast log repair. Networking is abstracted
// behind the Transport interface so the same core runs against both the
// in-process fake transport (§6 Step B) and real gRPC (§6 Step C).
//
// Per spec §7, snapshotting, the Client Proxy, Circuit Breaker wiring, and
// Service Discovery integration are explicit non-goals of this package —
// they live in internal/snapshot, internal/proxy, internal/circuitbreaker,
// and internal/discovery respectively.
package raft

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"b5-kvstore/internal/raft/persistence"
	"b5-kvstore/internal/snapshotfile"
	"b5-kvstore/pkg/pb"
)

// ErrLeadershipLost is delivered to any pending ProposeSync caller when this
// node steps down from Leader before its proposed entry commits. The entry
// may or may not end up committed under a different leader — this node can
// no longer promise either way, so it must not leave the caller blocked.
var ErrLeadershipLost = errors.New("raft: lost leadership before entry committed")

// defaultTick is how often the background tickers check elapsed time
// against the election/heartbeat deadlines. It must be small relative to
// HEARTBEAT_INTERVAL_MS (default 50ms) to keep timing accurate.
const defaultTick = 10 * time.Millisecond

// defaultRPCTimeout bounds a single RequestVote/AppendEntries RPC attempt
// so a wedged/partitioned peer can't stall the sending goroutine forever.
const defaultRPCTimeout = 2 * time.Second

// TimingConfig holds the election/heartbeat timing knobs from §2, sourced
// from env vars with the defaults and constraints from the spec table.
type TimingConfig struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// LoadTimingConfigFromEnv reads ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS
// and HEARTBEAT_INTERVAL_MS, applying the defaults 150/300/50ms, and
// validates the constraints from §2: max must exceed min, and the heartbeat
// interval must stay well below the minimum election timeout.
func LoadTimingConfigFromEnv() (TimingConfig, error) {
	minMS, err := envIntDefault("ELECTION_TIMEOUT_MIN_MS", 150)
	if err != nil {
		return TimingConfig{}, err
	}
	maxMS, err := envIntDefault("ELECTION_TIMEOUT_MAX_MS", 300)
	if err != nil {
		return TimingConfig{}, err
	}
	heartbeatMS, err := envIntDefault("HEARTBEAT_INTERVAL_MS", 50)
	if err != nil {
		return TimingConfig{}, err
	}
	if maxMS <= minMS {
		return TimingConfig{}, fmt.Errorf("ELECTION_TIMEOUT_MAX_MS (%d) must be > ELECTION_TIMEOUT_MIN_MS (%d)", maxMS, minMS)
	}
	if heartbeatMS >= minMS {
		return TimingConfig{}, fmt.Errorf("HEARTBEAT_INTERVAL_MS (%d) must stay well below ELECTION_TIMEOUT_MIN_MS (%d)", heartbeatMS, minMS)
	}
	return TimingConfig{
		ElectionTimeoutMin: time.Duration(minMS) * time.Millisecond,
		ElectionTimeoutMax: time.Duration(maxMS) * time.Millisecond,
		HeartbeatInterval:  time.Duration(heartbeatMS) * time.Millisecond,
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

// quorumSize returns floor(n/2)+1 for a cluster of n nodes. Kept as a
// helper (not a hardcoded constant) since the scalability tests run with
// 3, 5, and 7 nodes.
func quorumSize(n int) int {
	return n/2 + 1
}

// ApplyMsg is a single committed log entry handed to the caller-supplied
// apply callback, in commit order. Building a real state machine is out of
// scope for this package (§7); this is the extension point internal/proxy
// and internal/statemachine (and tests) hook into.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Config configures a new Node.
type Config struct {
	ID      string
	Address string
	DataDir string
	// Peers is the static list of other cluster members' addresses/ids.
	// Per §7's non-goals for this package, peer addressing is static
	// config, not Service Discovery — this is the permanent design for the
	// consensus layer, not a temporary shortcut.
	Peers     []string
	Timing    TimingConfig
	Transport Transport
	// ApplyFn, if set, is invoked synchronously (Node's lock held) once per
	// newly committed entry, in order. It must not block or call back into
	// Node.
	ApplyFn func(ApplyMsg)
	// Tick overrides the internal timer-check resolution; defaults to
	// defaultTick. Tests may shrink it to speed up election scenarios.
	Tick time.Duration
	// RPCTimeout overrides the per-RPC-attempt timeout; defaults to
	// defaultRPCTimeout.
	RPCTimeout time.Duration

	// Snapshot configures the compaction/catch-up integration (§5.1/§5.3).
	// Zero value uses the documented defaults.
	Snapshot SnapshotConfig
	// SnapshotCatalog is this node's client to the Snapshot & Backup
	// service's Snapshot Catalog API (§9.5), driving the periodic catch-up
	// and local-compaction loop (§5.3). Nil disables the loop entirely —
	// tests that don't exercise snapshotting (e.g. the plain harness
	// scenarios) simply omit it.
	SnapshotCatalog SnapshotCatalogClient
	// LocalSnapshot is the latest locally-persisted snapshot file found in
	// DataDir at startup (§3.6's startup sequence: "reads the latest
	// snapshot-*.dat... to seed lastApplied and the state machine, then
	// replays log.dat from the snapshot's lastIncludedIndex onward"), if
	// any — the caller (cmd/consensus-node) is responsible for finding it
	// via snapshotfile.Latest and for calling Restore() on its own state
	// machine with its State before constructing the Node, since Node
	// itself only owns the log/indices, not the external state machine.
	// Nil means "no local snapshot yet" (brand-new node or one that has
	// never compacted/caught up), in which case NewNode starts
	// logOffset/commitIndex/lastApplied at 0.
	LocalSnapshot *snapshotfile.File
	// StateMachine gives the catch-up loop (§5.3) restore access to the
	// full state-machine map when a fetched snapshot is adopted. Structural
	// interface (Restore(map[string]string)) so this package doesn't need
	// to import internal/statemachine — *statemachine.KV satisfies it
	// as-is. Nil alongside a nil SnapshotCatalog is fine (loop disabled);
	// required if SnapshotCatalog is set.
	StateMachine StateMachineSnapshotter
}

// Node is a single Raft-lite cluster member. All exported RPC-handler
// methods (RequestVote, AppendEntries, GetStatus, InstallSnapshot) satisfy
// pb.ConsensusServer.
type Node struct {
	// Embedding satisfies protoc-gen-go-grpc's forced forward-compatibility
	// requirement (mustEmbedUnimplementedConsensusServer). Node implements
	// all four ConsensusServer methods itself (election.go, replication.go)
	// so the embedded fallbacks are never actually reached.
	pb.UnimplementedConsensusServer
	// Same forward-compatibility requirement for pb.SnapshotTransferServer
	// (§9.4): Node implements all three methods itself
	// (snapshottransfer.go).
	pb.UnimplementedSnapshotTransferServer

	mu sync.Mutex

	id      string
	address string
	dataDir string
	peers   []string

	timing     TimingConfig
	tick       time.Duration
	rpcTimeout time.Duration
	transport  Transport
	applyFn    func(ApplyMsg)
	rng        *rand.Rand

	snapshotCfg     SnapshotConfig
	snapshotCatalog SnapshotCatalogClient
	// stateMachine, if set, is consulted/mutated by the catch-up loop
	// (§5.3) to read/restore the full state-machine map around a fetched
	// snapshot. Distinct from applyFn (which only ever sees one committed
	// command at a time) because adopting a snapshot baseline is a
	// wholesale replace, not a sequence of individual applies.
	stateMachine StateMachineSnapshotter

	// Persistent state (mirrored in memory; every mutation goes through
	// persistence before being observable to an RPC caller).
	currentTerm uint64
	votedFor    string
	// log is positional: log[i] has Index == logOffset + i + 1. Before any
	// compaction, logOffset is 0 (log[i] has Index == i+1). After
	// compaction (§5.1/§5.3), log[0] is a placeholder anchor
	// entry {Term: lastIncludedTerm, Index: logOffset+1} standing in for
	// every entry at or before that index, which now exist only in the
	// Snapshot & Backup service's catalog, not in this node's own log.
	log       []persistence.LogEntry
	logOffset uint64
	// currentSnapshotPath is the on-disk path (§3.6: /data/<nodeId>/
	// snapshot-<lastIncludedIndex>.dat) of the local snapshot file backing
	// the current anchor, if any — tracked so a newer one written by either
	// compaction path (compaction.go, snapshotcatchup.go) can clean up its
	// predecessor ("the previous snapshot file... may be deleted", §3.6).
	currentSnapshotPath string

	// Volatile state (§2: recomputed on restart, never persisted directly).
	role        Role
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state (§5), reset on becoming leader.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// currentLeader holds the current leader's dialable address (not a bare
	// node ID): the leaderId carried on AppendEntries is populated with the
	// sender's own advertised address (see broadcastAppendEntries) so that
	// followers can use it directly as a Transport target — for the
	// Read-Index handshake and for populating redirect_leader in
	// KVService responses.
	currentLeader string

	lastHeartbeat   time.Time
	electionTimeout time.Duration

	// lastAppendEntriesAt is the time of the last valid AppendEntries this
	// node accepted from a leader, kept solely to report
	// time_since_last_heartbeat on ELECTION_START — unlike lastHeartbeat
	// (which also moves on vote-grant/election-start to drive the timeout),
	// this only ever moves in AppendEntries. Zero until the first one ever
	// arrives.
	lastAppendEntriesAt time.Time

	// applyWake is closed and replaced every time lastApplied advances
	// (applyCommittedLocked), letting FollowerLinearizableRead's wait
	// block on a channel instead of busy-polling.
	applyWake chan struct{}

	stopCh   chan struct{}
	stopOnce sync.Once

	// waiters holds, per pending ProposeSync call, the channel to notify
	// once the entry at that index is either committed (nil) or can no
	// longer be guaranteed by this node (non-nil error). Populated only by
	// ProposeSync; Propose (the non-blocking entry point) never touches it.
	waiters map[uint64]chan error
}

// NewNode loads persistent state from cfg.DataDir and constructs a Node.
// The node always starts as Follower (§2), regardless of the persisted
// term/vote. Absent a local snapshot (cfg.LocalSnapshot), commitIndex/
// lastApplied start at 0 and are re-derived through normal protocol
// operation, per §2 ("in-memory only; on restart, both are recomputed from
// the loaded log" — a restarted node cannot know what the cluster had
// actually committed beyond what consensus re-establishes). If a local
// snapshot is present (§3.6), both instead seed from its
// lastIncludedIndex: everything up to and including it is already reflected
// in the state machine the caller restored, and log entries at or before
// that index are dropped from the loaded log — replaying them again would
// re-derive the same state redundantly at best, and at worst reference log
// positions the snapshot has already superseded (e.g. after a crash between
// writing the snapshot and truncating log.dat, §3.6's exact "replays
// log.dat from the snapshot's lastIncludedIndex onward").
func NewNode(cfg Config) (*Node, error) {
	if cfg.Tick == 0 {
		cfg.Tick = defaultTick
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = defaultRPCTimeout
	}
	if cfg.Snapshot.LogCapacityEntries == 0 {
		cfg.Snapshot.LogCapacityEntries = defaultLogCapacityEntries
	}
	if cfg.Snapshot.PollInterval == 0 {
		cfg.Snapshot.PollInterval = defaultSnapshotPollInterval
	}
	currentTerm, votedFor, err := persistence.LoadState(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	log, err := persistence.ReadAllLogEntries(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raft: read log: %w", err)
	}

	var logOffset, initialApplied uint64
	var snapshotPath string
	if cfg.LocalSnapshot != nil && cfg.LocalSnapshot.LastIncludedIndex > 0 {
		anchor := persistence.LogEntry{Term: cfg.LocalSnapshot.LastIncludedTerm, Index: cfg.LocalSnapshot.LastIncludedIndex}
		suffix := log[:0]
		for _, e := range log {
			if e.Index > anchor.Index {
				suffix = append(suffix, e)
			}
		}
		log = append([]persistence.LogEntry{anchor}, suffix...)
		logOffset = anchor.Index - 1
		initialApplied = anchor.Index
		snapshotPath = filepath.Join(cfg.DataDir, snapshotfile.FileName(anchor.Index))
	} else if len(log) > 0 {
		// Fallback for the case where log.dat itself already starts with an
		// anchor (Index > 1) but no LocalSnapshot was supplied — keeps
		// entryAtLocked/lastLogIndexLocked consistent even if the caller's
		// own snapshot lookup was skipped for some reason.
		logOffset = log[0].Index - 1
		if logOffset > 0 {
			initialApplied = logOffset + 1
		}
	}

	n := &Node{
		id:                  cfg.ID,
		address:             cfg.Address,
		dataDir:             cfg.DataDir,
		peers:               append([]string(nil), cfg.Peers...),
		timing:              cfg.Timing,
		tick:                cfg.Tick,
		rpcTimeout:          cfg.RPCTimeout,
		transport:           cfg.Transport,
		applyFn:             cfg.ApplyFn,
		rng:                 rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(fnvHash(cfg.ID)))),
		snapshotCfg:         cfg.Snapshot,
		snapshotCatalog:     cfg.SnapshotCatalog,
		stateMachine:        cfg.StateMachine,
		currentTerm:         currentTerm,
		votedFor:            votedFor,
		log:                 log,
		logOffset:           logOffset,
		commitIndex:         initialApplied,
		lastApplied:         initialApplied,
		currentSnapshotPath: snapshotPath,
		role:                Follower,
		nextIndex:           make(map[string]uint64),
		matchIndex:          make(map[string]uint64),
		applyWake:           make(chan struct{}),
		stopCh:              make(chan struct{}),
		waiters:             make(map[uint64]chan error),
	}
	n.resetElectionDeadlineLocked()
	return n, nil
}

func fnvHash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// Start launches the background election/heartbeat tickers. Safe to call
// once per Node.
func (n *Node) Start() {
	go n.electionTicker()
	go n.heartbeatTicker()
	if n.snapshotCatalog != nil {
		go n.snapshotPollLoop()
	}
}

// Stop terminates the background tickers. Safe to call multiple times.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

func (n *Node) ID() string { return n.id }

// Status returns a snapshot of the node's externally-visible state.
func (n *Node) Status() (role Role, term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.currentTerm
}

// LeaderAddress returns the dialable address of the leader most recently
// observed via a valid AppendEntries (or "" if unknown/never observed).
// Best-effort: may briefly lag reality around elections.
func (n *Node) LeaderAddress() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentLeader
}

// LastApplied returns the highest log index this node has applied to its
// state machine so far. Exposed mainly so tests can observe catch-up/
// compaction progress (§5.3) without reaching into Node's internals.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// --- election/heartbeat deadline handling -------------------------------

// resetElectionDeadlineLocked draws a fresh random timeout in
// [ElectionTimeoutMin, ElectionTimeoutMax) and marks "now" as the last time
// we had a reason not to start an election. Must be called with n.mu held.
func (n *Node) resetElectionDeadlineLocked() {
	n.lastHeartbeat = time.Now()
	span := n.timing.ElectionTimeoutMax - n.timing.ElectionTimeoutMin
	jitter := time.Duration(0)
	if span > 0 {
		jitter = time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionTimeout = n.timing.ElectionTimeoutMin + jitter
}

func (n *Node) electionTicker() {
	t := time.NewTicker(n.tick)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.mu.Lock()
			role := n.role
			elapsed := time.Since(n.lastHeartbeat)
			timeout := n.electionTimeout
			n.mu.Unlock()
			if role != Leader && elapsed >= timeout {
				n.startElection()
			}
		}
	}
}

func (n *Node) heartbeatTicker() {
	t := time.NewTicker(n.timing.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.mu.Lock()
			isLeader := n.role == Leader
			n.mu.Unlock()
			if isLeader {
				n.broadcastAppendEntries()
			}
		}
	}
}

// --- shared helpers -------------------------------------------------------

func (n *Node) lastLogIndexLocked() uint64 {
	return n.logOffset + uint64(len(n.log))
}

func (n *Node) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// entryAtLocked returns the entry at 1-indexed index, or nil if index is
// out of range: either index 0 (§1: always means "no entry"), at or before
// logOffset (compacted away — see §5.1/§5.3, only recoverable via the
// Snapshot Catalog, never from this node's own log again), or past the end
// of the log.
func (n *Node) entryAtLocked(index uint64) *persistence.LogEntry {
	if index <= n.logOffset || index > n.logOffset+uint64(len(n.log)) {
		return nil
	}
	return &n.log[index-n.logOffset-1]
}

func (n *Node) persistStateLocked() error {
	return persistence.SaveState(n.dataDir, n.currentTerm, n.votedFor)
}

// setRoleLocked transitions to Follower or Candidate. Leader transitions go
// through becomeLeaderLocked, which needs to also initialize nextIndex/
// matchIndex. Must be called with n.mu held.
func (n *Node) setRoleLocked(r Role) {
	if n.role == Leader && r != Leader {
		n.abortPendingWaitersLocked(ErrLeadershipLost)
	}
	n.role = r
}

// abortPendingWaitersLocked delivers err to every ProposeSync call still
// waiting on a commit and clears them out. Must be called with n.mu held.
func (n *Node) abortPendingWaitersLocked(err error) {
	for idx, ch := range n.waiters {
		ch <- err
		delete(n.waiters, idx)
	}
}

// observeTermLocked implements the universal higher-term rule (§3): any
// RPC request or reply carrying a term higher than ours means we must
// update currentTerm, persist it, clear votedFor, and revert to Follower —
// before doing anything else. It is the single shared choke point every
// RPC handler and every RPC-reply path must call, so a Leader that missed a
// newer election reverts exactly like a Candidate would. Returns whether it
// actually stepped down (term was higher). source identifies the RPC that
// carried the higher term (e.g. "AppendEntries", "RequestVoteReply"), for
// the STEPPED_DOWN log line — this is the single place that line is
// emitted, so it can never be duplicated per call site.
func (n *Node) observeTermLocked(term uint64, source string) (bool, error) {
	if term <= n.currentTerm {
		return false, nil
	}
	oldTerm := n.currentTerm
	n.currentTerm = term
	n.votedFor = ""
	n.setRoleLocked(Follower)
	if err := n.persistStateLocked(); err != nil {
		return true, err
	}
	log.Printf("consensus-node[%s]: STEPPED_DOWN term=%d (saw_higher_term=%d from=%s)", n.id, oldTerm, term, source)
	return true, nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
