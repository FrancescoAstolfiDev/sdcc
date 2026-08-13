# Instructions for Claude Code — Week 5: Snapshot & Backup Service

## Task Overview

This picks up from Week 4 / `week4-mid.md`: the Client Proxy, Service Discovery, and Circuit Breaker are working, and — critically — a real gRPC transport bug was found and fixed during Week 4's manual verification (see §0 below, do not skip it). Week 5 implements the third and final node type, the Snapshot & Backup service (`internal/snapshot`, `cmd/snapshot-backup`), and its counterpart client-side logic inside `internal/raft` that lets every consensus node pull snapshots and compact its own log (spec §5.3).

This is a **two-directional** integration, not a one-way client: the Snapshot & Backup service calls out to consensus nodes (leader for status/compaction confirmation, any node for bulk log download), **and** every consensus node calls back into the Snapshot & Backup service (`GetLatestSnapshotInfo`/`FetchSnapshot`, §9.5) on its own periodic timer. Both directions need the same transport fix already applied in Week 4 — see §0.

---

## 0. Prerequisites check — do this before writing any new gRPC client code

- [ ] Confirm `api/proto` already defines `SnapshotTransfer` (§9.4: `GetLogStatus`, `StreamLogRange`, `ConfirmCompaction`) and `SnapshotCatalog` (§9.5: `GetLatestSnapshotInfo`, `FetchSnapshot`) from Week 1. If either is still a placeholder, implement it fully now, matching the message shapes already fixed in the specification.

- [ ] **Critical, non-negotiable: reuse the Week 4 transport fix.** During Week 4's manual fault-injection testing, the gRPC client in `internal/raft/grpctransport` was found dialing peers with gRPC-Go's **default `dns` resolver scheme**, which caches DNS resolution and does not reliably re-resolve a peer's hostname after that peer's container is killed and restarted (Docker can assign it a new IP on the bridge network). The fix was: (a) dial every peer as `passthrough:///<host>:<port>` instead of a bare `<host>:<port>` target, so every reconnection attempt re-resolves via a fresh `net.Dial` against Docker's embedded DNS instead of a cached gRPC resolver; (b) pass `grpc.WaitForReady(true)` as a call option on `AppendEntries` and `RequestReadIndex` (deliberately **not** on `RequestVote`, where fail-fast is correct so a candidate doesn't block on an unreachable peer during quorum collection).
  Any new gRPC client created in this phase — the Snapshot & Backup service's client toward consensus nodes, **and** each consensus node's new client toward the Snapshot & Backup service for `SnapshotCatalog` — **must** use the same `passthrough:///` dialing pattern from the start. Do not reintroduce the bug in a second transport implementation; if a shared internal package can be factored out of `internal/raft/grpctransport` for this (e.g. a small `dialPassthrough(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error)` helper), do that instead of copy-pasting the fix.
  Apply `grpc.WaitForReady(true)` on: `StreamLogRange`, `ConfirmCompaction` (Snapshot & Backup → leader — these should tolerate a brief post-restart DNS/connect gap the same way `AppendEntries` does), and `FetchSnapshot` (consensus node → Snapshot & Backup — a node catching up should wait rather than fail-fast if the snapshot service is mid-restart). Leave `GetLogStatus` and `GetLatestSnapshotInfo` as fail-fast (cheap, frequent polling calls; a transient failure just means "try again next poll interval," per §1/§2 below).

- [ ] Confirm §3.6's persistence layer (`log.dat`, `state.json`, and the not-yet-used `snapshot-<lastIncludedIndex>.dat` naming convention) is already in place from Week 2–3, and that `TruncateLogFrom` (persistence package) is available for reuse — this phase needs it for local compaction (§3 below), not a new truncation implementation.

---

## 1. Snapshot & Backup service — polling & compaction (`internal/snapshot`, `cmd/snapshot-backup`, spec §5.1)

1. Query Service Discovery (`GetClusterView`, §9.3) to learn the current leader — never hard-code a leader address, the leader changes after elections (as Week 4's testing demonstrated at length).
2. On a periodic timer — new config var `SNAPSHOT_BACKUP_POLL_INTERVAL_MS`, default **5000ms**, configurable — call `GetLogStatus` on the leader to check log occupancy.
3. When occupancy reaches **30%** (configurable, `COMPACTION_THRESHOLD_PERCENT`): start compaction. Call `StreamLogRange` to download the oldest entries — **this call may target any node, leader or follower**, since the entries being read are already committed (no linearizability concern applies to historical, already-committed data, unlike client reads in §4.1/§4.2). Prefer the leader if reachable; fall back to a follower from the cached cluster view if the leader itself is under load or briefly unreachable — this is a deliberate optimization, not required for correctness.
4. Concurrency guard: if occupancy reaches **90%** while a compaction is already in flight, wait for that cycle to finish (entries removed from the leader's log) before starting a new one — never run two compactions concurrently.
5. **Before calling `ConfirmCompaction`, re-verify via Service Discovery that the target is still the current leader.** If leadership changed mid-cycle, abort without truncating anything and restart the cycle against the new leader. This is not optional defensive programming — Week 4's testing showed leadership can change multiple times within a few seconds under real conditions, so this check will be exercised in practice, not just in theory.
6. Persist the snapshot to `/data/<nodeId>/snapshot-<lastIncludedIndex>.dat` (per §3.6's naming convention) using `encoding/gob`, containing: the serialized state-machine map, `lastIncludedIndex`, `lastIncludedTerm`. This file format must be readable by the consensus-node side implemented in §3 below — write the encoder/decoder as a small shared type (e.g. in a package both `internal/snapshot` and `internal/raft` can import) rather than duplicating the struct definition in two places.

---

## 2. Snapshot Catalog API — server side (`internal/snapshot`, spec §9.5)

Implement the gRPC server for `SnapshotCatalog`, exposed by the Snapshot & Backup service:

- `GetLatestSnapshotInfo` — returns `{lastIncludedIndex, lastIncludedTerm, available}` for the most recently completed compaction. Must be cheap and non-blocking (same spirit as `GetStatus` in §3.5) — this is polled by every consensus node every 5s (§10.1's "Snapshot poll interval").
- `FetchSnapshot` — a streaming RPC returning the snapshot file's contents in chunks (`SnapshotChunk{data, isLast}`). Stream directly from the `.dat` file on disk; don't load the entire snapshot into memory before starting to send if it can be avoided, though for this project's scale a simple in-memory read-then-chunk implementation is acceptable — don't over-engineer this.

---

## 3. Node catch-up & local compaction — client side (`internal/raft`, spec §5.3)

This extends the Raft core built in Week 2–3, adding a new periodic loop alongside the existing election/heartbeat timers.

1. Every consensus node, on a periodic timer (`SNAPSHOT_POLL_INTERVAL_MS` — this is the one already listed in §10.1's timing table, default 5000ms; do not confuse it with §1's `SNAPSHOT_BACKUP_POLL_INTERVAL_MS`, they are different services checking different things), calls `GetLatestSnapshotInfo` on the Snapshot & Backup service.
2. **Catch-up rule:** if the returned `lastIncludedIndex` is greater than the node's own `lastApplied`, call `FetchSnapshot`, apply the received state as the new state-machine baseline, and **anchor** the local log by writing a single placeholder `LogEntry{Term: lastIncludedTerm, Index: lastIncludedIndex}` with no command payload. This anchor exists solely to satisfy the `prevLogIndex`/`prevLogTerm` consistency check in future `AppendEntries` calls (§3.3) — it must never be applied to the state machine (it's already reflected in the snapshot state) and must never be returned from a client read.
3. This same rule covers both a brand-new node with no local state and an already-attached node that fell behind a compaction point while connected — no separate code path for the two cases, per the specification's explicit design.
4. **Failure handling:** if `FetchSnapshot` fails (transient network/service issue — with `waitForReady` per §0, this should already tolerate brief gaps), do not discard existing local state or reset to zero. Keep whatever log/state the node already has and retry at the next periodic interval.
5. **Local compaction rule — applies to every node including the leader, not just followers:** if a node's own `lastApplied` is already `>=` the latest `lastIncludedIndex` reported by the Snapshot & Backup service, that node may truncate its own local log up to that index via the existing `TruncateLogFrom` (persistence package, Week 2–3) — a durable copy already exists in the backup store. This is the mechanism that makes log compaction actually happen on every node over time, not only on whichever node the Snapshot & Backup service happened to download from during its own compaction cycle in §1.

---

## 4. Observability logging (extends the pattern from `week4-mid.md`)

Following the same convention already established — log on state transitions only, never on every polling tick, using the existing `component[identifier]: EVENT_NAME key=value` format:

```go
// Snapshot & Backup service (internal/snapshot)
log.Printf("snapshot-backup: COMPACTION_START leader=%s occupancy=%d%%", leaderAddr, occupancyPct)
log.Printf("snapshot-backup: COMPACTION_COMPLETE lastIncludedIndex=%d durationMs=%d", index, ms)
log.Printf("snapshot-backup: COMPACTION_ABORTED_LEADER_CHANGED old=%s new=%s", oldLeader, newLeader)

// Consensus node catch-up (internal/raft)
log.Printf("consensus-node[%s]: SNAPSHOT_CATCHUP_START lastApplied=%d targetIndex=%d", nodeID, lastApplied, targetIndex)
log.Printf("consensus-node[%s]: SNAPSHOT_CATCHUP_COMPLETE newLastApplied=%d", nodeID, lastApplied)
log.Printf("consensus-node[%s]: LOCAL_LOG_COMPACTED truncatedTo=%d", nodeID, index)
```

- [ ] `COMPACTION_ABORTED_LEADER_CHANGED` must actually fire in a test (§5) that forces a leadership change mid-compaction — don't leave this path unverified given how often leadership changed during Week 4's real testing.

---

## 5. Testing strategy

- **Unit tests:** compaction threshold logic (30%/90% boundaries, the concurrency guard) against a fake/mock leader client — no real gRPC needed for this. Snapshot file round-trip (write via `internal/snapshot`'s encoder, read via `internal/raft`'s decoder, confirm the state machine and indices match) — this is the test that catches a shared-type mismatch between the two packages early, before it becomes a Docker-only symptom the way Week 4's transport bug did.
- **Catch-up logic:** extend the Week 2–3 in-process harness with a fake `SnapshotCatalog` implementation (in-memory, no gRPC) so the catch-up state machine (§3, points 1–5) can be tested at unit-test speed: a node starting with `lastApplied=0` against a fake catalog reporting a snapshot at index 50 should anchor correctly and resume replication from there, without spinning up containers.
- **Leadership-change-mid-compaction:** a dedicated test (in-process or real gRPC, whichever the existing harness supports more easily) that starts a compaction cycle, forces a new election before `ConfirmCompaction` is called, and confirms the cycle aborts cleanly and restarts against the new leader rather than truncating against a stale one.
- **End-to-end:** bring up the full stack via `docker-compose.yml`, write enough entries to cross the 30% compaction threshold, confirm a `snapshot-*.dat` file appears in the Snapshot & Backup service's own volume and that `log.dat` on the source node shrinks correspondingly. Then run the same kill+restart scenario from Week 4 (`docker compose kill -s SIGKILL` a node, wait, `docker compose start` it), but this time wait long enough that the leader has compacted past what the restarted node has — confirm via `SNAPSHOT_CATCHUP_START`/`COMPLETE` logs that it recovers via the catalog rather than getting stuck (this was explicitly the gap called out as untested in Week 4's summary).

---

## 6. Explicit non-goals for this phase (do not implement yet)

- **No EC2 deployment** — Week 6. Continue testing against the local Docker Compose stack.
- **No load generator or evaluation scenarios** (§12 scalability/fault-tolerance measurements) — Week 6, now that the full three-node-type system exists.
- **No report writing** — Week 7.
- **No further changes to the transport bug fix itself** (§0) beyond reusing it correctly in new clients — it's already fixed and verified; this phase applies the existing pattern, it doesn't revisit it.
- **No Pre-Vote implementation** — still an accepted, documented limitation (the isolated-node behavior from Week 4), unrelated to this phase's scope.

---

## 7. Deliverables checklist for Week 5

- [ ] `SnapshotTransfer` and `SnapshotCatalog` gRPC services fully implemented (not placeholders) in `api/proto`/`pkg/pb`.
- [ ] Every new gRPC client created this phase (both directions) uses the `passthrough:///` dialing pattern from §0, via a shared helper if practical rather than duplicated code.
- [ ] `internal/snapshot`: polling loop (`SNAPSHOT_BACKUP_POLL_INTERVAL_MS`, default 5s), 30%/90% threshold compaction logic with the concurrency guard, pre-`ConfirmCompaction` leader re-verification, snapshot file written per §3.6's naming/format convention.
- [ ] `internal/snapshot` `SnapshotCatalog` server: `GetLatestSnapshotInfo` (cheap, non-blocking) and `FetchSnapshot` (streaming) implemented.
- [ ] `internal/raft`: periodic catch-up loop (`SNAPSHOT_POLL_INTERVAL_MS`, default 5s — distinct from the backup service's own poll interval), log anchoring, failure handling that never discards existing state, local compaction rule applied uniformly including to the leader itself.
- [ ] Observability logging per §4, with `COMPACTION_ABORTED_LEADER_CHANGED` confirmed to fire in a real test, not just implemented and left unverified.
- [ ] Unit tests: compaction threshold logic, snapshot file round-trip between the writer and reader packages, catch-up state machine against a fake catalog.
- [ ] Integration test: leadership change forced mid-compaction, cycle aborts and restarts cleanly.
- [ ] End-to-end test via `docker-compose.yml`: compaction triggers and shrinks `log.dat` under real write load; a node restarted after falling behind a compaction point recovers via the catalog (the specific gap left open at the end of Week 4).
- [ ] `README.md`/`Makefile` updated with a target for this phase's test suite (e.g. `make test-snapshot`), separate from the existing `make test-raft`/proxy targets.
