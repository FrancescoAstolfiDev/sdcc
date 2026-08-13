# Instructions for Claude Code — Week 2–3: Raft-lite Core (Election, Replication, Persistence)

## Task Overview

This picks up directly from the Week 1 skeleton (`md-week1.md`): the Go module, the `api/proto` contracts, the generated `pkg/pb` code, the empty service stubs under `cmd/`, and the `docker-compose.yml` used to validate Docker networking are all assumed to already exist and to build successfully.

Week 2–3 implements the actual consensus logic inside `internal/raft/`: persistent state, randomized election timeout, `RequestVote`, `AppendEntries` (heartbeat + log replication + commit), and log repair. Per the project specification, this is explicitly the highest-bug-risk part of the whole project — correctness bugs here (a stale leader that doesn't step down, a commit rule that ignores term boundaries, an off-by-one in log indexing) tend to surface only under crash/partition testing, not under normal operation. The specification's own recommended build order is followed here: unit tests first with no networking at all, then an in-process fake transport to validate protocol correctness cheaply, and only then real gRPC between containers.

**Do not implement snapshotting, the Client Proxy, Circuit Breaker wiring, or Service Discovery integration in this phase** — see §7 "Explicit non-goals for this phase" below. Those are Week 4–5 work and depend on this phase being solid first.

---

## 0. Prerequisites check (from Week 1) — do this before anything else

Before writing any Raft logic, verify and, if needed, extend what Week 1 produced:

- [ ] `api/proto/consensus.proto` exists and compiles into `pkg/pb`.
- [ ] **Gap check against the full spec:** Week 1's proto scope (`AppendEntries`, `RequestVote`, `ClientCommand`, `RegisterNode`/`Heartbeat`) does **not** explicitly list every field the Raft-lite algorithm needs. Confirm/add the following to `RequestVoteRequest`/`Reply` and `AppendEntriesRequest`/`Reply` if missing:
  - `RequestVoteRequest`: `term`, `candidateId`, `lastLogIndex`, `lastLogTerm`.
  - `RequestVoteReply`: `term`, `voteGranted`.
  - `AppendEntriesRequest`: `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit`.
  - `AppendEntriesReply`: `term`, `success`, **plus `conflictIndex` and `conflictTerm`** (needed for the fast log-repair path in §5 below — easy to forget since it's marked "optional but recommended" in the wire format, but it is required for this phase's log-repair implementation).
  - A `GetStatusRequest`/`GetStatusReply` (`role`, `term`, `address`, `nodeId`) and a `Role` enum (`FOLLOWER`, `CANDIDATE`, `LEADER`) — not used yet until Week 4's Service Discovery, but define it now so the node's internal role tracking has a canonical wire representation from the start.
- [ ] **Naming reconciliation, not blocking:** Week 1's plan mentions `RegisterNode`/`Heartbeat` for discovery (push-based, self-registration). The full specification's Service Discovery (§7) is poll-based instead: Service Discovery calls `GetStatus()` on each node periodically; nodes never push. This divergence only matters for Week 4 (Service Discovery) and can be left as-is for now — just don't build Raft-lite logic that assumes a push-based registration exists.
- [ ] Regenerate `pkg/pb` after any proto changes (`make proto` or equivalent) and confirm `cmd/consensus-node` still builds against the regenerated package.

---

## 1. Persistent state & on-disk layout

Implement a `persistence` sub-package under `internal/raft/` (e.g. `internal/raft/persistence/`) with no networking dependencies at all — this must be unit-testable in complete isolation.

**Fields and types:**
- `currentTerm uint64`
- `votedFor string` (empty string = no vote cast this term)
- `log []LogEntry`, where `LogEntry{Term uint64; Index uint64; Command []byte}`

**Log indexing convention (fix this before writing anything else — it is the single most common source of off-by-one bugs in hand-rolled Raft):**
- `log[]` is **1-indexed**. Index `0` is reserved to mean "no entries" and is never itself a real entry.
- For an empty log: `lastLogIndex = 0`, `lastLogTerm = 0`.
- This convention must be used consistently in the vote-granting comparison (§3 below) and in the log-repair logic (§5 below).

**On-disk file layout**, one independent directory per node (matches the Docker volume convention `/data/<nodeId>/` from the deployment spec):
- `log.dat` — append-only file of serialized `LogEntry` records. Use `encoding/gob`. Each record must be **length-prefixed** so that a partially-written last record (from a crash mid-append) can be detected and truncated on restart, rather than corrupting the whole file. Every append to this file must be followed by an explicit flush + `fsync` **before** the node acknowledges the corresponding `AppendEntries` call — this is what "durable" means throughout this phase, and there is no accepted shortcut here.
- `state.json` — a small JSON file holding `{"currentTerm": ..., "votedFor": ...}`, rewritten via write-to-temp-file-then-rename on every change (never rewrite in place). Per the project's accepted-limitation trade-off, this file is **not** required to be synchronously `fsync`'d before replying to a vote/append RPC — document this explicitly as a code comment where the write happens, referencing the accepted double-voting-on-crash-restart risk, so it reads as a deliberate choice and not an oversight if the professor asks about it.

**Required functions** (with unit tests covering each, including crash-simulation tests — e.g. truncate `log.dat` mid-record and confirm the loader recovers cleanly):
- `LoadState(dir string) (currentTerm uint64, votedFor string, err error)`
- `SaveState(dir string, currentTerm uint64, votedFor string) error`
- `AppendLogEntries(dir string, entries []LogEntry) error` (append + fsync)
- `ReadAllLogEntries(dir string) ([]LogEntry, error)` (used at startup to rebuild in-memory log tail)
- `TruncateLogFrom(dir string, index uint64) error` (used by the leader's log-repair path, §5, to discard conflicting entries before re-appending)

---

## 2. In-memory node state & timing configuration

In `internal/raft/node.go` (or similar), define the runtime `Node` struct wrapping the persistent state above plus:

- `role` — one of `Follower`, `Candidate`, `Leader`; **always starts as `Follower`** on boot, regardless of what `state.json` contains.
- `commitIndex uint64`, `lastApplied uint64` — in-memory only; on restart, both are recomputed from the loaded log (not persisted directly).
- Election timer and heartbeat timer, both driven by configurable durations (env vars, with these defaults):

| Env var | Default | Constraint |
|---|---|---|
| `ELECTION_TIMEOUT_MIN_MS` | 150 | — |
| `ELECTION_TIMEOUT_MAX_MS` | 300 | must be > MIN; each election timeout is randomly drawn fresh from `[MIN, MAX]` every time the timer resets |
| `HEARTBEAT_INTERVAL_MS` | 50 | must stay well below `ELECTION_TIMEOUT_MIN_MS`, or followers will spuriously time out between heartbeats |

- Quorum size is `floor(N/2) + 1` for a cluster of `N` nodes — implement this as a small helper (`quorumSize(n int) int`), not a hardcoded constant, since the scalability tests later (Week 6) run with 3, 5, and 7 nodes.

---

## 3. `RequestVote` and election logic

**Vote-granting rule** (implement exactly this, in this order — both conditions must hold):
1. The voter has **not already voted for a different candidate** in the candidate's term (`votedFor` is empty, or already equals this candidate, for that specific term).
2. The candidate's log is **at least as up-to-date** as the voter's own log: compare `lastLogTerm` first (higher wins outright); only if equal, compare `lastLogIndex` (higher-or-equal wins).

If either condition fails: reject (`voteGranted = false`) and return the voter's own current term (this lets a stale candidate discover it's behind and step down).

If the vote is granted: persist the updated `votedFor` (via `SaveState`), **and reset the voter's own election timer at that same moment** — without this, a follower that just voted for someone else can still time out moments later and start a redundant competing election, needlessly increasing the split-vote rate.

**Candidate behavior:**
- On election timeout (no heartbeat received): increment `currentTerm`, vote for self, persist both, reset the election timer, send `RequestVote` to all peers in parallel.
- Become Leader immediately upon receiving votes from a quorum (self-vote counts).
- If the election times out with no quorum (split vote), start a new election with an incremented term and a freshly-randomized timeout — do not reuse the same timeout value.

**Universal higher-term rule — implement this as a single shared helper, called from every RPC handler and every RPC response path, not duplicated per role:**

> If a node — in **any** role, Follower, Candidate, or Leader — observes a term higher than its own `currentTerm`, in **any** incoming RPC request or **any** RPC reply it receives back, it must immediately: update `currentTerm` to that higher value, persist it, clear `votedFor`, and revert to `Follower` — before doing anything else, including before finishing whatever action was in progress.

This is easy to implement correctly for the Candidate case alone (it's the "obvious" one) and easy to forget for an active Leader — a Leader that keeps sending heartbeats and accepting writes after a newer election has already happened elsewhere is exactly the failure mode this rule exists to prevent. Write one test specifically for this: a Leader receives an `AppendEntries` or `RequestVote` reply carrying a higher term, and must be observed reverting to Follower before any subsequent action.

---

## 4. `AppendEntries`: heartbeat, replication, and the commit rule

**Leader side:**
1. On a write request (from the in-process test harness in this phase — the Client Proxy doesn't exist until Week 4): append the entry to the local log first (write-ahead, on disk, via `AppendLogEntries`), *before* replicating.
2. Replicate to all followers via `AppendEntries`.
3. Advance `commitIndex` **only when both**: (a) a quorum of nodes (leader included) have acknowledged the entry, **and** (b) the entry was appended during the leader's **own current term**. Never commit an older-term entry directly on the basis of quorum replication alone — such entries are only committed *indirectly*, once a later entry from the leader's current term is itself committed (which implicitly commits everything before it). This is the single most commonly-omitted safety rule in hand-written Raft implementations (it corresponds to the "Figure 8" scenario in the original Raft paper) and the one most worth a dedicated test: simulate a leader crash immediately after replicating (but not committing) an entry, elect a new leader, and confirm the old entry is not silently exposed as committed until covered by a same-leader-term entry.
4. Apply to the local state machine only *after* commit — never speculatively.
5. Send periodic heartbeats (empty `AppendEntries`, no `entries[]`) at `HEARTBEAT_INTERVAL_MS` to suppress spurious follower elections.

**Follower side:**
1. On receiving `AppendEntries`, check log consistency using `prevLogIndex`/`prevLogTerm` against the follower's own log **before** appending anything.
2. If inconsistent, reject and return `conflictIndex`/`conflictTerm` (see §5) instead of just `success = false`.
3. If consistent, append the new entries and **persist + fsync before acknowledging** — an in-memory-only append does not count as durable, and the leader must never receive an ack for something that isn't safely on disk yet.
4. Apply newly-committed entries to the local state machine using the `leaderCommit` field carried in the *next* `AppendEntries` call (do not apply speculatively ahead of what the leader has confirmed).
5. Reset the election timer on every valid `AppendEntries` from the current leader (heartbeat or not).

---

## 5. Log repair after a rejected `AppendEntries` (`nextIndex` / `matchIndex`)

Each leader keeps, in memory only, `nextIndex[]` and `matchIndex[]` per follower (reset on becoming leader: `nextIndex` = leader's last log index + 1 for all followers, `matchIndex` = 0 for all followers).

- On a **successful** `AppendEntries`: set `matchIndex[follower]` to the index of the last entry sent, and `nextIndex[follower]` to that index + 1.
- On a **rejected** `AppendEntries`: do **not** simply decrement `nextIndex` by one and retry — this converges too slowly whenever many entries conflict. Instead, use the `conflictIndex`/`conflictTerm` returned by the follower to jump `nextIndex` directly to the first entry of the conflicting term, skipping the rest of that term's entries in a single round-trip. If the follower doesn't have `conflictTerm` in its own log at all, jump straight to `conflictIndex`.

Write a dedicated test that forces a multi-entry log divergence (e.g. a follower that missed several terms' worth of entries while "partitioned" in the in-process harness) and confirms the leader converges the follower's log within a small, bounded number of RPC round-trips — not one round-trip per conflicting entry.

---

## 6. Testing strategy — in-process before gRPC (do not skip this order)

**Step A — pure unit tests, no networking at all:** cover the `persistence` package and the log/state-machine data structures in isolation, including the crash-truncation cases mentioned in §1.

**Step B — in-process fake transport:** build a small test harness in `internal/raft` (or a dedicated `internal/raft/harness` test-only package) that runs N `Node` instances as goroutines in the same test binary, connected via Go channels standing in for RPC calls, with **deterministic, controllable message delivery** (the harness should be able to pause, drop, or delay individual messages on command — this is what makes leader-crash and partition scenarios cheap to simulate here, versus expensive to simulate later via Docker). Use this harness to validate, before touching gRPC at all:
- Normal leader election with no faults.
- Heartbeats correctly suppress follower elections under normal operation.
- Log replication and quorum commit with one follower artificially unresponsive.
- Leader crash (stop delivering its messages) followed by re-election, with the commit-safety test from §4 (point 3) and the vote-granting safety test from §3.
- Log repair (§5) after simulating a follower that missed a batch of entries.

**Step C — real gRPC, only after Step B passes reliably:** wire the same `Node` core to the real `pkg/pb` gRPC bindings from Week 1, running as separate OS processes (or containers, using the Week 1 `docker-compose.yml`). Re-run a smaller version of the Step B scenarios against real processes — this is confirmation, not primary debugging, since by this point protocol bugs should already have been caught cheaply in Step B.

---

## 7. Explicit non-goals for this phase (do not implement yet)

- **No snapshotting** — no `InstallSnapshot`-equivalent, no log compaction, no Snapshot & Backup service integration. That's Week 5. If the in-process test harness needs a long log for a repair test, just let it grow; don't build truncation logic here.
- **No Client Proxy / REST layer** — Week 4. The in-process test harness in §6 stands in for client writes during this phase.
- **No Circuit Breaker wiring** — Week 4, and only relevant once the Client Proxy exists.
- **No Service Discovery integration** — Week 4. Peer addresses for `RequestVote`/`AppendEntries` can be static config (a hardcoded list or a simple env var/config file) for now; this is in fact the permanent design for consensus-node peer addressing per the specification, not a temporary shortcut — Service Discovery is deliberately kept out of the consensus layer's critical path.

---

## 8. Deliverables checklist for Week 2–3

- [ ] `internal/raft/persistence` package: `LoadState`/`SaveState`/`AppendLogEntries`/`ReadAllLogEntries`/`TruncateLogFrom`, each with unit tests, including at least one crash/partial-write test.
- [ ] `internal/raft` core: `Node` struct, role transitions, election timer + heartbeat timer wired to configurable env vars.
- [ ] `RequestVote` handler implementing the two-condition vote-granting rule, with the voter's-own-timer-reset behavior.
- [ ] `AppendEntries` handler implementing heartbeat, replication, the term-boundary commit rule, and follower-side consistency checking with `conflictIndex`/`conflictTerm`.
- [ ] Leader-side `nextIndex`/`matchIndex` tracking with fast log repair.
- [ ] The universal higher-term-revert-to-follower rule implemented once, shared across all RPC paths, with a dedicated test proving it fires for an active Leader (not just a Candidate).
- [ ] In-process fake-transport test harness covering all scenarios listed in §6 Step B, passing reliably and repeatably (run each fault-injection test multiple times if timing-sensitive, to catch flakiness early).
- [ ] Real gRPC wiring of the same core logic, confirmed working between separate processes/containers using the Week 1 `docker-compose.yml`.
- [ ] `go.mod`/proto regenerated and committed if §0 required any additions.
- [ ] `README.md`/`Makefile` updated with a separate target for the in-process test suite (e.g. `make test-raft`) distinct from any full-stack/integration target, so the fast in-process tests can be run on every change without spinning up containers.
