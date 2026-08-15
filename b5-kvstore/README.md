# b5-kvstore

Replicated, strongly consistent key-value store in Go (Raft-lite consensus,
gRPC/Protobuf internal RPC, REST/JSON client boundary). See
`Progetto_B5_Full_Technical_Spec_EN.pdf` for the full spec.

## Status: Week 5 — Snapshot & Backup Service

Week 1 (done): Go module/layout (§13), Protocol Buffer contracts for all
four services, executable stubs for all four binaries, `docker-compose.yml`
for network verification.

Week 2-3 (done), all under `internal/raft/`:
- `internal/raft/persistence`: on-disk `currentTerm`/`votedFor`
  (`state.json`, temp-then-rename) and replicated log (`log.dat`,
  length-prefixed `gob` records, fsync'd before ack, crash/torn-write
  recovery on load). Fully unit-tested in isolation, no networking.
- `internal/raft`: `Node` core — randomized-timeout election
  (`RequestVote`), heartbeat + log replication (`AppendEntries`) with the
  term-boundary commit rule, fast log repair via `conflictIndex`/
  `conflictTerm`, and the universal higher-term-reverts-to-follower rule
  shared by every RPC path.
- `internal/raft/harness`: in-process fake transport (`Network`/`Cluster`)
  with controllable per-message drop/delay, used to validate election,
  replication, leader-crash/re-election commit-safety, and log repair
  cheaply before touching real gRPC.
- `internal/raft/grpctransport` + `cmd/consensus-node`: the same `Node`
  core wired to real `pkg/pb` gRPC, registered as `pb.ConsensusServer`.

Week 4 (done), building on top of Week 2-3:
- `internal/statemachine`: the KV map + apply logic (§3.1's `stateMachine`
  field — a gap left open since Week 2-3, filled in now as a Week 4
  prerequisite) and `Server`, the `pb.KVServiceServer` implementation
  registered on every `consensus-node`, wired to `raft.Node.ProposeSync`
  for writes and `raft.Node.FollowerLinearizableRead` for follower reads.
- `internal/raft/readindex.go`: the Read-Index handshake — leader side
  (`RequestReadIndex`, confirming leadership via a fresh quorum-acked
  heartbeat round before answering) and follower side
  (`FollowerLinearizableRead`, waiting for `lastApplied` to catch up via a
  no-busy-wait channel). Mandatory for this phase per `md-week4.md`'s scope
  override (§4.2 of the PDF spec treats it as optional/deferrable; this
  phase does not).
- `internal/discovery` + `cmd/discovery`: pull-based polling registry
  (`GetStatus` on a fixed interval, default 2s), in-memory `ClusterView`,
  no persistence, no dynamic node registration.
- `internal/circuitbreaker`: one `sony/gobreaker` instance per known node
  address (trip after 3 consecutive failures, 2s open timeout, 1 half-open
  probe, 30s closed-state counter reset), wrapping every proxy→node call.
- `internal/proxy` + `cmd/client-proxy`: REST↔gRPC translation
  (`/v1/kv/{key}` + `/healthz`), write routing to the cached Leader with
  redirect-following (capped at 3 hops), read routing round-robin across
  cached followers via Read-Index by default, with the Read-Index-specific
  single-hop-to-leader fallback kept as a distinct code path from the
  generic redirect-following one.

Week 4-mid (done): pure observability addition, no logic changes — structured
`component[id]: EVENT_NAME key=value` log lines at existing election/
discovery/proxy state-transition points (`ELECTION_START`, `BECAME_LEADER`,
`VOTE_GRANTED`/`VOTE_REJECTED`, `STEPPED_DOWN`, `VIEW_UPDATED`,
`NODE_STATUS_CHANGED`, `LEADER_CONTACT_FAILED`, `LEADER_UPDATED`,
`REDIRECT_FOLLOWED`, `REQUEST_FAILED_EXHAUSTED`).

Week 5 (done), the third and final node type — a **two-directional**
integration, not a one-way client:
- A shared gRPC dialing fix, reused from Week 4's manual fault-injection
  testing: `internal/grpcutil.DialPassthrough` dials every peer as
  `passthrough:///<host>:<port>` instead of a bare target, so every
  reconnection attempt (including automatic backoff retries) re-resolves via
  a fresh `net.Dial` against Docker's embedded DNS instead of a cached gRPC
  resolver. `internal/raft/grpctransport`, the new
  `internal/raft/snapshotcatalogclient`, and `internal/snapshot`'s own
  `SnapshotTransfer` client all go through it.
- `internal/raft`: the consensus-side of both new directions —
  `pb.SnapshotTransferServer` (`GetLogStatus`/`StreamLogRange`/
  `ConfirmCompaction`, called by the Snapshot & Backup service against the
  leader or any node) and the periodic catch-up/local-compaction loop
  (`SNAPSHOT_POLL_INTERVAL_MS`, querying the Snapshot Catalog API). The
  positional log (`log[i]` has `Index == logOffset + i + 1`) now supports a
  compacted prefix represented by a single anchor placeholder entry; a
  node's own restart recovery (§3.6) loads the latest local
  `snapshot-<index>.dat` first, seeding `lastApplied`/the state machine
  before replaying `log.dat` from there.
- `internal/snapshot` + `cmd/snapshot-backup`: the polling/compaction cycle
  (occupancy vs. leader, `COMPACTION_THRESHOLD_PERCENT`/
  `COMPACTION_CONCURRENCY_GUARD_PERCENT` boundaries, pre-`ConfirmCompaction`
  leader re-verification via Service Discovery) and the `SnapshotCatalog`
  server (`GetLatestSnapshotInfo`/`FetchSnapshot`) that every consensus node
  polls independently — the system's sole snapshot-delivery path; the leader
  never pushes snapshots to followers directly.
- `internal/snapshotfile`: the shared `gob`-encoded snapshot format (state
  machine map + `lastIncludedIndex`/`lastIncludedTerm`), imported by both the
  writer (`internal/snapshot`) and the reader (`internal/raft`) so the two
  sides can't silently drift apart.
- `internal/statemachine`: `Snapshot()`/`Restore()` added to `KV`, for
  building and adopting a compaction checkpoint.

Explicit non-goals for this phase (see `md-week5.md` §6): EC2 deployment and
the load generator/evaluation scenarios (Week 6, now that the full
three-node-type system exists); Pre-Vote (still an accepted, documented
limitation — the isolated-node disruptive-term-climb behavior confirmed
during Week 4-mid); no further changes to the Week 4 transport fix itself
beyond reusing it in the two new clients. Peer addressing for consensus
nodes remains static config (`PEERS` env var), unchanged from Week 2-3.

## Prerequisites

- Go 1.22+ (module currently built/tested with `go1.23.6`)
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for `make proto`)
- Docker / Docker Compose (for `make up`; not available in every dev
  environment — `cmd/consensus-node` can also be run directly as plain OS
  processes for local testing, see below)

## Usage

```sh
make proto      # generate pkg/pb from api/proto/*.proto
make build      # go mod tidy && go build ./...
make test-raft     # fast in-process Raft-lite suite (persistence + harness), no containers
make test-proxy    # fast Week 4 suite (proxy, discovery, circuitbreaker, statemachine), no containers
make test-snapshot # fast Week 5 suite (snapshot compaction/threshold logic, snapshotfile round-trip), no containers
make test          # everything above
make up         # docker compose up --build (verifies inter-container networking)
make down
```

To run the full stack as plain local processes (e.g. when Docker isn't
available), set `PEERS` to the full shared "host:port" list (including
self — each consensus node filters itself out by matching its own port),
plus per-service ports/IDs, then run each built binary once:

```sh
go build -o /tmp/consensus-node ./cmd/consensus-node
go build -o /tmp/discovery ./cmd/discovery
go build -o /tmp/client-proxy ./cmd/client-proxy
go build -o /tmp/snapshot-backup ./cmd/snapshot-backup

PEERS=127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003

SNAPSHOT_PORT=18600 DATA_DIR=/tmp/b5-data/snapshot-backup DISCOVERY_ADDR=127.0.0.1:18500 /tmp/snapshot-backup

NODE_ID=node-1 CONSENSUS_PORT=19001 DATA_DIR=/tmp/b5-data PEERS=$PEERS SNAPSHOT_ADDR=127.0.0.1:18600 /tmp/consensus-node
# repeat for node-2/19002 and node-3/19003

DISCOVERY_PORT=18500 PEERS=$PEERS /tmp/discovery

HTTP_PORT=18080 DISCOVERY_ADDR=127.0.0.1:18500 /tmp/client-proxy
```

`client-proxy` is the only service published to the host, on `:8080`
(REST: `POST/PUT/GET/DELETE /v1/kv/{key}`, `GET /healthz`). All other
services are reachable only on the internal `b5-net` Docker network, per
§11.2.

### Config (env vars, Week 4 additions)

| Var | Default | Used by |
|---|---|---|
| `DISCOVERY_POLL_INTERVAL_MS` | 2000 | `discovery` |
| `PROXY_REFRESH_INTERVAL_MS` | 2000 | `client-proxy` |
| `PROXY_RPC_TIMEOUT_MS` | 2000 | `client-proxy` |
| `DISCOVERY_ADDR` | — (required) | `client-proxy`, `snapshot-backup` |

### Config (env vars, Week 5 additions)

| Var | Default | Used by |
|---|---|---|
| `SNAPSHOT_ADDR` | — (required) | `consensus-node` (its client toward `snapshot-backup`'s Snapshot Catalog API) |
| `SNAPSHOT_POLL_INTERVAL_MS` | 5000 | `consensus-node` (§5.3 catch-up/local-compaction loop) |
| `LOG_CAPACITY_ENTRIES` | 1000 | `consensus-node` (denominator for `GetLogStatus`'s `occupancyPercent` — the spec leaves this unspecified beyond "occupancy/size", so it's made an explicit, configurable per-node capacity) |
| `SNAPSHOT_BACKUP_POLL_INTERVAL_MS` | 5000 | `snapshot-backup` (leader log-occupancy poll, §1) |
| `COMPACTION_THRESHOLD_PERCENT` | 30 | `snapshot-backup` |
| `COMPACTION_CONCURRENCY_GUARD_PERCENT` | 90 | `snapshot-backup` |
| `SNAPSHOT_BACKUP_RPC_TIMEOUT_MS` | 2000 | `snapshot-backup` |

## Deployment scenarios: 3 / 5 / 7 consensus nodes

`deployments/docker-compose.yml` defines all seven `consensus-node-1..7`
services up front (Week 6 scalability testing needs 3, 5, and 7-node
clusters — see `quorumSize` in `internal/raft/node.go`). Which ones actually
start is controlled by [Compose
profiles](https://docs.docker.com/compose/how-tos/profiles/), not by editing
the compose file:

- `consensus-node-1..3` have no `profiles:` key, so they always start.
- `consensus-node-4/5` are tagged `profiles: ["5nodes", "7nodes"]`.
- `consensus-node-6/7` are tagged `profiles: ["7nodes"]`.

The cluster-wide `PEERS` list (and every other shared env var) is supplied
via `--env-file`, not via a fixed `.env` — each scenario has its own file:
`deployments/.env.3nodes`, `.env.5nodes`, `.env.7nodes`. Only the `PEERS`
value differs between them; ports/timeouts/etc. are identical.

### First-time setup

The `.env.<scenario>` files are gitignored (they're the per-scenario
runtime config, kept out of version control per deployment). Before the
first deploy of a given scenario, copy its tracked `.example` template:

```sh
cd deployments
cp .env.3nodes.example .env.3nodes   # only the scenario(s) you plan to run
cp .env.5nodes.example .env.5nodes
cp .env.7nodes.example .env.7nodes
```

### Launching a scenario

```sh
# 3 nodes (default — no profile flag needed)
docker compose --env-file .env.3nodes up -d --build

# 5 nodes
docker compose --env-file .env.5nodes --profile 5nodes up -d --build

# 7 nodes
docker compose --env-file .env.7nodes --profile 7nodes up -d --build
```

Run these from `deployments/` (or add `-f deployments/docker-compose.yml`
from the repo root). Tear down with the matching `--env-file`/`--profile`
pair, e.g. `docker compose --env-file .env.5nodes --profile 5nodes down -v`.

`make up`/`make down`/`make logs` always target the default 3-node scenario
(`deployments/.env.3nodes`); use the `docker compose` commands above
directly for the 5/7-node scenarios.

To confirm which services a given scenario actually starts (e.g. that
`--profile 5nodes` brings up exactly `consensus-node-1..5` and not
`-6`/`-7`), inspect the resolved config instead of starting containers:

```sh
docker compose --env-file .env.5nodes --profile 5nodes config --services
```
