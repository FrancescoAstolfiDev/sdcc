# b5-kvstore

Replicated, strongly consistent key-value store in Go (Raft-lite consensus,
gRPC/Protobuf internal RPC, REST/JSON client boundary). See
`../documentation/Progetto_B5_Full_Technical_Spec_EN.pdf` for the full spec.


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


### Config (env vars)

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

### Scalability testing (`loadgen`)

Once a scenario is up (`client-proxy` published on `:8080` regardless of
node count — §11.2), drive it with `cmd/loadgen` to measure throughput/
latency at that cluster size:

```sh
# 1. bring the scenario up (from deployments/)
cd deployments
docker compose --env-file .env.7nodes --profile 7nodes up -d --build

# 2. run the load generator (from the repo root, b5-kvstore/)
cd ..
go run ./cmd/loadgen -target http://localhost:8080 -requests 1000 -concurrency 20 -read-pct 70
```

`-target` is the client-proxy base URL, `-requests` the total request
count, `-concurrency` the number of parallel workers, `-read-pct` the
percentage of requests that are reads (GET) vs writes (POST), 0-100
(defaults: `http://localhost:8080`, 1000, 10, 80 — see
`cmd/loadgen/main.go`). Repeat against the `.env.3nodes`/`.env.5nodes`/
`.env.7nodes` scenarios to compare scalability across cluster sizes (Week 6).

### Fault-tolerance testing (`run_fault_test.sh`)

`experiments/fault-tolerance/run_fault_test.sh` kills the current Raft
leader against an already-running cluster and measures detection,
election, and recovery time (spec §12.2), appending a row to
`experiments/fault-tolerance/results.csv`.

```sh
# 1. bring the scenario up (from deployments/)
cd deployments
docker compose --env-file .env.7nodes --profile 7nodes up -d --build

# 2. run the fault test (from experiments/fault-tolerance/)
cd ../experiments/fault-tolerance
TERM_THRESHOLD=50 bash run_fault_test.sh .env.7nodes 7nodes 10
```

The three arguments are `[env_file] [profile] [expected_up_services]` and
must match how the stack was actually started — `expected_up_services` is
6 for `.env.3nodes`, 8 for `.env.5nodes`, 10 for `.env.7nodes` (see the
script header for why this is checked explicitly rather than assumed).
`TERM_THRESHOLD` (env override, default 10) is the Raft term above which
the cluster is considered stale/leftover from a previous run; raise it if
you're re-running fault tests repeatedly against the same long-lived
cluster and hitting false "stale term" aborts.

On Windows, run it through Git Bash rather than PowerShell directly, e.g.
from a PowerShell prompt:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -c "cd /c/dev/sdcc/b5-kvstore/experiments/fault-tolerance && TERM_THRESHOLD=50 bash run_fault_test.sh .env.7nodes 7nodes 10"
```

(adjust the `/c/dev/sdcc/...` path to wherever the repo is checked out;
the script itself requires only `docker compose`, `curl`, `awk`, `sed`,
and GNU `date`, and avoids PCRE `grep -P` for Git Bash compatibility.)
