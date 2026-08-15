PROTO_DIR := api/proto
PB_DIR := pkg/pb
# make up/down/logs always target the default 3-node scenario (consensus-node-1..3,
# no profile). For 5/7-node scenarios, run docker compose directly per README.md.
COMPOSE := docker compose -f deployments/docker-compose.yml --env-file deployments/.env.3nodes

.PHONY: proto tidy build test-raft test-proxy test-snapshot test up down logs clean

proto:
	protoc \
		--go_out=$(PB_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(PB_DIR) --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/*.proto

tidy:
	go mod tidy

build: tidy
	go build ./...

# Fast in-process Raft-lite test suite (persistence unit tests + the
# in-process fake-transport harness scenarios from md-week2 §6 Step B). No
# networking, no containers — safe to run on every change.
test-raft:
	go test ./internal/raft/...

# Week 4 test suite (md-week4 §6): REST<->gRPC translation and
# routing/redirect/Read-Index logic (internal/proxy, the latter run against
# the in-process raft harness via internal/proxy's adapter — no real gRPC),
# Service Discovery's polling/aggregation logic, the state machine's apply
# logic, and the circuit breaker's trip/timeout/half-open-probe thresholds.
# No networking, no containers — kept separate from test-raft so either
# suite can be run on its own.
test-proxy:
	go test ./internal/proxy/... ./internal/discovery/... ./internal/circuitbreaker/... ./internal/statemachine/...

# Week 5 test suite (md-week5 §5): the Snapshot & Backup service's
# compaction threshold/concurrency-guard/leader-re-verification logic
# (internal/snapshot, against fake Discovery/SnapshotTransfer clients — no
# real gRPC) and the shared snapshot file format's encode/decode round-trip
# (internal/snapshotfile). The catch-up/local-compaction side of §5.3 lives
# in internal/raft and its harness, so it's already exercised by test-raft,
# not duplicated here.
test-snapshot:
	go test ./internal/snapshot/... ./internal/snapshotfile/...

# Full fast test suite: everything above, no containers.
test: test-raft test-proxy test-snapshot

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

clean:
	rm -f $(PB_DIR)/*.pb.go
