# Instructions for Claude Code — Week 4: Client Proxy, Service Discovery, Circuit Breaker

## Task Overview

This picks up from Week 2–3 (`md-week2.md`): the Raft-lite core in `internal/raft/` is assumed to be working, unit-tested in-process, and wired to real gRPC between separate processes/containers. Week 4 builds the two remaining supporting pieces the Client Proxy depends on — Service Discovery and the Circuit Breaker — and the Client Proxy itself: REST↔gRPC translation, request routing, redirect-following, and the Read-Index protocol for follower reads.

**Important scope note relative to the specification PDF you may have on disk:** the PDF's §4.2 frames Read-Index as a protocol the project "targets... from an early milestone" with an explicit safe fallback if it's incomplete. For this phase, **Read-Index is not optional and not deferrable** — it is a hard requirement of Week 4's deliverables, not a stretch goal. The Leader-only read path still exists, but only as a **runtime degradation fallback** for transient failures (e.g. a follower can't reach the leader to confirm a read index right now), never as a substitute for not having implemented Read-Index at all. Treat every "optional" or "if time allows" phrasing about Read-Index elsewhere in the spec text as superseded by this instruction.

---

## 0. Prerequisites check & proto extension — do this before anything else

- [ ] Confirm `internal/raft` builds, passes its in-process test suite, and the real-gRPC integration path from Week 2–3 still works via the Week 1 `docker-compose.yml`.
- [ ] Confirm `api/proto` already defines the `Discovery` service (`GetClusterView`, `NodeInfo`, `ClusterView` — spec §9.3). If it's still a placeholder from Week 1, implement it fully now.
- [ ] **Proto extension required:** the wire contract from Week 1/§9.1 has no RPC for the Read-Index handshake between a follower and the leader. Add the following to `consensus.proto` and regenerate `pkg/pb`:

```protobuf
service Consensus {
  rpc RequestVote (RequestVoteRequest) returns (RequestVoteReply);
  rpc AppendEntries (AppendEntriesRequest) returns (AppendEntriesReply);
  rpc GetStatus (GetStatusRequest) returns (GetStatusReply);
  rpc RequestReadIndex (ReadIndexRequest) returns (ReadIndexReply); // new
}

message ReadIndexRequest {
  string followerId = 1;
}
message ReadIndexReply {
  bool   ok             = 1; // false if this node is not (or no longer) leader
  uint64 readIndex      = 2; // the leader's confirmed commit index, valid only if ok = true
  string redirectLeader = 3; // set when ok = false and the leader address is known
}
```

- [ ] Confirm `KVService` (§9.2: `Get`/`Put`/`Update`/`Delete`) is already generated and callable from `internal/proxy`.

---

## 1. Service Discovery — pull-based registry (`internal/discovery`, `cmd/discovery`)

This is a **pull-based** design: Service Discovery polls nodes, nodes never register themselves. Do not build a push/self-registration mechanism, even though it may have been the original Week 1 sketch — it would conflict with §10.6's requirement that consensus-node peer addressing never depends on any runtime service.

- **Bootstrap:** static peer list, from the same shared config source used to bootstrap consensus-node peer addressing (§10.6) — one `.env`/config file mounted identically into the Discovery container and every consensus-node container, per the Week 1–3 convention. No dynamic node registration, ever.
- **Polling loop:** every node in the static list is polled via `GetStatus()` (§3.5/§9.1) on a fixed interval — default **2s**, configurable via env var (`DISCOVERY_POLL_INTERVAL_MS`), consistent with the timing table in the specification.
- **In-memory state:** a `ClusterView{leaderAddress, followers[], allNodes[]}`, rebuilt from the latest poll round. No persistence needed — if Discovery restarts, it rebuilds its view from scratch within one polling interval.
- **A node that doesn't respond** to `GetStatus()` within the poll's RPC timeout is dropped from `allNodes`/`followers` for that round, not marked with a sticky "down" flag — the next successful poll re-adds it automatically. Keep this stateless-per-round; don't build a health-tracking state machine here, that's the Circuit Breaker's job (§5 below), not Discovery's.
- **Exposed API:** `GetClusterView()` (§9.3), called by the Client Proxy and, later, by the Snapshot & Backup service (Week 5). Discovery itself never touches the KV data path.

---

## 2. Client Proxy — external REST contract (`internal/proxy`, `cmd/client-proxy`)

Resource-oriented endpoints, one path per key, HTTP verb encodes the operation:

| Client verb | HTTP method | Endpoint | Internal RPC |
|---|---|---|---|
| push | `POST` | `/v1/kv/{key}` | `Put` |
| update | `PUT` | `/v1/kv/{key}` | `Update` |
| get | `GET` | `/v1/kv/{key}` | `Get` |
| delete | `DELETE` | `/v1/kv/{key}` | `Delete` |
| liveness | `GET` | `/healthz` | — (proxy-only liveness, not cluster status) |

**Request/response bodies:**

```
POST /v1/kv/{key}                 PUT /v1/kv/{key}
  Request:  {"value": "..."}        Request:  {"value": "..."}
  201 Created                       200 OK
  {"key": "...", "commitIndex": N}  {"key": "...", "commitIndex": N}

GET /v1/kv/{key}
  200 OK   {"key": "...", "value": "..."}
  404      {"error": {"code": "not_found", "message": "key not found"}}

DELETE /v1/kv/{key}
  204 No Content
```

- Every write response (Put/Update/Delete) carries header `X-Commit-Index: <uint64>` — pulled from `WriteReply.commitIndex` (§9.2, already marked "for testing/observability"). Keep it out of the JSON body so the Week 6 load-generator scripts can read it with a header lookup instead of a JSON parse.
- **Uniform error envelope** for all non-2xx responses: `{"error": {"code": "...", "message": "..."}}`.

| Status | code | When |
|---|---|---|
| 400 | `bad_request` | malformed JSON, missing `value` on push/update |
| 404 | `not_found` | GET on a nonexistent key |
| 503 | `unavailable` | redirect-hop budget exhausted (§3), or every circuit breaker for known nodes is open — include `Retry-After` header |
| 500 | `internal_error` | unhandled gRPC error / translation failure |

---

## 3. Client Proxy — routing, redirect-following, and cache invalidation

- **Cluster map cache:** the proxy caches the last `ClusterView` fetched from Service Discovery (§1), refreshed on a schedule and invalidated early on: (a) a circuit breaker trip for a cached node, (b) a `redirectLeader` received from any node.
- **Write routing:** always to the cached Leader.
- **Read routing (default, mandatory — see the scope note above):** to a follower, via the Read-Index protocol (§4 below). Follower selection policy: round-robin across the followers currently listed in the cached `ClusterView`. If the cache currently lists zero followers (e.g. right after a Discovery refresh during an election), route the Get directly to the Leader instead of blocking or erroring.
- **Redirect-following (applies to writes and to the Leader-only fallback path for reads — this is separate from, and simpler than, the Read-Index-specific fallback in §4):** if the contacted node replies with a non-empty `redirectLeader` (`WriteReply`/`GetReply`, §9.2), the proxy follows it internally and retries against that address — **never** surfaced to the external client as an HTTP redirect (3xx); this stays consistent with the specification's §2 MUST NOT on clients ever reaching a consensus node directly. Cap internal hops at **3**; if the budget is exhausted, return `503 unavailable` (§2 above) and force a `ClusterView` refresh from Service Discovery before the next request.

---

## 4. Read-Index protocol (mandatory) — proxy + consensus-node follower + leader

This spans two components: the proxy's routing decision (§3 above) and the actual handshake logic inside `internal/raft`, since it touches `lastApplied` and quorum confirmation directly.

**Follower side** (triggered when the proxy routes a `Get` here):
1. Call `RequestReadIndex` (§0) on the currently-known leader.
2. If the reply has `ok = false`: the follower's notion of who's leader is stale. **Do not retry against another follower** — fall back immediately by returning a redirect-equivalent to the proxy so it retries the same `Get` directly against the Leader (a single hop, per the fallback rule below). This is the deliberate design choice discussed for this phase: Read-Index failures degrade to Leader reads, they don't bounce across followers.
3. If `ok = true`: wait until local `lastApplied >= readIndex` (§3.1's field, no busy-wait — a condition variable or polling channel signaled by the apply loop is fine).
4. Once satisfied, read the local state machine and reply. This is linearizable by construction (spec §4.2, points 12–15).
5. If the wait in step 3 can't be satisfied because the needed entries were already compacted off this follower's log (only relevant once Week 5's Snapshot & Backup service exists — see §7 non-goals below), this currently has no working fallback within Week 4's scope: document it as a known limitation, don't attempt to build the §5.3 catch-up interaction yet.

**Leader side** (handling `RequestReadIndex`):
1. If not currently leader: reply `ok = false`, with `redirectLeader` set if known.
2. Otherwise, confirm current leadership is still valid by completing a **fresh round of heartbeats acknowledged by a quorum** before answering (spec §4.2, point 13) — this is what prevents a partitioned former leader from handing out a stale read index. Reuse the existing heartbeat/AppendEntries machinery from Week 2–3; do not build a second heartbeat mechanism just for this.
3. Reply `ok = true` with the current `commitIndex` as `readIndex`.

**Runtime fallback rule (distinct from §3's generic redirect-following):** if the follower's `RequestReadIndex` call itself times out or errors (not just `ok = false`, but a transport failure), the proxy retries that specific `Get` once, directly against the cached Leader, and does not attempt a second follower. This keeps failure handling bounded and simple rather than adding retry-budget logic duplicated from §3.

---

## 5. Circuit Breaker (`internal/circuitbreaker`)

Wrap every outbound proxy→node gRPC call (to any leader or follower address) in a `sony/gobreaker` instance, one breaker per node address, keyed by address and re-created when Service Discovery reports a node's address has changed.

| gobreaker parameter | Value |
|---|---|
| `ReadyToTrip` | trip after 3 consecutive failures |
| `Timeout` (open → half-open) | 2s |
| `MaxRequests` (half-open probes) | 1 |
| `Interval` (closed-state counter reset) | 30s |

- On trip (breaker opens): stop routing to that node immediately, and trigger an out-of-schedule `ClusterView` refresh from Service Discovery rather than waiting for the next periodic refresh.
- If every currently-known node's breaker is open, return `503 unavailable` per §2's error table, with `Retry-After` set to roughly the breaker `Timeout` (2s).
- Breakers are purely a proxy-side concern — no consensus node or Service Discovery instance needs to know breaker state.

---

## 6. Testing strategy

- **Unit tests:** REST↔gRPC translation (request parsing, response/error mapping for every status code in §2's table), independent of any real backend — mock the `KVService` client.
- **Routing/redirect/Read-Index logic:** reuse the Week 2–3 in-process fake-transport harness. Extend it (or build a thin adapter) so the proxy's routing code can run against the in-process `Node` instances instead of real gRPC — this lets you test redirect-following, the Read-Index handshake, and the two distinct fallback paths (§3's generic redirect-following vs. §4's Read-Index-specific single-hop fallback) at unit-test speed, before touching containers.
- **Circuit breaker:** dedicated unit tests simulating exactly 3 consecutive failures (confirm the breaker opens on the 3rd, not the 2nd or 4th), confirm it stays open for the configured `Timeout`, and confirm exactly one probe request is allowed through in half-open state.
- **End-to-end:** once the above pass, bring up the full stack (proxy + N consensus nodes + discovery) via the Week 1 `docker-compose.yml` and exercise the REST API directly (`curl`/integration test) against a real cluster, including killing a follower mid-test to confirm the breaker trips and Discovery's view updates within one polling interval.

---

## 7. Explicit non-goals for this phase (do not implement yet)

- **No Snapshot & Backup service** — Week 5. A follower that's too far behind to satisfy a Read-Index wait (§4, follower-side step 5) has no working catch-up path yet in this phase; that's an accepted, documented gap until Week 5 lands, not a bug to work around now.
- **No EC2 deployment** — Week 6. Continue testing against the local Docker Compose stack.
- **No load generator or evaluation scenarios (§12 scalability/fault-tolerance measurements)** — Week 6, once the full system (including snapshotting) exists.
- **No dynamic node registration for Service Discovery** — static bootstrap config only, per §1 above; this is the permanent design, not a temporary simplification.

---

## 8. Deliverables checklist for Week 4

- [ ] `consensus.proto` extended with `RequestReadIndex`/`ReadIndexRequest`/`ReadIndexReply`; `pkg/pb` regenerated.
- [ ] `internal/discovery`: pull-based polling loop (default 2s, configurable), static bootstrap config shared with consensus-node peer addressing, in-memory `ClusterView`, `GetClusterView` gRPC handler.
- [ ] `internal/proxy`: REST↔gRPC translation for all four verbs plus `/healthz`, matching the endpoint/body/status-code contract in §2 exactly.
- [ ] Routing logic: writes to Leader; reads to a round-robin follower via Read-Index by default, with the two distinct fallback paths from §3 and §4 both implemented and tested separately (they are not the same code path).
- [ ] Read-Index handshake implemented on both the follower side and the leader side inside `internal/raft`, reusing the existing heartbeat/quorum machinery rather than duplicating it.
- [ ] `internal/circuitbreaker`: one `gobreaker` instance per known node address, configured exactly per §5's table, wired around every proxy→node call.
- [ ] Unit tests: REST translation, routing/redirect/Read-Index logic against the in-process harness, circuit-breaker threshold behavior (3-failure trip, 2s open, 1 half-open probe).
- [ ] End-to-end test via `docker-compose.yml`: full stack up, REST calls against a live cluster, breaker trip + Discovery view update confirmed after killing a follower.
- [ ] `README.md`/`Makefile` updated with a target for this phase's test suite, separate from Week 2–3's `make test-raft`.
