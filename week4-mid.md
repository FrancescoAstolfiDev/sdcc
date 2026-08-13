# Instructions for Claude Code — Week 4-mid: Observability Logging (Election, Discovery, Proxy)

## Task Overview

During manual end-to-end verification of Week 4's deliverables (`md-week4.md`), three observability gaps made it impossible to confirm what the system was actually doing from logs alone — every conclusion had to be reconstructed indirectly from `applied index=N term=T` lines, which was slow and, in one case (a node isolated after two consecutive kills, its term climbing into the thousands with no corresponding leader elected), nearly indistinguishable from a real bug without manually tracing timestamps across three log streams.

This phase is a **pure observability addition** before starting Week 5: add structured log lines at existing state-transition points in `internal/raft`, `internal/discovery`, and `internal/proxy`. **Do not modify any election, voting, commit, routing, or discovery logic** — every log line goes at a point where a decision has already been made by existing code; this phase only makes that decision visible.

Week 5 (Snapshot & Backup) depends on this: it re-verifies leadership before every `ConfirmCompaction` (spec §5.1, point 21), and debugging that logic without reliable, direct visibility into leader identity and transitions would repeat the same indirect-reconstruction problem, at a point where the extra moving part (compaction cycles) makes it worse, not better.

---

## 0. Scope guardrails — read before touching any file

- [ ] **No behavior changes.** If implementing a log line requires touching logic (e.g. a code path that doesn't currently distinguish *why* an RPC failed), add the minimum categorization needed to log it accurately — don't refactor beyond that.
- [ ] **No new blocking calls on hot paths.** `AppendEntries`/heartbeat handling and the election timer must not be delayed by logging. Plain `log.Printf` to stdout (matching the existing style already in the codebase, e.g. `applied index=...`) is fine — don't introduce buffered channels, async loggers, or structured-logging libraries for this; that's out of scope.
- [ ] **Log on state change, not on every poll/heartbeat tick.** Service Discovery polls every 2s (§10.1) and the leader sends heartbeats every 50ms — logging unconditionally on every cycle would flood the log and made this exact problem harder to debug, not easier. Every log point below is a *transition*, not a periodic status dump.
- [ ] Keep the existing log line format/prefix convention (`component[identifier]: EVENT_NAME key=value ...`) consistent with what's already in the codebase, so `Select-String`/`grep` patterns keep working the way they already do for `term=`/`applied index=`.

---

## 1. Election logs (`internal/raft`)

Find the existing role-transition points first:
```bash
grep -rn "role = .*Leader\|role = .*Candidate\|role = .*Follower" internal/raft/
```

Add these five log lines at the corresponding existing transitions — do not add new transitions, only log the ones that already exist:

```go
// Follower's election timeout fires, becomes Candidate (spec §3.4)
log.Printf("consensus-node[%s]: ELECTION_START term=%d (previous_term=%d)", nodeID, newTerm, oldTerm)

// Candidate receives votes from a quorum, becomes Leader (spec §3.4)
log.Printf("consensus-node[%s]: BECAME_LEADER term=%d", nodeID, term)

// This node, as voter, grants a vote (spec §10.3)
log.Printf("consensus-node[%s]: VOTE_GRANTED term=%d candidate=%s", nodeID, term, candidateID)

// This node, as voter, rejects a vote — reason must be one of: "already_voted", "stale_log"
// (the two conditions in §10.3's vote-granting rule), not a generic string
log.Printf("consensus-node[%s]: VOTE_REJECTED term=%d candidate=%s reason=%s", nodeID, term, candidateID, reason)

// Any role reverts to Follower after observing a higher term (spec §3.2 point 7 for an active
// Leader, §3.4 for a Candidate) — this MUST be logged from the single shared helper function
// that implements the universal higher-term rule (md-week2.md §3), not duplicated at each call site
log.Printf("consensus-node[%s]: STEPPED_DOWN term=%d (saw_higher_term=%d from=%s)", nodeID, oldTerm, higherTerm, source)
```

- [ ] Confirm `STEPPED_DOWN` fires from the **one** shared helper (per the universal higher-term rule already established), not copy-pasted into the Leader's and Candidate's code paths separately — if it's duplicated, that's a sign the shared helper isn't actually being reused, which is itself worth flagging back.
- [ ] `source` in `STEPPED_DOWN` should identify the RPC that carried the higher term (e.g. `"AppendEntriesReply"`, `"RequestVote"`) — this is what will let you distinguish, in a future kill scenario, whether a leader stepped down because of a legitimate new leader or because of an isolated node's runaway term (the exact ambiguity that triggered this phase).

---

## 2. Service Discovery logs (`internal/discovery`)

Add these two log lines, only on change between one polling round and the next — compare against the previously-held `ClusterView`, don't log unconditionally every 2s:

```go
// The resolved leader (or the follower set) differs from the previous poll round
log.Printf("discovery: VIEW_UPDATED leader=%s followers=%v (was_leader=%s)", newLeader, followers, oldLeader)

// A specific node's reachability flipped since the last poll round (§1 of md-week4.md:
// dropped-for-one-round nodes are not sticky, so this should fire on both directions)
log.Printf("discovery: NODE_STATUS_CHANGED node=%s reachable=%v", nodeID, reachable)
```

- [ ] `VIEW_UPDATED` must fire even if only the leader changed and the follower set didn't (that's the case that matters most for Week 5's `ConfirmCompaction` re-verification).
- [ ] Do not add persistence or a "last known good" fallback here — Discovery's stateless-per-round design (`md-week4.md` §1) is intentional and out of scope for this phase; just log the transitions the existing polling loop already computes.

---

## 3. Client Proxy logs (`internal/proxy`) — the least-covered gap

This is the gap that caused the most confusion during manual testing: distinguishing "the proxy hasn't refreshed its cache yet" from "the proxy tried and got an error" from "the circuit breaker is open and didn't even try" was impossible from the outside. Add explicit categorization at the point where each of these is already decided:

```go
// A call to a node the proxy believed was the leader failed — reason must be one of:
// "transport_error" (connection refused/timeout), "redirect_received" (explicit non-leader reply,
// §9.2 redirectLeader field), or "breaker_open" (circuit breaker skipped the call entirely, §5)
log.Printf("client-proxy: LEADER_CONTACT_FAILED node=%s reason=%s detail=%q", addr, reason, detail)

// The proxy's cached notion of who the leader is changed — source must be one of:
// "discovery_refresh" (§1) or "redirect_hop" (§3's redirect-following)
log.Printf("client-proxy: LEADER_UPDATED old=%s new=%s source=%s", oldLeader, newLeader, source)

// Each internal redirect hop taken while following a redirectLeader (§3) — log every hop,
// not just the outcome, since the hop count itself is what's bounded at 3
log.Printf("client-proxy: REDIRECT_FOLLOWED hop=%d/%d from=%s to=%s", hopNum, maxHops, from, to)

// The redirect-hop budget was exhausted (§3) — this is the direct cause of every 503
// "redirect-hop budget exhausted" response; log it so the cause is visible without
// having to correlate an HTTP response against node logs after the fact
log.Printf("client-proxy: REQUEST_FAILED_EXHAUSTED key=%s hops_attempted=%d", key, maxHops)
```

- [ ] `LEADER_CONTACT_FAILED` must fire for **every** failed attempt in a redirect chain, not just the first or the last — during this phase's manual testing, not being able to see which specific hop failed and why was the single biggest time sink.
- [ ] Keep `reason` as a small fixed set of string constants (not free-form error messages) in the main log line, with the raw error/detail in a separate `detail` field — this keeps the log grep-able (`Select-String "reason=breaker_open"`) while not discarding the underlying error text.
- [ ] Do **not** add this same level of detail to the Read-Index-specific fallback path (`md-week4.md` §4) in this phase — that path already has its own documented behavior and isn't what caused the debugging difficulty here; if it turns out to need the same treatment later, that's a follow-up, not part of this pass.

---

## 4. Verification — re-run the scenario that motivated this phase

Once the above compiles and passes the existing test suite unchanged:

```bash
go test -race ./internal/raft/... -count=20
```

Then rebuild and repeat, as closely as practical, the manual scenario that surfaced the gap: bring the stack up, kill two of three consensus nodes in sequence (leaving one isolated), wait, then restore one node and observe recovery. With the new logs in place, confirm you can now directly answer, without inferring from term numbers alone:

- [ ] Does the isolated node show repeated `ELECTION_START` with no corresponding `BECAME_LEADER`? (This is the direct confirmation of the disruptive-isolated-node behavior discussed earlier — document this as an accepted limitation once confirmed, not as a bug to fix in this phase.)
- [ ] When the isolated node rejoins and a new election happens, do the `VOTE_GRANTED`/`VOTE_REJECTED` lines show it winning on log up-to-dateness once it's caught up, consistent with §10.3's rule having no memory of past disruptive behavior?
- [ ] Does `discovery`'s `VIEW_UPDATED` timing line up with when the proxy's `LEADER_UPDATED` fires — i.e. is the ~2s polling interval visible end-to-end, or is there an unexplained additional delay?
- [ ] For any request that returned `503 redirect-hop budget exhausted` during testing, can you now trace the exact `LEADER_CONTACT_FAILED` reason at each of the 3 hops, instead of only knowing the final outcome?

---

## 5. Explicit non-goals for this phase (do not implement yet)

- **No Pre-Vote extension.** The disruptive-isolated-node behavior this phase makes visible is a known, real gap (the classic Raft "disruptive server" problem) — but fixing it is out of scope here. Once confirmed via the logs above, document it as an accepted limitation (parallel to the existing one in spec §4.3), not as a defect to patch in this pass.
- **No change to election, vote-granting, commit, or log-repair logic** (§10.2–§10.5) — this phase only observes decisions those rules already make.
- **No change to Client Proxy routing or redirect-following behavior** (`md-week4.md` §3–§4) — only its logging.
- **No Snapshot & Backup service work** — still Week 5, unchanged.
- **No structured/JSON logging migration** — plain `log.Printf` matching the existing style is sufficient; don't introduce a logging framework for this.

---

## 6. Deliverables checklist for Week 4-mid

- [ ] `internal/raft`: `ELECTION_START`, `BECAME_LEADER`, `VOTE_GRANTED`, `VOTE_REJECTED` (with `reason` ∈ {`already_voted`, `stale_log`}), `STEPPED_DOWN` (from the single shared higher-term helper, with `source` identifying the triggering RPC).
- [ ] `internal/discovery`: `VIEW_UPDATED` (fires on leader change even without a follower-set change), `NODE_STATUS_CHANGED` (both directions, non-sticky per §1's existing design).
- [ ] `internal/proxy`: `LEADER_CONTACT_FAILED` (every failed hop, `reason` ∈ {`transport_error`, `redirect_received`, `breaker_open`}), `LEADER_UPDATED` (`source` ∈ {`discovery_refresh`, `redirect_hop`}), `REDIRECT_FOLLOWED` (every hop, not just the outcome), `REQUEST_FAILED_EXHAUSTED`.
- [ ] `go test -race ./internal/raft/... -count=20` clean, no regressions from the logging additions.
- [ ] Manual re-run of the two-kills-then-rejoin scenario, with all four verification questions in §4 answered directly from logs (not inferred from term numbers).
- [ ] Isolated-node disruptive behavior confirmed via logs and written up as an accepted limitation (for the next PDF/Word update, alongside §4.3's existing one) — not fixed in this phase.
