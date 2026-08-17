#!/usr/bin/env bash
#
# run_fault_test.sh — measures the fault-tolerance scenario (spec §12.2):
# kills the current Raft leader and measures detection time, election time,
# and recovery time, against the b5-kvstore cluster already running via
# deployments/docker-compose.yml.
#
# Why this exists: wasting hours on leftover Docker state (volumes/term from
# a previous run) that was never verified before proceeding is exactly the
# problem this script avoids — BEFORE killing anything, it explicitly
# verifies both the number of Up services and that the last ELECTION_START
# does not carry a stale high term. No implicit assumptions: if something
# doesn't add up, abort with a clear message instead of proceeding blindly.
#
# Usage:
#   experiments/fault-tolerance/run_fault_test.sh [env_file] [profile] [expected_up_services]
#
# The stack must be queried with the SAME --env-file/--profile it was
# started with (e.g. 'docker compose --env-file .env.7nodes --profile
# 7nodes up'), otherwise 'docker compose ps'/'logs' see a different Compose
# project than the one actually running (wrong set of services, silent/
# stuck behavior with no explicit errors). This script therefore does NOT
# assume a default env-file/profile: they must be passed explicitly for the
# 5nodes/7nodes scenarios.
#
# env_file: path, relative to deployments/, of the file passed as
#   --env-file (e.g. .env.7nodes). Empty ("" or omitted) = no --env-file
#   (fine for the default 3nodes scenario, which doesn't require one).
# profile: value passed as --profile (e.g. 7nodes). Empty ("" or
#   omitted) = no --profile (fine for the 3nodes scenario).
# expected_up_services: number of services that 'docker compose ps' must
#   show in running state BEFORE proceeding (default: 3). Must match
#   exactly the stack you have running — the full 3-node stack from
#   deployments/docker-compose.yml has 6 services (discovery +
#   consensus-node-1..3 + snapshot-backup + client-proxy), the 5nodes one
#   has 8, the 7nodes one has 10. The default of 3 is only a minimal
#   placeholder, not a "correct" value for every scenario — always
#   override it explicitly if you're not sure.
#
# Examples:
#   experiments/fault-tolerance/run_fault_test.sh
#     # default 3nodes scenario, no env-file/profile, 3 services expected
#     # (placeholder: for the full 3nodes stack pass 6, see above)
#   experiments/fault-tolerance/run_fault_test.sh .env.7nodes 7nodes 10
#     # 7nodes scenario (10 services expected)
#
# Env overrides:
#   PROXY_URL        base URL of the client-proxy (default: http://localhost:8080)
#   TERM_THRESHOLD    term above which the cluster is considered stale (default: 10)
#
# Requires: docker compose, curl, awk, sed, GNU date. No dependency on
# grep -P (PCRE) — not available in Git Bash on Windows — all extractions
# use sed with portable BRE (GNU/WSL and Git Bash).

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
deployments_dir="$repo_root/deployments"
results_csv="$script_dir/results.csv"

env_file="${1:-}"
profile="${2:-}"
expected_up="${3:-3}"
proxy_url="${PROXY_URL:-http://localhost:8080}"
term_threshold="${TERM_THRESHOLD:-10}"
poll_interval_s=0.1
recovery_timeout_s=30

compose=(docker compose -f "$deployments_dir/docker-compose.yml")
if [ -n "$env_file" ]; then
	compose+=(--env-file "$deployments_dir/$env_file")
fi
if [ -n "$profile" ]; then
	compose+=(--profile "$profile")
fi
echo "docker compose invoked as: ${compose[*]}"

die() {
	echo "error: $*" >&2
	exit 1
}

# Extracts, from the logs (with --timestamps) of one or more services, the
# lines that contain $pattern, as "timestamp<TAB>full_line" pairs sorted
# chronologically (the timestamp is the one injected by docker compose
# logs -t, RFC3339Nano/UTC — not the Go logger's internal timestamp, which
# only has second precision and is therefore unsuitable for ordering
# events coming from different nodes).
extract_timestamped_events() {
	local pattern="$1"
	shift
	"${compose[@]}" logs --timestamps --no-color "$@" 2>/dev/null |
		grep -F "$pattern" |
		awk '{
			idx = index($0, "| ")
			if (idx == 0) next
			rest = substr($0, idx + 2)
			split(rest, a, " ")
			print a[1] "\t" $0
		}' |
		sort -t $'\t' -k1,1
}

to_epoch_ms() {
	date -u -d "$1" +%s%3N
}

now_iso() {
	date -u +%Y-%m-%dT%H:%M:%S.%3NZ
}

echo "== step 1/6: explicit verification of cluster state =="

# 1a. number of services actually Up.
up_count=$("${compose[@]}" ps --status running --services 2>/dev/null | wc -l | tr -d ' ')
echo "docker compose ps: $up_count services running (expected: $expected_up)"
if [ "$up_count" -ne "$expected_up" ]; then
	die "the cluster does not have the expected number of Up services ($up_count instead of $expected_up)." \
		" verify manually with '${compose[*]} ps' before proceeding — not assuming anything."
fi

# 1b. stale term: the last ELECTION_START, chronologically, on any
# running consensus node.
mapfile -t consensus_services < <("${compose[@]}" ps --status running --services 2>/dev/null | grep '^consensus-node-' || true)
[ "${#consensus_services[@]}" -gt 0 ] || die "no consensus-node-* service found in running state."

last_election=$(extract_timestamped_events "ELECTION_START" "${consensus_services[@]}" | tail -n1 || true)
if [ -n "$last_election" ]; then
	# Anchored to "ELECTION_START term=", not just "term=": the line
	# also contains "previous_term=N", and with a greedy .* a sed
	# pattern on "term=" alone would capture that one (the rightmost
	# occurrence) instead of the current term.
	last_term=$(printf '%s\n' "$last_election" | sed -n 's/.*ELECTION_START term=\([0-9]\+\).*/\1/p' | head -n1)
	echo "last ELECTION_START observed (across all running consensus nodes): term=$last_term"
	if [ -n "$last_term" ] && [ "$last_term" -ge "$term_threshold" ]; then
		die "cluster in a stale state, term=$last_term too high, run docker compose down -v + docker volume prune -f first"
	fi
else
	echo "no ELECTION_START in the logs — proceeding (no stale term to check)."
fi

echo "== step 2/6: identifying the current leader (VIEW_UPDATED from discovery) =="
last_view=$("${compose[@]}" logs --no-color discovery 2>/dev/null | grep -F "VIEW_UPDATED" | tail -n1 || true)
[ -n "$last_view" ] || die "no VIEW_UPDATED found in discovery logs — no leader ever observed, the cluster is not ready."
# Anchored to " leader=" (with a leading space), not just "leader=":
# the line also contains "(was_leader=...)", which also ends in
# "leader=" but is preceded by "_" rather than a space — without the
# anchor the greedy .* would capture was_leader instead of the current
# leader.
leader_addr=$(printf '%s\n' "$last_view" | sed -n 's/.* leader=\([^ ]*\).*/\1/p' || true)
[ -n "$leader_addr" ] || die "the last VIEW_UPDATED does not report a leader (leader=empty) — no leader currently elected, the cluster is not ready."
leader_service="${leader_addr%%:*}"
echo "current leader: $leader_service (address=$leader_addr)"

case " ${consensus_services[*]} " in
*" $leader_service "*) ;;
*) die "the leader reported by discovery ('$leader_service') is not among the running consensus services ($(
	IFS=,
	echo "${consensus_services[*]}"
)) — inconsistent state, aborting." ;;
esac

leader_container=$("${compose[@]}" ps -q "$leader_service" 2>/dev/null)
[ -n "$leader_container" ] || die "unable to resolve the container ID for service '$leader_service'."

echo "== step 3/6: killing the leader ($leader_service) =="
kill_ts="$(now_iso)"
kill_ms=$(to_epoch_ms "$kill_ts")
echo "kill timestamp (ISO8601 UTC): $kill_ts"
docker stop "$leader_container" >/dev/null
echo "container $leader_container ($leader_service) stopped with 'docker stop'."

echo "== step 4/6: polling every ${poll_interval_s}s on POST /v1/kv until the first 201 (timeout ${recovery_timeout_s}s) =="
key="faulttest-$(date -u +%Y%m%dT%H%M%S%3N)-$$"
url="$proxy_url/v1/kv/$key"
deadline_ms=$((kill_ms + recovery_timeout_s * 1000))
success_ts=""
while true; do
	now_ms=$(date -u +%s%3N)
	if [ "$now_ms" -ge "$deadline_ms" ]; then
		die "timeout: no successful write (HTTP 201 on $url) within ${recovery_timeout_s}s of the kill — the cluster did not recover."
	fi
	status=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
		-d '{"value":"faulttest"}' "$url" 2>/dev/null || echo "000")
	if [ "$status" = "201" ]; then
		success_ts="$(now_iso)"
		break
	fi
	sleep "$poll_interval_s"
done
success_ms=$(to_epoch_ms "$success_ts")
recovery_ms=$((success_ms - kill_ms))
echo "first successful write (201) at $success_ts"

echo "== step 5/6: analyzing logs of surviving consensus nodes =="
surviving_services=()
for s in "${consensus_services[@]}"; do
	[ "$s" = "$leader_service" ] || surviving_services+=("$s")
done
[ "${#surviving_services[@]}" -gt 0 ] || die "no surviving consensus node to analyze."

election_events=$(extract_timestamped_events "ELECTION_START" "${surviving_services[@]}" || true)
first_election_after_kill=""
while IFS=$'\t' read -r ts _line; do
	[ -n "$ts" ] || continue
	ts_ms=$(to_epoch_ms "$ts")
	if [ "$ts_ms" -gt "$kill_ms" ]; then
		first_election_after_kill="$ts"
		break
	fi
done <<<"$election_events"
[ -n "$first_election_after_kill" ] || die "no ELECTION_START detected on surviving nodes after the kill — cannot compute detection time."
election_start_ms=$(to_epoch_ms "$first_election_after_kill")
detection_ms=$((election_start_ms - kill_ms))

leader_events=$(extract_timestamped_events "BECAME_LEADER" "${surviving_services[@]}" || true)
first_became_leader=""
while IFS=$'\t' read -r ts _line; do
	[ -n "$ts" ] || continue
	ts_ms=$(to_epoch_ms "$ts")
	if [ "$ts_ms" -ge "$election_start_ms" ]; then
		first_became_leader="$ts"
		break
	fi
done <<<"$leader_events"
[ -n "$first_became_leader" ] || die "no BECAME_LEADER detected on surviving nodes after the first ELECTION_START — cannot compute election time."
became_leader_ms=$(to_epoch_ms "$first_became_leader")
election_ms=$((became_leader_ms - election_start_ms))

echo "== step 6/6: summary and recording results =="
printf 'detection time  (kill -> first ELECTION_START on surviving node):    %dms\n' "$detection_ms"
printf 'election time   (first ELECTION_START -> BECAME_LEADER):            %dms\n' "$election_ms"
printf 'recovery time   (kill -> first 201 on /v1/kv):                      %dms\n' "$recovery_ms"

if [ ! -f "$results_csv" ]; then
	echo "run_number,node_killed,detection_ms,election_ms,recovery_ms" >"$results_csv"
fi
run_number=$(wc -l <"$results_csv" | tr -d ' ')
printf '%d,%s,%d,%d,%d\n' "$run_number" "$leader_service" "$detection_ms" "$election_ms" "$recovery_ms" >>"$results_csv"
echo "results appended to $results_csv (run #$run_number)"
