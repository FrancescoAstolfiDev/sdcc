#!/usr/bin/env bash
#
# run-scenario.sh — bring up a 3/5/7-node scenario, wait for an actual
# leader (not a fixed sleep), then run cmd/loadgen against it.
#
# Why this exists: a fixed "sleep N" before launching the load generator
# has to be re-tuned by hand for every cluster size (build time, discovery
# registration, and leader election all take longer as node count grows —
# a 7-node cluster measurably needs more than the ~5s that's enough for 3
# nodes). Guessing wrong either wastes time waiting on a small cluster or,
# worse, starts the load generator against a cluster with no leader yet,
# which shows up as a burst of proxy 503s at the start of the run and
# skews the results. This script polls the consensus-node container logs
# for the "BECAME_LEADER" line raft/election.go emits instead, so it scales
# to whatever the cluster actually needs, with a bounded timeout as a
# safety net rather than proceeding blind.
#
# Usage:
#   experiments/scalability/run-scenario.sh <3nodes|5nodes|7nodes> [-- <loadgen flags>]
#
# Examples:
#   experiments/scalability/run-scenario.sh 5nodes
#   experiments/scalability/run-scenario.sh 7nodes -- -requests 2000 -concurrency 20
#
# Env overrides:
#   LEADER_WAIT_TIMEOUT_SECS   max seconds to wait for BECAME_LEADER (default: 30)
#   LEADER_WAIT_POLL_SECS      polling interval in seconds (default: 1)
#   SKIP_COMPOSE_UP            if set to 1, don't run `docker compose up` (assume already running)
#   SKIP_COMPOSE_DOWN          if set to 1, don't tear the stack down on exit
#
# Requires: docker compose, go (to run cmd/loadgen).

set -euo pipefail

usage() {
	echo "Usage: $0 <3nodes|5nodes|7nodes> [-- <loadgen flags>]" >&2
	exit 1
}

[ $# -ge 1 ] || usage
scenario="$1"
shift

case "$scenario" in
3nodes | 5nodes | 7nodes) ;;
*)
	echo "error: unknown scenario '$scenario' (expected 3nodes, 5nodes, or 7nodes)" >&2
	usage
	;;
esac

if [ $# -gt 0 ]; then
	[ "$1" = "--" ] || usage
	shift
fi
loadgen_args=("$@")

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
deployments_dir="$repo_root/deployments"

env_file="$deployments_dir/.env.$scenario"
if [ ! -f "$env_file" ]; then
	echo "error: $env_file not found — copy it from .env.$scenario.example first (see README.md)" >&2
	exit 1
fi

compose=(docker compose -f "$deployments_dir/docker-compose.yml" --env-file "$env_file")
if [ "$scenario" != "3nodes" ]; then
	compose+=(--profile "$scenario")
fi

leader_wait_timeout="${LEADER_WAIT_TIMEOUT_SECS:-30}"
leader_wait_poll="${LEADER_WAIT_POLL_SECS:-1}"

cleanup() {
	if [ "${SKIP_COMPOSE_DOWN:-0}" != "1" ]; then
		echo "run-scenario: tearing down $scenario..."
		"${compose[@]}" down -v >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

if [ "${SKIP_COMPOSE_UP:-0}" != "1" ]; then
	echo "run-scenario: starting $scenario..."
	"${compose[@]}" up -d --build
fi

echo "run-scenario: waiting up to ${leader_wait_timeout}s for a leader (polling every ${leader_wait_poll}s for BECAME_LEADER)..."
start_ts=$(date +%s)
leader_found=0
while true; do
	if "${compose[@]}" logs 2>/dev/null | grep -q "BECAME_LEADER"; then
		leader_found=1
		break
	fi
	elapsed=$(($(date +%s) - start_ts))
	if [ "$elapsed" -ge "$leader_wait_timeout" ]; then
		break
	fi
	sleep "$leader_wait_poll"
done

elapsed=$(($(date +%s) - start_ts))
if [ "$leader_found" != "1" ]; then
	echo "error: no leader elected within ${leader_wait_timeout}s — cluster is not ready, refusing to start the load generator blind." >&2
	echo "       check 'docker compose -f $deployments_dir/docker-compose.yml --env-file $env_file logs' for details." >&2
	exit 1
fi
echo "run-scenario: leader elected after ${elapsed}s, starting load generator."

cd "$repo_root"
go run ./cmd/loadgen -target http://localhost:8080 "${loadgen_args[@]}"
