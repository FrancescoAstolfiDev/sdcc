#!/usr/bin/env bash
#
# run_fault_test.sh — misura lo scenario di fault-tolerance (spec §12.2):
# kill del leader Raft corrente e misura di detection time, election time
# e recovery time, contro il cluster b5-kvstore già in piedi via
# deployments/docker-compose.yml.
#
# Perché esiste: perdere ore per stato Docker residuo (volumi/term di un
# run precedente) non verificato prima di procedere è esattamente il
# problema che questo script evita — PRIMA di killare qualunque cosa,
# verifica esplicitamente sia il numero di servizi Up sia che l'ultimo
# ELECTION_START non porti un term residuo alto. Nessuna assunzione
# implicita: se qualcosa non torna, abort con un messaggio chiaro invece
# di procedere alla cieca.
#
# Usage:
#   experiments/fault-tolerance/run_fault_test.sh [env_file] [profile] [expected_up_services]
#
# Lo stack va interrogato con GLI STESSI --env-file/--profile con cui è
# stato avviato (es. 'docker compose --env-file .env.7nodes --profile
# 7nodes up'), altrimenti 'docker compose ps'/'logs' vedono un progetto
# Compose diverso da quello realmente attivo (set di servizi sbagliato,
# comportamento bloccato/silenzioso senza errori espliciti). Questo
# script quindi NON assume un env-file/profile di default: vanno passati
# esplicitamente per gli scenari 5nodes/7nodes.
#
# env_file: percorso, relativo a deployments/, del file passato come
#   --env-file (es. .env.7nodes). Vuoto ("" o omesso) = nessun --env-file
#   (va bene per lo scenario 3nodes di default, che non ne richiede uno).
# profile: valore passato come --profile (es. 7nodes). Vuoto ("" o
#   omesso) = nessun --profile (va bene per lo scenario 3nodes).
# expected_up_services: numero di servizi che 'docker compose ps' deve
#   mostrare in stato running PRIMA di procedere (default: 3). Deve
#   corrispondere esattamente allo stack che hai in piedi — lo stack
#   3-nodi completo di deployments/docker-compose.yml ha 6 servizi
#   (discovery + consensus-node-1..3 + snapshot-backup + client-proxy),
#   il 5nodes ne ha 8, il 7nodes ne ha 10. Il default 3 è solo un
#   placeholder minimo, non un valore "giusto" per tutti gli scenari —
#   sovrascrivilo sempre esplicitamente se non sei sicuro.
#
# Esempi:
#   experiments/fault-tolerance/run_fault_test.sh
#     # scenario 3nodes di default, nessun env-file/profile, 3 servizi attesi
#     # (placeholder: per lo stack 3nodes completo passa 6, vedi sopra)
#   experiments/fault-tolerance/run_fault_test.sh .env.7nodes 7nodes 10
#     # scenario 7nodes (10 servizi attesi)
#
# Env overrides:
#   PROXY_URL        base URL del client-proxy (default: http://localhost:8080)
#   TERM_THRESHOLD    term oltre il quale il cluster è considerato residuo (default: 10)
#
# Requires: docker compose, curl, awk, sed, GNU date. Nessuna dipendenza
# da grep -P (PCRE) — non disponibile in Git Bash su Windows — tutte le
# estrazioni usano sed con BRE portabile (GNU/WSL e Git Bash).

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
echo "docker compose invocato come: ${compose[*]}"

die() {
	echo "error: $*" >&2
	exit 1
}

# Estrae, dai log (con --timestamps) di uno o più servizi, le righe che
# contengono $pattern, come coppie "timestamp<TAB>riga_completa" ordinate
# cronologicamente (il timestamp è quello iniettato da docker compose
# logs -t, RFC3339Nano/UTC — non il timestamp interno del logger Go, che
# ha solo precisione al secondo ed è quindi inadatto a ordinare eventi
# provenienti da nodi diversi).
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

echo "== step 1/6: verifica esplicita dello stato del cluster =="

# 1a. numero di servizi effettivamente Up.
up_count=$("${compose[@]}" ps --status running --services 2>/dev/null | wc -l | tr -d ' ')
echo "docker compose ps: $up_count servizi running (attesi: $expected_up)"
if [ "$up_count" -ne "$expected_up" ]; then
	die "il cluster non ha il numero atteso di servizi Up ($up_count invece di $expected_up)." \
		" verifica manualmente con '${compose[*]} ps' prima di procedere — non sto assumendo nulla."
fi

# 1b. term residuo: l'ultimo ELECTION_START, cronologicamente, su
# qualunque nodo consensus running.
mapfile -t consensus_services < <("${compose[@]}" ps --status running --services 2>/dev/null | grep '^consensus-node-' || true)
[ "${#consensus_services[@]}" -gt 0 ] || die "nessun servizio consensus-node-* in stato running trovato."

last_election=$(extract_timestamped_events "ELECTION_START" "${consensus_services[@]}" | tail -n1 || true)
if [ -n "$last_election" ]; then
	# Ancorato a "ELECTION_START term=", non al solo "term=": la riga
	# contiene anche "previous_term=N", e con .* greedy un pattern sed
	# su "term=" da solo catturerebbe quest'ultimo (l'occorrenza più a
	# destra) invece del term corrente.
	last_term=$(printf '%s\n' "$last_election" | sed -n 's/.*ELECTION_START term=\([0-9]\+\).*/\1/p' | head -n1)
	echo "ultimo ELECTION_START osservato (su tutti i nodi consensus running): term=$last_term"
	if [ -n "$last_term" ] && [ "$last_term" -ge "$term_threshold" ]; then
		die "cluster in stato residuo, term=$last_term troppo alto, fai prima docker compose down -v + docker volume prune -f"
	fi
else
	echo "nessun ELECTION_START nei log — procedo (nessun term residuo da verificare)."
fi

echo "== step 2/6: identificazione del leader corrente (VIEW_UPDATED da discovery) =="
last_view=$("${compose[@]}" logs --no-color discovery 2>/dev/null | grep -F "VIEW_UPDATED" | tail -n1 || true)
[ -n "$last_view" ] || die "nessun VIEW_UPDATED trovato nei log di discovery — nessun leader mai osservato, il cluster non è pronto."
# Ancorato a " leader=" (con lo spazio davanti), non al solo "leader=":
# la riga contiene anche "(was_leader=...)", che termina anch'esso in
# "leader=" ma è preceduto da "_" non da uno spazio — senza l'ancora lo
# .* greedy catturerebbe was_leader invece del leader corrente.
leader_addr=$(printf '%s\n' "$last_view" | sed -n 's/.* leader=\([^ ]*\).*/\1/p' || true)
[ -n "$leader_addr" ] || die "l'ultimo VIEW_UPDATED non riporta un leader (leader=vuoto) — nessun leader attualmente eletto, il cluster non è pronto."
leader_service="${leader_addr%%:*}"
echo "leader corrente: $leader_service (address=$leader_addr)"

case " ${consensus_services[*]} " in
*" $leader_service "*) ;;
*) die "il leader riportato da discovery ('$leader_service') non è tra i servizi consensus running ($(
	IFS=,
	echo "${consensus_services[*]}"
)) — stato inconsistente, non procedo." ;;
esac

leader_container=$("${compose[@]}" ps -q "$leader_service" 2>/dev/null)
[ -n "$leader_container" ] || die "impossibile risolvere il container ID per il servizio '$leader_service'."

echo "== step 3/6: kill del leader ($leader_service) =="
kill_ts="$(now_iso)"
kill_ms=$(to_epoch_ms "$kill_ts")
echo "kill timestamp (ISO8601 UTC): $kill_ts"
docker stop "$leader_container" >/dev/null
echo "container $leader_container ($leader_service) fermato con 'docker stop'."

echo "== step 4/6: polling ogni ${poll_interval_s}s su POST /v1/kv fino al primo 201 (timeout ${recovery_timeout_s}s) =="
key="faulttest-$(date -u +%Y%m%dT%H%M%S%3N)-$$"
url="$proxy_url/v1/kv/$key"
deadline_ms=$((kill_ms + recovery_timeout_s * 1000))
success_ts=""
while true; do
	now_ms=$(date -u +%s%3N)
	if [ "$now_ms" -ge "$deadline_ms" ]; then
		die "timeout: nessuna scrittura riuscita (HTTP 201 su $url) entro ${recovery_timeout_s}s dal kill — il cluster non si è ripreso."
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
echo "prima scrittura riuscita (201) alle $success_ts"

echo "== step 5/6: analisi log dei nodi consensus superstiti =="
surviving_services=()
for s in "${consensus_services[@]}"; do
	[ "$s" = "$leader_service" ] || surviving_services+=("$s")
done
[ "${#surviving_services[@]}" -gt 0 ] || die "nessun nodo consensus superstite da analizzare."

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
[ -n "$first_election_after_kill" ] || die "nessun ELECTION_START rilevato sui nodi superstiti dopo il kill — impossibile calcolare il detection time."
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
[ -n "$first_became_leader" ] || die "nessun BECAME_LEADER rilevato sui nodi superstiti dopo il primo ELECTION_START — impossibile calcolare l'election time."
became_leader_ms=$(to_epoch_ms "$first_became_leader")
election_ms=$((became_leader_ms - election_start_ms))

echo "== step 6/6: riepilogo e registrazione risultati =="
printf 'detection time  (kill -> primo ELECTION_START su nodo superstite): %dms\n' "$detection_ms"
printf 'election time   (primo ELECTION_START -> BECAME_LEADER):            %dms\n' "$election_ms"
printf 'recovery time   (kill -> primo 201 su /v1/kv):                      %dms\n' "$recovery_ms"

if [ ! -f "$results_csv" ]; then
	echo "run_number,node_killed,detection_ms,election_ms,recovery_ms" >"$results_csv"
fi
run_number=$(wc -l <"$results_csv" | tr -d ' ')
printf '%d,%s,%d,%d,%d\n' "$run_number" "$leader_service" "$detection_ms" "$election_ms" "$recovery_ms" >>"$results_csv"
echo "risultati appesi a $results_csv (run #$run_number)"
