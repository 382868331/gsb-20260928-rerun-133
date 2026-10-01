#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT:-8080}}"
CURL_ARGS=()
if [[ -n "${UNIX_SOCKET:-}" ]]; then
	CURL_ARGS=(--unix-socket "$UNIX_SOCKET")
fi

api() {
	local method=$1 path=$2 body=${3:-{}}
	curl -sS "${CURL_ARGS[@]}" -X "$method" "$BASE_URL$path" \
		-H "Content-Type: application/json" \
		--data "$body"
}

main() {
	echo "programs: $(api GET /programs)"

	local session pause continue duplicate wait
	session=$(api POST /sessions '{"program":"raise"}')
	echo "session: $session"
	local id=1
	pause=$(jq -r '.pauseId' <<<"$session")

	continue=$(api POST "/sessions/$id/continue" "{\"pauseId\":$pause}")
	echo "continue: $continue"
	jq -e '.terminated == true and .reason == "exception"' <<<"$continue" >/dev/null

	# The executor has exited; every pending/late request must finish instead
	# of blocking. Duplicate continue is rejected with 410 Gone.
	duplicate=$(curl -sS "${CURL_ARGS[@]}" -o /tmp/debug-client-duplicate.json -w '%{http_code}' \
		-X POST "$BASE_URL/sessions/$id/continue" \
		-H "Content-Type: application/json" \
		--data "{\"pauseId\":$pause}")
	wait=$(api POST "/sessions/$id/wait" '{}')
	echo "duplicate status: $duplicate body: $(cat /tmp/debug-client-duplicate.json)"
	echo "wait after exit: $wait"
	[[ "$duplicate" == "410" ]]
	jq -e '.state == "terminated" and .reason == "exception"' <<<"$wait" >/dev/null

	local loop_session loop_pause second_continue
	loop_session=$(api POST /sessions '{"program":"loop","breakpoints":[2]}')
	local loop_id=2
	loop_pause=$(jq -r '.pauseId' <<<"$loop_session")
	second_continue=$(api POST "/sessions/$loop_id/continue" "{\"pauseId\":$loop_pause}")
	echo "breakpoint continue: $second_continue"
	jq -e '.paused.state == "stopped" and .paused.reason == "breakpoint" and .paused.line == 2' <<<"$second_continue" >/dev/null

	local stale_status
	stale_status=$(curl -sS "${CURL_ARGS[@]}" -o /tmp/debug-client-stale.json -w '%{http_code}' \
		-X POST "$BASE_URL/sessions/$loop_id/continue" \
		-H "Content-Type: application/json" \
		--data "{\"pauseId\":$loop_pause}")
	echo "old pause continue status: $stale_status body: $(cat /tmp/debug-client-stale.json)"
	[[ "$stale_status" == "409" ]]

	# Attach two clients: the first controls, the second observes.
	local ctrl obs ctrl_token obs_token obs_status
	ctrl=$(api POST "/sessions/$loop_id/attach" '{"client":"ctrl"}')
	obs=$(api POST "/sessions/$loop_id/attach" '{"client":"obs"}')
	echo "controller: $ctrl"
	echo "observer: $obs"
	jq -e '.role == "controller" and .snapshot.controller == "ctrl"' <<<"$ctrl" >/dev/null
	jq -e '.role == "observer"' <<<"$obs" >/dev/null
	ctrl_token=$(jq -r '.token' <<<"$ctrl")
	obs_token=$(jq -r '.token' <<<"$obs")

	# Reconnecting with the same client id returns the same token and role.
	jq -e --arg t "$ctrl_token" '.token == $t and .role == "controller"' +		<<<"$(api POST "/sessions/$loop_id/attach" '{"client":"ctrl"}')" >/dev/null

	# The observer may query but not advance execution.
	local current_pause
	current_pause=$(jq -r '.snapshot.pauseId' <<<"$ctrl")
	jq -e '.vars' <<<"$(api POST "/sessions/$loop_id/variables" "{\"pauseId\":$current_pause}")" >/dev/null
	obs_status=$(curl -sS "${CURL_ARGS[@]}" -o /tmp/debug-client-obs.json -w '%{http_code}' \
		-X POST "$BASE_URL/sessions/$loop_id/continue" \
		-H "Content-Type: application/json" \
		--data "{\"pauseId\":$current_pause,\"token\":\"$obs_token\"}")
	echo "observer continue status: $obs_status body: $(cat /tmp/debug-client-obs.json)"
	[[ "$obs_status" == "403" ]]

	# Idempotent continue: the replay is explained as a duplicate and does
	# not advance the program again.
	local first replay
	first=$(api POST "/sessions/$loop_id/continue" \
		"{\"pauseId\":$current_pause,\"token\":\"$ctrl_token\",\"requestId\":\"req-1\"}")
	replay=$(api POST "/sessions/$loop_id/continue" \
		"{\"pauseId\":$current_pause,\"token\":\"$ctrl_token\",\"requestId\":\"req-1\"}")
	echo "idempotent first: $first"
	echo "idempotent replay: $replay"
	jq -e '.duplicate != true' <<<"$first" >/dev/null
	jq -e '.duplicate == true' <<<"$replay" >/dev/null
	[[ "$(jq -r '.paused.pauseId' <<<"$first")" == "$(jq -r '.paused.pauseId' <<<"$replay")" ]]

	# The controller releases; the observer claims and may advance.
	api POST "/sessions/$loop_id/release" "{\"token\":\"$ctrl_token\"}" >/dev/null
	jq -e '.role == "controller"' \
		<<<"$(api POST "/sessions/$loop_id/claim" "{\"token\":\"$obs_token\"}")" >/dev/null
	local new_pause
	new_pause=$(jq -r '.snapshot.pauseId' <<<"$(api POST "/sessions/$loop_id/attach" '{"client":"obs"}')")
	jq -e '.paused or .terminated' <<<"$(api POST "/sessions/$loop_id/continue" \
		"{\"pauseId\":$new_pause,\"token\":\"$obs_token\",\"requestId\":\"req-2\"}")" >/dev/null

	echo "debug client checks passed"
}

main "$@"
