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

	echo "debug client checks passed"
}

main "$@"
