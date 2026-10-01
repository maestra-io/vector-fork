#!/usr/bin/env bash
# E2E for the maestra nats source: an http sink with BOUNDED retries whose
# endpoint is down longer than those retries must lose nothing. The driver
# reports exhausted retries as Rejected; the source must NAK (redeliver), not
# TERM. Needs: a nats-server with JetStream on $NATS_URL, docker (nats-box),
# python3, and the vector binary as $1.
# Usage: bounded-retries-e2e.sh <vector-binary> [N] [DOWN_SECS]
set -uo pipefail
VECTOR=$1; N=${2:-2000}; DOWN=${3:-40}
NATS_URL=${NATS_URL:-nats://127.0.0.1:4222}; PORT=${PORT:-18080}
WORK=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$WORK"' EXIT
nb() { docker run --rm --network host natsio/nats-box:latest nats -s "$NATS_URL" "$@"; }
die() { echo "FAIL: $*"; exit 1; }

nb stream rm E2E_BR -f >/dev/null 2>&1
nb stream add E2E_BR --subjects 'e2ebr.>' --storage memory --defaults >/dev/null || die "stream add"
nb consumer add E2E_BR C --pull --ack explicit --wait 60s --deliver all --max-pending 20000 --defaults >/dev/null || die "consumer add"
nb pub e2ebr.x '{"seq":{{Count}}}' --count "$N" --jetstream >/dev/null 2>&1 || die "publish"

cat > "$WORK/vector.yaml" <<YAML
data_dir: $WORK
sources:
  js:
    type: nats
    url: $NATS_URL
    connection_name: e2e-br
    subject: e2ebr.>
    jetstream: { stream: E2E_BR, consumer: C }
    decoding: { codec: json }
sinks:
  out:
    type: http
    inputs: [js]
    uri: http://127.0.0.1:$PORT/
    encoding: { codec: json }
    framing: { method: newline_delimited }
    batch: { max_events: 100, timeout_secs: 1 }
    request: { retry_attempts: 2, retry_initial_backoff_secs: 1, retry_max_duration_secs: 2 }
    healthcheck: { enabled: false }
    acknowledgements: { enabled: true }
YAML
cat > "$WORK/recv.py" <<'PY'
import http.server, sys
out = open(sys.argv[2], "ab", buffering=0)
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        out.write(self.rfile.read(int(self.headers["Content-Length"])))
        self.send_response(200); self.end_headers()
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY

"$VECTOR" --config "$WORK/vector.yaml" > "$WORK/vector.log" 2>&1 &
sleep "$DOWN"   # the sink endpoint is down: every request exhausts its retries
python3 "$WORK/recv.py" "$PORT" "$WORK/out.jsonl" &
for _ in $(seq 1 120); do
  info=$(nb consumer info E2E_BR C --json 2>/dev/null)
  [ "$(jq '.num_pending' <<<"$info")" = 0 ] && [ "$(jq '.num_ack_pending' <<<"$info")" = 0 ] && break
  sleep 1
done
sleep 2
uniq=$(jq -r .seq "$WORK/out.jsonl" 2>/dev/null | sort -un | wc -l | tr -d ' ')
lines=$(jq -c . "$WORK/out.jsonl" 2>/dev/null | wc -l | tr -d ' ')
exhausted=$(grep -c 'retries exhausted\|Retries exhausted\|Not retriable; dropping the request\|Service call failed' "$WORK/vector.log")
echo "published=$N sink_down=${DOWN}s delivered_unique=$uniq lines=$lines lost=$((N-uniq)) dups=$((lines-uniq)) failed_requests_logged=$exhausted"
[ "$exhausted" -gt 0 ] || die "the sink never gave up: the test did not exercise exhausted retries"
[ "$uniq" -eq "$N" ] || die "lost $((N-uniq)) events"
echo PASS
