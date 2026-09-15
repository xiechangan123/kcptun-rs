#!/usr/bin/env bash
# Smoke test: server restart, client stays up, new TCP connections must recover.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release"
TARGET=19801
SRV=19802
CLI=19803
KEY="smoke-reconnect-key"
LOGDIR="$(mktemp -d)"
echo "logs: $LOGDIR"

cleanup() {
  lsof -ti:$TARGET -ti:$SRV -ti:$CLI 2>/dev/null | xargs kill -9 2>/dev/null || true
}
trap cleanup EXIT
cleanup
sleep 0.3

python3 -u -c "
import socket,threading as t
def h(c):
 d=c.recv(65536)
 if d: c.sendall(d)
 c.close()
s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('127.0.0.1',$TARGET));s.listen(128)
while 1:
 t.Thread(target=h,args=(s.accept()[0],),daemon=True).start()
" >"$LOGDIR/echo.log" 2>&1 &
echo $! >"$LOGDIR/echo.pid"
sleep 0.3

start_server() {
  RUST_LOG=info \
  "$BIN/kcptun-server" \
    -l ":$SRV" -t "127.0.0.1:$TARGET" \
    --key "$KEY" --crypt null --mode fast --keepalive 2 --nocomp \
    >"$LOGDIR/server.log" 2>&1 &
  echo $! >"$LOGDIR/srv.pid"
  sleep 0.6
}

start_client() {
  RUST_LOG=info \
  "$BIN/kcptun-client" \
    -l ":$CLI" -r "127.0.0.1:$SRV" \
    --key "$KEY" --crypt null --mode fast --keepalive 2 --nocomp --conn 2 \
    >"$LOGDIR/client.log" 2>&1 &
  echo $! >"$LOGDIR/cli.pid"
  sleep 0.6
}

echo_probe() {
  python3 -u -c "
import socket,sys
s=socket.create_connection(('127.0.0.1',$CLI),timeout=3)
s.sendall(b'hello-$1')
s.settimeout(3)
try:
 d=s.recv(64)
 print('OK' if d==b'hello-$1' else 'BAD:'+repr(d))
except Exception as e:
 print('ERR:'+type(e).__name__)
s.close()
"
}

start_server
start_client

echo "=== baseline ==="
for i in 1 2 3; do echo_probe "base$i"; done

echo "=== kill server ==="
kill -9 "$(cat "$LOGDIR/srv.pid")" || true
sleep 0.2

echo "=== restart server ==="
start_server
sleep 0.3

echo "=== probe after restart (no client restart) ==="
ok=0
for i in $(seq 1 25); do
  out=$(echo_probe "post$i" || echo ERR)
  echo "  probe$i: $out"
  if [ "$out" = "OK" ]; then ok=$((ok+1)); fi
  sleep 0.4
done
echo "ok=$ok/25"

echo "=== client log ==="
grep -E 'reconnect|dead|error|Error|keepalive|closed|refused|failed' "$LOGDIR/client.log" || true
echo "--- last 30 client ---"
tail -30 "$LOGDIR/client.log" || true
