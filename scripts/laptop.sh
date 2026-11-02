#!/usr/bin/env bash
# Runs the pipeline by hand on a laptop, so any process can be killed by hand.
#
#   scripts/laptop.sh up              Kafka and TigerBeetle in Docker, the topic, the fence accounts, two demo accounts
#   scripts/laptop.sh edge [port]     an edge in this terminal, on port 8080 unless told otherwise
#   scripts/laptop.sh resolver        a resolver in this terminal
#   scripts/laptop.sh authorize KEY   holds 100 from demo account 1 under this key
#   scripts/laptop.sh capture KEY     captures that hold in full
#   scripts/laptop.sh cancel KEY      cancels it
#   scripts/laptop.sh down            stops Kafka and TigerBeetle, and deletes what they held
#
# Kill an edge or a resolver with `pkill -9 -f EdgeMain` or `pkill -9 -f ResolverMain`, and start it again.
set -euo pipefail
cd "$(dirname "$0")/.."

export KAFKA_BOOTSTRAP=localhost:9092 LEDGER_ADDRESSES=127.0.0.1:3000 PARTITIONS=4 REPLICATION=1 MIN_INSYNC=1

classpath() {
  if [ ! -s target/laptop.classpath ] || [ -n "$(find shell/src core/src build.sbt -newer target/laptop.classpath -print -quit)" ]; then
    sbt -batch "shell/writeClasspath" > /dev/null
  fi
  cat target/laptop.classpath
}

main() { exec java --enable-native-access=ALL-UNNAMED -cp "$(classpath)" "$@"; }

# A key's creation time is part of its identity, so a later command about a key must send the same one.
created() {
  local file="target/laptop-key-$1"
  mkdir -p target
  [ -s "$file" ] || date +%s000 > "$file"
  cat "$file"
}

post() { curl -sS -w ' [%{http_code}]\n' -H 'Content-Type: application/json' -d "$2" "http://localhost:${PORT:-8080}/$1"; }

case "${1:-}" in
  up)
    docker compose up -d --wait
    until nc -z localhost 3000 && nc -z localhost 9092; do sleep 1; done
    cp="$(classpath)"
    java --enable-native-access=ALL-UNNAMED -cp "$cp" ledger.shell.ProvisionMain
    java --enable-native-access=ALL-UNNAMED -cp "$cp" ledger.shell.LaptopAccountsMain
    ;;
  edge) PORT="${2:-8080}" main ledger.shell.EdgeMain ;;
  resolver) main ledger.shell.ResolverMain ;;
  authorize) post authorize "{\"createdAt\":$(created "$2"),\"key\":\"$2\",\"debit\":\"1\",\"credit\":\"2\",\"amount\":\"100\",\"ledger\":7}" ;;
  capture) post capture "{\"createdAt\":$(created "$2"),\"key\":\"$2\",\"amount\":\"full\"}" ;;
  cancel) post cancel "{\"createdAt\":$(created "$2"),\"key\":\"$2\"}" ;;
  down) docker compose down -v; rm -f target/laptop-key-* ;;
  *) sed -n '2,12p' "$0"; exit 2 ;;
esac
