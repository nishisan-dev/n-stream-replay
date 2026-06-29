#!/usr/bin/env bash
set -euo pipefail

# Diretório do script (raiz do deploy: jar + config/ ao lado)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

JAR="${JAR:-$(ls -1 n-stream-replay-*.jar target/n-stream-replay-*.jar 2>/dev/null | head -1)}"
if [[ -z "${JAR:-}" || ! -f "$JAR" ]]; then
  echo "ERRO: fat jar n-stream-replay-*.jar não encontrado. Rode 'mvn clean package'." >&2
  exit 1
fi

# ZGC geracional; --enable-native-access silencia o warning do LZ4 do Kafka.
JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx2g -XX:+UseZGC -XX:+ZGenerational -XX:+AlwaysPreTouch \
-Djava.net.preferIPv4Stack=true --enable-native-access=ALL-UNNAMED}"

# A config de app (sources/sinks/pipelines) é lida de ./config/n-stream-replay.yaml
# via spring.config.import do application.yml interno. Para apontar outro diretório:
#   --spring.config.additional-location=file:/etc/nsr/
exec java $JAVA_OPTS -jar "$JAR" \
  --spring.config.additional-location="file:./config/" \
  "$@"
