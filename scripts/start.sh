#!/usr/bin/env bash
set -euo pipefail

# Diretório do script (raiz do deploy: jar + config/ ao lado)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

if [[ -n "${JAR:-}" ]]; then
  if [[ ! -f "$JAR" ]]; then
    echo "ERRO: JAR informado não existe: $JAR" >&2
    exit 1
  fi
else
  shopt -s nullglob
  JARS=(n-stream-replay-*.jar target/n-stream-replay-*.jar)
  shopt -u nullglob
  case "${#JARS[@]}" in
    0)
      echo "ERRO: fat jar n-stream-replay-*.jar não encontrado. Rode 'mvn clean package'." >&2
      exit 1
      ;;
    1)
      JAR="${JARS[0]}"
      ;;
    *)
      echo "ERRO: múltiplos jars encontrados; limpe o build ou informe JAR explicitamente:" >&2
      printf '  %s\n' "${JARS[@]}" >&2
      exit 1
      ;;
  esac
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
