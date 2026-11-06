#!/usr/bin/env bash
# Partie 5 : releve kafka-consumer-groups.sh --describe aux moments cles de l'execution de reference
# (arret de la requete Silver, reprise, fin), d'apres les marqueurs du journal Spark.
# Lancement depuis la racine du projet : bash scripts/releve_consumer_groups.sh
set -u
# Sous Git Bash (Windows), sans cela /opt/kafka/... serait reecrit en chemin Windows
export MSYS_NO_PATHCONV=1
LOG=rapport/resultats/partie5_console.txt
OUT=rapport/resultats/partie5_consumer_groups.txt
: > "$OUT"

describe() {
  {
    echo "===== $1 ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
    for group in urgencenet-silver urgencenet-silver-suivi; do
      docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 --describe --group "$group" 2>&1
      echo
    done
  } >> "$OUT"
}

wait_for() {
  until grep -q "$1" "$LOG" 2>/dev/null; do sleep 3; done
}

wait_for '^\[ARRET SILVER\]'
describe "Requete Silver arretee"
sleep 60
describe "Requete Silver arretee depuis une minute, producteur actif"
wait_for '^\[REPRISE SILVER\] premier batch'
describe "Apres la reprise de la requete Silver"
wait_for '^\[GOLD\] relecture'
describe "Fin de l'execution"
