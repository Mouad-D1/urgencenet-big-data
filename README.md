# UrgenceNet.ma - Pipeline Big Data

Pipeline d'analyse des passages aux urgences et des transferts entre les 40 établissements fictifs
du réseau UrgenceNet.ma. Les données sont entièrement synthétiques.

```
producer.py -> Kafka (urgences) -> Spark Structured Streaming -> MinIO (Bronze -> Silver -> Gold) -> GraphX / GraphFrames -> Parquet
```

## Versions

| Composant | Version | Image Docker |
|---|---|---|
| Apache Spark (Scala 2.12, Java 17, Python 3) | 3.5.7 | `urgencenet/spark:3.5.7`, construite depuis `apache/spark` |
| Apache Kafka (KRaft) | 3.9.1 | `apache/kafka:3.9.1` |
| MinIO | 2025-04-22 | `bitnamilegacy/minio:2025.4.22` |
| Client MinIO `mc` | 2025-04-16 | `bitnamilegacy/minio-client:2025.4.16` |
| Delta Lake | 3.3.2 | jar inclus dans l'image Spark |
| hadoop-aws / AWS SDK | 3.3.4 / 1.12.262 | jars inclus dans l'image Spark |

Les paquets Python sont figés dans [docker/spark/requirements.txt](docker/spark/requirements.txt).

Les images officielles `minio/minio` et `minio/mc` et les binaires de `dl.min.io` ne sont plus
disponibles : le projet utilise les builds figés `bitnamilegacy`.

## Prérequis

Docker Desktop avec au moins 8 Go de mémoire alloués et environ 5 Go d'espace disque.

## Lancement

```bash
docker compose up -d --build
```

Le premier lancement construit l'image Spark, ce qui peut prendre plus de 10 minutes.
Il crée aussi le topic `urgences` (3 partitions) et le bucket `lake`.

| Interface | URL | Accès |
|---|---|---|
| Spark Master | http://localhost:8080 | |
| Spark Workers | http://localhost:8081, http://localhost:8082 | |
| Spark UI de l'application en cours | http://localhost:4040 | |
| Console MinIO | http://localhost:9001 | `minioadmin` / `minioadmin` |
| JupyterLab | http://localhost:8888 | sans mot de passe |

Ces ports ne sont publiés que sur `127.0.0.1`. Un fichier `.env` (non versionné) permet d'adapter
la configuration : `SPARK_MASTER_UI_PORT` si le port 8080 est déjà pris sur la machine,
`MINIO_ROOT_USER` et `MINIO_ROOT_PASSWORD` pour changer les identifiants MinIO.

Le code s'exécute dans le conteneur `jupyter`, où le projet est monté sur `/workspace`.
Par défaut, une application Spark occupe les 4 cœurs du cluster : une seconde application reste en
attente tant que la première n'est pas arrêtée (penser à fermer les notebooks inutilisés).

## Partie 1 : vérification du cluster

```bash
docker compose exec jupyter spark-submit spark/partie1_cluster.py
```

```bash
docker compose exec mc mc ls local/
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic urgences
```

## Générateur de données

`data/facilities.json` contient le référentiel des 40 établissements (un objet JSON par ligne).

```bash
docker compose exec jupyter python3 producer.py --mode file --n 50000
```

```bash
docker compose exec mc mc cp /data/urgences_history.jsonl local/lake/bronze/history/urgences_history.jsonl
```

```bash
docker compose exec mc mc cp /data/facilities.json local/lake/bronze/facilities/facilities.json
```

```bash
docker compose exec jupyter python3 producer.py --mode kafka --rate 20
```

Options utiles : `--seed` rend la génération reproductible (42 par défaut en mode `file`),
`--duration` limite la durée du flux, `--log-late 10` journalise les événements émis avec au moins
10 minutes de retard (utile pour la question 6 de la Partie 4).

En mode `file`, le générateur injecte aussi des cas inhabituels pour la Partie 6 et les décrit dans
`data/anomalies_ground_truth.json`, ce qui permet de mesurer les faux positifs et les cas manqués.

## Partie 2 : RDD

L'historique doit d'abord être généré et copié en Bronze (section précédente).

```bash
docker compose exec jupyter spark-submit spark/partie2_rdd.py --hold 600
```

`--hold 600` garde l'application active 10 minutes pour consulter le DAG sur http://localhost:4040.
Les résultats et les temps sont écrits dans `rapport/resultats/partie2.json`.

## Partie 3 : Spark SQL et DataFrames

```bash
docker compose exec jupyter spark-submit spark/partie3_sql.py --hold 600
```

Le script écrit la couche Silver dans `lake/silver/urgences_history/`, les résultats dans
`rapport/resultats/partie3.json` et les plans dans `rapport/resultats/partie3_explain.txt`.
Pour le vérifier :

```bash
docker compose exec mc mc ls --recursive local/lake/silver/
```

## Partie 4 : Structured Streaming

Dans un premier terminal, lancer les requêtes (arrêt par Ctrl+C) :

```bash
docker compose exec jupyter spark-submit spark/partie4_streaming.py
```

Dans un second terminal, lancer le producteur :

```bash
docker compose exec jupyter python3 producer.py --mode kafka --rate 20 --log-late 10
```

Les micro-batches s'affichent dans le premier terminal et sur http://localhost:4040/StreamingQuery/.
Les messages arrivent dans `lake/bronze/urgences_raw/`, et chaque requête reprend à son checkpoint après un arrêt.

Pour reproduire l'analyse du watermark, recréer d'abord un topic vide, puis lancer le script avec
`--duration 545 --reset` et le producteur avec `--duration 480`. Le producteur doit s'arrêter avant le script.
`--reset` supprime les checkpoints de cette partie et le dossier `urgences_raw`. L'analyse est écrite dans
`rapport/resultats/partie4.json`.

## Partie 5 : pipeline de production

Démonstration (arrêt par Ctrl+C), avec le producteur dans un second terminal comme en Partie 4 :

```bash
docker compose exec jupyter spark-submit spark/partie5_pipeline.py
```

La requête `silver` lit Kafka, dédoublonne avec un watermark de 10 minutes, joint le référentiel, ajoute
`wait_band` et écrit `lake/silver/urgences_clean/` (partitionné par `city` et `date`). Trois requêtes relisent
Silver en flux et écrivent en Delta Lake : `lake/gold/kpi_facility_10min/` (fenêtre tumbling),
`lake/gold/taux_prioritaires_10min/` (fenêtre glissante) et `lake/gold/sessions_triage/` (fenêtre de session).

Exécution de référence, avec arrêt de la requête Silver pendant 2 minutes puis reprise sur son checkpoint.
Recréer d'abord un topic vide, puis lancer ces trois commandes dans trois terminaux :

```bash
docker compose exec jupyter spark-submit spark/partie5_pipeline.py --reset --duration 1560 --stop-at 600 --pause 120
```

```bash
docker compose exec jupyter python3 producer.py --mode kafka --rate 20 --duration 1440
```

```bash
bash scripts/releve_consumer_groups.sh
```

Le dernier script relève `kafka-consumer-groups.sh --describe` pendant l'arrêt, après la reprise et à la fin.

Pendant que le pipeline tourne, la commande suivante affiche les derniers indicateurs Gold et l'historique Delta.
Elle n'utilise qu'un cœur, pour laisser travailler le pipeline :

```bash
docker compose exec jupyter spark-submit spark/demo_gold.py
```
Consulter les groupes à la main :

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group urgencenet-silver-suivi
```

## Partie 6 : GraphFrames et GraphX

Versions : Spark 3.5.7 et GraphX intégré, Scala 2.12.18, GraphFrames 0.8.4-spark3.5-s_2.12. Les trois étapes
s'enchaînent dans cet ordre et lisent l'historique Silver de la Partie 3.

1. Graphe, motifs, degrés, composantes et règles d'alerte avec GraphFrames. GraphFrames est ajouté par `--packages`,
   module Python compris. `spark.jars.ivy` est nécessaire, car l'utilisateur `spark` de l'image n'a pas de répertoire
   personnel.

```bash
docker compose exec jupyter spark-submit --driver-memory 2g --conf spark.jars.ivy=/home/spark/.ivy2 --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 --repositories https://repos.spark-packages.org spark/partie6_graphframes.py
```

2. Degrés, composantes connexes et PageRank pondéré par groupe hospitalier avec GraphX, en Scala :

```bash
docker compose exec jupyter spark-shell --master spark://spark-master:7077 --conf spark.cores.max=4 --conf spark.sql.shuffle.partitions=8 -i spark/partie6_graphx.scala
```

3. Comparaison des deux analyses, table de métriques par établissement et figure :

```bash
docker compose exec jupyter spark-submit spark/partie6_restitution.py
```

Les résultats sont écrits sous `lake/gold/graph_analysis/`, partitionnés par `hospital_group`. La figure est dans
`rapport/captures/partie6_graphe_transferts.png`.

## Rapport

Le rapport final (`rapport/rapport_final.pdf`, Parties 1 à 6) et celui du jalon 1 (`rapport/rapport_jalon1.pdf`) se
régénèrent sous Windows depuis la racine du projet. Les scripts utilisent `reportlab` et `Pillow`, les polices Calibri
et Consolas, la mise en forme commune de `rapport/pdf_style.py` et les chiffres de `rapport/resultats/`. Le script du
rapport final s'arrête en erreur si le document dépasse 10 pages.

```bash
python rapport/build_rapport_final.py
```

```bash
python rapport/build_rapport.py
```

Les notes détaillées de chaque partie sont dans `rapport/partie1.md` à `rapport/partie6.md`.

## Arrêt

```bash
docker compose down
```

Ajouter `-v` supprime aussi les volumes, donc le contenu du Data Lake et du topic Kafka.

## Organisation du dépôt

```
docker-compose.yml        Kafka, Spark Master + 2 Workers, MinIO, mc, Jupyter
docker/spark/             image Spark (jars, dépendances Python, spark-defaults.conf)
producer.py               générateur d'événements (fichier ou Kafka)
data/facilities.json      référentiel des établissements
spark/                    traitements PySpark par partie, et l'analyse GraphX en Scala (partie6_graphx.scala)
scripts/                  relevé des consumer groups Kafka (Partie 5)
rapport/                  rapports PDF, notes par partie, captures et résultats mesurés
```
