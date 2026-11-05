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

## Rapport

`rapport/rapport_jalon1.pdf` se régénère avec la commande suivante, à lancer sous Windows depuis la racine du projet.
Elle utilise `reportlab` et `Pillow`, les polices Calibri et Consolas, et les chiffres de `rapport/resultats/`.

```bash
python rapport/build_rapport.py
```

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
spark/                    traitements PySpark, un script par partie
rapport/                  notes du rapport PDF, captures et résultats mesurés
```
