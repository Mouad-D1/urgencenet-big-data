"""Partie 4 : Structured Streaming depuis Kafka.

Lancement pour la demonstration, jusqu'a Ctrl+C :
    docker compose exec jupyter spark-submit spark/partie4_streaming.py
Dans un autre terminal :
    docker compose exec jupyter python3 producer.py --mode kafka --rate 20 --log-late 10

Avec --duration, le script s'arrete seul et analyse le devenir des evenements en retard. Le producteur
doit alors etre arrete avant la fin de cette duree, pour que toutes les requetes voient les memes donnees.
"""
import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from schemas import EVENT_SCHEMA

KAFKA_OPTIONS = {"kafka.bootstrap.servers": "kafka:9092", "subscribe": "urgences", "startingOffsets": "earliest"}
BRONZE_PATH = "s3a://lake/bronze/urgences_raw/"
CHECKPOINTS = "s3a://lake/checkpoints/partie4/"
RESULTS_PATH = Path("rapport/resultats/partie4.json")
TRIGGER = "10 seconds"
WATERMARK = "10 minutes"
WINDOW_MINUTES = 10
WATERMARK_QUERY = "watermark_par_passage"
LATE_BUCKETS = [(0, 60), (60, 300), (300, 600), (600, 750), (750, 900), (900, 10**9)]

parser = argparse.ArgumentParser()
parser.add_argument("--duration", type=int, default=0, help="duree en secondes, suivie de l'analyse (0 = jusqu'a Ctrl+C)")
parser.add_argument("--reset", action="store_true", help="supprime d'abord les checkpoints et le Bronze de cette partie")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie4-Streaming")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 4)
    .config("spark.scheduler.minRegisteredResourcesRatio", 1.0)
    # Peu de donnees par micro-batch : les 200 partitions par defaut creeraient 200 taches et 200 fichiers d'etat
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

if args.reset:
    for location in (BRONZE_PATH, CHECKPOINTS):
        path = sc._jvm.org.apache.hadoop.fs.Path(location)
        path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(path, True)


class WatermarkLog(StreamingQueryListener):
    """Journalise, a chaque micro-batch, le watermark et les lignes rejetees."""

    def __init__(self):
        self.batches = []

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        if progress.name != WATERMARK_QUERY or not progress.stateOperators:
            return
        batch = {
            "batch": progress.batchId,
            "start": progress.timestamp,
            "input_rows": progress.numInputRows,
            "max_event_time": progress.eventTime.get("max"),
            "watermark": progress.eventTime.get("watermark"),
            "dropped": progress.stateOperators[0].numRowsDroppedByWatermark,
            "state_rows": progress.stateOperators[0].numRowsTotal,
        }
        self.batches.append(batch)
        print(f"[WATERMARK] batch={batch['batch']} debut={batch['start']} lignes={batch['input_rows']} "
              f"max_event_time={batch['max_event_time']} watermark={batch['watermark']} "
              f"rejetees={batch['dropped']} etat={batch['state_rows']}", flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


watermark_log = WatermarkLog()
spark.streams.addListener(watermark_log)

# 1. Source : chaque message Kafka devient une ligne de la table infinie
raw = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()

# 2. Colonnes typees, avec le schema de la Partie 3
events = (
    raw
    .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"), F.col("timestamp").alias("kafka_time"))
    .select("event.*", "kafka_time")
)

# 3. Flux brut en console
console_raw = (
    events
    .select("visit_id", "city", "status", "priority", "wait_minutes", "event_time")
    .writeStream.queryName("console_flux_brut")
    .format("console").outputMode("append")
    .option("numRows", 5).option("truncate", False)
    .trigger(processingTime=TRIGGER)
    .start()
)

# 4. Couche Bronze : les messages tels que lus dans Kafka, avec leur origine (partition et offset)
bronze = (
    raw
    .select(
        "topic", "partition", "offset", "timestamp",
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
    )
    .writeStream.queryName("bronze_urgences_raw")
    .format("json").outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINTS + "bronze")
    .trigger(processingTime=TRIGGER)
    .start()
)

# 5 et 6. Passages par ville et priorite, avec watermark. En mode complete, Spark doit conserver tout
# l'historique de la table de resultats : le watermark n'y supprime ni etat ni ligne en retard.
triaged = events.filter(F.col("status") == "TRIAGED").withWatermark("event_time", WATERMARK)
by_city_priority = triaged.groupBy("city", "priority").count().orderBy("city", "priority")
console_complete = (
    by_city_priority
    .writeStream.queryName("console_ville_priorite")
    .format("console").outputMode("complete")
    .option("numRows", 25).option("truncate", False)
    .option("checkpointLocation", CHECKPOINTS + "console_ville_priorite")
    .trigger(processingTime=TRIGGER)
    .start()
)
# Meme calcul dans une table en memoire, pour comparer son total en fin d'execution
memory_complete = (
    by_city_priority
    .writeStream.queryName("total_complete")
    .format("memory").outputMode("complete")
    .option("checkpointLocation", CHECKPOINTS + "total_complete")
    .trigger(processingTime=TRIGGER)
    .start()
)

# 6. Effet du watermark sur une fenetre : le passage fait partie de la cle, ce qui permet de savoir
# ensuite, evenement par evenement, lesquels ont ete retenus.
accepted = {}


def remember_accepted(batch, _batch_id):
    for row in batch.collect():
        accepted[row["visit_id"]] = row["count"]


watermark_query = (
    triaged
    .groupBy(F.window("event_time", f"{WINDOW_MINUTES} minutes"), "visit_id").count()
    .writeStream.queryName(WATERMARK_QUERY)
    .outputMode("update")
    .foreachBatch(remember_accepted)
    .option("checkpointLocation", CHECKPOINTS + WATERMARK_QUERY)
    .trigger(processingTime=TRIGGER)
    .start()
)

queries = [console_raw, bronze, console_complete, memory_complete, watermark_query]

if not args.duration:
    spark.streams.awaitAnyTermination()

spark.streams.awaitAnyTermination(args.duration)
for query in queries:
    query.processAllAvailable()
    query.stop()


def parse_time(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def window_end(event_time):
    start = event_time.replace(minute=event_time.minute - event_time.minute % WINDOW_MINUTES, second=0, microsecond=0)
    return start + timedelta(minutes=WINDOW_MINUTES)


# Analyse : comparaison au contenu exact du topic
topic = (
    spark.read.format("kafka").options(**KAFKA_OPTIONS).option("endingOffsets", "latest").load()
    .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"), F.col("timestamp").alias("kafka_time"))
    .select("event.*", "kafka_time")
    .cache()
)
bronze_rows = spark.read.json(BRONZE_PATH)
triaged_rows = topic.filter(F.col("status") == "TRIAGED")
first_arrivals = (
    triaged_rows.groupBy("visit_id", "event_time").agg(F.min("kafka_time").alias("arrival")).collect()
)

batches = watermark_log.batches
buckets = {low: {"total": 0, "retenus": 0, "rejetes": 0} for low, _ in LATE_BUCKETS}
examples = []
prediction_errors = 0
for row in first_arrivals:
    event_time = row["event_time"].replace(tzinfo=timezone.utc)
    arrival = row["arrival"].replace(tzinfo=timezone.utc)
    delay = (arrival - event_time).total_seconds()
    is_accepted = row["visit_id"] in accepted
    low = next(l for l, high in LATE_BUCKETS if delay < high)
    buckets[low]["total"] += 1
    buckets[low]["retenus" if is_accepted else "rejetes"] += 1

    # Le micro-batch qui lit l'evenement est le premier a demarrer apres son arrivee. Depuis Spark 3.4,
    # les retards y sont filtres avec le watermark du batch precedent ; le watermark courant sert a purger l'etat.
    index = next((i for i, b in enumerate(batches) if parse_time(b["start"]) >= arrival), None)
    if not index:
        continue
    batch = batches[index]
    watermark = parse_time(batches[index - 1]["watermark"])
    predicted_accepted = window_end(event_time) > watermark
    prediction_errors += predicted_accepted != is_accepted
    if delay >= 600:
        examples.append({
            "visit_id": row["visit_id"],
            "event_time": event_time.isoformat(),
            "arrival": arrival.isoformat(),
            "delay_minutes": round(delay / 60, 2),
            "batch": batch["batch"],
            "watermark_for_late_events": batches[index - 1]["watermark"],
            "window_end": window_end(event_time).isoformat(),
            "retenu": is_accepted,
        })

examples.sort(key=lambda example: -example["delay_minutes"])
results = {
    "kafka_rows": topic.count(),
    "bronze_rows": bronze_rows.count(),
    "bronze_distinct_offsets": bronze_rows.select("partition", "offset").distinct().count(),
    "triaged_rows": triaged_rows.count(),
    "total_complete_mode": spark.table("total_complete").agg(F.sum("count")).first()[0],
    "accepted_rows_window": sum(accepted.values()),
    "dropped_rows_window": sum(b["dropped"] for b in batches),
    "batches_with_input": sum(1 for b in batches if b["input_rows"]),
    "prediction_errors": prediction_errors,
    "late_buckets_seconds": {f"{low}-{high}": buckets[low] for low, high in LATE_BUCKETS},
    "late_examples": examples,
}
RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
RESULTS_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(json.dumps({key: value for key, value in results.items() if key != "late_examples"}, indent=2), flush=True)

spark.stop()
