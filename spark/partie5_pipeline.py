"""Partie 5 : pipeline de production Kafka -> Silver (Parquet) -> Gold (Delta Lake).

Demonstration, jusqu'a Ctrl+C :
    docker compose exec jupyter spark-submit spark/partie5_pipeline.py
Dans un autre terminal :
    docker compose exec jupyter python3 producer.py --mode kafka --rate 20

Execution de reference, avec arret puis reprise de la requete Silver et analyse finale (le producteur doit
s'arreter avant la fin de --duration) :
    docker compose exec jupyter spark-submit spark/partie5_pipeline.py --reset --duration 1560 --stop-at 600 --pause 120

Spark 3.5 n'accepte pas, dans une meme requete en mode update, un dedoublonnage suivi d'une agregation : la
couche Silver est donc ecrite par une premiere requete, que les requetes Gold relisent en flux.
"""
import argparse
import json
import time
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StructType

from schemas import EVENT_SCHEMA, FACILITY_SCHEMA

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "urgences"
SPARK_GROUP = "urgencenet-silver"
# Spark ne commite pas ses offsets dans Kafka : ce groupe recopie ceux du checkpoint pour le suivi du retard
MONITOR_GROUP = "urgencenet-silver-suivi"
FACILITIES_PATH = "s3a://lake/bronze/facilities/"
SILVER_PATH = "s3a://lake/silver/urgences_clean/"
GOLD_KPI_PATH = "s3a://lake/gold/kpi_facility_10min/"
GOLD_PRIORITY_PATH = "s3a://lake/gold/taux_prioritaires_10min/"
GOLD_SESSIONS_PATH = "s3a://lake/gold/sessions_triage/"
CHECKPOINTS = "s3a://lake/checkpoints/partie5/"
RESULTS_DIR = Path("rapport/resultats")
WATERMARK = "10 minutes"
# 30 secondes plutot que 10 : chaque micro-batch cree au moins un fichier par ville dans Silver
TRIGGER = "30 seconds"

parser = argparse.ArgumentParser()
parser.add_argument("--duration", type=int, default=0, help="duree totale en secondes, puis analyse (0 = jusqu'a Ctrl+C)")
parser.add_argument("--stop-at", type=int, default=0, help="arreter la requete Silver apres ce nombre de secondes")
parser.add_argument("--pause", type=int, default=120, help="duree de l'arret de la requete Silver, en secondes")
parser.add_argument("--reset", action="store_true", help="supprime d'abord Silver, Gold et les checkpoints de cette partie")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie5-Pipeline")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 4)
    .config("spark.scheduler.minRegisteredResourcesRatio", 1.0)
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")
hadoop_fs = sc._jvm.org.apache.hadoop.fs


def lake_path(location):
    path = hadoop_fs.Path(location)
    return path, path.getFileSystem(sc._jsc.hadoopConfiguration())


if args.reset:
    for location in (SILVER_PATH, GOLD_KPI_PATH, GOLD_PRIORITY_PATH, GOLD_SESSIONS_PATH, CHECKPOINTS):
        path, fs = lake_path(location)
        fs.delete(path, True)

# Les requetes Gold lisent Silver en flux : le dossier doit exister avant le premier fichier
silver_dir, silver_fs = lake_path(SILVER_PATH)
silver_fs.mkdirs(silver_dir)


class PipelineLog(StreamingQueryListener):
    """Journalise chaque micro-batch et recopie les offsets traites par Silver dans le groupe de suivi."""

    def __init__(self):
        self.batches = []
        self.consumer = None

    def onQueryStarted(self, event):
        print(f"[DEMARRAGE] {event.name} id={event.id} run={event.runId}", flush=True)

    def onQueryProgress(self, event):
        progress = event.progress
        state = progress.stateOperators[0] if progress.stateOperators else None
        batch = {
            "query": progress.name,
            "run": str(progress.runId),
            "batch": progress.batchId,
            "start": progress.timestamp,
            "input_rows": progress.numInputRows,
            "watermark": progress.eventTime.get("watermark"),
            "start_offset": progress.sources[0].startOffset if progress.sources else None,
            "end_offset": progress.sources[0].endOffset if progress.sources else None,
            "dropped_late": state.numRowsDroppedByWatermark if state else 0,
            "dropped_duplicates": state.customMetrics.get("numDroppedDuplicateRows", 0) if state else 0,
        }
        self.batches.append(batch)
        if progress.name == "silver" and progress.numInputRows:
            print(f"[SILVER] batch={batch['batch']} lignes={batch['input_rows']} doublons={batch['dropped_duplicates']} "
                  f"retards_rejetes={batch['dropped_late']} debut={batch['start_offset']} fin={batch['end_offset']}", flush=True)
            self.commit_monitor_offsets(batch["end_offset"])

    def commit_monitor_offsets(self, end_offset):
        if self.consumer is None:
            self.consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, group_id=MONITOR_GROUP, enable_auto_commit=False)
        offsets = json.loads(end_offset)[TOPIC]
        extra = ("",) if len(OffsetAndMetadata._fields) == 2 else ("", -1)
        self.consumer.commit({TopicPartition(TOPIC, int(p)): OffsetAndMetadata(o, *extra) for p, o in offsets.items()})

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[ARRET] id={event.id} run={event.runId} exception={event.exception}", flush=True)


log = PipelineLog()
spark.streams.addListener(log)

facilities = F.broadcast(
    spark.read.schema(FACILITY_SCHEMA).json(FACILITIES_PATH).select("facility_id", "name", "hospital_group", "type", "beds")
)
wait_band = (
    F.when(F.col("wait_minutes") < 15, "court")
    .when(F.col("wait_minutes") <= 45, "moyen")
    .when(F.col("wait_minutes") > 45, "long")
)


def silver_stream():
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", SPARK_GROUP)
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"), F.col("timestamp").alias("kafka_time"))
        .select("event.*", "kafka_time")
    )
    # 1. Dedoublonnage avec watermark. dropDuplicates(["visit_id", "status"]) garderait chaque cle pour toujours,
    # faute de colonne de temps dans la cle ; dropDuplicatesWithinWatermark purge l'etat une fois le watermark passe.
    deduplicated = events.withWatermark("event_time", WATERMARK).dropDuplicatesWithinWatermark(["visit_id", "status"])
    # 2. Enrichissement : jointure flux-statique avec le referentiel, puis tranche d'attente
    return (
        deduplicated
        .join(facilities, "facility_id", "left")
        .withColumn("wait_band", wait_band)
        .withColumn("date", F.to_date("event_time"))
    )


# A la relecture d'un dossier partitionne, les colonnes de partition viennent apres les colonnes des fichiers :
# le schema fourni doit suivre cet ordre, sinon les colonnes sont decalees
PARTITION_COLUMNS = ["city", "date"]
_silver_fields = silver_stream().schema.fields
silver_schema = StructType(
    [f for f in _silver_fields if f.name not in PARTITION_COLUMNS]
    + [f for name in PARTITION_COLUMNS for f in _silver_fields if f.name == name]
)


def start_silver():
    # 3. Sink Silver en Parquet, partitionne par ville et par date
    return (
        silver_stream()
        .repartition("city", "date")
        .writeStream.queryName("silver")
        .format("parquet").outputMode("append")
        .partitionBy(*PARTITION_COLUMNS)
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINTS + "silver")
        .trigger(processingTime=TRIGGER)
        .start()
    )


def delta_table_exists(location):
    path, fs = lake_path(location + "_delta_log")
    return fs.exists(path)


def merge_into(location, keys):
    """foreachBatch idempotent : rejouer un micro-batch reecrit les memes valeurs sur les memes cles."""
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)

    def upsert(batch, _batch_id):
        if batch.isEmpty():
            return
        batch = batch.withColumn("updated_at", F.current_timestamp())
        if not delta_table_exists(location):
            batch.write.format("delta").mode("append").save(location)
            return
        batch.createOrReplaceTempView("updates")
        batch.sparkSession.sql(
            f"MERGE INTO delta.`{location}` t USING updates s ON {condition} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )

    return upsert


def gold_source():
    return spark.readStream.schema(silver_schema).parquet(SILVER_PATH).withWatermark("event_time", WATERMARK)


def is_status(status):
    return (F.col("status") == status).cast("int")


# 4 et 5. Fenetre tumbling de 10 minutes par etablissement, ecrite dans la table Delta Gold
kpi = (
    gold_source()
    .groupBy(F.window("event_time", "10 minutes"), "facility_id", "city", "hospital_group")
    .agg(
        F.sum(is_status("ARRIVED")).alias("passages"),
        F.sum(is_status("CARE_STARTED")).alias("prises_en_charge"),
        F.round(F.avg("wait_minutes"), 2).alias("attente_moyenne"),
    )
    .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
            "facility_id", "city", "hospital_group", "passages", "prises_en_charge", "attente_moyenne")
)
# Fenetre glissante de 10 minutes, toutes les 5 minutes : part des priorites 1 et 2 parmi les triages
priority_rate = (
    gold_source()
    .filter(F.col("status") == "TRIAGED")
    .groupBy(F.window("event_time", "10 minutes", "5 minutes"), "facility_id", "city", "hospital_group")
    .agg(F.count("*").alias("triages"), F.sum((F.col("priority") <= 2).cast("int")).alias("prioritaires"))
    .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
            "facility_id", "city", "hospital_group", "triages", "prioritaires",
            F.round(F.col("prioritaires") / F.col("triages"), 4).alias("taux_prioritaires"))
)
# Fenetre de session par poste de triage : une session se ferme apres 5 minutes sans triage
desk_sessions = (
    gold_source()
    .filter(F.col("status") == "TRIAGED")
    .groupBy(F.session_window("event_time", "5 minutes"), "desk_id", "facility_id", "city", "hospital_group")
    .agg(F.count("*").alias("triages"), F.min("event_time").alias("premier_triage"), F.max("event_time").alias("dernier_triage"))
    .select(F.col("session_window.start").alias("session_start"), F.col("session_window.end").alias("session_end"),
            "desk_id", "facility_id", "city", "hospital_group", "triages", "premier_triage", "dernier_triage",
            F.round((F.unix_timestamp("dernier_triage") - F.unix_timestamp("premier_triage")) / 60, 2).alias("duree_activite_min"))
)

silver = start_silver()
gold_queries = [
    kpi.writeStream.queryName("gold_kpi_10min").outputMode("update")
    .foreachBatch(merge_into(GOLD_KPI_PATH, ["facility_id", "window_start"]))
    .option("checkpointLocation", CHECKPOINTS + "gold_kpi_10min").trigger(processingTime=TRIGGER).start(),
    priority_rate.writeStream.queryName("gold_taux_prioritaires").outputMode("update")
    .foreachBatch(merge_into(GOLD_PRIORITY_PATH, ["facility_id", "window_start"]))
    .option("checkpointLocation", CHECKPOINTS + "gold_taux_prioritaires").trigger(processingTime=TRIGGER).start(),
    # Le mode update n'est pas disponible pour les fenetres de session : une session n'est ecrite qu'une fois close
    desk_sessions.writeStream.queryName("gold_sessions_triage").outputMode("append")
    .format("delta").option("path", GOLD_SESSIONS_PATH)
    .option("checkpointLocation", CHECKPOINTS + "gold_sessions_triage").trigger(processingTime=TRIGGER).start(),
]

if not args.duration:
    spark.streams.awaitAnyTermination()

results = {}
started = time.time()


def read_text(location):
    path, fs = lake_path(location)
    stream = fs.open(path)
    content = bytes(sc._jvm.org.apache.commons.io.IOUtils.toByteArray(stream)).decode("utf-8")
    stream.close()
    return content


def silver_count():
    rows = spark.read.parquet(SILVER_PATH)
    return rows.count(), rows.select("visit_id", "status").distinct().count()


# 6. Arret de la requete Silver pendant que le producteur continue, puis reprise sur le meme checkpoint
if args.stop_at:
    spark.streams.awaitAnyTermination(args.stop_at)
    silver.stop()
    # Sans cela, les attentes suivantes rendraient la main aussitot, la requete Silver etant terminee
    spark.streams.resetTerminated()
    rows, keys = silver_count()
    offsets_dir, fs = lake_path(CHECKPOINTS + "silver/offsets")
    offset_files = sorted((s.getPath().getName() for s in fs.listStatus(offsets_dir)), key=lambda name: int(name) if name.isdigit() else -1)
    commit_files = [s.getPath().getName() for s in fs.listStatus(lake_path(CHECKPOINTS + "silver/commits")[0])]
    last_batch = max(int(name) for name in offset_files if name.isdigit())
    last_committed = max(int(name) for name in commit_files if name.isdigit())
    silver_batches = [b for b in log.batches if b["query"] == "silver" and b["input_rows"]]
    results["before_restart"] = {
        "silver_rows": rows,
        "silver_distinct_keys": keys,
        "last_processed_end_offset": silver_batches[-1]["end_offset"] if silver_batches else None,
        "checkpoint_last_offsets_batch": last_batch,
        "checkpoint_last_commit_batch": last_committed,
        "checkpoint_offsets_file": read_text(CHECKPOINTS + f"silver/offsets/{last_batch}"),
    }
    print(f"[ARRET SILVER] lignes Silver={rows} cles={keys} dernier batch offsets={last_batch} "
          f"dernier batch commite={last_committed}", flush=True)
    print(f"[CHECKPOINT] offsets/{last_batch} :\n{results['before_restart']['checkpoint_offsets_file']}", flush=True)
    time.sleep(args.pause)
    silver = start_silver()
    print("[REPRISE SILVER] meme checkpoint, nouveau runId", flush=True)
    while not any(b["query"] == "silver" and b["run"] == str(silver.runId) and b["input_rows"] for b in log.batches):
        time.sleep(2)
    first = next(b for b in log.batches if b["query"] == "silver" and b["run"] == str(silver.runId) and b["input_rows"])
    results["after_restart_first_batch"] = first
    print(f"[REPRISE SILVER] premier batch={first['batch']} debut={first['start_offset']} fin={first['end_offset']} "
          f"lignes={first['input_rows']}", flush=True)

remaining = args.duration - (time.time() - started)
if remaining > 0:
    spark.streams.awaitAnyTermination(remaining)

# Vidage : Silver d'abord, puis les requetes Gold qui le relisent
silver.processAllAvailable()
silver.stop()
for query in gold_queries:
    query.processAllAvailable()
    query.stop()

# Analyse : bilan exact entre le topic et la couche Silver
topic = (
    spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("subscribe", TOPIC)
    .option("startingOffsets", "earliest").option("endingOffsets", "latest").load()
    .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"), F.col("timestamp").alias("kafka_time"))
    .select("event.*", "kafka_time")
    .cache()
)
silver_rows = spark.read.parquet(SILVER_PATH).cache()
topic_keys = topic.groupBy("visit_id", "status").agg(
    F.min("kafka_time").alias("arrival"), F.first("event_time").alias("event_time")
)
missing = topic_keys.join(silver_rows.select("visit_id", "status"), ["visit_id", "status"], "left_anti").withColumn(
    "delay_minutes", (F.unix_timestamp("arrival") - F.unix_timestamp("event_time")) / 60
)
present_delays = topic_keys.join(silver_rows.select("visit_id", "status"), ["visit_id", "status"]).select(
    ((F.unix_timestamp("arrival") - F.unix_timestamp("event_time")) / 60).alias("delay_minutes")
)
silver_batches = [b for b in log.batches if b["query"] == "silver"]
kafka_rows, kafka_keys = topic.count(), topic_keys.count()
silver_total, silver_keys = silver_rows.count(), silver_rows.select("visit_id", "status").distinct().count()
missing_keys = missing.count()
results.update({
    "kafka_rows": kafka_rows,
    "kafka_distinct_keys": kafka_keys,
    "kafka_duplicate_rows": kafka_rows - kafka_keys,
    "silver_rows": silver_total,
    "silver_distinct_keys": silver_keys,
    # Bilan independant des metriques de Spark : chaque cle du topic est dans Silver ou rejetee pour retard
    "keys_balance_ok": silver_total == silver_keys and silver_keys + missing_keys == kafka_keys,
    # Un batch interrompu puis rejoue a la reprise n'est pas rapporte au listener : ses lignes manquent ici
    "reported_silver_input_rows": sum(b["input_rows"] for b in silver_batches),
    "silver_unknown_facility": silver_rows.filter(F.col("hospital_group").isNull()).count(),
    "dropped_duplicates": sum(b["dropped_duplicates"] for b in silver_batches),
    "dropped_late": sum(b["dropped_late"] for b in silver_batches),
    "missing_keys": missing_keys,
    "missing_min_delay_minutes": missing.agg(F.min("delay_minutes")).first()[0],
    "present_max_delay_minutes": present_delays.agg(F.max("delay_minutes")).first()[0],
    "silver_files": len(spark.read.parquet(SILVER_PATH).inputFiles()),
    "silver_by_city": {r["city"]: r["count"] for r in silver_rows.groupBy("city").count().collect()},
})

# Gold : comparaison des fenetres closes avec un recalcul batch depuis Silver
gold_kpi = spark.read.format("delta").load(GOLD_KPI_PATH)
final_watermark = max((b["watermark"] for b in log.batches if b["query"] == "gold_kpi_10min" and b["watermark"]), default=None)
batch_kpi = (
    silver_rows.groupBy(F.window("event_time", "10 minutes"), "facility_id")
    .agg(F.sum(is_status("ARRIVED")).alias("passages_batch"), F.sum(is_status("CARE_STARTED")).alias("prises_batch"))
    .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "facility_id",
            "passages_batch", "prises_batch")
    .filter(F.col("window_end") <= F.lit(final_watermark).cast("timestamp"))
)
compared = batch_kpi.join(gold_kpi, ["window_start", "facility_id"], "left")
results["gold"] = {
    "kpi_rows": gold_kpi.count(),
    "kpi_closed_windows_compared": compared.count(),
    "kpi_exact_matches": compared.filter(
        (F.col("passages") == F.col("passages_batch")) & (F.col("prises_en_charge") == F.col("prises_batch"))
    ).count(),
    "kpi_dropped_late": sum(b["dropped_late"] for b in log.batches if b["query"] == "gold_kpi_10min"),
    "final_watermark": final_watermark,
    "priority_rows": spark.read.format("delta").load(GOLD_PRIORITY_PATH).count(),
    "session_rows": spark.read.format("delta").load(GOLD_SESSIONS_PATH).count() if delta_table_exists(GOLD_SESSIONS_PATH) else 0,
    "history_versions": spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_KPI_PATH}`").count(),
}

# Relecture batch de la table Gold et de son historique
print("\n[GOLD] relecture batch de kpi_facility_10min", flush=True)
gold_kpi.orderBy(F.desc("window_start"), "facility_id").show(10, truncate=False)
history = spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_KPI_PATH}`").select(
    "version", "timestamp", "operation", "operationMetrics.numTargetRowsInserted",
    "operationMetrics.numTargetRowsUpdated", "operationMetrics.numOutputRows",
)
print("[GOLD] DESCRIBE HISTORY", flush=True)
history.show(12, truncate=False)
(RESULTS_DIR / "partie5_describe_history.txt").write_text(
    history._jdf.showString(1000, 0, False), encoding="utf-8"
)
print("[GOLD] taux de cas prioritaires (fenetre glissante)", flush=True)
spark.read.format("delta").load(GOLD_PRIORITY_PATH).orderBy(F.desc("window_start")).show(5, truncate=False)
if delta_table_exists(GOLD_SESSIONS_PATH):
    print("[GOLD] sessions des postes de triage", flush=True)
    spark.read.format("delta").load(GOLD_SESSIONS_PATH).orderBy("session_start").show(5, truncate=False)
    results["gold"]["session_duration_minutes"] = (
        spark.read.format("delta").load(GOLD_SESSIONS_PATH)
        .agg(F.round(F.avg("duree_activite_min"), 2).alias("moyenne"), F.min("duree_activite_min").alias("min"),
             F.max("duree_activite_min").alias("max")).first().asDict()
    )

results["silver_batches"] = [b for b in silver_batches if b["input_rows"]]
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
(RESULTS_DIR / "partie5.json").write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
print(json.dumps({k: v for k, v in results.items() if k != "silver_batches"}, indent=2, default=str), flush=True)
spark.stop()
