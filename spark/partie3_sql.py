"""Partie 3 : Spark SQL et DataFrames sur l'historique.

Lancement : docker compose exec jupyter spark-submit spark/partie3_sql.py
L'option --hold 600 garde l'application active 10 minutes pour consulter Spark UI sur http://localhost:4040.
"""
import argparse
import contextlib
import io
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from schemas import EVENT_SCHEMA, FACILITY_SCHEMA

HISTORY_PATH = "s3a://lake/bronze/history/"
FACILITIES_PATH = "s3a://lake/bronze/facilities/"
SILVER_PATH = "s3a://lake/silver/urgences_history/"
RESULTS_DIR = Path("rapport/resultats")
BENCHMARK_RUNS = 5

parser = argparse.ArgumentParser()
parser.add_argument("--hold", type=int, default=0, help="secondes pendant lesquelles garder Spark UI ouverte")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie3-SQL")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 4)
    .config("spark.scheduler.minRegisteredResourcesRatio", 1.0)
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")


def section(title):
    print(f"\n=== {title}")
    sc.setJobGroup(title, title)


def rows_as_dicts(df):
    return [row.asDict() for row in df.collect()]


def benchmark(*actions):
    """Mediane par action, apres une execution de chauffe ecartee, en alternant les actions.

    Chaque action doit construire un nouveau DataFrame : relancer collect() sur un DataFrame deja
    execute reutilise son plan adaptatif et ses fichiers de shuffle, ce qui fausserait la mesure.
    """
    for action in actions:
        action()
    timings = [[] for _ in actions]
    for _ in range(BENCHMARK_RUNS):
        for action, measures in zip(actions, timings):
            start = time.perf_counter()
            action()
            measures.append(time.perf_counter() - start)
    return [{"runs": [round(t, 3) for t in measures], "median": round(statistics.median(measures), 3)}
            for measures in timings]


def explain_text(df, extended=False):
    """Execute explain(), l'affiche et retourne son texte pour l'archiver."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(extended)
    print(buffer.getvalue())
    return buffer.getvalue()


results = {}

# 1. Lecture avec schema explicite, sans inference
section("lecture")
events = spark.read.schema(EVENT_SCHEMA).json(HISTORY_PATH)
facilities = spark.read.schema(FACILITY_SCHEMA).json(FACILITIES_PATH)
events.printSchema()
results["events_rows"] = events.count()
results["facilities_rows"] = facilities.count()
results["null_event_time"] = events.filter(F.col("event_time").isNull()).count()
print(f"Evenements : {results['events_rows']}, etablissements : {results['facilities_rows']}, "
      f"event_time nuls : {results['null_event_time']}")

# 2. Attente moyenne par ville : API DataFrame puis SQL
section("attente_par_ville")
care_started = events.filter(F.col("status") == "CARE_STARTED")
wait_api = (
    care_started
    .groupBy("city")
    .agg(F.count("*").alias("prises_en_charge"), F.avg("wait_minutes").alias("attente_moyenne"))
    .orderBy("city")
)
wait_api.show(truncate=False)

events.createOrReplaceTempView("urgences")
wait_sql = spark.sql("""
    SELECT city, COUNT(*) AS prises_en_charge, AVG(wait_minutes) AS attente_moyenne
    FROM urgences
    WHERE status = 'CARE_STARTED'
    GROUP BY city
    ORDER BY city
""")
wait_sql.show(truncate=False)
results["wait_by_city"] = rows_as_dicts(wait_api)
results["api_equals_sql"] = results["wait_by_city"] == rows_as_dicts(wait_sql)
results["api_sql_same_plan"] = wait_api.sameSemantics(wait_sql)
print(f"Memes resultats API/SQL : {results['api_equals_sql']}, meme plan : {results['api_sql_same_plan']}")

# 3. Jointure avec le referentiel. Le filtre est ecrit apres la jointure :
# c'est Catalyst qui le repousse sous la jointure (voir explain en question 7).
section("jointure_type_ville")


def care_by_type_and_city(source):
    return (
        source
        .join(facilities.select("facility_id", "type"), "facility_id")
        .filter(F.col("status") == "CARE_STARTED")
        .groupBy("type", "city")
        .agg(F.count("*").alias("prises_en_charge"), F.round(F.avg("wait_minutes"), 2).alias("attente_moyenne"))
        .orderBy("type", "city")
    )


by_type_city = care_by_type_and_city(events)
by_type_city.show(truncate=False)
results["care_by_type_city_raw"] = rows_as_dicts(by_type_city)


# 4. Tranches d'attente : UDF Python puis expression native
section("tranches_attente")


def wait_band(wait_minutes):
    if wait_minutes is None:
        return None
    if wait_minutes < 15:
        return "court"
    if wait_minutes <= 45:
        return "moyen"
    return "long"


wait_band_udf = F.udf(wait_band, StringType())
wait_band_native = (
    F.when(F.col("wait_minutes") < 15, "court")
    .when(F.col("wait_minutes") <= 45, "moyen")
    .when(F.col("wait_minutes") > 45, "long")
)

# Le cache isole le cout du calcul de la tranche de celui de la lecture du JSON
events_cached = events.cache()
events_cached.count()


def count_by_band(band_column):
    return events_cached.withColumn("wait_band", band_column).groupBy("wait_band").count().orderBy("wait_band")


bands_udf = count_by_band(wait_band_udf("wait_minutes"))
bands_native = count_by_band(wait_band_native)
bands_udf.show()
results["bands_raw"] = rows_as_dicts(bands_udf)
results["udf_equals_native"] = results["bands_raw"] == rows_as_dicts(bands_native)
print(f"Memes resultats UDF/when : {results['udf_equals_native']}")

print("Plan physique avec UDF :")
plan_udf = explain_text(bands_udf)
print("Plan physique avec when/otherwise :")
plan_native = explain_text(bands_native)
results["benchmark_band_udf_seconds"], results["benchmark_band_native_seconds"] = benchmark(
    lambda: count_by_band(wait_band_udf("wait_minutes")).collect(),
    lambda: count_by_band(wait_band_native).collect(),
)
print(f"UDF : mediane {results['benchmark_band_udf_seconds']['median']} s, "
      f"when/otherwise : mediane {results['benchmark_band_native_seconds']['median']} s")
events_cached.unpersist()

# 6. Dedoublonnage, necessaire des la question 5 : un doublon multiplierait les lignes de l'auto-jointure
section("dedoublonnage")
deduped = events.dropDuplicates(["visit_id", "status"])
results["rows_before_dedup"] = events.count()
results["rows_after_dedup"] = deduped.count()
results["duplicates_removed"] = results["rows_before_dedup"] - results["rows_after_dedup"]
print(f"Avant : {results['rows_before_dedup']}, apres : {results['rows_after_dedup']}, "
      f"doublons supprimes : {results['duplicates_removed']}")
status_before = events.groupBy("status").count().withColumnRenamed("count", "avant")
status_after = deduped.groupBy("status").count().withColumnRenamed("count", "apres")
dedup_by_status = status_before.join(status_after, "status").orderBy("status")
dedup_by_status.show(truncate=False)
results["dedup_by_status"] = rows_as_dicts(dedup_by_status)


# 5. Recalcul du delai par auto-jointure ARRIVED / CARE_STARTED
section("auto_jointure")


def arrived_care_pairs(source):
    arrived = source.filter(F.col("status") == "ARRIVED").select("visit_id", F.col("event_time").alias("arrived_time"))
    care = source.filter(F.col("status") == "CARE_STARTED").select(
        "visit_id", "city", "wait_minutes", F.col("event_time").alias("care_time")
    )
    return care.join(arrived, "visit_id")


pairs = arrived_care_pairs(deduped).withColumn(
    "computed_wait", (F.unix_timestamp("care_time") - F.unix_timestamp("arrived_time")) / 60
)
results["self_join_rows_with_duplicates"] = arrived_care_pairs(events).count()
results["self_join_rows"] = pairs.count()
results["care_started_after_dedup"] = deduped.filter(F.col("status") == "CARE_STARTED").count()
results["wait_mismatches"] = pairs.filter(F.col("computed_wait") != F.col("wait_minutes")).count()
print(f"Lignes de l'auto-jointure : {results['self_join_rows_with_duplicates']} avec doublons, "
      f"{results['self_join_rows']} sans doublons, pour {results['care_started_after_dedup']} CARE_STARTED uniques")
print(f"Ecarts entre delai recalcule et wait_minutes : {results['wait_mismatches']}")
wait_compare = (
    pairs.groupBy("city")
    .agg(F.round(F.avg("computed_wait"), 4).alias("attente_recalculee"),
         F.round(F.avg("wait_minutes"), 4).alias("attente_champ"))
    .orderBy("city")
)
wait_compare.show(truncate=False)
results["wait_compare"] = rows_as_dicts(wait_compare)

by_type_city_clean = care_by_type_and_city(deduped)
by_type_city_clean.show(truncate=False)
results["care_by_type_city_clean"] = rows_as_dicts(by_type_city_clean)

# Ecriture Silver : repartition par ville pour eviter une multitude de petits fichiers par dossier
section("ecriture_silver")
deduped.repartition("city").write.mode("overwrite").partitionBy("city").parquet(SILVER_PATH)
silver = spark.read.parquet(SILVER_PATH)
results["silver_rows"] = silver.count()
silver_by_city = silver.groupBy("city").count().orderBy("city")
silver_by_city.show(truncate=False)
results["silver_by_city"] = rows_as_dicts(silver_by_city)
print(f"Lignes relues depuis Silver : {results['silver_rows']}")

# 7. Plans logique, optimise et physique de la jointure
section("explain_jointure")
plan_join = explain_text(by_type_city, extended=True)
by_type_city.collect()

# 8. Temps de reference, a comparer a la mediane RDD de la Partie 2 : meme calcul, lecture complete
section("mesure_attente_moyenne")
[results["benchmark_average_wait_seconds"]] = benchmark(
    lambda: (
        spark.read.schema(EVENT_SCHEMA).json(HISTORY_PATH)
        .filter(F.col("status") == "CARE_STARTED")
        .groupBy("city")
        .agg(F.count("*"), F.avg("wait_minutes"))
        .collect()
    )
)
print(f"Attente moyenne par ville, {BENCHMARK_RUNS} executions : "
      f"mediane {results['benchmark_average_wait_seconds']['median']} s")

RESULTS_DIR.mkdir(parents=True, exist_ok=True)
(RESULTS_DIR / "partie3_explain.txt").write_text(
    "=== explain(True) de la jointure evenements / etablissements\n" + plan_join
    + "\n=== Plan physique avec UDF\n" + plan_udf
    + "\n=== Plan physique avec when/otherwise\n" + plan_native,
    encoding="utf-8",
)
(RESULTS_DIR / "partie3.json").write_text(
    json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
)

if args.hold:
    print(f"\nSpark UI disponible sur http://localhost:4040 pendant {args.hold} s", flush=True)
    time.sleep(args.hold)

spark.stop()
