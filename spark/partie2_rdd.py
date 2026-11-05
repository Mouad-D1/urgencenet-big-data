"""Partie 2 : traitement batch de l'historique avec l'API RDD.

Lancement : docker compose exec jupyter spark-submit spark/partie2_rdd.py
L'option --hold 600 garde l'application active 10 minutes pour consulter le DAG sur http://localhost:4040.

Conformement au sujet, les doublons sont conserves ici : ils ne sont supprimes qu'en Partie 3.
"""
import argparse
import json
import statistics
import time
from operator import add
from pathlib import Path

from pyspark.sql import SparkSession

HISTORY_PATH = "s3a://lake/bronze/history/"
RESULTS_PATH = Path("rapport/resultats/partie2.json")
BENCHMARK_RUNS = 5

parser = argparse.ArgumentParser()
parser.add_argument("--hold", type=int, default=0, help="secondes pendant lesquelles garder Spark UI ouverte")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie2-RDD")
    .master("spark://spark-master:7077")
    # Attend les 4 coeurs du cluster : sinon defaultParallelism vaut 2 au moment de textFile
    .config("spark.cores.max", 4)
    .config("spark.scheduler.minRegisteredResourcesRatio", 1.0)
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")


def run_action(label, action):
    """Execute une action dans un groupe de jobs nomme, puis mesure sa duree et ses stages."""
    sc.setJobGroup(label, label)
    start = time.perf_counter()
    result = action()
    elapsed = time.perf_counter() - start
    tracker = sc.statusTracker()
    jobs = sorted(tracker.getJobIdsForGroup(label))
    stages = sum(len(tracker.getJobInfo(job).stageIds) for job in jobs)
    print(f"[{label}] {elapsed:.2f} s, job(s) {jobs}, {stages} stage(s)")
    return result, {"seconds": round(elapsed, 3), "jobs": jobs, "stages": stages}


def load_care_started():
    return (
        sc.textFile(HISTORY_PATH, minPartitions=sc.defaultParallelism)
        .map(json.loads)
        .filter(lambda e: e["status"] == "CARE_STARTED")
    )


def average_wait_by_city(care_events):
    return (
        care_events
        .map(lambda e: (e["city"], e["wait_minutes"]))
        .aggregateByKey(
            (0, 0),
            lambda acc, wait: (acc[0] + wait, acc[1] + 1),
            lambda left, right: (left[0] + right[0], left[1] + right[1]),
        )
        .mapValues(lambda acc: acc[0] / acc[1])
    )


# 1. Chargement et parsing : transformations paresseuses, rien n'est encore execute
events = sc.textFile(HISTORY_PATH, minPartitions=sc.defaultParallelism).map(json.loads)

# 2. Prises en charge, dont le delai d'attente est connu
care_started = events.filter(lambda e: e["status"] == "CARE_STARTED")

# 3. Nombre de prises en charge par ville
care_by_city_rdd = care_started.map(lambda e: (e["city"], 1)).reduceByKey(add)

# 4. Delai d'attente moyen par ville : somme et nombre calcules en un seul passage
wait_by_city_rdd = average_wait_by_city(care_started)

# 5. Passages par niveau de priorite, a partir des evenements TRIAGED
triaged_by_priority_rdd = (
    events
    .filter(lambda e: e["status"] == "TRIAGED")
    .map(lambda e: (e["priority"], 1))
    .reduceByKey(add)
)

# 6. Top 5 des etablissements par nombre de prises en charge
care_by_facility_rdd = care_started.map(lambda e: (e["facility_id"], 1)).reduceByKey(add)

# Les actions ci-dessous declenchent les jobs
care_by_city, care_metrics = run_action("prises_en_charge_par_ville", care_by_city_rdd.collect)
wait_by_city, wait_metrics = run_action("attente_moyenne_par_ville", wait_by_city_rdd.collect)
by_priority, priority_metrics = run_action("passages_par_priorite", triaged_by_priority_rdd.collect)
top_facilities, top_metrics = run_action(
    "top5_etablissements",
    lambda: care_by_facility_rdd.takeOrdered(5, key=lambda item: (-item[1], item[0])),
)

care_counts = dict(care_by_city)
print("\nVille        | Prises en charge | Attente moyenne (min)")
for city, average in sorted(wait_by_city):
    print(f"{city:<12} | {care_counts[city]:>16} | {average:>21.2f}")

print("\nPriorite | Passages")
for priority, count in sorted(by_priority):
    print(f"{priority:>8} | {count:>8}")

print("\nTop 5 des etablissements par prises en charge")
for facility, count in top_facilities:
    print(f"{facility} : {count}")

# 7. Lineage : toDebugString decrit le plan sans lancer de job
print("\nLineage de l'attente moyenne par ville :")
print(wait_by_city_rdd.toDebugString().decode())

# Relancer une action sur le meme RDD reutilise les fichiers de shuffle : Spark UI affiche
# alors le premier stage comme "skipped", sans relire ni reparser le fichier
_, reuse_metrics = run_action("reutilisation_shuffle", wait_by_city_rdd.collect)

# Temps de reference pour la comparaison RDD / DataFrame de la Partie 3. Le RDD est reconstruit
# a chaque execution : sans cela, le shuffle reutilise fausserait la mesure.
sc.setJobGroup("mesure_attente_moyenne", "mesure_attente_moyenne")
timings = []
for _ in range(BENCHMARK_RUNS):
    start = time.perf_counter()
    average_wait_by_city(load_care_started()).collect()
    timings.append(time.perf_counter() - start)
print(f"\nAttente moyenne par ville, {BENCHMARK_RUNS} executions : mediane {statistics.median(timings):.2f} s")

RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
RESULTS_PATH.write_text(json.dumps({
    "care_by_city": dict(sorted(care_by_city)),
    "average_wait_by_city": {city: round(value, 4) for city, value in sorted(wait_by_city)},
    "passages_by_priority": {str(priority): count for priority, count in sorted(by_priority)},
    "top5_facilities": top_facilities,
    "jobs": {
        "prises_en_charge_par_ville": care_metrics,
        "attente_moyenne_par_ville": wait_metrics,
        "passages_par_priorite": priority_metrics,
        "top5_etablissements": top_metrics,
        "reutilisation_shuffle": reuse_metrics,
    },
    "benchmark_average_wait_seconds": {
        "runs": [round(t, 3) for t in timings],
        "median": round(statistics.median(timings), 3),
    },
}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

if args.hold:
    print(f"\nSpark UI disponible sur http://localhost:4040 pendant {args.hold} s", flush=True)
    time.sleep(args.hold)

spark.stop()
