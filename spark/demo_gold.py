"""Soutenance : affiche les derniers indicateurs Gold et l'historique de la table Delta.

Lancement : docker compose exec jupyter spark-submit spark/demo_gold.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_KPI_PATH = "s3a://lake/gold/kpi_facility_10min/"

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Demo-Gold")
    .master("spark://spark-master:7077")
    # Laisse des coeurs libres pour le pipeline de streaming qui tourne pendant la demonstration
    .config("spark.cores.max", 1)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

kpi = spark.read.format("delta").load(GOLD_KPI_PATH)
print(f"Lignes dans kpi_facility_10min : {kpi.count()}")
kpi.orderBy(F.desc("window_start"), F.desc("passages")).show(10, truncate=False)
spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_KPI_PATH}`").select(
    "version", "timestamp", "operation", "operationMetrics.numTargetRowsInserted", "operationMetrics.numTargetRowsUpdated"
).show(8, truncate=False)
spark.stop()
