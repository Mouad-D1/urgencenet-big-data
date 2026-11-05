"""Partie 1 : connexion au cluster standalone et verification de l'environnement.

Lancement : docker compose exec jupyter spark-submit spark/partie1_cluster.py
"""
import time

from pyspark.sql import SparkSession

EXPECTED_EXECUTORS = 2
LAKE_TEST_PATH = "s3a://lake/_healthcheck"

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie1")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")


def executor_count():
    # getExecutorMemoryStatus compte aussi le Driver
    return sc._jsc.sc().getExecutorMemoryStatus().size() - 1


# defaultParallelism vaut 2 tant que les Executors ne sont pas enregistres aupres du Driver
deadline = time.time() + 60
while executor_count() < EXPECTED_EXECUTORS and time.time() < deadline:
    time.sleep(1)

print("=" * 60)
print(f"Version de Spark     : {spark.version}")
print(f"Master               : {sc.master}")
print(f"appName              : {sc.appName}")
print(f"Executors enregistres: {executor_count()}")
print(f"sc.defaultParallelism: {sc.defaultParallelism}")
print("=" * 60)

# Le calcul doit reellement s'executer sur les deux Workers
hosts = (
    sc.parallelize(range(sc.defaultParallelism * 4), sc.defaultParallelism * 4)
    .map(lambda _: __import__("socket").gethostname())
    .distinct()
    .collect()
)
print(f"Workers ayant execute des taches : {sorted(hosts)}")

# Ecriture puis relecture dans le bucket lake via S3A, depuis les Executors
spark.range(1000).write.mode("overwrite").parquet(LAKE_TEST_PATH)
count = spark.read.parquet(LAKE_TEST_PATH).count()
print(f"Aller-retour Parquet sur {LAKE_TEST_PATH} : {count} lignes")

path = sc._jvm.org.apache.hadoop.fs.Path(LAKE_TEST_PATH)
path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(path, True)

spark.stop()
