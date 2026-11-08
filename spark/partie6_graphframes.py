"""Partie 6 : graphe patients / etablissements / services avec GraphFrames.

Lancement (ajoute le package GraphFrames, module Python compris) :
    docker compose exec jupyter spark-submit --driver-memory 2g \
        --conf spark.jars.ivy=/home/spark/.ivy2 \
        --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 \
        --repositories https://repos.spark-packages.org \
        spark/partie6_graphframes.py

Source : l'historique nettoye de la Partie 3 (lake/silver/urgences_history/). Le graphe est exporte en Parquet sous
lake/gold/graph_analysis/graphe/ pour l'analyse GraphX en Scala (spark/partie6_graphx.scala).
"""
import json
import time
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from schemas import FACILITY_SCHEMA

SILVER_HISTORY = "s3a://lake/silver/urgences_history/"
FACILITIES_PATH = "s3a://lake/bronze/facilities/"
OUTPUT = "s3a://lake/gold/graph_analysis/"
CHECKPOINT_DIR = "s3a://lake/checkpoints/partie6_graphframes/"
GROUND_TRUTH = Path("data/anomalies_ground_truth.json")
RESULTS_PATH = Path("rapport/resultats/partie6_graphframes.json")

# Seuils explicites des regles d'alerte
MIN_TRANSFERS_PER_PATIENT = 4
MAX_SHARE_OF_GROUP = 0.20  # part des transferts du groupe recus (ou emis) par un etablissement qui n'est pas un CHU

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie6-GraphFrames")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 4)
    .config("spark.scheduler.minRegisteredResourcesRatio", 1.0)
    .config("spark.sql.shuffle.partitions", 8)
    # connectedComponents est iteratif : le texte de son plan, construit pour Spark UI, finissait par saturer le Driver
    .config("spark.sql.maxPlanStringLength", 100000)
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")
# connectedComponents de GraphFrames sauvegarde ses iterations : le repertoire doit etre partage par les Workers
sc.setCheckpointDir(CHECKPOINT_DIR)

results = {}
facilities = spark.read.schema(FACILITY_SCHEMA).json(FACILITIES_PATH)
events = spark.read.parquet(SILVER_HISTORY).join(
    facilities.select("facility_id", "hospital_group", "type"), "facility_id"
)

# 1. Sommets : prefixes P-, F- et D- distincts. Un service est rattache a un groupe hospitalier (D-<groupe>-<service>),
# pour que chaque sommet appartienne a un seul groupe ; un patient prend le groupe de sa premiere admission.
department_id = F.concat(F.lit("D-"), F.col("hospital_group"), F.lit("-"), F.regexp_replace("department", " ", "_"))

first_arrival = (
    events.filter(F.col("status") == "ARRIVED")
    .withColumn("rank", F.row_number().over(Window.partitionBy("patient_id").orderBy("event_time")))
    .filter(F.col("rank") == 1)
)
patient_vertices = first_arrival.select(
    F.col("patient_id").alias("id"), F.lit("PATIENT").alias("vertex_type"), F.col("patient_id").alias("name"),
    "hospital_group", F.lit(None).cast("string").alias("facility_type"),
)
facility_vertices = facilities.select(
    F.col("facility_id").alias("id"), F.lit("ETABLISSEMENT").alias("vertex_type"), "name",
    "hospital_group", F.col("type").alias("facility_type"),
)
department_vertices = events.select(
    department_id.alias("id"), F.lit("SERVICE").alias("vertex_type"), F.col("department").alias("name"),
    "hospital_group", F.lit(None).cast("string").alias("facility_type"),
).distinct()
vertices = patient_vertices.unionByName(facility_vertices).unionByName(department_vertices).cache()

# Aretes : une par passage (ADMITTED_AT, TREATED_IN, TRANSFERRED_TO), une par couple etablissement-service
visit_columns = ["visit_id", "patient_id", "event_time", "hospital_group"]
arrivals = events.filter(F.col("status") == "ARRIVED")
admitted = arrivals.select(F.col("patient_id").alias("src"), F.col("facility_id").alias("dst"),
                           F.lit("ADMITTED_AT").alias("relationship"), *visit_columns)
treated = arrivals.select(F.col("patient_id").alias("src"), department_id.alias("dst"),
                          F.lit("TREATED_IN").alias("relationship"), *visit_columns)
has_department = events.select(
    F.col("facility_id").alias("src"), department_id.alias("dst"), F.lit("HAS_DEPARTMENT").alias("relationship"),
    F.lit(None).cast("string").alias("visit_id"), F.lit(None).cast("string").alias("patient_id"),
    F.lit(None).cast("timestamp").alias("event_time"), "hospital_group",
).distinct()
transferred = events.filter(F.col("status") == "TRANSFERRED").select(
    F.col("facility_id").alias("src"), F.col("transfer_to").alias("dst"),
    F.lit("TRANSFERRED_TO").alias("relationship"), *visit_columns,
)
edges = admitted.unionByName(treated).unionByName(has_department).unionByName(transferred).cache()

# Export pour GraphX, separe par groupe hospitalier
vertices.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "graphe/sommets")
edges.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "graphe/aretes")

# Le graphe est relu depuis cet export : connectedComponents est iteratif et allonge le plan a chaque tour. Construit
# a partir des unions, jointures et fenetres ci-dessus, ce plan saturait la memoire du Driver (OutOfMemoryError).
vertices = spark.read.parquet(OUTPUT + "graphe/sommets").cache()
edges = spark.read.parquet(OUTPUT + "graphe/aretes").cache()
facility_vertices = vertices.filter(F.col("vertex_type") == "ETABLISSEMENT")
admitted = edges.filter(F.col("relationship") == "ADMITTED_AT")
transferred = edges.filter(F.col("relationship") == "TRANSFERRED_TO")

# 2. GraphFrame et controles
graph = GraphFrame(vertices, edges)
results["vertices"] = graph.vertices.count()
results["edges"] = graph.edges.count()
results["vertices_by_group_type"] = [r.asDict() for r in
                                     vertices.groupBy("hospital_group", "vertex_type").count().orderBy("hospital_group", "vertex_type").collect()]
results["edges_by_group_type"] = [r.asDict() for r in
                                  edges.groupBy("hospital_group", "relationship").count().orderBy("hospital_group", "relationship").collect()]
results["duplicate_vertex_ids"] = vertices.groupBy("id").count().filter("count > 1").count()

facility_info = vertices.filter(F.col("vertex_type") == "ETABLISSEMENT").select(
    F.col("id").alias("facility"), F.col("hospital_group").alias("group"), F.col("facility_type").alias("ftype"))
transfers = (
    transferred
    .join(facility_info.withColumnRenamed("facility", "src").withColumnRenamed("group", "src_group")
          .withColumnRenamed("ftype", "src_type"), "src", "left")
    .join(facility_info.withColumnRenamed("facility", "dst").withColumnRenamed("group", "dst_group")
          .withColumnRenamed("ftype", "dst_type"), "dst", "left")
    .cache()
)
results["consistency"] = {
    "transfers": transfers.count(),
    "transfers_to_unknown_facility": transfers.filter(F.col("dst_group").isNull()).count(),
    "self_transfers": transfers.filter(F.col("src") == F.col("dst")).count(),
    "edges_with_unknown_endpoint": edges.join(vertices.select(F.col("id").alias("src")), "src", "left_anti").count()
    + edges.join(vertices.select(F.col("id").alias("dst")), "dst", "left_anti").count(),
}
cross_group = transfers.filter(F.col("src_group") != F.col("dst_group"))
results["cross_group"] = {
    "total": cross_group.count(),
    "to_chu": cross_group.filter(F.col("dst_type") == "CHU").count(),
    "to_non_chu": cross_group.filter(F.col("dst_type") != "CHU").count(),
}

# 3. Motifs sur le graphe des transferts (une arete par transfert)
transfer_graph = GraphFrame(facility_vertices, transferred)
same_patient_after = (F.col("e1.patient_id") == F.col("e2.patient_id")) & (F.col("e1.event_time") < F.col("e2.event_time"))
chains = (
    transfer_graph.find("(a)-[e1]->(b); (b)-[e2]->(c)")
    .filter(same_patient_after & (F.col("a.id") != F.col("c.id")))
    .select(F.col("e1.patient_id").alias("patient_id"), F.col("a.id").alias("a"), F.col("b.id").alias("b"),
            F.col("c.id").alias("c"), F.col("e1.visit_id").alias("visit_1"), F.col("e2.visit_id").alias("visit_2"),
            F.col("a.hospital_group").alias("hospital_group"))
)
loops = (
    transfer_graph.find("(a)-[e1]->(b); (b)-[e2]->(a)")
    .filter(same_patient_after)
    .select(F.col("e1.patient_id").alias("patient_id"), F.col("a.id").alias("a"), F.col("b.id").alias("b"),
            F.col("e1.visit_id").alias("visit_1"), F.col("e2.visit_id").alias("visit_2"),
            F.col("a.hospital_group").alias("hospital_group"))
)
# Boucle au niveau des etablissements seulement (n'importe quels patients) : regle volontairement naive
reciprocal_pairs = (
    transfer_graph.find("(a)-[]->(b); (b)-[]->(a)").filter(F.col("a.id") < F.col("b.id"))
    .select(F.col("a.id").alias("a"), F.col("b.id").alias("b")).distinct()
)
admission_graph = GraphFrame(vertices, admitted)
multi_facility = (
    admission_graph.find("(p)-[e1]->(f1); (p)-[e2]->(f2)").filter(F.col("f1.id") < F.col("f2.id"))
    .select(F.col("p.id").alias("patient_id"), F.col("p.hospital_group").alias("hospital_group")).distinct()
)
chains.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "graphframes/motifs_chaines")
loops.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "graphframes/motifs_boucles")
results["motifs"] = {
    "chains_a_b_c": chains.count(),
    "chain_patients": chains.select("patient_id").distinct().count(),
    "loops_a_b_a": loops.count(),
    "loop_patients": loops.select("patient_id").distinct().count(),
    "reciprocal_facility_pairs": reciprocal_pairs.count(),
    "patients_in_several_facilities": multi_facility.count(),
    "chain_examples": [r.asDict() for r in chains.orderBy("patient_id").limit(3).collect()],
    "loop_examples": [r.asDict() for r in loops.orderBy("patient_id").limit(3).collect()],
}

# Degres du graphe etablissement -> etablissement, et temps de calcul (compare a GraphX en question 5)
start = time.perf_counter()
in_degrees = transfer_graph.inDegrees
out_degrees = transfer_graph.outDegrees
degrees = (
    facility_vertices.select("id", "name", "hospital_group", "facility_type")
    .join(in_degrees, "id", "left").join(out_degrees, "id", "left")
    .fillna(0, ["inDegree", "outDegree"])
    .cache()
)
degree_rows = degrees.count()
results["degrees_seconds"] = round(time.perf_counter() - start, 3)
group_totals = transfers.filter(F.col("src_group") == F.col("dst_group")).groupBy(F.col("src_group").alias("hospital_group")).agg(
    F.count("*").alias("group_transfers"))
intra_received = transfers.filter(F.col("src_group") == F.col("dst_group")).groupBy(F.col("dst").alias("id")).agg(
    F.count("*").alias("received_in_group"))
intra_sent = transfers.filter(F.col("src_group") == F.col("dst_group")).groupBy(F.col("src").alias("id")).agg(
    F.count("*").alias("sent_in_group"))
degrees = (
    degrees.join(intra_received, "id", "left").join(intra_sent, "id", "left").join(group_totals, "hospital_group")
    .fillna(0, ["received_in_group", "sent_in_group"])
    .withColumn("share_received", F.round(F.col("received_in_group") / F.col("group_transfers"), 4))
    .withColumn("share_sent", F.round(F.col("sent_in_group") / F.col("group_transfers"), 4))
    .cache()
)
degrees.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "graphframes/degres_etablissements")
results["top_receivers"] = [r.asDict() for r in degrees.orderBy(F.desc("inDegree")).limit(5).select(
    "id", "facility_type", "hospital_group", "inDegree", "share_received").collect()]
results["top_senders"] = [r.asDict() for r in degrees.orderBy(F.desc("outDegree")).limit(5).select(
    "id", "facility_type", "hospital_group", "outDegree", "share_sent").collect()]

# Composantes connexes du graphe complet et du graphe des transferts
start = time.perf_counter()
components = graph.connectedComponents()
results["components_seconds"] = round(time.perf_counter() - start, 3)
results["components_full_graph"] = components.select("component").distinct().count()
results["largest_component_vertices"] = components.groupBy("component").count().agg(F.max("count")).first()[0]
transfer_components = transfer_graph.connectedComponents()
results["components_transfer_graph"] = [
    r.asDict() for r in transfer_components.groupBy("component").agg(
        F.count("*").alias("facilities"), F.collect_set("hospital_group").alias("groups")).orderBy(F.desc("facilities")).collect()
]
transfer_components.select("id", "hospital_group", "component").write.mode("overwrite").partitionBy("hospital_group").parquet(
    OUTPUT + "graphframes/composantes_transferts")

# 6. Regles d'alerte, avec des seuils explicites
transfers_per_patient = transfers.groupBy("patient_id").agg(
    F.count("*").alias("transfers"), F.first("src_group").alias("hospital_group"))
alerts = (
    transfers_per_patient.filter(F.col("transfers") >= MIN_TRANSFERS_PER_PATIENT)
    .select(F.lit("transferts_frequents").alias("rule"), F.col("patient_id").alias("subject"), "hospital_group",
            F.concat(F.col("transfers"), F.lit(" transferts")).alias("detail"))
    .unionByName(
        loops.groupBy("patient_id", "hospital_group").agg(F.first(F.concat("a", F.lit(" -> "), "b", F.lit(" -> "), "a")).alias("detail"))
        .select(F.lit("boucle_transfert").alias("rule"), F.col("patient_id").alias("subject"), "hospital_group", "detail"))
    .unionByName(
        cross_group.filter(F.col("dst_type") != "CHU")
        .select(F.lit("inter_groupes_hors_chu").alias("rule"), F.col("visit_id").alias("subject"),
                F.col("src_group").alias("hospital_group"),
                F.concat("src", F.lit(" -> "), "dst", F.lit(" ("), "dst_type", F.lit(", "), "dst_group", F.lit(")")).alias("detail")))
    .unionByName(
        degrees.filter((F.col("facility_type") != "CHU") & (F.col("share_received") >= MAX_SHARE_OF_GROUP))
        .select(F.lit("part_recue_elevee").alias("rule"), F.col("id").alias("subject"), "hospital_group",
                F.concat(F.round(F.col("share_received") * 100, 1), F.lit(" % des transferts du groupe")).alias("detail")))
    .unionByName(
        degrees.filter((F.col("facility_type") != "CHU") & (F.col("share_sent") >= MAX_SHARE_OF_GROUP))
        .select(F.lit("part_emise_elevee").alias("rule"), F.col("id").alias("subject"), "hospital_group",
                F.concat(F.round(F.col("share_sent") * 100, 1), F.lit(" % des transferts du groupe")).alias("detail")))
    .cache()
)
alerts.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "anomalies")
alert_rows = [r.asDict() for r in alerts.orderBy("rule", "subject").collect()]
results["alerts"] = alert_rows

# Pertinence des regles : comparaison avec les cas injectes par le generateur
truth = json.loads(GROUND_TRUTH.read_text(encoding="utf-8"))
expected = {
    "transferts_frequents": {c["patient_id"] for c in truth if c["rule"] == "frequent_transfers"},
    "boucle_transfert": {c["patient_id"] for c in truth if c["rule"] == "transfer_loop"},
    "inter_groupes_hors_chu": {c["visit_ids"][0] for c in truth if c["rule"] == "cross_group_non_chu"},
    "part_recue_elevee": {c["facility_id"] for c in truth if c["rule"] == "receiver_hub"},
    "part_emise_elevee": {c["facility_id"] for c in truth if c["rule"] == "sender_hub"},
}
evaluation = {}
for rule, wanted in expected.items():
    found = {a["subject"] for a in alert_rows if a["rule"] == rule}
    evaluation[rule] = {
        "injectes": len(wanted), "detectes": len(found), "vrais_positifs": len(found & wanted),
        "detectes_non_injectes": sorted(found - wanted), "manques": sorted(wanted - found),
    }
results["rule_evaluation"] = evaluation
results["degree_share_by_type"] = [r.asDict() for r in degrees.groupBy("hospital_group", "facility_type").agg(
    F.round(F.max("share_received"), 3).alias("max_share_received"), F.round(F.max("share_sent"), 3).alias("max_share_sent")
).orderBy("hospital_group", "facility_type").collect()]

RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
RESULTS_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
print(json.dumps({k: v for k, v in results.items() if k not in ("alerts",)}, indent=2, ensure_ascii=False, default=str))
print(f"\n{len(alert_rows)} alertes ; degres calcules pour {degree_rows} etablissements")
spark.stop()
