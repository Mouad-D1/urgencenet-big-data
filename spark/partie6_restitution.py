"""Partie 6 : comparaison GraphX / GraphFrames, table de metriques par etablissement et visualisation.

A lancer apres spark/partie6_graphframes.py et spark/partie6_graphx.scala :
    docker compose exec jupyter spark-submit spark/partie6_restitution.py
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.lines import Line2D
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUTPUT = "s3a://lake/gold/graph_analysis/"
RESULTS = Path("rapport/resultats")
FIGURE = Path("rapport/captures/partie6_graphe_transferts.png")

# Palette categorielle de reference (3 premieres teintes) et gris neutre ; la forme double la couleur
TYPE_STYLE = {
    "CHU": ("#2a78d6", "o", "CHU"),
    "HOPITAL_REGIONAL": ("#eb6834", "s", "Hôpital régional"),
    "CLINIQUE": ("#1baf7a", "D", "Clinique"),
    "CENTRE_SANTE": ("#8a8984", "^", "Centre de santé"),
}
INK, MUTED, EDGE = "#0b0b0b", "#52514e", "#b5b4ae"

spark = (
    SparkSession.builder
    .appName("UrgenceNet-Partie6-Restitution")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 4)
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

graphx = spark.read.parquet(OUTPUT + "graphx/metriques_etablissements")
graphframes = spark.read.parquet(OUTPUT + "graphframes/degres_etablissements").select(
    F.col("id").alias("facility_id"), "name", "inDegree", "outDegree", "share_received", "share_sent")
alerts = spark.read.parquet(OUTPUT + "anomalies")

# 5. Memes degres sur le meme sous-graphe (tous les transferts) ?
compared = graphx.join(graphframes, "facility_id", "full")
mismatches = compared.filter(
    (F.col("transferts_recus") != F.col("inDegree")) | (F.col("transferts_emis") != F.col("outDegree"))
    | F.col("inDegree").isNull() | F.col("transferts_recus").isNull()
).count()

facility_alerts = (
    alerts.filter(F.col("rule").isin("part_recue_elevee", "part_emise_elevee"))
    .groupBy(F.col("subject").alias("facility_id")).agg(F.concat_ws(", ", F.collect_list("rule")).alias("alertes"))
)
metrics = (
    compared.join(facility_alerts, "facility_id", "left")
    .select("hospital_group", "facility_id", "name", "facility_type",
            F.col("inDegree").alias("transferts_recus"), F.col("outDegree").alias("transferts_emis"),
            "transferts_recus_groupe", "transferts_emis_groupe", "partenaires_amont_groupe", "partenaires_aval_groupe",
            F.round("share_received", 3).alias("part_recue_groupe"), F.round("share_sent", 3).alias("part_emise_groupe"),
            F.round("pagerank", 4).alias("pagerank"), "composante", F.coalesce("alertes", F.lit("")).alias("alertes"))
    .orderBy("hospital_group", F.desc("pagerank"))
    .cache()
)
metrics.write.mode("overwrite").partitionBy("hospital_group").parquet(OUTPUT + "metriques_etablissements")
rows = [r.asDict() for r in metrics.collect()]
RESULTS.mkdir(parents=True, exist_ok=True)
metrics.toPandas().to_csv(RESULTS / "partie6_metriques_etablissements.csv", index=False, encoding="utf-8")

# 7. Visualisation des transferts internes a chaque groupe
weights = spark.read.parquet(OUTPUT + "graphx/transferts_ponderes").collect()
groups = sorted({r["hospital_group"] for r in rows})
fig, axes = plt.subplots(1, len(groups), figsize=(18, 6.6), facecolor="white")
max_rank = max(r["pagerank"] for r in rows)
max_weight = max(r["poids"] for r in weights)
for ax, group in zip(axes, groups):
    nodes = [r for r in rows if r["hospital_group"] == group]
    graph = nx.DiGraph()
    for r in nodes:
        graph.add_node(r["facility_id"], **r)
    for w in weights:
        if w["hospital_group"] == group:
            graph.add_edge(w["src"], w["dst"], weight=w["poids"])
    positions = nx.spring_layout(graph, weight="weight", seed=7, k=1.6)
    nx.draw_networkx_edges(
        graph, positions, ax=ax, edge_color=EDGE, arrows=True, arrowsize=8, connectionstyle="arc3,rad=0.12",
        width=[0.4 + 3.6 * graph.edges[e]["weight"] / max_weight for e in graph.edges], node_size=500,
    )
    for ftype, (color, marker, _) in TYPE_STYLE.items():
        members = [n for n in graph.nodes if graph.nodes[n]["facility_type"] == ftype]
        if not members:
            continue
        flagged = [n for n in members if graph.nodes[n]["alertes"]]
        nx.draw_networkx_nodes(
            graph, positions, nodelist=members, ax=ax, node_color=color, node_shape=marker,
            node_size=[160 + 1400 * graph.nodes[n]["pagerank"] / max_rank for n in members],
            edgecolors=["#0b0b0b" if n in flagged else "white" for n in members],
            linewidths=[2.5 if n in flagged else 1.0 for n in members],
        )
    # Etiquette sous le noeud, d'autant plus bas que le noeud est grand, sur fond blanc pour rester lisible
    for n, (x, y) in positions.items():
        offset = 0.09 + 0.12 * graph.nodes[n]["pagerank"] / max_rank
        label = n + (" · alerte" if graph.nodes[n]["alertes"] else "")
        ax.text(x, y - offset, label, fontsize=7.5, color=INK, ha="center", va="top",
                fontweight="bold" if graph.nodes[n]["alertes"] else "normal",
                bbox={"boxstyle": "round,pad=0.15", "facecolor": "white", "edgecolor": "none", "alpha": 0.85})
    ax.set_title(f"{group} : {graph.number_of_nodes()} établissements, "
                 f"{sum(d['weight'] for _, _, d in graph.edges(data=True))} transferts internes",
                 fontsize=11, color=INK, loc="left")
    ax.axis("off")

legend = [Line2D([0], [0], marker=m, linestyle="", markerfacecolor=c, markeredgecolor="white", markersize=10, label=label)
          for c, m, label in TYPE_STYLE.values()]
legend.append(Line2D([0], [0], marker="o", linestyle="", markerfacecolor="white", markeredgecolor="#0b0b0b",
                     markeredgewidth=2.5, markersize=10, label="Établissement en alerte"))
fig.legend(handles=legend, loc="lower center", ncol=5, frameon=False, fontsize=9.5)
fig.suptitle("Transferts entre établissements d'un même groupe hospitalier. Taille : PageRank pondéré (GraphX), "
             "épaisseur : nombre de transferts.", fontsize=11.5, color=MUTED, x=0.01, ha="left")
fig.tight_layout(rect=(0, 0.06, 1, 0.95))
FIGURE.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(FIGURE, dpi=150)

summary = {"degree_mismatches_graphx_vs_graphframes": mismatches, "facilities": len(rows), "figure": str(FIGURE)}
(RESULTS / "partie6_restitution.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2, ensure_ascii=False))
metrics.drop("name").show(40, truncate=False)
spark.stop()
