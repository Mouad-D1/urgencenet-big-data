#!/usr/bin/env python3
"""Genere rapport/rapport_jalon1.pdf (reportlab, polices Windows Calibri et Consolas).

Lancement depuis la racine du projet : python rapport/build_rapport.py
Les chiffres des tableaux viennent de rapport/resultats/, produits par les scripts spark/.
"""
import io
import json
import re
from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    KeepTogether,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

HERE = Path(__file__).resolve().parent
CAPTURES = HERE / "captures"
OUTPUT = HERE / "rapport_jalon1.pdf"
REPOSITORY = "https://github.com/Mouad-D1/urgencenet-big-data"

AUTHORS = "Mouad DIOUANE et Achraf MALKI"

FONTS = Path("C:/Windows/Fonts")
pdfmetrics.registerFont(TTFont("Body", FONTS / "calibri.ttf"))
pdfmetrics.registerFont(TTFont("Body-Bold", FONTS / "calibrib.ttf"))
pdfmetrics.registerFont(TTFont("Body-Italic", FONTS / "calibrii.ttf"))
pdfmetrics.registerFont(TTFont("Mono", FONTS / "consola.ttf"))
pdfmetrics.registerFontFamily("Body", normal="Body", bold="Body-Bold", italic="Body-Italic", boldItalic="Body-Bold")

INK = colors.HexColor("#1F2933")
ACCENT = colors.HexColor("#1F4E79")
MUTED = colors.HexColor("#5F6B76")
RULE = colors.HexColor("#C9D1D9")
SHADE = colors.HexColor("#F2F5F8")

BODY = ParagraphStyle("body", fontName="Body", fontSize=10.5, leading=13.4, textColor=INK, alignment=TA_JUSTIFY, spaceAfter=5)
TITLE = ParagraphStyle("title", fontName="Body-Bold", fontSize=19, leading=23, textColor=ACCENT, spaceAfter=2)
SUBTITLE = ParagraphStyle("subtitle", fontName="Body", fontSize=12.5, leading=16, textColor=INK, spaceAfter=4)
META = ParagraphStyle("meta", fontName="Body", fontSize=9.5, leading=12.5, textColor=MUTED, spaceAfter=8)
H1 = ParagraphStyle("h1", fontName="Body-Bold", fontSize=14, leading=17, textColor=ACCENT, spaceBefore=11, spaceAfter=5, keepWithNext=1)
H2 = ParagraphStyle("h2", fontName="Body-Bold", fontSize=11, leading=14, textColor=INK, spaceBefore=7, spaceAfter=3, keepWithNext=1)
BULLET = ParagraphStyle("bullet", parent=BODY, leftIndent=12, bulletIndent=2, spaceAfter=3)
CELL = ParagraphStyle("cell", fontName="Body", fontSize=9.5, leading=11.6, textColor=INK)
CELL_HEAD = ParagraphStyle("cellhead", parent=CELL, fontName="Body-Bold")
CAPTION = ParagraphStyle("caption", fontName="Body-Italic", fontSize=9, leading=11, textColor=MUTED, alignment=TA_CENTER, spaceBefore=2)
CODE = ParagraphStyle("code", fontName="Mono", fontSize=7.4, leading=9.2, textColor=INK)

CONTENT_WIDTH = A4[0] - 4 * cm


def markup(text):
    """Echappe le texte, puis applique `code`, **gras** et les espaces insecables du francais."""
    text = escape(text)
    text = re.sub(r"`([^`]+)`", r'<font name="Mono" size="9">\1</font>', text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    return re.sub(r" ([:;?!%])(?=[\s<]|$)", "\u00a0\\1", text)


def p(text, style=BODY):
    return Paragraph(markup(text), style)


def bullets(*items):
    return [Paragraph(markup(item), BULLET, bulletText="•") for item in items]


def code(text):
    table = Table([[Preformatted(text.strip("\n"), CODE)]], colWidths=[CONTENT_WIDTH])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), SHADE),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return [table, Spacer(1, 6)]


def table(rows, widths):
    total = sum(widths)
    col_widths = [w / total * CONTENT_WIDTH for w in widths]
    cells = [[p(str(c), CELL_HEAD if i == 0 else CELL) for c in row] for i, row in enumerate(rows)]
    result = Table(cells, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
    style = [
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, ACCENT),
        ("LINEBELOW", (0, 1), (-1, -1), 0.3, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 2.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.8),
    ]
    result.setStyle(TableStyle(style))
    return [result, Spacer(1, 7)]


def picture(name, width, crop=None):
    """Capture a la largeur voulue, recadree sur crop (gauche, haut, droite, bas) en pixels si besoin."""
    with PILImage.open(CAPTURES / name) as image:
        selection = image.crop(crop) if crop else image.copy()
    buffer = io.BytesIO()
    selection.save(buffer, format="PNG")
    buffer.seek(0)
    result = Image(buffer, width=width, height=width * selection.height / selection.width)
    result.hAlign = "CENTER"
    return result


def figure(name, caption, width=CONTENT_WIDTH, crop=None):
    return KeepTogether([picture(name, width, crop), p(caption, CAPTION), Spacer(1, 6)])


def figures_side_by_side(left, right, left_share=0.5, left_crop=None, right_crop=None):
    """Deux captures cote a cote : left et right sont des couples (fichier, legende)."""
    gap = 0.4 * cm
    left_width = (CONTENT_WIDTH - gap) * left_share
    right_width = CONTENT_WIDTH - gap - left_width
    grid = Table(
        [[picture(left[0], left_width, left_crop), picture(right[0], right_width, right_crop)],
         [p(left[1], CAPTION), p(right[1], CAPTION)]],
        colWidths=[left_width + gap / 2, right_width + gap / 2],
    )
    grid.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN", (0, 0), (-1, 0), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
    ]))
    return KeepTogether([grid, Spacer(1, 6)])


def number(value):
    return f"{value:,}".replace(",", "\u00a0")


def decimal(value, digits=2):
    return f"{value:.{digits}f}".replace(".", ",")


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Body", 8.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(2 * cm, A4[1] - 1.25 * cm, "Écosystème Big Data 2")
    canvas.drawRightString(A4[0] - 2 * cm, A4[1] - 1.25 * cm, "5ème année EMSI")
    canvas.drawString(2 * cm, 1.15 * cm, "UrgenceNet.ma : rapport du jalon 1")
    canvas.drawRightString(A4[0] - 2 * cm, 1.15 * cm, f"Page {doc.page}")
    canvas.restoreState()


p2 = json.loads((HERE / "resultats" / "partie2.json").read_text(encoding="utf-8"))
p3 = json.loads((HERE / "resultats" / "partie3.json").read_text(encoding="utf-8"))

rdd_time = p2["benchmark_average_wait_seconds"]
df_time = p3["benchmark_average_wait_seconds"]
udf_time = p3["benchmark_band_udf_seconds"]
native_time = p3["benchmark_band_native_seconds"]
reuse_time = p2["jobs"]["reutilisation_shuffle"]["seconds"]
first_job_time = p2["jobs"]["prises_en_charge_par_ville"]["seconds"]
bands = {row["wait_band"]: row["count"] for row in p3["bands_raw"]}

story = []

# En-tete
story += [
    p("Pipeline Big Data pour un réseau hospitalier", TITLE),
    p("UrgenceNet.ma. Rapport du jalon 1 : Parties 1, 2 et 3 (traitement batch)", SUBTITLE),
    Paragraph(
        markup(f"Binôme : {AUTHORS}. Année universitaire 2026–2027. Rendu : 22/11/2026.")
        + "<br/>" + markup(f"Dépôt : {REPOSITORY}"),
        META,
    ),
]

# Contexte
story.append(p("Environnement et données", H1))
story.append(p(
    "L'environnement est déployé par Docker Compose : Kafka en mode KRaft, un Master et deux Workers Spark en mode "
    "standalone, MinIO et un conteneur Jupyter qui héberge le Driver. Une image unique sert au Master, aux Workers et à "
    "Jupyter, ce qui garantit les mêmes versions partout. Le lancement est décrit dans le README du dépôt."
))
story += table([
    ["Spark", "Scala / Java", "Python", "Kafka", "MinIO", "Delta Lake", "hadoop-aws"],
    ["3.5.7", "2.12 / 17", "3.10.12", "3.9.1", "2025-04-22", "3.3.2", "3.3.4"],
], [1, 1.2, 1.5, 1, 1.2, 1.1, 1.2])
story.append(p(
    f"Les données sont entièrement synthétiques. Le générateur `producer.py` et le référentiel `facilities.json`, "
    f"annoncés comme fournis par le sujet, ne figuraient pas dans les éléments reçus : nous les avons écrits d'après sa "
    f"spécification. L'historique compte {number(p3['rows_before_dedup'])} événements pour "
    f"{number(p3['care_started_after_dedup'])} passages, dont "
    f"{number(p3['duplicates_removed'])} doublons (1,95 %) et environ 4,9 % d'événements en retard. "
    f"Les résultats des Parties 2 et 3 ont été recoupés par un recalcul indépendant en Python, sans Spark, et sont identiques."
))

# Partie 1
story.append(p("Partie 1 : cluster et positionnement de Spark", H1))
story += bullets(
    "**Driver** : le processus PySpark du conteneur `jupyter`, en mode client. Il exécute notre code, construit le DAG, "
    "le découpe en stages et en tâches, puis planifie ces tâches sur les Executors.",
    "**Cluster Manager** : le Master standalone (`spark://spark-master:7077`). Il connaît les Workers et leurs ressources "
    "et les attribue aux applications. Il ne traite aucune donnée.",
    "**Executors** : à la demande du Master, chaque Worker lance un Executor pour notre application. Nous en obtenons deux, "
    "de 2 cœurs et 1,5 Go. Ils exécutent les tâches, gardent les données en cache et accèdent directement à MinIO.",
    "**SparkContext** : point d'entrée créé dans le Driver. Il établit la connexion au Master, obtient les Executors et "
    "leur envoie les tâches.",
)
story.append(p(
    "`spark/partie1_cluster.py` affiche : version **3.5.7**, master **spark://spark-master:7077**, appName "
    "**UrgenceNet-Partie1**, `sc.defaultParallelism` = **4** (2 Workers de 2 cœurs). Cette valeur vaut 2 tant que les "
    "Executors ne sont pas enregistrés : le script attend donc leur enregistrement avant de l'afficher."
))
story.append(p("Deux limites de MapReduce/Hadoop corrigées par Spark", H2))
story += bullets(
    "**Traitement uniquement batch.** Un job MapReduce traite un jeu de données fini, puis se termine. Or UrgenceNet.ma "
    "reçoit un flux continu d'événements Kafka, dont environ 5 % en retard. Structured Streaming le traite par "
    "micro-batches, avec fenêtres temporelles, watermark et reprise sur checkpoint (Parties 4 et 5).",
    "**Résultats intermédiaires sur disque.** MapReduce écrit sur disque entre les phases et entre les jobs, ce qui "
    "pénalise les traitements itératifs. Spark enchaîne les transformations dans un même DAG et peut garder les données "
    "en mémoire, ce qui est décisif pour PageRank et les composantes connexes (Partie 6).",
)
story.append(figures_side_by_side(
    ("partie1_spark_master.jpg", "Figure 1 : Spark Master, 2 Workers actifs, 4 cœurs. Publié ici sur le port 18080, le 8080 "
                                 "de la machine étant déjà occupé par un autre service."),
    ("partie1_minio_bucket.jpg", "Figure 2 : console MinIO avec le bucket lake."),
    left_share=0.62,
    left_crop=(0, 0, 800, 245),
    right_crop=(160, 0, 800, 175),
))

# Partie 2
story.append(p("Partie 2 : traitement batch avec SparkCore (RDD)", H1))
story.append(p("Résultats", H2))
story.append(p(
    "Conformément au sujet, les doublons ne sont supprimés qu'en Partie 3 : les effectifs ci-dessous sont donc légèrement "
    f"surestimés ({number(sum(p2['care_by_city'].values()))} événements CARE_STARTED pour "
    f"{number(p3['care_started_after_dedup'])} passages)."
))
story += table(
    [["Ville", "Prises en charge", "Attente moyenne (min)"]]
    + [[city, number(p2["care_by_city"][city]), decimal(p2["average_wait_by_city"][city])] for city in sorted(p2["care_by_city"])],
    [2, 1.5, 1.8],
)
story += table(
    [["Priorité"] + sorted(p2["passages_by_priority"]),
     ["Passages"] + [number(p2["passages_by_priority"][k]) for k in sorted(p2["passages_by_priority"])]],
    [1.4, 1, 1, 1, 1, 1],
)
top5 = ", ".join(f"{facility} ({count})" for facility, count in p2["top5_facilities"])
story.append(p(
    f"Top 5 des établissements par prises en charge : {top5}, tous des CHU. L'écart entre villes, inférieur à une "
    "demi-minute, n'est pas significatif : dans ces données synthétiques, le délai dépend de la priorité et non de la ville."
))
story.append(p("Transformations, actions et lineage", H2))
story.append(p(
    "`textFile`, `map`, `filter`, `reduceByKey`, `aggregateByKey` et `mapValues` sont des **transformations** paresseuses : "
    "elles complètent le lineage sans rien calculer. `collect` et `takeOrdered` sont des **actions** : elles lancent un job "
    "et ramènent un résultat au Driver. `toDebugString` décrit le plan sans lancer de job :"
))
story += code("""
(4) PythonRDD[19] at collect at /workspace/spark/partie2_rdd.py:42 []
 |  MapPartitionsRDD[9] at mapPartitions at PythonRDD.scala:160 []
 |  ShuffledRDD[8] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(4) PairwiseRDD[7] at aggregateByKey at /workspace/spark/partie2_rdd.py:63 []
    |  PythonRDD[6] at aggregateByKey at /workspace/spark/partie2_rdd.py:63 []
    |  s3a://lake/bronze/history/ MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0 []
    |  s3a://lake/bronze/history/ HadoopRDD[0] at textFile at NativeMethodAccessorImpl.java:0 []
""")
story.append(p(
    "`(4)` est le nombre de partitions et le décalage `+-` marque la frontière de shuffle. Le parsing, le `filter` et le `map` "
    "n'apparaissent pas séparément : PySpark les enchaîne dans un seul `PythonRDD[6]`."
))
story.append(p("DAG, stages et shuffle", H2))
story.append(p(
    "Chaque action de calcul donne un job de **2 stages** et 8 tâches (figure 3). `reduceByKey` et `aggregateByKey` doivent "
    "réunir toutes les valeurs d'une clé dans une seule partition, alors que les événements d'une ville sont répartis dans "
    "toutes les partitions d'entrée. Cette dépendance large coupe le DAG en deux stages : le premier agrège localement chaque "
    "partition et écrit le résultat sur le disque des Executors, réparti par clé ; le second relit ces blocs à travers le "
    "réseau et fusionne les agrégats."
))
story.append(p(
    "Cette agrégation locale explique le faible volume observé : 12,8 Mio lus en entrée pour seulement 856 octets échangés, "
    "car il ne reste qu'une somme et un compte par ville et par partition. Un `groupByKey` aurait fait transiter chaque valeur."
))
story.append(p(
    f"Relancer `collect` sur le même RDD saute le premier stage (figure 4), car les fichiers de shuffle existent encore : "
    f"{decimal(reuse_time)} s contre {decimal(rdd_time['median'])} s pour un calcul complet."
))
story.append(p("Tolérance aux pannes", H2))
story.append(p(
    "Un RDD ne stocke pas ses données mais la recette pour les produire. Si un Executor tombe, Spark recalcule seulement les "
    "partitions perdues : il relit les blocs concernés dans MinIO, source durable, puis rejoue les transformations du lineage "
    "sur un autre Executor. Si des fichiers de shuffle sont perdus, seules les tâches du stage précédent qui les avaient "
    "produits sont relancées. Ce mécanisme est décrit ici sans avoir été provoqué expérimentalement."
))
story.append(figures_side_by_side(
    ("partie2_dag_job1.jpg", "Figure 3 : attente moyenne par ville, 2 stages, shuffle de 856 octets."),
    ("partie2_dag_job4_stage_ignore.jpg", "Figure 4 : même RDD relancé, le stage 8 est ignoré (skipped)."),
    left_crop=(0, 35, 799, 545),
    right_crop=(0, 35, 799, 545),
))

# Partie 3
story.append(p("Partie 3 : Spark SQL et DataFrames", H1))
story.append(p(
    "Les données sont lues avec des schémas explicites (`spark/schemas.py`), sans inférence. L'attente moyenne par ville est "
    "identique avec l'API DataFrame, en SQL et avec les RDD de la Partie 2. `sameSemantics` confirme que l'API et SQL "
    "produisent le même plan : ce sont deux façons d'écrire la même requête."
))
story.append(p("Prises en charge et attente moyenne par type et par ville", H2))
clean = {(row["type"], row["city"]): row for row in p3["care_by_type_city_clean"]}
cities = sorted({city for _, city in clean})
types = sorted({kind for kind, _ in clean})
story += table(
    [["Type (après dédoublonnage)"] + cities]
    + [[kind] + [f"{number(clean[kind, city]['prises_en_charge'])} ({decimal(clean[kind, city]['attente_moyenne'])} min)" for city in cities]
       for kind in types],
    [1.8, 1.3, 1.3, 1.3, 1.3],
)
story.append(p(
    "Ces écarts ne traduisent pas une différence de qualité : le délai dépend ici de la priorité, et les attentes les plus "
    "élevées (40,02 et 39,34 min) portent sur de petits effectifs."
))
story.append(p("UDF ou expression native", H2))
story.append(p(
    f"Les deux versions de `wait_band` donnent le même résultat sur les données brutes : court {number(bands['court'])}, moyen "
    f"{number(bands['moyen'])}, long {number(bands['long'])}. La version `when/otherwise` est environ "
    f"{decimal(udf_time['median'] / native_time['median'], 1)} fois plus rapide ({decimal(native_time['median'])} s contre "
    f"{decimal(udf_time['median'])} s, médianes de 5 exécutions alternées sur {number(p3['rows_before_dedup'])} lignes en cache). "
    "Le plan l'explique : l'UDF apparaît dans `BatchEvalPython`, hors génération de code, alors que l'expression native est "
    "un `CASE WHEN` dans `*(1) Project`. L'UDF est une boîte noire pour Catalyst et chaque valeur doit être sérialisée de la "
    "JVM vers un processus Python, puis renvoyée. L'expression native reste dans la JVM et Tungsten la compile avec le reste "
    "de l'étape."
))
story.append(p("Auto-jointure, dédoublonnage et couche Silver", H2))
story.append(p(
    f"L'auto-jointure entre ARRIVED et CARE_STARTED donne {number(p3['self_join_rows'])} lignes, et le délai recalculé diffère de "
    f"`wait_minutes` pour **{p3['wait_mismatches']} passage** : le champ est cohérent avec les horodatages. Le dédoublonnage doit "
    f"précéder cette jointure : sur les données brutes, elle produit {number(p3['self_join_rows_with_duplicates'])} lignes, car "
    "chaque doublon multiplie les correspondances."
))
status_rows = {row["status"]: row for row in p3["dedup_by_status"]}
order = ["ARRIVED", "TRIAGED", "CARE_STARTED", "DISCHARGED", "TRANSFERRED"]
story += table(
    [["Événements"] + order + ["Total"],
     ["Avant"] + [number(status_rows[s]["avant"]) for s in order] + [number(p3["rows_before_dedup"])],
     ["Après"] + [number(status_rows[s]["apres"]) for s in order] + [number(p3["rows_after_dedup"])]],
    [1.1, 1, 1, 1.3, 1.2, 1.3, 0.9],
)
story.append(p(
    f"{number(p3['duplicates_removed'])} doublons sont supprimés. Le résultat est écrit en Parquet dans "
    f"`lake/silver/urgences_history/`, partitionné par `city` (figure 5), et relu : {number(p3['silver_rows'])} lignes. Le "
    "`repartition(\"city\")` préalable évite de multiplier les petits fichiers. Le référentiel est stocké dans "
    "`lake/bronze/facilities/` : ce dossier s'ajoute à l'organisation du sujet, car les Workers n'ont pas accès aux fichiers "
    "locaux du conteneur Jupyter."
))
story.append(figure(
    "partie3_minio_silver.jpg",
    "Figure 5 : couche Silver partitionnée par ville dans la console MinIO.",
    width=CONTENT_WIDTH * 0.6,
    crop=(178, 75, 772, 315),
))
story.append(p("Lecture de explain(True) sur la jointure", H2))
story.append(p(
    "Dans le code, le filtre sur le statut est volontairement écrit **après** la jointure. Extrait de la sortie, la version "
    "complète étant dans `rapport/resultats/partie3_explain.txt` :"
))
story += code("""
== Analyzed Logical Plan ==
   +- Filter (status#4 = CARE_STARTED)
      +- Project [facility_id#2, visit_id#0, patient_id#1, city#3, status#4, ... , type#26]
         +- Join Inner, (facility_id#2 = facility_id#22)

== Optimized Logical Plan ==
   +- Project [city#3, wait_minutes#6, type#26]
      +- Join Inner, (facility_id#2 = facility_id#22)
         :- Project [facility_id#2, city#3, wait_minutes#6]
         :  +- Filter ((isnotnull(status#4) AND (status#4 = CARE_STARTED)) AND isnotnull(facility_id#2))
         +- Project [facility_id#22, type#26]
            +- Filter isnotnull(facility_id#22)

== Physical Plan ==
*(2) BroadcastHashJoin [facility_id#2], [facility_id#22], Inner, BuildRight, false
:- *(2) Filter ((isnotnull(status#4) AND (status#4 = CARE_STARTED)) AND isnotnull(facility_id#2))
:  +- FileScan json [facility_id#2,city#3,status#4,wait_minutes#6]
:       PushedFilters: [IsNotNull(status), EqualTo(status,CARE_STARTED), IsNotNull(facility_id)]
+- BroadcastQueryStage 0
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false)
""")
story += bullets(
    "**Plan logique analysé** : les colonnes sont résolues et typées, et la structure suit l'ordre de notre code.",
    "**Plan optimisé par Catalyst** : le filtre passe sous la jointure (predicate pushdown), seules les colonnes utiles "
    "restent (column pruning), et des `isnotnull` sont ajoutés, une clé nulle ne pouvant jamais satisfaire la jointure.",
    "**Plan physique** : la stratégie est un **BroadcastHashJoin**. Le référentiel (7,9 Kio) est sous le seuil "
    "`spark.sql.autoBroadcastJoinThreshold` de 10 Mio : Spark l'envoie à chaque Executor au lieu de faire transiter les "
    "événements par un shuffle, comme le ferait un SortMergeJoin.",
)
story.append(p(
    "Spark UI le confirme (figure 6) : le scan renvoie 12 496 lignes sur 50 001, le filtre étant appliqué dès le parsing, et "
    "seules 40 lignes sont diffusées. Le fichier JSON reste lu en entier (12,6 Mio), car ce format ne permet pas de sauter des "
    "blocs. Dans la figure 7, deux stages sont ignorés car l'exécution adaptative (AQE) lance la requête en plusieurs jobs : "
    "ils ont été calculés par les jobs 21 à 24 de la même requête. `AQEShuffleRead coalesced` indique qu'AQE a fusionné les "
    "200 partitions de shuffle par défaut."
))
story.append(figures_side_by_side(
    ("partie3_plan_jointure.jpg", "Figure 6 : plan de la requête dans Spark UI, onglet SQL : BroadcastHashJoin."),
    ("partie3_dag_job25.jpg", "Figure 7 : DAG du dernier job de la requête, 3 stages dont 2 ignorés."),
))
story.append(p("Comparaison RDD, DataFrame et Dataset", H2))
story.append(p(
    f"Même calcul (attente moyenne par ville), lecture complète du fichier à chaque exécution, médiane de 5 exécutions : "
    f"**{decimal(rdd_time['median'])} s** avec les RDD ({decimal(min(rdd_time['runs']))} à {decimal(max(rdd_time['runs']))} s) contre "
    f"**{decimal(df_time['median'])} s** avec les DataFrames ({decimal(min(df_time['runs']))} à {decimal(max(df_time['runs']))} s), soit "
    f"environ {decimal(rdd_time['median'] / df_time['median'], 0)} fois plus vite. Avec les RDD, chaque ligne est parsée en entier par `json.loads` "
    "dans un processus Python, y compris les 37 505 lignes filtrées ensuite. Le DataFrame ne parse que les 3 colonnes utiles sur 11 "
    "et filtre dès le scan, dans la JVM."
))
story += table([
    ["Critère", "RDD", "DataFrame", "Dataset (Scala/Java)"],
    ["Typage", "Typé à la compilation en Scala, aucun contrôle en Python", "Schéma vérifié à l'analyse, à l'exécution", "Vérifié à la compilation"],
    ["Catalyst", "Non : Spark ne voit pas l'intérieur des lambdas", "Oui", "Oui, sauf dans les lambdas typées"],
    ["Tungsten", "Non", "Oui", "Oui, avec un coût de conversion vers les objets"],
    ["En PySpark", "Oui", "Oui", "Non"],
], [0.9, 2.2, 1.7, 1.9])
story.append(p("Limites de ces mesures", H2))
story += bullets(
    "Le Dataset n'a pas été mesuré, car cette API n'existe pas en PySpark (un DataFrame est un `Dataset[Row]`).",
    "Les temps RDD et DataFrame viennent de deux exécutions distinctes, sur seulement 13 Mio : l'ordre de grandeur compte "
    "davantage que la valeur exacte. Une partie de l'écart vient de Python, et des RDD en Scala n'ont pas été testés.",
    f"Le premier job d'une application n'est pas comparable ({decimal(first_job_time)} s), car il inclut le démarrage à froid.",
    "Relancer une action sur un même RDD ou DataFrame réutilise les fichiers de shuffle et fausse la mesure : une première mesure "
    "donnait ainsi l'UDF plus rapide que `when/otherwise`. Chaque mesure reconstruit donc un nouvel objet.",
)

# Preuves
story.append(p("Preuves du jalon 1", H1))
story += table([
    ["Preuve demandée", "Emplacement"],
    ["Spark Master UI avec 2 Workers actifs ; bucket lake", "Figures 1 et 2"],
    ["DAG et stages dans Spark UI (Parties 2 et 3)", "Figures 3, 4, 6 et 7 ; autres vues dans `rapport/captures/`"],
    ["Sortie de explain(True)", "Partie 3 et `rapport/resultats/partie3_explain.txt`"],
    ["Parquet partitionné dans MinIO", "Figure 5"],
    ["Code, versions et lancement", "`docker-compose.yml`, `spark/`, `producer.py` et `README.md` du dépôt"],
], [2, 3])
story.pop()  # sans l'espace final, qui creerait une derniere page blanche

document = SimpleDocTemplate(
    str(OUTPUT),
    pagesize=A4,
    leftMargin=2 * cm,
    rightMargin=2 * cm,
    topMargin=1.8 * cm,
    bottomMargin=1.7 * cm,
    title="UrgenceNet.ma : rapport du jalon 1",
    author=AUTHORS,
    subject="Écosystème Big Data 2 : Parties 1, 2 et 3",
)
document.build(story, onFirstPage=footer, onLaterPages=footer)
print(f"{OUTPUT} : {document.page} pages")
