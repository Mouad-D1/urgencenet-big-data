#!/usr/bin/env python3
"""Genere rapport/rapport_final.pdf : rapport des six parties, 10 pages au plus.

Lancement depuis la racine du projet : python rapport/build_rapport_final.py
Les chiffres viennent de rapport/resultats/, produits par les scripts spark/.
"""
import json

from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import KeepTogether, Paragraph, SimpleDocTemplate

from pdf_style import (
    AUTHORS, BODY, CONTENT_WIDTH, H1, H2, HERE, META, REPOSITORY, SUBTITLE, TITLE,
    bullets, code, decimal, figure, figures_side_by_side, make_footer, markup, number, p, table,
)

OUTPUT = HERE / "rapport_final.pdf"
MAX_PAGES = 10
footer = make_footer("UrgenceNet.ma : rapport final")
# Paragraphes riches en noms de code : la justification y creerait de grands blancs
LEFT = ParagraphStyle("left", parent=BODY, alignment=TA_LEFT)


def load(name):
    return json.loads((HERE / "resultats" / f"{name}.json").read_text(encoding="utf-8"))


p2, p3, p4, p5 = load("partie2"), load("partie3"), load("partie4"), load("partie5")
gf, gx = load("partie6_graphframes"), load("partie6_graphx")

story = [
    p("Pipeline Big Data pour un réseau hospitalier", TITLE),
    p("UrgenceNet.ma. Rapport final : Parties 1 à 6", SUBTITLE),
    Paragraph(
        markup(f"Binôme : {AUTHORS}. Année universitaire 2026–2027. Rendu : 20/12/2026.")
        + "<br/>" + markup(f"Dépôt : {REPOSITORY}"),
        META,
    ),
]

# Environnement
story.append(p("Environnement et données", H1))
story.append(p(
    "Docker Compose déploie Kafka (KRaft, 3 partitions), un Master et deux Workers Spark en mode standalone, MinIO et un "
    "conteneur Jupyter qui héberge le Driver. Une image unique sert au Master, aux Workers et à Jupyter, d'où des versions "
    "identiques partout. Chaîne : `producer.py` → Kafka → Structured Streaming → MinIO (Bronze → Silver → Gold) → "
    "GraphFrames et GraphX → Parquet."
))
story += table([
    ["Spark / GraphX", "Scala / Java", "Python", "Kafka", "MinIO", "Delta Lake", "GraphFrames"],
    ["3.5.7", "2.12.18 / 17", "3.10.12", "3.9.1", "2025-04-22", "3.3.2", "0.8.4-spark3.5-s_2.12"],
], [1.2, 1.2, 0.9, 0.8, 1.1, 1, 1.9])
story.append(p(
    "Les données sont synthétiques. `producer.py` et `facilities.json`, annoncés comme fournis, ne figuraient pas dans les "
    "éléments reçus : nous les avons écrits d'après le sujet (40 établissements, environ 5 % d'événements en retard "
    f"jusqu'à 15 minutes, environ 2 % de doublons). L'historique compte {number(p3['rows_before_dedup'])} événements pour "
    f"{number(p3['care_started_after_dedup'])} passages. Tous les résultats chiffrés ont été recoupés par un calcul "
    "indépendant (Python sans Spark, recalcul batch ou bilan contre le contenu exact du topic Kafka)."
))

# Partie 1
story.append(p("Partie 1 : cluster et positionnement de Spark", H1))
story += bullets(
    "**Driver** : le processus PySpark du conteneur `jupyter` (mode client). Il construit le DAG, le découpe en stages et "
    "en tâches, et planifie celles-ci.",
    "**Cluster Manager** : le Master standalone (`spark://spark-master:7077`). Il attribue aux applications les ressources "
    "des Workers, sans traiter de données.",
    "**Executors** : un par Worker pour notre application (2 cœurs, 1,5 Go). Ils exécutent les tâches, gardent le cache "
    "et lisent ou écrivent directement dans MinIO.",
    "**SparkContext** : point d'entrée créé dans le Driver. Il se connecte au Master, obtient les Executors et leur "
    "envoie les tâches.",
)
story.append(p(
    "Résultat : Spark **3.5.7**, master **spark://spark-master:7077**, `sc.defaultParallelism` = **4**. Cette valeur vaut "
    "2 tant que les Executors ne sont pas enregistrés : nos scripts attendent les 4 cœurs avant de démarrer. Deux limites "
    "de MapReduce corrigées par Spark : le **traitement uniquement batch**, alors que nos événements arrivent en continu par "
    "Kafka (Parties 4 et 5), et **l'écriture sur disque entre chaque étape**, pénalisante pour les algorithmes itératifs "
    "comme PageRank (Partie 6), que Spark exécute en mémoire."
))
story.append(figures_side_by_side(
    ("partie1_spark_master.jpg", "Figure 1 : Spark Master, 2 Workers actifs, 4 cœurs (port 18080, le 8080 étant pris "
                                 "sur la machine de démonstration)."),
    ("partie1_minio_bucket.jpg", "Figure 2 : bucket lake dans MinIO."),
    left_share=0.62, left_crop=(0, 0, 800, 245), right_crop=(160, 0, 800, 175),
))

# Partie 2
story.append(p("Partie 2 : traitement batch avec SparkCore (RDD)", H1))
cities = sorted(p2["care_by_city"])
story += table(
    [["Ville"] + cities, ["Prises en charge"] + [number(p2["care_by_city"][c]) for c in cities],
     ["Attente moyenne (min)"] + [decimal(p2["average_wait_by_city"][c]) for c in cities]],
    [1.6, 1, 1, 1, 1],
)
priorities = sorted(p2["passages_by_priority"])
top5 = ", ".join(f"{f} ({c})" for f, c in p2["top5_facilities"])
story.append(p(
    "Passages par priorité (1 à 5) : " + ", ".join(number(p2["passages_by_priority"][k]) for k in priorities)
    + f". Top 5 des établissements : {top5}, tous des CHU. Les doublons, supprimés seulement en Partie 3, surestiment "
    "légèrement ces effectifs, et l'écart d'attente entre villes n'est pas significatif (le délai dépend de la priorité)."
))
story.append(p(
    "`textFile`, `map`, `filter`, `reduceByKey`, `aggregateByKey` et `mapValues` sont des **transformations** paresseuses ; "
    "`collect` et `takeOrdered` sont des **actions**. Chaque calcul donne un job de **2 stages** : `reduceByKey` doit réunir "
    "les valeurs d'une clé dispersées dans toutes les partitions, ce qui impose un shuffle. Le premier stage agrège "
    "localement chaque partition, d'où seulement 856 octets échangés pour 12,8 Mio lus. Le lineage (`toDebugString`, "
    "décalage `+-` à la frontière de shuffle) permet de recalculer seulement les partitions perdues en cas de panne, en "
    "relisant MinIO."
))
story.append(figures_side_by_side(
    ("partie2_dag_job1.jpg", "Figure 3 : DAG de l'attente moyenne par ville, 2 stages."),
    ("partie2_dag_job4_stage_ignore.jpg", "Figure 4 : même RDD relancé, stage ignoré (shuffle réutilisé)."),
    left_crop=(0, 35, 799, 545), right_crop=(0, 35, 799, 545),
))

# Partie 3
rdd_time, df_time = p2["benchmark_average_wait_seconds"]["median"], p3["benchmark_average_wait_seconds"]["median"]
udf_time, native_time = p3["benchmark_band_udf_seconds"]["median"], p3["benchmark_band_native_seconds"]["median"]
story.append(p("Partie 3 : Spark SQL et DataFrames", H1))
story.append(p(
    "Lecture avec schémas explicites (`StructType`). L'attente moyenne par ville est identique avec l'API DataFrame, en SQL "
    "(même plan selon `sameSemantics`) et avec les RDD. La jointure avec le référentiel donne les prises en charge par type "
    "et par ville (16 combinaisons, de 232 à 1 345). L'UDF `wait_band` et l'expression `when/otherwise` donnent le même "
    f"résultat, mais la version native est {decimal(udf_time / native_time, 1)} fois plus rapide "
    f"({decimal(native_time)} s contre {decimal(udf_time)} s). L'UDF apparaît dans `BatchEvalPython`, hors génération de "
    "code : c'est une boîte noire pour Catalyst, et chaque valeur fait l'aller-retour entre la JVM et Python."
))
story.append(p(
    f"Le dédoublonnage supprime {number(p3['duplicates_removed'])} lignes ({number(p3['rows_before_dedup'])} → "
    f"{number(p3['rows_after_dedup'])}), et Silver est écrit en Parquet partitionné par ville. Il doit précéder "
    f"l'auto-jointure ARRIVED/CARE_STARTED, qui produirait sinon {number(p3['self_join_rows_with_duplicates'])} lignes au lieu "
    f"de {number(p3['self_join_rows'])}. Le délai recalculé diffère de `wait_minutes` pour **0 passage**. Extrait de "
    "`explain(True)`, le filtre ayant été écrit après la jointure :"
))
story += code("""
== Optimized Logical Plan ==
   +- Join Inner, (facility_id#2 = facility_id#22)
      :- Project [facility_id#2, city#3, wait_minutes#6]
      :  +- Filter ((isnotnull(status#4) AND (status#4 = CARE_STARTED)) AND isnotnull(facility_id#2))
      +- Project [facility_id#22, type#26]
== Physical Plan ==
*(2) BroadcastHashJoin [facility_id#2], [facility_id#22], Inner, BuildRight, false
:  +- FileScan json [facility_id#2,city#3,status#4,wait_minutes#6]
:       PushedFilters: [IsNotNull(status), EqualTo(status,CARE_STARTED), IsNotNull(facility_id)]
""")
story.append(p(
    "Catalyst a déplacé le filtre sous la jointure (predicate pushdown), retiré les colonnes inutiles (column pruning) et "
    "ajouté des `isnotnull`. La stratégie physique est un **BroadcastHashJoin** : le référentiel (7,9 Kio) est sous le seuil "
    "de 10 Mio et part vers chaque Executor, sans shuffle des événements (figure 5). À calcul égal, le DataFrame met "
    f"**{decimal(df_time)} s** contre **{decimal(rdd_time)} s** pour les RDD : il ne parse que 3 colonnes sur 11 et filtre "
    "dès le scan, dans la JVM, avec Catalyst et Tungsten. Les RDD n'ont pas d'optimiseur et passent chaque ligne par Python. "
    "Le Dataset, typé à la compilation, n'existe qu'en Scala et Java : il n'a pas été mesuré."
))
story.append(figures_side_by_side(
    ("partie3_plan_jointure.jpg", "Figure 5 : plan de la jointure dans Spark UI (BroadcastHashJoin)."),
    ("partie3_minio_silver.jpg", "Figure 6 : Silver partitionné par ville dans MinIO."),
    left_share=0.5, left_crop=(0, 40, 799, 560), right_crop=(178, 75, 772, 315),
))

# Partie 4
buckets = p4["late_buckets_seconds"]
story.append(p("Partie 4 : Structured Streaming depuis Kafka", H1))
story.append(p(
    "Cinq requêtes lisent le topic en micro-batches de 10 secondes : flux brut typé par `from_json` en console (mode append), "
    "messages bruts en JSON dans `bronze/urgences_raw/` avec leur checkpoint, comptage par ville et priorité en mode complete "
    "avec un watermark de 10 minutes, et deux requêtes de contrôle. Sur 8 minutes : "
    f"{number(p4['kafka_rows'])} messages dans Kafka, {number(p4['bronze_rows'])} en Bronze, sans perte ni doublon. Les "
    f"{number(p4['triaged_rows'])} événements TRIAGED sont tous comptés en mode complete."
))
story.append(p(
    "**Que devient un événement en retard de 15 minutes ?** En mode complete, il est compté : Spark doit réafficher toute la "
    "table, le watermark n'y a aucun effet. Dans une agrégation par fenêtre de 10 minutes (append ou update), il n'est rejeté "
    "que si **sa fenêtre est déjà close** (fin de fenêtre ≤ watermark = max(event_time) − 10 min). Deux exemples tirés de nos "
    "journaux :"
))
story += code("""
V-1601067300 (retard 14,6 min)  event_time 14:28:07, lu au batch 11, watermark 14:32:30
                                fenetre [14:20, 14:30[ close    -> rejete
V-1601067698 (retard 14,9 min)  event_time 14:31:01, lu au batch 30, watermark 14:35:39
                                fenetre [14:30, 14:40[ ouverte  -> retenu
""")
labels = {"0-60": "moins de 1 min", "60-300": "1 à 5 min", "300-600": "5 à 10 min", "600-750": "10 à 12,5 min",
          "750-900": "12,5 à 15 min"}
story.append(KeepTogether(table(
    [["Retard à l'arrivée des événements TRIAGED", "Événements", "Retenus", "Rejetés"]]
    + [[label, number(buckets[key]["total"]), number(buckets[key]["retenus"]), number(buckets[key]["rejetes"])]
       for key, label in labels.items()],
    [3, 1, 1, 1],
)))
story.append(p(
    "Le watermark garantit donc qu'un événement en retard de moins de 10 minutes n'est jamais rejeté ; au-delà, le rejet est "
    "possible mais pas certain. Nos journaux montrent aussi que les retards sont filtrés avec le watermark du micro-batch "
    "**précédent** : avec cette règle, nous prédisons sans erreur le sort de chaque événement."
))
story.append(p(
    "**Modèle** : le flux est une table infinie ; chaque déclenchement lit les nouveaux offsets (source), met à jour le "
    "résultat avec l'état conservé (transformation) et écrit dans le sink. **Micro-batch** (déclencheur `processingTime`) : "
    "latence de l'ordre du déclencheur, toutes les opérations, exactly-once. **Continu** (déclencheur `continuous`) : "
    "latence de l'ordre de la milliseconde, mais seulement des opérations ligne à ligne, en at-least-once (non testé). "
    "**Modes de sortie** : append (nouvelles lignes définitives), update (lignes modifiées), complete (toute la table, "
    "sans purge d'état)."
))
story.append(figure(
    "partie4_console_zoom.png", "Figure 7 : sink console dans un terminal : batch 9 du flux brut (append), ligne de suivi "
    "du watermark, puis batch 7 du comptage par ville et priorité (complete), qui réaffiche toute la table à chaque "
    "micro-batch.", width=CONTENT_WIDTH * 0.85, crop=(0, 10, 560, 272),
))
story.append(p(
    "Cette capture vient d'une exécution de démonstration du 26/11/2026, reprise à zéro (`--reset`) : le checkpoint "
    "précédent pointait vers des offsets d'un topic recréé depuis, et Spark refusait de reprendre "
    "(`OffsetOutOfRangeException`). Un checkpoint est lié au topic qu'il a lu."
))

# Partie 5
before, after = p5["before_restart"], p5["after_restart_first_batch"]
gold = p5["gold"]
story.append(p("Partie 5 : pipeline de production Spark-Kafka", H1))
story.append(p(
    "Spark 3.5 n'enchaîne pas, dans une même requête en mode update, un dédoublonnage et une agrégation. Le pipeline a donc "
    "deux étages. La requête `silver` lit Kafka, dédoublonne sur (visit_id, status) avec un watermark de 10 minutes, joint "
    "le référentiel en flux-statique (broadcast), ajoute `wait_band` et écrit `silver/urgences_clean/` (Parquet, partitionné "
    "par ville et date). Nous utilisons `dropDuplicatesWithinWatermark` : un simple `dropDuplicates` sur ces deux colonnes "
    "garderait chaque clé en mémoire pour toujours, faute de colonne de temps dans la clé. Trois requêtes relisent Silver "
    "en flux et écrivent en Delta Lake : fenêtre tumbling de 10 minutes (passages, attente moyenne) dans "
    "`gold/kpi_facility_10min/` par `MERGE`, fenêtre glissante 10/5 minutes (taux de priorités 1 et 2), fenêtre de session "
    "de 5 minutes par poste de triage.",
    LEFT,
))
story.append(KeepTogether(table([
    ["Exécution de référence (24 min de flux)", "Valeur"],
    ["Messages Kafka = lignes Silver + doublons rejetés + retards rejetés",
     f"{number(p5['kafka_rows'])} = {number(p5['silver_rows'])} + {number(p5['dropped_duplicates'])} + {number(p5['dropped_late'])}"],
    ["Doublons dans Silver / clés absentes de Silver (retard minimal)",
     f"0 / {number(p5['missing_keys'])} ({decimal(p5['missing_min_delay_minutes'], 1)} min)"],
    ["Gold : fenêtres closes identiques au recalcul batch depuis Silver",
     f"{gold['kpi_exact_matches']} / {gold['kpi_closed_windows_compared']}"],
    ["DESCRIBE HISTORY de kpi_facility_10min", f"{gold['history_versions']} versions : WRITE puis un MERGE par micro-batch"],
], [3.2, 2])))
story.append(p(
    f"**Arrêt et reprise.** La requête Silver a été arrêtée à 10 minutes ({number(before['silver_rows'])} lignes, dernier "
    f"fichier `offsets/{before['checkpoint_last_offsets_batch']}` et `commits/{before['checkpoint_last_commit_batch']}`), "
    f"puis relancée 2 minutes plus tard. Son batch {after['batch']} repart exactement des offsets du checkpoint, "
    f"`{after['start_offset']}`, et rattrape {number(after['input_rows'])} messages. Lors d'un premier essai, l'arrêt avait "
    "interrompu un batch (`offsets/21` sans `commits/21`) : Spark l'a rejoué avec les mêmes offsets, sans doublon. "
    "`kafka-consumer-groups.sh --describe` montre que Spark ne commite pas ses offsets dans Kafka (le groupe `urgencenet-silver` "
    "« n'existe pas »). Un groupe de suivi, alimenté depuis le checkpoint, montre un retard de 159 messages par partition à "
    "l'arrêt, 635 une minute plus tard, moins de 100 après la reprise, puis 0. Pendant l'arrêt, Delta n'enregistre aucune "
    "version (figure 8)."
))
story.append(p(
    "**Exactly-once** : une source rejouable (offsets Kafka), un checkpoint (`offsets/N` écrit avant le batch, `commits/N` "
    "après le sink, état des opérateurs) et des sinks idempotents ou transactionnels. Le sink fichier n'expose un batch qu'une "
    "fois inscrit dans `_spark_metadata`. Le sink Delta enregistre l'identifiant de la requête et le numéro de batch dans sa "
    "transaction. Le `MERGE` rend `foreachBatch` idempotent : rejouer un batch réécrit les mêmes valeurs sur les mêmes clés. "
    "Un sink console n'enregistre rien d'atomique : un batch rejoué est affiché deux fois. Limites : 1,5 % des événements, "
    "en retard de plus de 10 minutes, sont exclus de Silver ; les sessions mesurées (moyenne 4 minutes) sont tronquées par "
    "les bornes de l'exécution."
))
story.append(figure(
    "partie5_reprise_silver.jpg", "Figure 8 : la requête silver garde son identifiant, avec un run terminé au batch 20 et "
    "un nouveau run après la reprise.", width=CONTENT_WIDTH * 0.75, crop=(0, 40, 800, 430),
))

# Partie 6
rules = gf["rule_evaluation"]
groups = gx["groupes"]
story.append(p("Partie 6 : analyse de graphes avec GraphX et GraphFrames", H1))
story.append(p(
    f"Le graphe est construit depuis Silver : {number(gf['vertices'])} sommets et {number(gf['edges'])} arêtes. Les sommets "
    "sont des patients (`P-`), des établissements (`F-`) et des services (`D-<groupe>-<service>`), avec les colonnes id, "
    "vertex_type, name, hospital_group et facility_type, sans aucune collision d'identifiant. Les arêtes `ADMITTED_AT`, "
    "`TREATED_IN`, `HAS_DEPARTMENT` et `TRANSFERRED_TO` portent src, dst, relationship, visit_id, patient_id, event_time et "
    "hospital_group. Un patient prend le groupe de sa première admission. Les tables sont exportées en Parquet sous "
    "`gold/graph_analysis/`, partitionnées par `hospital_group`. GraphFrames est ajouté par "
    "`spark-submit --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12`, et GraphX est lancé en Scala avec "
    "`spark-shell -i spark/partie6_graphx.scala`.",
    LEFT,
))
counts = {(r["hospital_group"], r["vertex_type"]): r["count"] for r in gf["vertices_by_group_type"]}
counts.update({(r["hospital_group"], r["relationship"]): r["count"] for r in gf["edges_by_group_type"]})
kinds = [("PATIENT", "Sommets patients"), ("ETABLISSEMENT", "Sommets établissements"), ("SERVICE", "Sommets services"),
         ("ADMITTED_AT", "Arêtes ADMITTED_AT"), ("TREATED_IN", "Arêtes TREATED_IN"),
         ("HAS_DEPARTMENT", "Arêtes HAS_DEPARTMENT"), ("TRANSFERRED_TO", "Arêtes TRANSFERRED_TO")]
story.append(KeepTogether(table(
    [["Type"] + sorted(groups)] + [[label] + [number(counts[g, kind]) for g in sorted(groups)] for kind, label in kinds],
    [2.2, 1, 1, 1],
)))
cg = gf["cross_group"]
motifs = gf["motifs"]
story.append(p(
    f"**Contrôles** : {number(gf['consistency']['transfers'])} transferts, aucun vers un établissement inexistant, aucun vers "
    f"lui-même ; {cg['total']} transferts entre groupes, dont {cg['to_chu']} vers un CHU et **{cg['to_non_chu']} vers un "
    f"non-CHU**, ce qui est interdit. **Motifs GraphFrames** (`find`, même patient, ordre chronologique) : "
    f"{motifs['chains_a_b_c']} chaînes A → B → C, {motifs['loops_a_b_a']} boucles A → B → A et "
    f"{number(motifs['patients_in_several_facilities'])} patients admis dans plusieurs établissements. **GraphX** : chaque "
    "sommet reçoit un `VertexId` Long stable (rang alphabétique). Le PageRank est calculé sur le graphe à une arête par "
    "transfert, ce qui équivaut à un PageRank pondéré par le nombre de transferts."
))
pagerank_rows = []
for group, data in groups.items():
    best = ", ".join(f"{m['facility_id']} {m['type'].replace('HOPITAL_REGIONAL', 'HR').replace('CLINIQUE', 'clinique')} "
                     f"({decimal(m['pagerank'])})" for m in data["metriques"][:3])
    pagerank_rows.append([group, str(data["etablissements"]), str(data["liaisons"]), str(data["composantes"]), best])
story.append(KeepTogether(table(
    [["Groupe", "Établ.", "Liaisons", "Comp.", "PageRank pondéré le plus élevé (GraphX)"]] + pagerank_rows,
    [1.3, 0.6, 0.7, 0.6, 3.8],
)))
story.append(figure(
    "partie6_graphe_transferts.png", "Figure 9 : sous-graphe des transferts internes de GHT-Sud. Rond bleu : CHU ; "
    "carré orange : hôpital régional ; losange vert : clinique ; triangle gris : centre de santé. Taille : PageRank "
    "pondéré ; épaisseur : nombre de transferts ; contour noir : établissement en alerte (F-035). Les trois groupes sont "
    "dans rapport/captures/partie6_graphe_transferts.png.",
    width=CONTENT_WIDTH * 0.62, crop=(1790, 150, 2700, 900),
))
story.append(p(
    f"**Comparaison.** Sur le même sous-graphe (les {number(gf['consistency']['transfers'])} transferts), les degrés GraphX "
    "et GraphFrames sont identiques pour les 40 établissements, et les deux trouvent une seule composante connexe. Les "
    f"composantes prennent {decimal(gx['secondes_composantes_graphe_complet'], 1)} s avec GraphX (graphe en mémoire, RDD "
    f"typés, Scala) contre {decimal(gf['components_seconds'], 0)} s avec GraphFrames (jointures de DataFrames et checkpoints "
    "sur MinIO). GraphFrames, utilisable en Python, est plus simple pour exprimer des motifs. La centralité décrit la "
    "structure du réseau, pas la qualité des soins : les CHU dominent parce qu'ils reçoivent les cas graves. La 2e place de "
    "la clinique F-035 dans GHT-Sud est une structure inhabituelle à expliquer, pas la preuve d'un dysfonctionnement."
))
story.append(figure(
    "partie6_graphframes_spark_ui.jpg", "Figure 10 : analyse GraphFrames dans Spark UI (onglet SQL trié par durée) : les "
    "requêtes de ConnectedComponents sont les plus longues.", width=CONTENT_WIDTH * 0.8, crop=(0, 40, 799, 519),
))
story.append(figure(
    "partie6_graphx_spark_ui.jpg", "Figure 11 : analyse GraphX dans Spark UI : jobs Pregel (composantes connexes), "
    "VertexRDD (PageRank) et écriture des métriques depuis partie6_graphx.scala.", width=CONTENT_WIDTH * 0.8,
    crop=(0, 40, 799, 519),
))
rule_rows = [
    ["Patient avec au moins 4 transferts", rules["transferts_frequents"]],
    ["Boucle A → B → A d'un même patient", rules["boucle_transfert"]],
    ["Transfert entre groupes vers un non-CHU", rules["inter_groupes_hors_chu"]],
    ["Non-CHU recevant au moins 20 % des transferts du groupe", rules["part_recue_elevee"]],
    ["Non-CHU émettant au moins 20 % des transferts du groupe", rules["part_emise_elevee"]],
]
story += table(
    [["Règle (seuil fixé avant l'évaluation)", "Injectés", "Détectés", "Exacts", "Non injectés", "Manqués"]]
    + [[label, str(r["injectes"]), str(r["detectes"]), str(r["vrais_positifs"]),
        str(len(r["detectes_non_injectes"])), str(len(r["manques"]))] for label, r in rule_rows],
    [3.2, 0.8, 0.8, 0.7, 1, 0.8],
)
story.append(p(
    "Les 4 boucles non injectées sont de vraies boucles : la liste des cas injectés n'est pas exhaustive. Une règle de boucle "
    f"au niveau des établissements serait inutilisable ({motifs['reciprocal_facility_pairs']} paires échangent des patients "
    "dans les deux sens). F-013 est manqué avec 19,3 % des transferts émis, juste sous le seuil de 20 % : nous ne l'avons pas "
    "abaissé après coup, car ce serait ajuster la règle à la réponse attendue. Un seuil relatif au type d'établissement serait "
    "plus robuste. Limites : données synthétiques, choix de modélisation (groupe du patient, services par groupe) et temps "
    "mesurés sur une seule exécution."
))

# Preuves
story.append(p("Preuves demandées", H1))
story += table([
    ["Preuve", "Emplacement"],
    ["Spark Master UI ; DAG et stages ; explain(True)", "Figures 1, 3 à 5 ; `rapport/resultats/partie3_explain.txt`"],
    ["Sink console avec plusieurs micro-batches", "Figure 7 ; `rapport/resultats/partie4_console.txt`"],
    ["mc ls --recursive local/lake/ (bronze, silver, gold)", "`rapport/resultats/mc_ls_lake.txt` (5 285 objets)"],
    ["DESCRIBE HISTORY ; kafka-consumer-groups.sh --describe", "`rapport/resultats/partie5_describe_history.txt`, `partie5_consumer_groups.txt`"],
    ["Graphe, motifs, GraphX, contrôle inter-groupes, degrés, exports", "Partie 6, figures 9 à 11 ; `rapport/resultats/partie6_*` ; `lake/gold/graph_analysis/`"],
    ["Code, versions et lancement", "`docker-compose.yml`, `spark/`, `producer.py`, `README.md`"],
], [2.2, 3])
story.pop()

document = SimpleDocTemplate(
    str(OUTPUT), pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.8 * cm, bottomMargin=1.7 * cm,
    title="UrgenceNet.ma : rapport final", author=AUTHORS, subject="Écosystème Big Data 2 : Parties 1 à 6",
)
document.build(story, onFirstPage=footer, onLaterPages=footer)
print(f"{OUTPUT} : {document.page} pages")
if document.page > MAX_PAGES:
    raise SystemExit(f"Le rapport depasse {MAX_PAGES} pages")
