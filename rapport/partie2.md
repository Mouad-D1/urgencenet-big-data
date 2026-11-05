# Partie 2 : traitement batch avec SparkCore (RDD)

Script : `spark/partie2_rdd.py`. Source : `s3a://lake/bronze/history/` (50 001 événements). Les résultats ont été
recoupés par un recalcul indépendant en Python, sans Spark, et sont identiques.

## Résultats

| Ville | Prises en charge | Attente moyenne (min) |
|---|---:|---:|
| Casablanca | 4 334 | 37,45 |
| Marrakech | 3 142 | 37,67 |
| Rabat | 3 091 | 37,22 |
| Tanger | 1 929 | 37,43 |

| Priorité | 1 | 2 | 3 | 4 | 5 |
|---|---:|---:|---:|---:|---:|
| Passages | 596 | 1 886 | 4 398 | 3 676 | 1 926 |

Top 5 des établissements par prises en charge : F-025 (851), F-031 (809), F-015 (790), F-001 (667), F-002 (641).
Ce sont tous des CHU.

Limites de lecture :

- Les doublons ne sont supprimés qu'en Partie 3, comme le demande le sujet. Les effectifs sont donc légèrement
  surestimés : 12 496 événements CARE_STARTED et 12 482 TRIAGED pour 12 257 passages.
- L'écart entre villes (moins d'une demi-minute) n'est pas significatif : dans ces données synthétiques, le délai
  dépend de la priorité et non de la ville.

## Transformations et actions

| Type | Opérations du script | Effet |
|---|---|---|
| Transformations (lazy) | `textFile`, `map`, `filter`, `reduceByKey`, `aggregateByKey`, `mapValues` | Complètent le lineage, sans aucun calcul |
| Actions | `collect`, `takeOrdered` | Lancent un job et ramènent un résultat au Driver |

`toDebugString` n'est ni l'une ni l'autre : il décrit le plan sans lancer de job.

## Lineage de l'attente moyenne par ville

```
(4) PythonRDD[19] at collect at /workspace/spark/partie2_rdd.py:42 []
 |  MapPartitionsRDD[9] at mapPartitions at PythonRDD.scala:160 []
 |  ShuffledRDD[8] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(4) PairwiseRDD[7] at aggregateByKey at /workspace/spark/partie2_rdd.py:63 []
    |  PythonRDD[6] at aggregateByKey at /workspace/spark/partie2_rdd.py:63 []
    |  s3a://lake/bronze/history/ MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0 []
    |  s3a://lake/bronze/history/ HadoopRDD[0] at textFile at NativeMethodAccessorImpl.java:0 []
```

`(4)` est le nombre de partitions et le décalage `+-` marque la frontière de shuffle. Le parsing `json.loads`, le
`filter` et le `map` n'apparaissent pas séparément : PySpark les enchaîne dans un seul `PythonRDD[6]`.

## DAG, stages et shuffle

Chaque action de calcul donne un job de **2 stages** et 8 tâches (4 par stage), réparties sur les deux Executors.
Capture : `captures/partie2_dag_job1.jpg`. Vue d'ensemble des jobs : `captures/partie2_jobs.jpg`.

| Job (attente moyenne par ville) | Stage 2 : `aggregateByKey` | Stage 3 : `collect` |
|---|---|---|
| Lecture depuis MinIO | 12,8 Mio (50 001 lignes) | aucune |
| Shuffle | 856 o écrits | 856 o lus |

**Pourquoi un shuffle.** `reduceByKey` et `aggregateByKey` doivent réunir toutes les valeurs d'une même clé dans une
seule partition, alors que les événements d'une ville sont répartis dans toutes les partitions d'entrée. C'est une
dépendance large : Spark coupe donc le DAG en deux stages. Le premier agrège localement chaque partition et écrit le
résultat sur le disque des Executors, réparti par clé. Le second relit ces blocs à travers le réseau et fusionne les
agrégats.

L'agrégation locale explique le faible volume observé : 12,8 Mio lus en entrée, seulement 856 octets échangés, car il
ne reste qu'une somme et un compte par ville et par partition. Un `groupByKey` aurait au contraire fait transiter
chaque valeur.

**Réutilisation du shuffle.** Relancer `collect` sur le même RDD (job 4) saute le premier stage (`Stage 8 (skipped)`,
capture `captures/partie2_dag_job4_stage_ignore.jpg`), car les fichiers de shuffle sont toujours présents : 0,42 s
contre 1,19 s pour un calcul complet. Le temps de référence est donc mesuré en reconstruisant le RDD à chaque
exécution, sinon la comparaison avec les DataFrames serait faussée.

## Tolérance aux pannes par le lineage

Un RDD ne stocke pas ses données mais la recette pour les produire. Si un Executor tombe, Spark recalcule seulement
les partitions perdues : il relit les blocs concernés dans MinIO, source durable, puis rejoue les transformations du
lineage sur un autre Executor. Si des fichiers de shuffle sont perdus, seules les tâches du stage précédent qui les
avaient produits sont relancées. Aucune réplication coûteuse des résultats intermédiaires n'est nécessaire. Ce
mécanisme est décrit ici sans avoir été provoqué expérimentalement.

## Temps mesurés (pour la comparaison de la Partie 3)

Attente moyenne par ville, RDD reconstruit à chaque exécution, 5 exécutions : **médiane 1,19 s** (de 1,06 à 1,96 s).

Le premier job de l'application (8,75 s) n'est pas comparable : il inclut le démarrage à froid des Workers Python, le
chargement des classes et l'ouverture de la connexion S3A.
