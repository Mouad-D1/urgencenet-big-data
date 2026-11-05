# Partie 3 : Spark SQL et DataFrames

Script : `spark/partie3_sql.py`. Schémas explicites : `spark/schemas.py`. Tous les résultats ci-dessous ont été recoupés
par un recalcul indépendant en Python, sans Spark, et sont identiques. Plans complets :
`resultats/partie3_explain.txt`.

## Attente moyenne par ville

L'API DataFrame et la requête SQL donnent les mêmes résultats, identiques à ceux des RDD de la Partie 2.
`sameSemantics` confirme qu'elles produisent le même plan : SQL et API ne sont que deux façons d'écrire la même requête.

## Prises en charge et attente moyenne (min) par type et par ville, après dédoublonnage

| Type | Casablanca | Marrakech | Rabat | Tanger |
|---|---:|---:|---:|---:|
| CENTRE_SANTE | 488 (37,32) | 367 (36,80) | 348 (40,02) | 232 (39,34) |
| CHU | 1 281 (37,95) | 795 (37,69) | 779 (35,67) | 833 (37,20) |
| CLINIQUE | 1 134 (36,99) | 1 106 (37,73) | 974 (37,25) | 463 (36,71) |
| HOPITAL_REGIONAL | 1 345 (37,43) | 814 (37,71) | 938 (37,60) | 360 (37,52) |

Les écarts d'attente ne sont pas interprétables comme une différence de qualité : dans ces données synthétiques, le
délai dépend de la priorité, et les attentes les plus élevées (40,02 et 39,34) portent sur de petits effectifs.

## Tranches d'attente : UDF ou expression native

Les deux versions donnent le même résultat sur les données brutes : court 1 842, moyen 6 722, long 3 932.

| Version | Plan physique | Médiane (5 exécutions alternées) |
|---|---|---|
| UDF Python | `BatchEvalPython [wait_band(wait_minutes#6)]`, hors génération de code | 0,64 s |
| `when/otherwise` | `*(1) Project [CASE WHEN ...]`, dans la génération de code | 0,41 s |

La version native est environ 1,6 fois plus rapide sur 50 001 lignes en cache, ce qui isole le coût de la tranche de
celui de la lecture du JSON. L'UDF est une boîte noire pour Catalyst, qui ne peut pas l'optimiser. Chaque valeur doit
aussi être sérialisée de la JVM vers un processus Python, puis renvoyée. L'expression native reste dans la JVM et
Tungsten la compile en bytecode avec le reste de l'étape.

## Recalcul du délai par auto-jointure

L'auto-jointure entre ARRIVED et CARE_STARTED donne 12 257 lignes, une par CARE_STARTED unique. Le délai recalculé
diffère de `wait_minutes` pour **0 passage**, donc le champ est cohérent avec les horodatages.

Le dédoublonnage doit précéder cette jointure : sur les données brutes, elle produit 12 751 lignes, car chaque
doublon multiplie les correspondances.

## Dédoublonnage et couche Silver

| Statut | Avant | Après |
|---|---:|---:|
| ARRIVED | 12 510 | 12 257 |
| TRIAGED | 12 482 | 12 257 |
| CARE_STARTED | 12 496 | 12 257 |
| DISCHARGED | 11 334 | 11 099 |
| TRANSFERRED | 1 179 | 1 158 |
| **Total** | **50 001** | **49 028** |

973 doublons sont supprimés. Après dédoublonnage, chaque passage a exactement un événement par étape, et DISCHARGED +
TRANSFERRED = 12 257. Le résultat est écrit dans `lake/silver/urgences_history/`, avec un dossier et un fichier Parquet
par ville, et relu : 49 028 lignes (capture `captures/partie3_minio_silver.jpg`). Le `repartition("city")` évite de
multiplier les petits fichiers.

Le référentiel `facilities.json` est stocké dans `lake/bronze/facilities/` : ce dossier s'ajoute à l'organisation du
sujet, car les Workers n'ont pas accès aux fichiers locaux du conteneur Jupyter.

## Lecture de `explain(True)` sur la jointure

Dans le code, le filtre `status = 'CARE_STARTED'` est volontairement écrit **après** la jointure.

**Plan logique analysé.** Les colonnes sont résolues et typées, et la structure suit l'ordre du code :

```
Filter (status = CARE_STARTED)
+- Project [facility_id, visit_id, patient_id, city, status, ... , type]
   +- Join Inner, (facility_id = facility_id)
```

**Plan optimisé par Catalyst.** Le filtre passe sous la jointure et ne reste que les colonnes utiles :

```
Project [city, wait_minutes, type]
+- Join Inner, (facility_id = facility_id)
   :- Project [facility_id, city, wait_minutes]
   :  +- Filter ((isnotnull(status) AND (status = CARE_STARTED)) AND isnotnull(facility_id))
   +- Project [facility_id, type]
      +- Filter isnotnull(facility_id)
```

Catalyst a appliqué trois optimisations : déplacement du filtre (predicate pushdown), élimination des colonnes inutiles
(column pruning) et ajout de `isnotnull`, puisqu'une clé nulle ne peut jamais satisfaire la jointure.

**Plan physique.** La stratégie est un **BroadcastHashJoin** :

```
*(2) BroadcastHashJoin [facility_id], [facility_id], Inner, BuildRight, false
:- *(2) Filter (... status = CARE_STARTED ...)
:  +- FileScan json [facility_id, city, status, wait_minutes] PushedFilters: [IsNotNull(status), EqualTo(status,CARE_STARTED), ...]
+- BroadcastExchange
```

Le référentiel (7,9 Kio) est sous le seuil `spark.sql.autoBroadcastJoinThreshold` de 10 Mio : Spark l'envoie donc à
chaque Executor au lieu de faire transiter les événements par un shuffle, comme le ferait un SortMergeJoin. Le plan
initial et le plan final (`isFinalPlan=true`) choisissent tous les deux cette stratégie.

Spark UI confirme ces plans (`captures/partie3_plan_jointure.jpg`) : le scan renvoie 12 496 lignes sur 50 001, le
filtre étant appliqué dès le parsing, et seules 40 lignes sont diffusées. Le fichier JSON reste toutefois lu en entier
(12,6 Mio) : contrairement à Parquet, ce format ne permet pas de sauter des blocs.

## DAG et stages

`captures/partie3_dag_job25.jpg` montre la chaîne de la requête : stage 41 (scan, jointure et agrégation partielle),
stage 42 (agrégation finale) puis stage 43 (tri final). Les deux premiers sont marqués `skipped`, car l'exécution
adaptative (AQE) lance une requête en plusieurs jobs : ils ont été calculés par les jobs 21 à 24 de la même requête, et
le job 25 réutilise leurs résultats. `AQEShuffleRead coalesced` indique qu'AQE a fusionné les 200 partitions de shuffle
par défaut, presque vides ici.

## Comparaison RDD, DataFrame et Dataset

Même calcul (attente moyenne par ville), lecture complète du fichier à chaque exécution :

| API | Médiane | Min à max |
|---|---:|---:|
| RDD (Partie 2) | 1,19 s | 1,06 à 1,96 s |
| DataFrame | 0,60 s | 0,59 à 0,69 s |

Le DataFrame est environ 2 fois plus rapide. Avec les RDD, chaque ligne est parsée en entier par `json.loads` dans un
processus Python, y compris les 37 505 lignes qui seront ensuite filtrées. Le DataFrame ne parse que les colonnes utiles
(`status`, `city` et `wait_minutes`, soit 3 sur 11) et filtre dès le scan, dans la JVM.

| Critère | RDD | DataFrame | Dataset (Scala/Java) |
|---|---|---|---|
| Typage | Objets typés à la compilation en Scala, aucun contrôle en Python | Schéma vérifié à l'analyse, à l'exécution | Vérifié à la compilation |
| Catalyst | Non : Spark ne voit pas l'intérieur des lambdas | Oui | Oui, sauf dans les lambdas typées |
| Tungsten | Non | Oui | Oui, avec un coût de conversion vers les objets |
| Disponible en PySpark | Oui | Oui | Non |

Limites de cette mesure :

- Le Dataset n'a pas été mesuré, car l'API n'existe pas en PySpark. Un DataFrame est un `Dataset[Row]`.
- Les deux temps viennent de deux exécutions distinctes, sur seulement 13 Mio : l'ordre de grandeur compte davantage
  que la valeur exacte.
- Le coût des RDD observé ici est en partie celui de Python. L'écart serait probablement plus faible avec des RDD en
  Scala, ce qui n'a pas été testé.

**Piège de mesure.** Relancer `collect()` sur le même DataFrame réutilise son plan adaptatif et ses fichiers de shuffle.
Mesurée ainsi, l'UDF semblait plus rapide que `when/otherwise` (0,08 s contre 0,15 s), ce qui était faux. Chaque
mesure reconstruit donc un nouveau DataFrame.
