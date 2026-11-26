# Partie 4 : Structured Streaming depuis Kafka

Script : `spark/partie4_streaming.py`. Producteur : `producer.py --mode kafka --rate 20`. Exécution de référence :
8 minutes de flux, du 06/11/2026 14:40 à 14:48 UTC, sur un topic neuf. Journaux complets :
`resultats/partie4_console.txt` (Spark) et `resultats/partie4_producteur.txt` (producteur). Chiffres :
`resultats/partie4.json`.

## Requêtes lancées en parallèle

Cinq requêtes lisent le topic `urgences`, toutes avec un déclencheur `processingTime` de 10 secondes.

| Requête | Sortie | Mode | Rôle |
|---|---|---|---|
| `console_flux_brut` | console | append | flux brut typé par `from_json` et le schéma de la Partie 3 (questions 1 à 3) |
| `bronze_urgences_raw` | JSON dans `lake/bronze/urgences_raw/` | append | messages tels que lus dans Kafka, avec topic, partition, offset et horodatage (question 4) |
| `console_ville_priorite` | console | complete | passages par ville et priorité, événements TRIAGED, watermark de 10 minutes (questions 5 et 6) |
| `total_complete` | mémoire | complete | même calcul, pour vérifier son total en fin d'exécution |
| `watermark_par_passage` | foreachBatch | update | fenêtre de 10 minutes avec watermark, pour suivre chaque événement en retard (question 6) |

Chaque requête avec état ou avec fichiers de sortie a son propre checkpoint sous `lake/checkpoints/partie4/`.
`spark.sql.shuffle.partitions` est réduit à 8, car les 200 partitions par défaut créeraient 200 tâches et 200
fichiers d'état par micro-batch pour quelques centaines de lignes.

Captures : `captures/partie4_requetes_actives.jpg` (les 5 requêtes actives) et
`captures/partie4_statistiques_watermark.jpg` (débit et lignes par micro-batch). Elles ont été prises pendant une
première exécution identique, le 05/11/2026, et non pendant l'exécution de référence.

Captures du sink console dans un terminal : `captures/partie4_console_zoom.png`,
`captures/partie4_console_flux_brut.jpg` et `captures/partie4_console_microbatches.jpg`. Elles viennent d'une
exécution de démonstration du 26/11/2026, relancée avec `--reset` : le checkpoint précédent pointait vers un topic
recréé depuis, et Spark refusait de reprendre (`OffsetOutOfRangeException`).

## Micro-batches successifs en console

Extrait de `resultats/partie4_console.txt`. Le flux brut en mode append n'affiche que les nouvelles lignes du batch,
alors que le mode complete réaffiche à chaque batch toute la table de résultats mise à jour :

```
Batch: 19                               Batch: 20
|city      |priority|count|             |city      |priority|count|
|Casablanca|1       |24   |             |Casablanca|1       |26   |
|Casablanca|2       |55   |             |Casablanca|2       |55   |
|Casablanca|3       |133  |             |Casablanca|3       |135  |
|Casablanca|4       |105  |             |Casablanca|4       |114  |
...
```

## Vérifications

| Contrôle | Résultat |
|---|---|
| Messages dans le topic / lignes en Bronze / couples (partition, offset) distincts en Bronze | 9 101 / 9 101 / 9 101 |
| Événements TRIAGED dans le topic / total de la requête en mode complete | 2 284 / 2 284 |
| Fenêtre avec watermark : lignes retenues + rejetées | 2 278 + 6 = 2 284 |

La couche Bronze contient exactement une fois chaque message, sans perte ni doublon : le sink fichier ne publie
un micro-batch qu'une fois inscrit dans son journal `_spark_metadata`. Le prix à payer est un grand nombre de petits
fichiers (un par partition et par micro-batch), qu'il faudrait compacter en production.

## Que devient un événement arrivant avec 15 minutes de retard ?

La réponse dépend de la requête.

**Dans le comptage par ville et priorité (mode complete) : il est compté.** Les 2 284 événements TRIAGED sont
tous dans le total, y compris ceux arrivés avec près de 15 minutes de retard. En mode complete, Spark doit
réafficher toute la table de résultats à chaque batch : il ne peut donc jamais purger l'état, et le watermark n'a
aucun effet. De plus, sans fenêtre temporelle, aucun groupe n'est jamais « clos ».

**Dans une agrégation par fenêtre (modes append ou update) : il est rejeté seulement si sa fenêtre est déjà close.**
Le watermark vaut `max(event_time) vu - 10 minutes`. Un événement est écarté quand la fin de sa fenêtre est
antérieure ou égale au watermark. Être plus ancien que le watermark ne suffit pas.

Deux exemples tirés de nos journaux, avec à peu près le même retard et des sorts opposés :

```
producteur : [RETARD 14.6 min] V-1601067300 TRIAGED event_time=2026-11-06T14:28:07Z emis a 2026-11-06T14:42:40Z
Spark      : [WATERMARK] batch=10 ... watermark=2026-11-06T14:32:30.000Z rejetees=0
Spark      : [WATERMARK] batch=11 ... watermark=2026-11-06T14:32:39.000Z rejetees=1

producteur : [RETARD 14.9 min] V-1601067698 TRIAGED event_time=2026-11-06T14:31:01Z emis a 2026-11-06T14:45:54Z
Spark      : [WATERMARK] batch=29 ... watermark=2026-11-06T14:35:39.000Z rejetees=0
Spark      : [WATERMARK] batch=30 ... watermark=2026-11-06T14:35:49.000Z rejetees=0
```

- **V-1601067300** est lu par le batch 11. Sa fenêtre [14:20, 14:30[ se termine avant le watermark de 14:32:30 :
  elle est close, et l'événement est rejeté (`rejetees=1`).
- **V-1601067698** est encore plus en retard, et son `event_time` (14:31:01) est lui aussi antérieur au watermark
  (14:35:39). Mais sa fenêtre [14:30, 14:40[ est encore ouverte : il est retenu et compté.

Point observé dans nos journaux : les retards sont filtrés avec le watermark du **micro-batch précédent**
(14:32:30 pour le batch 11), tandis que le watermark du batch courant sert à purger l'état. Avec cette règle, nous
prédisons sans erreur, pour chaque événement, s'il est retenu ou rejeté. Avec le watermark du batch courant, une
première exécution donnait 2 erreurs sur 42 événements de plus de 10 minutes de retard.

Sur l'ensemble de l'exécution :

| Retard à l'arrivée | Événements | Retenus | Rejetés |
|---|---:|---:|---:|
| moins de 1 min | 2 131 | 2 131 | 0 |
| 1 à 5 min | 37 | 37 | 0 |
| 5 à 10 min | 40 | 40 | 0 |
| 10 à 12,5 min | 19 | 19 | 0 |
| 12,5 à 15 min | 22 | 16 | 6 |

Le watermark donne donc une garantie dans un seul sens : un événement en retard de moins de 10 minutes n'est jamais
rejeté. Au-delà, il peut être rejeté, mais ce n'est pas certain. Au tout début de la requête, le watermark n'existe
pas encore, et rien n'est rejeté.

Limites : un seul run de 8 minutes, donc peu de rejets (6). Les retards sont mesurés entre l'`event_time` et
l'horodatage d'écriture dans Kafka.

## Modèle source → transformation → sink

Structured Streaming présente le flux comme une **table infinie** : chaque message Kafka ajoute une ligne. La requête
s'écrit comme sur une table statique (`select`, `filter`, `groupBy`), et Spark l'exécute de façon incrémentale. À
chaque déclenchement, il lit les nouveaux offsets (source), met à jour le résultat en ne traitant que ces nouvelles
lignes et l'état conservé (transformation), puis écrit dans le sink la partie du résultat que demande le mode de sortie.

## Micro-batch ou traitement continu

- `trigger(processingTime="10 seconds")` (utilisé ici) : un micro-batch toutes les 10 secondes. La latence est de
  l'ordre du déclencheur, mais toutes les opérations sont possibles (agrégations, fenêtres, jointures, déduplication),
  avec une garantie exactly-once lorsque le sink le permet. Si un batch dure plus longtemps que l'intervalle, le
  suivant démarre dès la fin du précédent : Spark le signale par « Current batch is falling behind ».
- `trigger(continuous="1 second")` : des tâches de longue durée traitent les messages au fil de l'eau, avec une latence
  de l'ordre de la milliseconde. Le délai indiqué est la fréquence des checkpoints. Ce mode reste expérimental : il
  n'accepte que des opérations ligne à ligne (`select`, `filter`, `map`), sans agrégation ni fenêtre, et ne garantit
  que l'at-least-once. Il n'a pas été testé ici, car nos requêtes agrègent.

## Les trois modes de sortie

| Mode | Écrit à chaque batch | Usage ici |
|---|---|---|
| append | uniquement les nouvelles lignes, définitives. Avec une agrégation, il exige un watermark et n'émet une fenêtre qu'une fois close | flux brut et Bronze |
| update | les lignes du résultat modifiées par ce batch | fenêtre avec watermark |
| complete | toute la table de résultats, réservé aux agrégations ; l'état n'est jamais purgé | comptage par ville et priorité |
