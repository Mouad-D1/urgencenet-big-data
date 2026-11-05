# Partie 1 : cluster et positionnement de Spark

## Rôles dans notre déploiement

Le cluster fonctionne en mode standalone sous Docker Compose (un Master, deux Workers de 2 cœurs et 2 Go).

- **Driver** : le processus PySpark lancé dans le conteneur `jupyter` (notebook ou `spark-submit` en mode client).
  Il exécute notre code, construit le DAG, le découpe en stages et en tâches, puis planifie ces tâches sur les
  Executors. Sa mémoire est limitée à 1 Go : un `collect()` sur un gros volume le mettrait en difficulté.
- **Cluster Manager** : le Master standalone (`spark://spark-master:7077`). Il connaît les Workers et leurs
  ressources, et attribue celles-ci aux applications. Il ne traite aucune donnée et n'exécute aucune tâche.
- **Workers et Executors** : chaque Worker lance, à la demande du Master, un Executor pour notre application. Nous
  obtenons deux Executors de 2 cœurs et 1,5 Go. Ce sont eux qui exécutent les tâches, gardent les données en cache
  et lisent ou écrivent directement dans MinIO. Ils disparaissent à la fin de l'application.
- **SparkContext** : point d'entrée créé dans le Driver (accessible par `spark.sparkContext`). Il établit la
  connexion avec le Master, obtient les Executors et leur envoie les tâches.

Résultat mesuré par `spark/partie1_cluster.py` :

| Élément | Valeur |
|---|---|
| Version de Spark | 3.5.7 |
| Master | `spark://spark-master:7077` |
| appName | `UrgenceNet-Partie1` |
| Executors enregistrés | 2 |
| `sc.defaultParallelism` | 4 (2 Workers × 2 cœurs) |

Point d'attention : `defaultParallelism` vaut 2 tant que les Executors ne sont pas enregistrés auprès du Driver.
Le script attend donc leur enregistrement avant d'afficher la valeur.

## Deux limites de MapReduce/Hadoop corrigées par Spark

1. **Le traitement est uniquement batch.** Un job MapReduce traite un jeu de données fini, puis se termine. Or
   UrgenceNet.ma reçoit un flux continu d'événements Kafka (20 par seconde en démonstration), dont environ 5 %
   arrivent en retard. Structured Streaming traite ce flux par micro-batches, avec les fenêtres temporelles, le
   watermark et la reprise sur checkpoint des Parties 4 et 5.
2. **Les résultats intermédiaires passent par le disque.** MapReduce écrit sur disque entre les phases et entre les
   jobs, ce qui pénalise les traitements enchaînés et itératifs. Spark enchaîne les transformations dans un même DAG
   et peut garder les données en mémoire. C'est décisif pour PageRank et les composantes connexes (Partie 6), qui
   répètent des calculs sur le même graphe de transferts.

## Captures à insérer

- Interface Spark Master montrant les 2 Workers actifs.
- Console MinIO montrant le bucket `lake`.
