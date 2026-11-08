# Partie 6 : analyse de graphes avec GraphX et GraphFrames

Scripts : `spark/partie6_graphframes.py` (PySpark), `spark/partie6_graphx.scala` (Scala, API RDD) et
`spark/partie6_restitution.py` (comparaison, table de métriques, figure). Source : l'historique nettoyé de la Partie 3
(`lake/silver/urgences_history/`, 49 028 événements). Résultats dans `resultats/partie6_*` et dans
`lake/gold/graph_analysis/`.

## Versions et lancement

| Composant | Version |
|---|---|
| Spark / GraphX | 3.5.7 (`spark-graphx_2.12-3.5.7`, inclus dans Spark) |
| Scala / Java | 2.12.18 / 17 |
| GraphFrames | 0.8.4-spark3.5-s_2.12 (dépôt spark-packages), module Python inclus dans le jar |
| Python | 3.10.12 |

GraphFrames n'est pas dans l'image : `spark-submit` l'ajoute avec `--packages`, ce qui télécharge le jar, le distribue
aux Executors et place son module Python dans le `PYTHONPATH` :

```bash
docker compose exec jupyter spark-submit --driver-memory 2g --conf spark.jars.ivy=/home/spark/.ivy2 \
    --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 --repositories https://repos.spark-packages.org \
    spark/partie6_graphframes.py
```

`spark.jars.ivy` est nécessaire car l'utilisateur `spark` de l'image a pour répertoire personnel `/nonexistent`. Le code
GraphX est lancé en Scala dans `spark-shell`, qui contient déjà GraphX :

```bash
docker compose exec jupyter spark-shell --master spark://spark-master:7077 --conf spark.cores.max=4 -i spark/partie6_graphx.scala
```

## Modèle du graphe

| Sommets | Identifiant | Groupe hospitalier |
|---|---|---|
| Patient | `P-08812` | groupe de sa première admission |
| Établissement | `F-012` | celui du référentiel |
| Service | `D-<groupe>-<service>`, par exemple `D-GHT-Sud-Cardiologie` | le groupe de l'établissement |

Colonnes : `id`, `vertex_type`, `name`, `hospital_group`, `facility_type`. Les préfixes P-, F- et D- excluent toute
collision d'identifiants, et le contrôle `groupBy("id")` le confirme (0 doublon). Un service est rattaché à un groupe
pour que chaque sommet appartienne à un seul groupe.

| Arêtes | Sens | Source | Attributs |
|---|---|---|---|
| `ADMITTED_AT` | patient → établissement | événements ARRIVED | visit_id, patient_id, event_time |
| `TREATED_IN` | patient → service | événements ARRIVED | visit_id, patient_id, event_time |
| `HAS_DEPARTMENT` | établissement → service | couples distincts | |
| `TRANSFERRED_TO` | établissement → établissement | événements TRANSFERRED | visit_id, patient_id, event_time |

Colonnes : `src`, `dst`, `relationship`, attributs ci-dessus et `hospital_group` (celui de la source). Le graphe compte
6 804 sommets et 25 831 arêtes :

| Groupe | Patients | Établissements | Services | ADMITTED_AT | TREATED_IN | HAS_DEPARTMENT | TRANSFERRED_TO |
|---|---:|---:|---:|---:|---:|---:|---:|
| GHT-Atlantique | 2 337 | 14 | 7 | 4 248 | 4 248 | 57 | 419 |
| GHT-Nord | 2 731 | 16 | 7 | 4 927 | 4 927 | 63 | 418 |
| GHT-Sud | 1 675 | 10 | 7 | 3 082 | 3 082 | 39 | 321 |

## Contrôles de cohérence

- 1 158 transferts : aucun vers un établissement inexistant, aucun d'un établissement vers lui-même, aucune arête dont
  une extrémité n'est pas un sommet.
- 58 transferts entre groupes : 48 vers un CHU, ce qui est autorisé, et **10 vers un établissement qui n'est pas un
  CHU**, ce qui est interdit.

## Motifs et degrés avec GraphFrames

Les motifs sont cherchés avec `find()` sur le graphe des transferts (une arête par transfert), en imposant le même
patient et l'ordre chronologique des deux transferts :

| Motif | Résultat |
|---|---|
| Chaîne A → B → C (`(a)-[e1]->(b); (b)-[e2]->(c)`, A ≠ C) | 64 chaînes, 48 patients |
| Boucle A → B → A (`(a)-[e1]->(b); (b)-[e2]->(a)`) | 12 boucles, 12 patients |
| Patients admis dans plusieurs établissements (`(p)-[e1]->(f1); (p)-[e2]->(f2)`) | 3 120 patients |
| Paires d'établissements reliées dans les deux sens, tous patients confondus | 104 paires |

Les CHU reçoivent le plus de transferts (F-025 : 119, F-031 : 113, F-015 : 109). Les plus gros émetteurs sont un centre
de santé (F-013 : 78) et des hôpitaux régionaux. Le graphe complet forme une seule composante connexe : les transferts
entre groupes et les patients relient tout le réseau.

## Métriques GraphX par groupe

Chaque sommet reçoit un `VertexId` Long unique et stable : le rang de son identifiant texte dans l'ordre alphabétique
(`zipWithIndex`). Les calculs portent sur le graphe des transferts internes à chaque groupe.

Le PageRank est pondéré par le nombre de transferts. Le graphe garde une arête par transfert, et dans le PageRank de
GraphX, un sommet envoie `rang / degré sortant` sur chaque arête. k arêtes parallèles vers le même établissement lui
apportent donc k fois plus, ce qui équivaut à un poids égal au nombre de transferts. Les rangs sont normalisés pour
totaliser le nombre de sommets du groupe.

| Groupe | Établissements | Liaisons distinctes | Composantes | PageRank le plus élevé |
|---|---:|---:|---:|---|
| GHT-Atlantique | 14 | 108 | 1 | F-002 CHU (2,75), F-001 CHU (2,60), F-004 HR (1,95) |
| GHT-Nord | 16 | 135 | 1 | F-015 CHU (3,20), F-025 CHU (3,05), F-016 HR (1,69) |
| GHT-Sud | 10 | 68 | 1 | F-031 CHU (2,73), **F-035 clinique (2,00)**, F-033 HR (1,58) |

La table complète par établissement (degrés, transferts internes, partenaires en amont et en aval, parts du groupe,
PageRank, composante, alertes) est dans `resultats/partie6_metriques_etablissements.csv` et dans
`lake/gold/graph_analysis/metriques_etablissements/`. La figure `captures/partie6_graphe_transferts.png` représente les
trois sous-graphes.

**Lecture de la centralité.** Le PageRank mesure la position d'un établissement dans le réseau des transferts, pas la
qualité de ses soins. Les CHU dominent parce qu'ils sont destinataires des cas graves : c'est leur rôle. La position de
F-035, deuxième de son groupe alors que c'est une clinique, est une structure inhabituelle qui mérite une explication
(spécialité, convention, capacité). Elle ne prouve ni un dysfonctionnement ni une mauvaise pratique.

## Comparaison GraphX / GraphFrames

Sur le même sous-graphe (les 1 158 transferts), les degrés entrants et sortants des 40 établissements sont
**identiques** dans les deux analyses (0 écart). Les deux trouvent aussi une seule composante connexe pour le graphe
complet.

| Calcul | GraphFrames | GraphX |
|---|---:|---:|
| Degrés du graphe des transferts | 3,1 s (avec jointure aux sommets) | 0,7 s |
| Composantes connexes du graphe complet | 80,0 s | 4,1 s |
| PageRank et composantes, par groupe | non calculé | 7,4 à 16,0 s |

- **API** : GraphFrames manipule des DataFrames (colonnes nommées, motifs déclaratifs avec `find()`, résultats directement
  joignables et écrits en Parquet), utilisables en Python. GraphX travaille sur des RDD typés (`Graph[Sommet, String]`)
  en Scala, avec des identifiants Long obligatoires, et propose des opérateurs bas niveau (`subgraph`, `groupEdges`,
  `aggregateMessages`, Pregel). Il est plus verbeux pour exprimer un motif.
- **Temps** : GraphX garde le graphe en mémoire et itère directement sur ses partitions. Les composantes connexes de
  GraphFrames enchaînent des jointures de DataFrames et écrivent un checkpoint toutes les deux itérations, ici sur MinIO,
  d'où l'écart. Les temps GraphX excluent la construction du graphe et viennent d'exécutions uniques : seul l'ordre de
  grandeur est à retenir.
- **Mémoire** : `connectedComponents` de GraphFrames a d'abord saturé le Driver (1 Go). Le texte de son plan
  d'exécution, construit pour Spark UI, s'allonge à chaque itération. Nous relisons le graphe depuis son export Parquet
  (plan court), limitons `spark.sql.maxPlanStringLength` et donnons 2 Go au Driver.

## Règles d'alerte et pertinence

Seuils fixés avant toute comparaison avec les cas injectés par le générateur (`data/anomalies_ground_truth.json`) :

| Règle | Seuil | Injectés | Détectés | Bien détectés | Détectés non injectés | Manqués |
|---|---|---:|---:|---:|---:|---:|
| Transferts fréquents | patient avec au moins 4 transferts | 6 | 6 | 6 | 0 | 0 |
| Boucle de transferts | même patient A → B puis B → A | 8 | 12 | 8 | 4 | 0 |
| Transfert entre groupes hors CHU | destination d'un autre groupe qui n'est pas un CHU | 10 | 10 | 10 | 0 | 0 |
| Part reçue élevée | non-CHU recevant au moins 20 % des transferts de son groupe | 1 | 1 | 1 | 0 | 0 |
| Part émise élevée | non-CHU émettant au moins 20 % des transferts de son groupe | 1 | 0 | 0 | 0 | 1 |

Liste motivée des cas inhabituels : 29 alertes, dans `lake/gold/graph_analysis/anomalies/` et
`resultats/partie6_graphframes.json`. Par exemple : F-035 (clinique, GHT-Sud) reçoit 26,1 % des transferts internes de
son groupe ; P-02197 a été transféré 5 fois.

Pertinence des règles :

- **Les 4 boucles non injectées sont de vraies boucles.** Le générateur fait suivre un transfert d'une admission à
  destination, qui peut à son tour transférer le patient vers son établissement d'origine. La règle a raison, et la
  liste des cas injectés n'est pas exhaustive. Ces alertes relèvent d'une vérification humaine, pas d'une erreur de la
  règle.
- **Une règle de boucle au niveau des établissements serait inutilisable** : 104 paires d'établissements échangent des
  patients dans les deux sens, contre 8 boucles injectées. Le motif doit porter sur le même patient.
- **F-013 est manqué** : ce centre de santé émet 19,3 % des transferts de son groupe, juste sous le seuil de 20 %. Un
  seuil de 18 % l'aurait détecté sans fausse alerte, l'établissement suivant hors CHU étant à 17,2 %. Mais abaisser le
  seuil après avoir vu la réponse reviendrait à ajuster la règle aux données d'évaluation. Un seuil relatif (par
  exemple deux fois la part médiane des établissements de même type) serait plus robuste qu'un seuil fixe.

## Limites

- Données synthétiques : les anomalies injectées sont nettes, alors que de vraies données demanderaient des seuils
  calibrés sur l'historique.
- Choix de modélisation : un patient est rattaché au groupe de sa première admission, et un service à un groupe.
  D'autres choix (service par établissement, patient multi-groupes) changeraient les composantes et les degrés des
  services.
- Les règles ne sont évaluées que sur les cas injectés : un « faux positif » peut être une vraie structure inhabituelle,
  comme les 4 boucles.
- Les temps de calcul viennent d'exécutions uniques sur un petit graphe (6 804 sommets).
