# Partie 5 : pipeline de production Spark-Kafka

Script : `spark/partie5_pipeline.py`. Exécution de référence le 06/11/2026, de 15:47 à 16:15 UTC, sur un topic neuf :
24 minutes de flux à 20 événements par seconde, arrêt de la requête Silver à 10 minutes, puis reprise 2 minutes plus
tard. Journaux et chiffres dans `resultats/` : `partie5_console.txt`, `partie5.json`, `partie5_consumer_groups.txt`,
`partie5_describe_history.txt` et `mc_ls_lake.txt`.

## Architecture

```
Kafka urgences -> [silver] dédoublonnage + jointure référentiel + wait_band -> lake/silver/urgences_clean/ (Parquet)
                                                                                   |
   [gold_kpi_10min]         fenêtre tumbling 10 min    -> lake/gold/kpi_facility_10min/    (Delta, MERGE)
   [gold_taux_prioritaires] fenêtre glissante 10/5 min -> lake/gold/taux_prioritaires_10min/ (Delta, MERGE)
   [gold_sessions_triage]   fenêtre de session 5 min   -> lake/gold/sessions_triage/       (Delta, append)
```

Spark 3.5 refuse, dans une même requête en mode update, un dédoublonnage suivi d'une agrégation : plusieurs opérateurs
avec état ne peuvent s'enchaîner qu'en mode append. Le pipeline a donc deux étages. Les requêtes Gold relisent Silver
en flux, la source fichier ne lisant que les fichiers validés dans le journal `_spark_metadata` de Silver. Les
déclencheurs sont de 30 secondes, car chaque micro-batch crée au moins un fichier par ville dans Silver (184 fichiers
en 25 minutes).

## 1 à 3. Nettoyage, enrichissement et couche Silver

- **Dédoublonnage** : `withWatermark("event_time", "10 minutes").dropDuplicatesWithinWatermark(["visit_id", "status"])`.
  `dropDuplicates(["visit_id", "status"])`, combiné au watermark, rejetterait bien les retards, mais garderait chaque clé
  en mémoire pour toujours, faute de colonne de temps dans la clé. La variante `WithinWatermark` (Spark 3.5) purge une
  clé une fois le watermark passé. Elle convient ici, car nos doublons arrivent moins de 2 minutes après l'original.
- **Enrichissement** : jointure flux-statique (`broadcast`) avec `facilities.json` (nom, groupe, type, lits), puis
  `wait_band` en `when/otherwise`, la version native de la Partie 3. La jointure est externe gauche : un événement d'un
  établissement inconnu serait conservé avec un groupe nul (aucun cas observé).
- **Silver** : Parquet partitionné par `city` et `date`.

| Bilan exact topic / Silver | Valeur |
|---|---:|
| Messages dans le topic | 28 872 |
| Couples (visit_id, status) distincts dans le topic | 28 345 |
| Lignes dans Silver / clés distinctes dans Silver | 27 929 / 27 929 |
| Doublons rejetés / retards rejetés (métriques Spark) | 518 / 425 |
| **Contrôle : 27 929 + 518 + 425** | **28 872** |
| Clés absentes de Silver / retard minimal de ces clés | 416 / 10,65 min |

Silver ne contient aucun doublon. Chaque clé du topic est soit dans Silver, soit rejetée pour un retard de plus de
10 minutes. Le prix du watermark est donc mesuré : 1,5 % des événements, arrivés trop tard, sont exclus de Silver. En
production, il faudrait choisir un délai supérieur au retard maximal attendu (15 minutes ici), au prix d'indicateurs
finalisés plus tard.

À la reprise, un événement arrivé avec 14,9 minutes de retard a pourtant été conservé. Le premier batch après la reprise
utilise encore le watermark d'avant l'arrêt (15:46:59) : les événements accumulés pendant la pause sont jugés par rapport
à ce watermark ancien.

## 4 et 5. Fenêtres et couche Gold

- **Tumbling de 10 minutes** par établissement, avec ville et groupe : passages (`ARRIVED`), prises en charge et
  attente moyenne (`CARE_STARTED`). La requête écrit en mode update dans Delta, par `foreachBatch` et
  `MERGE INTO ... ON facility_id AND window_start`. Les indicateurs sont ainsi disponibles dès les premières minutes,
  puis corrigés à mesure que des événements de la fenêtre arrivent. En mode append, une fenêtre n'aurait été écrite
  qu'une fois close, soit 20 minutes après son début.
- **Glissante de 10 minutes, toutes les 5 minutes** : triages, cas de priorité 1 ou 2 et taux de cas prioritaires,
  écrits de la même façon (320 lignes).
- **Session par `desk_id`**, inactivité de 5 minutes : nombre de triages et durée d'activité (dernier moins premier
  triage), en mode append, le seul possible pour une fenêtre de session (65 sessions).

Contrôle de la table Gold : les 103 fenêtres closes à la fin (fin de fenêtre antérieure au watermark final de
16:01:15) ont été recalculées en batch depuis Silver. Les 103 sont identiques, en passages comme en prises en charge.

Relecture batch et historique (extrait de `DESCRIBE HISTORY`, 45 versions) :

```
|version|timestamp          |operation|numTargetRowsInserted|numTargetRowsUpdated|numOutputRows|
|22     |2026-11-06 16:01:18|MERGE    |40                   |19                  |134          |
|21     |2026-11-06 16:00:46|MERGE    |0                    |44                  |94           |
|20     |2026-11-06 16:00:20|MERGE    |0                    |42                  |94           |
|19     |2026-11-06 15:59:56|MERGE    |0                    |57                  |103          |
|18     |2026-11-06 15:57:42|MERGE    |0                    |48                  |103          |
|0      |2026-11-06 15:48:28|WRITE    |NULL                 |NULL                |47           |
```

La version 0 crée la table, puis chaque micro-batch non vide donne un MERGE. Une ligne est insérée quand une nouvelle fenêtre
apparaît (40 établissements à 16:01:18), et mise à jour sinon. L'historique ne montre aucune version entre 15:57:42 et
15:59:56 : la requête Silver était arrêtée, et Gold n'avait rien de nouveau à fusionner.

Limite pour les sessions : la moyenne observée de 4 minutes est réduite par les bornes de l'exécution. Le flux en temps
réel commence à 15:47, et les sessions précédentes ne contiennent que des événements arrivés en retard, souvent un seul
triage (durée 0). Par exemple, le poste T-MAR-01 était actif de 15:40 à 15:52 selon le générateur, mais sa session ne
commence qu'avec le flux. Les sessions suivantes ne sont pas encore closes à la fin. Mesurer des sessions complètes
demanderait une exécution d'au moins 45 minutes.

## 6. Arrêt, reprise et offsets

| Moment | Constat |
|---|---|
| Arrêt de Silver (15:57:16) | 11 333 lignes et 11 333 clés dans Silver ; dernier fichier `offsets/20`, dernier `commits/20` |
| Contenu de `checkpoints/partie5/silver/offsets/20` | `{"urgences":{"2":3824,"1":3987,"0":3869}}`, avec le watermark du batch |
| Premier batch après la reprise (batch 21) | début `{"2":3824,"1":3987,"0":3869}`, fin `{"2":4770,"1":4854,"0":4811}`, 2 755 lignes accumulées |
| Fin de l'exécution | bilan exact ci-dessus, aucun doublon dans Silver |

La reprise part exactement des offsets enregistrés : rien n'est perdu ni relu deux fois. La requête garde le même
identifiant et prend un nouveau `runId` (capture `captures/partie5_reprise_silver.jpg`).

**Batch interrompu (premier essai complet, `resultats/partie5_essai1.json`).** L'arrêt était tombé pendant le
batch 21 : `offsets/21` avait été écrit à 15:26:30, mais le dernier commit était `commits/20`. À la reprise, Spark a
rejoué le batch 21 avec les mêmes offsets, et `commits/21` n'a été écrit qu'à 15:28:40. Silver était là aussi sans
doublon, et chaque clé absente avait plus de 10 minutes de retard. Les fichiers éventuellement écrits par la tentative
interrompue ne figurent pas dans `_spark_metadata` : ils sont ignorés par les lecteurs.

**Comparaison avec `kafka-consumer-groups.sh --describe`.** Spark ne commite pas ses offsets dans Kafka. Il assigne
lui-même les partitions et garde ses offsets dans le checkpoint. Le groupe `urgencenet-silver`, configuré par
`kafka.group.id`, n'apparaît donc pas : « Consumer group 'urgencenet-silver' does not exist ». Pour suivre le retard
avec les outils Kafka, un listener recopie, après chaque micro-batch Silver, les offsets traités dans le groupe
`urgencenet-silver-suivi` :

```
                                           partition  CURRENT-OFFSET  LOG-END-OFFSET  LAG
Silver arrêtée (15:57:16)                  2          3824            3983            159
Silver arrêtée depuis 1 minute (15:58:24)  2          3824            4459            635
Après la reprise (15:59:23)                2          4848            4944            96
Fin (16:15:21)                             2          9622            9622            0
```

Pendant l'arrêt, `CURRENT-OFFSET` reste égal à l'offset du checkpoint, tandis que le producteur continue d'écrire : le
retard monte à 635 messages par partition. Après la reprise, il retombe au flux d'un micro-batch, puis à 0.

## 7. Garantie exactly-once

Le pipeline traite chaque événement exactement une fois, grâce à trois éléments :

1. **Une source rejouable.** Kafka conserve les messages, et Spark peut relire n'importe quelle plage d'offsets.
2. **Un checkpoint.** Avant de traiter un batch, Spark écrit sa plage d'offsets dans `offsets/N` (journal d'intention).
   Il écrit `commits/N` une fois le sink terminé. À la reprise, un batch sans commit est rejoué avec exactement les mêmes
   offsets. L'état des opérateurs (dédoublonnage, fenêtres) est lui aussi sauvegardé à chaque batch.
3. **Des sinks idempotents ou transactionnels.**
   - **Silver (Parquet)** : un batch n'est visible qu'une fois inscrit dans `_spark_metadata/N`, et un batch déjà
     inscrit est ignoré s'il est rejoué.
   - **Sessions (Delta en append)** : chaque écriture est une transaction qui enregistre l'identifiant de la requête et
     le numéro de batch, et Delta ignore un batch déjà validé.
   - **Gold KPI (MERGE)** : `foreachBatch` n'offre seul que de l'at-least-once. Le MERGE le rend idempotent : rejouer un
     batch réécrit les mêmes valeurs agrégées sur les mêmes clés.

Un sink console n'offre aucune de ces garanties. Il affiche le batch sans rien enregistrer d'atomique. Un batch rejoué
après une panne est donc affiché une seconde fois, et rien ne permet de savoir ce qui a déjà été montré.

Limites : un seul écrivain par table Delta. Sur un stockage S3, Delta n'assure pas les écritures concurrentes sans
LogStore adapté. Le nombre de petits fichiers dans Silver, Gold et les checkpoints (5 285 objets dans le lac) devrait
être réduit en production par compactage (`OPTIMIZE`) et nettoyage (`VACUUM`).
