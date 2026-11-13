# Big Data Processing — Groupe gr-04

## Contributeurs

- Benjamin Labos (`benjaminlabos`), benjamin.labos@edu.ece.fr
- Nicolas Rihana (`nicohris`), nrihana1@gmail.com

## Streaming Wikipédia et IMDb

### Rejeu de démonstration depuis un clone

Depuis la racine du dépôt, démarrer les services puis copier les deux fichiers de démonstration dans le conteneur Spark. Le rejeu crée un topic et un checkpoint propres.

```powershell
$compose = '05.structured-streaming/lab_docker_wikistreams/compose.yaml'
$demo = 'project/streaming/demo'
docker compose -f $compose up -d --build
docker compose -f $compose cp "$demo/demo-events.jsonl.gz" pyspark-notebook:/tmp/gr04-demo-events.jsonl.gz
docker compose -f $compose cp "$demo/demo-reference.csv" pyspark-notebook:/tmp/gr04-demo-reference.csv
docker compose -f $compose exec -T pyspark-notebook python /home/jovyan/jobs/replay_capture.py --capture /tmp/gr04-demo-events.jsonl.gz --reference /tmp/gr04-demo-reference.csv --output /tmp/gr04-demo
```

Les résultats attendus sont 1 845 identifiants et offsets réconciliés, 1 311 modifications admissibles et sept correspondances sur six articles. Le jeu est une projection de la capture Docker du 11 novembre, dans l’ordre d’origine. Il conserve les champs nécessaires au filtrage et aux correspondances. Son catalogue couvre les pages rapprochées dans cette capture. Les délais de la capture vivante sont documentés dans sa preuve. [Empreintes du jeu](project/streaming/demo/manifest.json) · [Preuve de la capture Docker](project/streaming/evidence/docker-live-2026-11-11.json)

Les commandes suivantes sont à lancer depuis PowerShell, à la racine du dépôt. Docker Desktop doit être démarré et configuré pour les conteneurs Linux. La première exécution nécessite un accès aux registres d’images, à Maven Central et à Wikimedia.

```powershell
Set-Location 05.structured-streaming/lab_docker_wikistreams
docker compose up -d --build
docker compose ps
```

Le TP fournit les services Kafka et PySpark/Jupyter dans `compose.yaml`, les dépendances dans `Dockerfile` et `requirements.txt`, l’administration du topic dans `admin.py`, et le producteur dans `wikistream_producer.py`. Le notebook `notebooks/wikistream_pyspark.ipynb` illustre la lecture Kafka et les fenêtres Spark.

### Vérifier le trajet avec une modification réelle

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/project_stream.py --languages en,fr --duration 60 --max-events 1
```

Le script crée un topic et un checkpoint pour l’essai, démarre Spark puis lance le producteur. Le test réussit si Kafka confirme la livraison d’un message et que Spark retrouve son identifiant Wikimedia (`smoke_event_id_verified: true`). La sortie fournit aussi le titre, l’offset Kafka et les horaires.

Le 10 novembre 2026, Kafka 4.1.1 et Spark 4.0.1 ont traité la même modification Wikipédia à la partition 0, offset 0, sous Ubuntu WSL. La révision a eu lieu à 16:45:57 UTC et le micro-lot Spark l’a traitée à 16:46:09.762 UTC. Cette page d’organisation de Wikipédia valide le transport. Le rapprochement IMDb vise les modifications d’articles dans l’espace principal. [Révision Wikipédia](https://en.wikipedia.org/w/index.php?oldid=1384885248), [sortie du test](project/streaming/evidence/smoke.json)

Le 11 novembre, Docker Compose sous Windows a confirmé le trajet avec Kafka 4.1.1 et Spark 4.2.0 sur une modification de l'article `Austria national football team`. L'audit du producteur et la ligne Spark ont le même identifiant à la partition 0, offset 0. L'API Wikipédia confirme le numéro et l'heure UTC de la révision. [Révision Wikipédia](https://en.wikipedia.org/w/index.php?oldid=1385068548), [sortie Docker](project/streaming/evidence/docker-smoke-2026-11-11.json)

### Observer les correspondances pendant 15 minutes

Le fichier `jobs/pilot_reference.csv` contient 32 liens anglais et français pour vérifier le format. Préparer une référence plus large avant la capture. La commande de génération fait une requête au service Wikidata par langue et affiche l’empreinte SHA-256 du CSV obtenu.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/build_reference.py --languages en --limit-per-wiki 40000 --output /home/jovyan/data/reference_en.csv
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/project_stream.py --languages en --duration 900 --reference /home/jovyan/data/reference_en.csv
```

Chaque dossier `data/project-runs/project-wiki-*` contient `summary.json`, `producer.log`, une copie du CSV utilisé, les événements en Parquet et les fichiers locaux d’audit et de reprise. Les références générées et les sorties locales sont ignorées par Git. `producer.seen` compte les messages lus sur EventStreams avant filtrage. `kafka_records` compte les messages effectivement relus par Spark, `eligible_wikipedia_edits` les modifications d’articles Wikipédia retenues, `matched_events` les identifiants d’événement distincts associés à IMDb et `matched_pages` les articles distincts correspondants. Un article lié à deux identifiants IMDb ne compte qu’une fois par modification. Les exemples contiennent l’URL de la page, le numéro de révision, les identifiants et les horaires. La durée mesurée inclut la confirmation des derniers envois à Kafka.

Le 10 novembre 2026, l’essai anglais a utilisé une référence figée de 39 951 liens Wikipédia–IMDb. En 15 min 3,149 s, confirmation des envois incluse, le producteur a lu 31 405 messages Wikimedia. Kafka a confirmé 1 824 livraisons et Spark a relu 1 824 événements distincts, dont 1 425 modifications admissibles et 10 correspondances IMDb sur 8 articles. Les compteurs ont été recalculés à partir de la capture brute, du catalogue et des Parquet Spark. Les dix numéros de révision et heures de modification correspondent à l’API de Wikipédia. [Compteurs et exemples](project/streaming/evidence/live.json)

Le contrôle de compatibilité sous Ubuntu WSL avec Spark 4.2.0 et Kafka 4.1.1 a confirmé la livraison d’une nouvelle modification réelle. Le rejeu de la capture du 10 novembre a restitué les 1 824 identifiants et offsets, les 1 425 modifications admissibles et les mêmes 10 liens événement–IMDb. Le rejeu séparé dans Docker Compose avec Spark 4.2.0 a retrouvé les mêmes valeurs et liens. [Sortie du rejeu Docker](project/streaming/evidence/docker-replay-2026-11-10.json)

Si l’essai donne moins de deux correspondances, produire un CSV avec plus de langues, passer les mêmes langues au consommateur et relancer sur un nouveau topic.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/build_reference.py --languages en,fr,de,es --limit-per-wiki 20000 --output /home/jovyan/data/reference_multilingual.csv
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/project_stream.py --languages en,fr,de,es --duration 900 --reference /home/jovyan/data/reference_multilingual.csv
```

### Interface avec les tables IMDb nettoyées

Le consommateur prend un CSV UTF-8 avec en-tête, une ligne par lien article–identifiant. `wiki` identifie l’édition Wikipédia (`enwiki`, `frwiki`), `title` est son titre canonique, `imdb_id` est un `tconst` ou `nconst`, et `wikidata_id` contient l’identifiant `Q…` lorsqu’il est disponible. Les espaces et tirets bas sont traités de la même manière. La casse, les accents et les qualificatifs comme `(film)` sont conservés.

Le script `jobs/prepare_reference.py` croise le catalogue Wikimedia avec les clés des Parquet `clean/title.basics` et `clean/name.basics`. Il utilise le filtre Spark de la PR #4, dont le correctif traite `___` comme un titre vide et déduplique après normalisation. Il conserve les titres et les personnes de l'ensemble des deux tables. La liaison Wikipédia repose sur les sitelinks et la [propriété Wikidata P345](https://www.wikidata.org/wiki/Property:P345).

Depuis la racine du dépôt, choisir le dossier contenant les Parquet de préparation et le code `imdb_preparation`. Le chemin `$imdbCode` peut pointer sur un worktree de la branche `feat/imdb-full-analysis` avant intégration des PR. Le catalogue large `reference_en.csv` doit avoir été généré dans le dossier `data` du TP.

```powershell
$lab = (Resolve-Path '05.structured-streaming/lab_docker_wikistreams').Path
$imdbCode = (Get-Location).Path
$imdbInput = (Resolve-Path 'results/imdb-full-2026-11-11').Path
$pythonPath = '/imdb-code:/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip'
docker run --rm --memory 2304m --cpus 2 --mount "type=bind,source=$imdbCode,target=/imdb-code,readonly" --mount "type=bind,source=$imdbInput,target=/imdb,readonly" --mount "type=bind,source=$lab,target=/lab" --env "PYTHONPATH=$pythonPath" --entrypoint python tp05-wikistreams-notebook:local /lab/jobs/prepare_reference.py --imdb-prepared-dir /imdb --reference /lab/data/reference_en.csv --output /lab/data/project-runs/integrated-reference
```

Si les deux Parquet nettoyés sont encore sur la machine de Nicolas, les deux fichiers complets `title.basics.tsv.gz` et `name.basics.tsv.gz` permettent de reproduire le contrôle des clés. Faire pointer `$imdbInput` sur ce dossier et passer `--imdb-raw-dir /imdb`. Ce mode applique les mêmes règles de normalisation, de déduplication et de rejet des clés conflictuelles aux identifiants candidats du catalogue. La sortie contient `streaming_reference.csv`, les motifs de rejet en Parquet et un résumé avec les empreintes des entrées et de la sortie.

Le 11 novembre, ce mode a été exécuté sous Docker avec Spark 4.2.0 et les deux fichiers IMDb complets dont les empreintes correspondent à l'instantané publié par Nicolas dans la PR #5. Sur 39 951 liens Wikipédia–IMDb, Spark a retenu 39 926 liens pour 39 924 pages et 39 922 identifiants IMDb distincts. Les 25 autres liens ont un identifiant absent après validation des clés. Le CSV et les rejets Parquet ont été relus et réconciliés. [Preuve et empreintes](project/streaming/evidence/docker-reference-2026-11-11.json)

Le chemin des Parquet préparés a aussi été contrôlé sous Docker sur un jeu synthétique. Sur sept lignes de référence, trois sont relues par le consommateur Spark, deux variantes sont réunies en une seule, deux entrées invalides sont rejetées et un identifiant absent est signalé. Le lecteur conserve aussi le saut de ligne d'un titre CSV cité. [Contrôle du contrat Parquet](project/streaming/evidence/docker-prepared-reference-contract-2026-11-11.json)

Depuis le dossier du TP, lancer la capture avec le catalogue filtré et demander un redémarrage de la requête Spark après cinq minutes. Le producteur continue d'écrire sur le même topic pendant la reprise.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/project_stream.py --languages en --duration 900 --restart-after 300 --reference /home/jovyan/data/project-runs/integrated-reference/streaming_reference.csv
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/checkpoint_check.py
```

Le premier test vérifie la reprise de l'identifiant de requête Spark, le changement d'identifiant d'exécution et la progression des offsets. Il compare aussi les identifiants de l'audit du producteur aux identifiants lus en ordre Kafka. Le deuxième test publie trois messages synthétiques, dont un événement répété, et vérifie deux événements rapprochés, trois offsets distincts et deux identifiants IMDb par correspondance. Ses sorties sont écrites dans un dossier `project-wiki-checkpoint-*`.

La capture Docker du 11 novembre a utilisé le catalogue anglais de 39 951 liens. Le contrôleur a mesuré 15 min 25,948 s jusqu'à la fin du producteur, confirmations Kafka incluses, pour une durée demandée de 15 minutes. Le producteur a vu 29 263 messages Wikimedia et livré 1 845 événements. Spark a relu 1 845 identifiants et offsets distincts, dont 1 311 modifications admissibles et 7 correspondances sur 6 articles. Les sept révisions et heures de modification ont été vérifiées via l'API de Wikipédia. [Capture Docker et contrôles](project/streaming/evidence/docker-live-2026-11-11.json)

La demande d'arrêt à 300,130 s a interrompu un micro-lot. La requête a repris avec le même checkpoint et un nouvel identifiant d'exécution. L'offset de fin exclusif est passé de 578 à 1 845. La relecture des Parquet confirmés par le journal Spark retrouve les 1 845 identifiants de l'audit dans l'ordre des offsets, sans doublon.

Dans le test synthétique du 11 novembre exécuté sous Docker, le même checkpoint a repris après l'offset exclusif 1 et a atteint l'offset exclusif 3. Les trois enregistrements occupent les offsets 0, 1 et 2. Deux identifiants Wikimedia distincts correspondent au même article. Le titre avec espaces et sa variante avec tirets bas donnent les mêmes deux identifiants IMDb. [Sortie du contrôle de reprise](project/streaming/evidence/docker-checkpoint-2026-11-11.json)

Le résumé de la capture mesure les délais modification–Kafka, Kafka–Spark et modification–Spark en millisecondes. Les statistiques portent sur la première occurrence Kafka de chaque événement rapproché. `p50_p95` contient la médiane et le 95e percentile calculés dans Spark. `negative_count` dénombre les écarts négatifs entre les horloges. Les exemples associent ces mesures au numéro de révision, à la partition et à l'offset.

Sur les sept correspondances Docker, la médiane et le 95e percentile sont respectivement de 4,806 s et 6,769 s entre modification et Kafka, de 15,359 s et 73,415 s entre Kafka et Spark et de 20,165 s et 78,908 s entre modification et Spark. Plusieurs micro-lots ont dépassé la cadence demandée de 10 s pendant cet essai.

Pour rejouer une capture locale, renseigner les chemins de l'audit et du catalogue intégré dans le conteneur. Le script crée un topic et un checkpoint distincts. La sortie porte le périmètre `replay_of_existing_wikimedia_capture` et réconcilie tous les offsets et les identifiants avec l'audit d'origine.

```powershell
$capture = '/home/jovyan/data/project-runs/docker-live-2026-11-11/project-wiki-76da32d10fe6/capture.jsonl'
$reference = '/home/jovyan/data/project-runs/integrated-reference-2026-11-11/streaming_reference.csv'
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/replay_capture.py --capture $capture --reference $reference --output /tmp/gr04-streaming-replay-2026-11-11
```

Le rejeu Docker de la capture du 11 novembre avec le catalogue intégré de 39 926 liens a restitué les 1 845 identifiants dans le même ordre Kafka, aux offsets 0 à 1 844. La comparaison des Parquet a confirmé les 1 311 modifications admissibles et les mêmes sept associations événement–IMDb sur six articles. [Preuve du rejeu intégré](project/streaming/evidence/docker-integrated-replay-2026-11-11.json)

### Horaires et test des événements tardifs

`event_time` vient du timestamp Wikimedia de la modification. `kafka_time` est le timestamp de création du message Kafka (`CreateTime`). `processing_time` est l’horodatage du micro-lot calculé par Spark. Les trois valeurs sont en UTC.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/late_events.py
```

L’exercice synthétique injecte d’abord `12:00:20`, puis `12:05:00` pour avancer le watermark à `12:03:00`. Il injecte ensuite `12:04:30` et `12:00:30`. Avec des fenêtres d’une minute et un watermark de deux minutes, le test attend une ligne acceptée dans la fenêtre de 12 h 04 et une ligne rejetée pour une fenêtre déjà close. La mesure `numRowsDroppedByWatermark` doit valoir 1. Cet exercice utilise une agrégation Spark avec état. La jointure du flux avec le catalogue statique traite aussi les événements désordonnés. [Documentation Spark](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html#semantics-of-aggregation-with-watermarking)

Le test sous Ubuntu WSL avec Spark 4.0.1 a confirmé les trois fenêtres attendues et un rejet par le watermark. Le contrôle sous Docker Compose avec Spark 4.2.0 a obtenu les mêmes trois fenêtres et un rejet. [Sortie WSL](project/streaming/evidence/late-events.jsonl), [sortie Docker](project/streaming/evidence/docker-late-events-2026-11-10.jsonl)

### Déclaration d’usage de l’IA

OpenAI Codex a aidé à inspecter et réutiliser le TP Wikimedia, préparer le catalogue pilote et le raccorder aux identifiants IMDb, écrire et vérifier le consommateur Spark, instrumenter la capture, les compteurs et les latences, vérifier la reprise du checkpoint et les doublons, préparer l’exercice sur le watermark et documenter les commandes de reproduction.
