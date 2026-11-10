# Big Data Processing — Groupe gr-04

## Contributeurs

- Benjamin Labos (`benjaminlabos`), benjamin.labos@edu.ece.fr
- Nicolas Rihana (`nicohris`), nrihana1@gmail.com

## Streaming Wikipédia et IMDb

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

### Observer les correspondances pendant 15 minutes

Le fichier `jobs/pilot_reference.csv` contient 32 liens anglais et français pour vérifier le format. Préparer une référence plus large avant la capture. La commande de génération fait une requête au service Wikidata par langue et affiche l’empreinte SHA-256 du CSV obtenu.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/build_reference.py --languages en --limit-per-wiki 40000 --output /home/jovyan/data/reference_en.csv
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/project_stream.py --languages en --duration 900 --reference /home/jovyan/data/reference_en.csv
```

Chaque dossier `data/project-runs/project-wiki-*` contient `summary.json`, `producer.log`, une copie du CSV utilisé, les événements en Parquet et les fichiers locaux d’audit et de reprise. Les références générées et les sorties locales sont ignorées par Git. `producer.seen` compte les messages lus sur EventStreams avant filtrage. `kafka_records` compte les messages effectivement relus par Spark, `eligible_wikipedia_edits` les modifications d’articles Wikipédia retenues, `matched_events` les identifiants d’événement distincts associés à IMDb et `matched_pages` les articles distincts correspondants. Un article lié à deux identifiants IMDb ne compte qu’une fois par modification. Les exemples contiennent l’URL de la page, le numéro de révision, les identifiants et les horaires. La durée mesurée inclut la confirmation des derniers envois à Kafka.

Le 10 novembre 2026, l’essai anglais a utilisé une référence figée de 39 951 liens Wikipédia–IMDb. En 15 min 3,149 s, confirmation des envois incluse, le producteur a lu 31 405 messages Wikimedia. Kafka a confirmé 1 824 livraisons et Spark a relu 1 824 événements distincts, dont 1 425 modifications admissibles et 10 correspondances IMDb sur 8 articles. Les compteurs ont été recalculés à partir de la capture brute, du catalogue et des Parquet Spark. Les dix numéros de révision et heures de modification correspondent à l’API de Wikipédia. [Compteurs et exemples](project/streaming/evidence/live.json)

Le contrôle de compatibilité sous Ubuntu WSL avec Spark 4.2.0 et Kafka 4.1.1 a confirmé la livraison d’une nouvelle modification réelle. Le rejeu de la capture du 10 novembre a restitué les 1 824 identifiants et offsets, les 1 425 modifications admissibles et les mêmes 10 liens événement–IMDb. L’exercice du watermark a également réussi sous Spark 4.2.0.

Si l’essai donne moins de deux correspondances, produire un CSV avec plus de langues, passer les mêmes langues au consommateur et relancer sur un nouveau topic.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/build_reference.py --languages en,fr,de,es --limit-per-wiki 20000 --output /home/jovyan/data/reference_multilingual.csv
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/project_stream.py --languages en,fr,de,es --duration 900 --reference /home/jovyan/data/reference_multilingual.csv
```

### Interface avec les tables IMDb nettoyées

Le consommateur prend un CSV UTF-8 avec en-tête, une ligne par lien article–identifiant. `wiki` identifie l’édition Wikipédia (`enwiki`, `frwiki`), `title` est son titre canonique, `imdb_id` est un `tconst` ou `nconst`, et `wikidata_id` contient l’identifiant `Q…` lorsqu’il est disponible. Les espaces et tirets bas sont traités de la même manière. La casse, les accents et les qualificatifs comme `(film)` sont conservés.

Pour passer aux données du projet, Nicolas peut fournir les colonnes `tconst` et `nconst` de ses tables nettoyées. Une jointure Spark entre ces identifiants distincts et le CSV Wikidata donne le catalogue final au même format, à passer avec `--reference`. La liaison Wikipédia repose sur les sitelinks et la [propriété Wikidata P345](https://www.wikidata.org/wiki/Property:P345).

### Horaires et test des événements tardifs

`event_time` vient du timestamp Wikimedia de la modification. `kafka_time` est le timestamp du message Kafka. `processing_time` est l’heure à laquelle Spark traite le micro-lot. Les trois valeurs sont en UTC.

```powershell
docker compose exec -T pyspark-notebook python /home/jovyan/jobs/late_events.py
```

L’exercice synthétique injecte d’abord `12:00:20`, puis `12:05:00` pour avancer le watermark à `12:03:00`. Il injecte ensuite `12:04:30` et `12:00:30`. Avec des fenêtres d’une minute et un watermark de deux minutes, le test attend une ligne acceptée dans la fenêtre de 12 h 04 et une ligne rejetée pour une fenêtre déjà close. La mesure `numRowsDroppedByWatermark` doit valoir 1. Cet exercice utilise une agrégation Spark avec état. La jointure du flux avec le catalogue statique traite aussi les événements désordonnés. [Documentation Spark](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html#semantics-of-aggregation-with-watermarking)

Le test sous Spark 4.0.1 a confirmé les trois fenêtres attendues et un rejet par le watermark. [Sortie du test synthétique](project/streaming/evidence/late-events.jsonl)

### Déclaration d’usage de l’IA

OpenAI Codex a aidé à inspecter et réutiliser le TP Wikimedia, préparer le catalogue pilote, écrire et vérifier le consommateur Spark, instrumenter la capture et les compteurs, préparer l’exercice sur le watermark et documenter les commandes de reproduction.
