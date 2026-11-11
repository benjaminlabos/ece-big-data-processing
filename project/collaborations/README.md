# Question libre IMDb

Question de travail à valider ensemble. La note IMDb d’un film diffère-t-elle entre la première année de collaboration observée d’un binôme réalisateur–interprète et ses années suivantes, sur les films sortis de 1980 à 2025 ?

Le traitement lit les Parquet nettoyés `title.basics`, `title.crew`, `title.principals` et `title.ratings` de la préparation IMDb. `title.crew.directors` est un tableau d’identifiants. Les films ont `titleType = movie`. Les crédits d’interprétation ont la catégorie `actor` ou `actress`. Chaque triplet réalisateur, interprète et film contribue au plus une fois. Les triplets où réalisateur et interprète ont le même identifiant sont exclus par défaut et peuvent être inclus avec `--include-self`.

Le classement chronologique repose sur les années connues de toutes les collaborations observées jusqu’à `--end-year`, y compris les films sans note et ceux sortis avant `--start-year`. Toutes les sorties de la première année connue sont dans le même groupe. Les sorties ultérieures forment le second groupe. Les années inconnues sont comptées dans les diagnostics et écartées du calcul chronologique. Pour chaque combinaison de seuils, le calcul conserve les binômes qui ont au moins un film noté de chaque groupe dans la période et qui satisfont le minimum d’années distinctes observées. Il donne le même poids à chaque binôme et calcule, dans chacun d’eux, la différence entre les moyennes des deux groupes. Un film peut contribuer à plusieurs binômes. La comparaison reste descriptive.

## Exécuter l’analyse

Depuis la racine du clone avec Spark 4.2.0, Java 17 ou 21 et Python 3.10 ou supérieur, indiquer le dossier `clean` produit par la préparation et son manifeste source. Choisir un nouveau dossier de sortie à chaque essai.

```bash
spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=24 \
  project/collaborations/analyse.py \
  --clean-dir /data/preparation/clean --input-manifest /data/preparation/input_manifest.json \
  --scope full --output /data/collaborations-full \
  --start-year 1980 --end-year 2025 --min-votes 0 100 1000 --min-history-years 2 3
```

`comparisons/` contient le tableau de sensibilité en Parquet. `summary.json` contient les mêmes résultats, les paramètres, l’empreinte du manifeste et les diagnostics. Une combinaison vide conserve des effectifs nuls et des moyennes absentes. Le dossier final devient visible après la relecture des Parquet. Les paramètres restent provisoires.

Les projections utilisées pour l’essai peuvent aussi être préparées depuis l’archive IMDb vérifiée, en ajoutant au `PYTHONPATH` le dossier du projet de préparation. Le script réutilise ses fonctions de lecture du manifeste, de normalisation, de nettoyage et de réparation des références. Fournir un CSV à une seule colonne `tconst` avec `--selected` pour limiter la préparation à des titres et utiliser alors `--scope sample` pour l’analyse. Le calcul des premières années se limite aux films de cet échantillon. Sans `--selected`, la préparation couvre tous les films des sources.

```bash
PYTHONPATH="/clone/imdb-preparation:${PYTHONPATH:-}" spark-submit \
  --master 'local[2]' --conf spark.sql.shuffle.partitions=24 \
  project/collaborations/prepare_inputs.py --raw-dir /data/imdb-source \
  --output /data/collaboration-inputs --as-of-year 2026
```

### Historique complet sous Docker Desktop

Depuis la racine du clone de la question libre, indiquer le dossier qui contient le `manifest.json` et les sept fichiers gzip vérifiés. Indiquer aussi la racine du clone de préparation. Dans un dépôt intégré, cette dernière est la racine courante. La projection s’exécute sur le disque Linux du conteneur et conserve l’empreinte du lot brut.

```powershell
$raw = (Resolve-Path 'data/raw/imdb-full-2026-11-11').Path
$preparationRoot = (Get-Location).Path
$container = 'gr04-collaborations-full'
docker run -d --name $container --pull=never --cpus 5 --memory 5200m `
  --entrypoint /bin/sleep tp05-wikistreams-notebook:local infinity
docker exec $container mkdir -p /tmp/gr04/raw /tmp/gr04/project /tmp/gr04/prep /tmp/gr04/results /tmp/gr04/shuffle
docker cp "$raw/." "${container}:/tmp/gr04/raw/"
docker cp "project/collaborations/." "${container}:/tmp/gr04/project/"
docker cp "$preparationRoot/imdb_preparation/." "${container}:/tmp/gr04/prep/imdb_preparation"
docker exec -w /tmp/gr04 -e PYTHONPATH=/tmp/gr04/prep `
  -e SPARK_LOCAL_DIRS=/tmp/gr04/shuffle -e SPARK_LOCAL_IP=127.0.0.1 $container `
  spark-submit --master 'local[4]' --driver-memory 3600m --conf spark.sql.shuffle.partitions=48 `
  --conf spark.ui.enabled=false project/prepare_inputs.py --raw-dir raw `
  --output results/projected-clean --as-of-year 2026
docker exec -w /tmp/gr04 -e SPARK_LOCAL_DIRS=/tmp/gr04/shuffle -e SPARK_LOCAL_IP=127.0.0.1 $container `
  spark-submit --master 'local[4]' --driver-memory 3600m --conf spark.sql.shuffle.partitions=48 `
  --conf spark.ui.enabled=false project/analyse.py --clean-dir results/projected-clean/clean `
  --input-manifest results/projected-clean/input_manifest.json --scope full --output results/full `
  --start-year 1980 --end-year 2025 --min-votes 0 100 1000 5000 10000 --min-history-years 2 3
New-Item -ItemType Directory -Path results -Force | Out-Null
docker cp "${container}:/tmp/gr04/results/full" results/collaborations-full
docker cp "${container}:/tmp/gr04/results/projected-clean" results/collaboration-inputs
```

Pour recalculer indépendamment les effectifs et les moyennes à partir des quatre Parquet utilisés, exécuter le contrôle SQL. Son entrée `clean` doit être copiée hors du conteneur si l’analyse a tourné dans Docker. Ce contrôle a relu les six cellules et tous les diagnostics de l’essai provisoire.

```powershell
python -m pip install duckdb==1.4.3
python project/collaborations/audit_sql.py results/collaboration-inputs/clean results/collaborations-full/summary.json
```

Pour reproduire l’essai sur la sélection provisoire, `slice_source.py` recopie les lignes TSV des titres choisis et des personnes référencées, sans modifier leurs valeurs. Il vérifie les SHA-256 des cinq fichiers gzip pendant la lecture. Spark vérifie le manifeste de cet extrait puis normalise, déduplique et valide les références. Cette projection évite de lire tout le gzip dans un seul cœur du conteneur Spark.

```bash
selected=/clone/imdb-preparation/validation/selected-titles-full-provisional-2026-11-11.csv
python project/collaborations/slice_source.py --raw-dir /data/imdb-source \
  --selected "$selected" --output /data/collaboration-source-slice
PYTHONPATH="/clone/imdb-preparation:${PYTHONPATH:-}" spark-submit \
  --master 'local[2]' --conf spark.sql.shuffle.partitions=16 \
  project/collaborations/prepare_inputs.py --raw-slice /data/collaboration-source-slice \
  --selected "$selected" --output /data/collaboration-top10-inputs --as-of-year 2026
spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=16 \
  project/collaborations/analyse.py --clean-dir /data/collaboration-top10-inputs/clean \
  --input-manifest /data/collaboration-top10-inputs/input_manifest.json \
  --scope sample --output /data/collaboration-top10 --min-votes 1000 5000 10000
```

L’essai du 11 novembre 2026 sous Spark 4.2.0 dans Docker couvre les 6 257 films de la sélection provisoire. Avec au moins 1 000 votes et deux années distinctes, le tableau contient 2 389 binômes, une moyenne de 7,5765 en première année observée et de 7,4823 sur les années suivantes. Les six comparaisons et les diagnostics ont été recalculés indépendamment à partir des Parquet. [Résultat et empreintes des entrées](../../validation/collaborations-top10-2026-11-11.json)

## Test contrôlé

```bash
spark-submit --master 'local[2]' project/collaborations/fixture.py --create /data/collab-fixture
spark-submit --master 'local[2]' --conf spark.sql.shuffle.partitions=4 \
  project/collaborations/analyse.py --clean-dir /data/collab-fixture \
  --scope fixture --output /data/collab-test --min-history-years 2 3 4
python project/collaborations/fixture.py --verify /data/collab-test/summary.json
```

Le test vérifie deux films de première année, des doublons de crédits, des rôles non pertinents, deux apparitions en tant que réalisateur-interprète, un film sans note, une année inconnue, une première collaboration avant la période, un film après la période et l’effet des seuils sur neuf comparaisons.

Le 11 novembre 2026, les neuf comparaisons et les diagnostics ont été vérifiés dans Docker sous Spark 4.2.0 et Java 21, puis recalculés à partir des Parquet. [Résultat du jeu contrôlé](../../validation/collaborations-fixture-2026-11-11.json)

Déclaration à intégrer à `PROJECT.md` lors du regroupement. OpenAI Codex a assisté le développement de l’analyse Spark, du jeu contrôlé et des contrôles d’exécution.
