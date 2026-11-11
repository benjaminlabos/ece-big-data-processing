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
