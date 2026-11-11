# Preuves techniques de préparation

- `fixture-v1.json` : résultats du jeu synthétique contrôlé, périmètre `fixture`.
- `imdb-sample-2026-11-10.json` : résultats réels des premières 1 000 lignes de
  chacune des sept tables IMDb, périmètre `sample`.
- `windows-docker-2026-11-10.json` : résumés des deux exécutions Docker Desktop
  sur le PC Windows et vérification des empreintes/profils par rapport au cloud.
- `preparation-review-fixes-2026-11-10.json` : tests et exécutions après correction
  des trois points signalés en relecture. Les preuves précédentes restent celles
  du commit `376b434`, avant correction du faux conflit `ordering=01/1`.

Les deux premiers fichiers rassemblent sans modification les sorties JSON `summary`,
`missing_profile`, `top5_missing` et `input_manifest` des exécutions indiquées.
Les manifestes permettent de vérifier les SHA256 des entrées conservées hors Git.
Les dates techniques des sorties sont en UTC. Les références manquantes dans
l'échantillon restent non vérifiées ; elles ne prouvent pas une anomalie IMDb.

Les bruts et Parquet sont exclus de Git. Pour les reproduire, suivre `PROJECT.md` ;
un nouveau téléchargement IMDb peut différer car les fichiers changent chaque jour.
Ces pièces ne remplacent ni la validation intégrale sur IMDb, ni les explications
et interprétations que les étudiants doivent rédiger eux-mêmes dans `REPORT.md`.

Les exports d'intégration disposent de preuves distinctes :

- `integration-exports-2026-11-10.json` : résumés des exécutions contrôlée et réelle.
- `integration-consumer-compatibility.json` : lecture des CSV par le lecteur de
  référence de Benjamin et son schéma de candidats sous Spark 4.2.0, avec les
  commits source utilisés. Cela ne réexécute pas le streaming ni la collecte.
- `integration-exports-windows-2026-11-10.json` : exécutions réelles des exports
  contrôlé et IMDb sur le PC Windows avec Docker Desktop.
- `integration-after-review-2026-11-10.json` : 27 tests et nouveaux exports après
  réintégration des corrections de la PR #1 dans la PR #4.
- `enrichment-arrow-review-2026-11-10.json` : fonction d'extraction exacte de
  Benjamin exécutée avec Arrow sur trois lignes synthétiques dans le conteneur,
  avec conversion `toPandas` sans repli. Aucun pilote réel ou collecte n'est répété.
  `check_enrichment_arrow.py` permet de reproduire ce contrôle.

Le lot complet possède un profil brut séparé :

- `full-download-2026-11-11.json` : manifeste des sept gzip complets vérifiés.
- `full-raw-profile-windows-2026-11-11.json` : 39 taux et top 5 sur
  216 477 904 lignes, sous Spark 4.2.0 dans Docker Desktop. Le nettoyage complet
  n'est pas validé par cette preuve (`cleaning_validated: false`).
- `original-languages-source-2026-11-11.json` : première collecte cloud des
  déclarations Wikidata P364 et de leur vocabulaire ISO. La collecte Windows,
  effectuée plus tard, a son manifeste distinct dans la preuve du profil complet.

`collect_full_results.ps1` crée `full-analysis-windows-2026-11-11.json` uniquement
après les trois marqueurs de succès préparation/exploration/crédits. Tant que ce
fichier n'existe pas, les résultats d'analyse complète restent à vérifier.

Les premières analyses réelles et les entrées d'enrichissement sont disponibles :

- `title-queries-full-provisional-2026-11-11.json` et `title-query-verification-2026-11-11.json` : Q1–Q3 et validation du classement ;
- `top10-full-provisional-2026-11-11.csv` : 9 089 places dans 979 groupes année–genre ;
- `selected-titles-full-provisional-2026-11-11.csv` : 6 257 films distincts ;
- `genres-by-year-full-provisional-2026-11-11.csv` et `genre-comparison-full-provisional-2026-11-11.csv` : 138 et 92 lignes ;
- `selected-credits-full-provisional-2026-11-11.json` et `credit-coverage-full-provisional-2026-11-11.csv` : contrôle des crédits des films retenus ;
- `principal-choice-full-provisional-2026-11-11.json`, `principal-choice-verification-2026-11-11.json` et les quatre CSV `enrichment-*` : 31 230 couples film–personne bornés à trois priorités et deux réserves, avec cinq films ayant moins de trois personnes.

Les périmètres `full_title_tables_subset` et `full_selected_credits_subset`
désignent des tables sources complètes utilisées pour des sorties ciblées. Ils
ne certifient pas la fin du nettoyage des sept tables. Les critères de sélection
restent non validés par les étudiants ; ces fichiers ne sont pas le rapport.
