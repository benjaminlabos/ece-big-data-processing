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
