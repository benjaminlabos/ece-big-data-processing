# Preuves techniques de préparation

- `fixture-v1.json` : résultats du jeu synthétique contrôlé, périmètre `fixture`.
- `imdb-sample-2026-11-10.json` : résultats réels des premières 1 000 lignes de
  chacune des sept tables IMDb, périmètre `sample`.

Ces fichiers rassemblent sans modification les sorties JSON `summary`,
`missing_profile`, `top5_missing` et `input_manifest` des exécutions indiquées.
Les manifestes permettent de vérifier les SHA256 des entrées conservées hors Git.
Les dates techniques des sorties sont en UTC. Les références manquantes dans
l'échantillon restent non vérifiées ; elles ne prouvent pas une anomalie IMDb.

Les bruts et Parquet sont exclus de Git. Pour les reproduire, suivre `PROJECT.md` ;
un nouveau téléchargement IMDb peut différer car les fichiers changent chaque jour.
Ces pièces ne remplacent ni la validation intégrale sur IMDb, ni les explications
et interprétations que les étudiants doivent rédiger eux-mêmes dans `REPORT.md`.
