La préparation couvre les sept tables IMDb et calcule le taux de valeurs
manquantes des 39 colonnes sur les lignes brutes, puis le top 5 global.
Le nettoyage PySpark produit des tables Parquet typées, un audit des corrections
et des quarantaines pour les clés invalides, conflictuelles ou orphelines.
Les compteurs réconcilient toutes les lignes, y compris les doublons retirés.

Le téléchargement conserve des manifestes et SHA256, distingue échantillons et
jeu complet et sépare les bruts des résultats. Les références absentes d'un
échantillon sont signalées sans suppression. `PROJECT.md` décrit l'installation,
les commandes, les sorties, les relances et l'usage de Codex. Le streaming, les
anciens TP et `REPORT.md` ne sont pas modifiés.

Validation Linux native : 7 tests réussis ; jeu contrôlé de 32 lignes (19 nettoyées,
12 en quarantaine, 1 doublon retiré) ; échantillon IMDb réel de 7 000 lignes,
sept tables et 39 colonnes profilées. Les preuves agrégées sont dans `validation/`.
Ruff et validation de configuration Compose réussis. Le traitement complet IMDb
et l'exécution Docker/Windows restent à vérifier.

À relire par Benjamin : règles de correction et de quarantaine, dénominateurs,
couverture des références, commandes de lancement et interface Parquet proposée
à sa branche streaming/enrichissement. L'interface et le partage des sections de
`PROJECT.md` restent des propositions. Fusion après son approbation réelle.
