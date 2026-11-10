La préparation couvre les sept tables IMDb : profil des valeurs manquantes des
39 colonnes sur les lignes brutes, top 5, Parquet typés, audit et quarantaines.
Les compteurs réconcilient les lignes nettoyées, rejetées et dédupliquées. Les
références absentes d'un échantillon sont signalées sans suppression.

Les trois points signalés par Benjamin sur `376b434` sont corrigés : contrôle du
Content-Length et de l'intégrité gzip avant publication d'un téléchargement
complet ; normalisation de `ordering` avant le dédoublonnage ; reconnaissance
des espaces Unicode, notamment insécables. Les vrais conflits restent en
quarantaine et les transferts incomplets ne produisent aucun manifeste final.

Validation Linux sous Spark 4.2.0 : **21 tests réussis**, dont les régressions
reproduites avant correction. Le jeu contrôlé donne désormais 32 lignes brutes,
20 nettoyées, 10 en quarantaine et 2 doublons retirés. Le nouvel essai IMDb réel
traite les 7 000 lignes et les 39 colonnes ; son profil reste identique à celui
de la première version. Les preuves anciennes restent historiques et la preuve
actuelle figure dans `validation/preparation-review-fixes-2026-11-10.json`.

Les **21 tests ont aussi réussi sous Docker sur le PC Windows**, avec pytest 9.0.2
installé dans le conteneur éphémère, sous Spark 4.2.0/Python 3.13.15.
Le lot IMDb complet reste à vérifier ; les tests de transport complets utilisent
des réponses HTTP contrôlées. `PROJECT.md` documente les commandes, les relances,
les règles corrigées et l'usage de Codex. L'interface inclut `clean/title.ratings`
avec `averageRating` et `numVotes`. Le classement définitif reste à convenir entre
les étudiants. Les anciens TP et `REPORT.md` ne sont pas modifiés.

Une nouvelle relecture de Benjamin est attendue avant approbation et fusion.
