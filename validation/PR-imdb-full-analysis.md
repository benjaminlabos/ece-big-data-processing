Cette tâche prépare l'exécution des sept tables IMDb complètes, réduit les caches
simultanés du nettoyage et publie la progression par table. Le téléchargement
complet du 11 novembre a fourni 2 003 620 065 octets compressés, avec contrôle
d'intégrité des sept gzip ; cela ne constitue pas encore une validation du nettoyage.

Le module Spark implémente les huit questions et le top 10 année–genre. Les seuils
restent provisoires et explicitement non validés par les étudiants. La question des
langues utilise une collecte distincte Wikidata P364 avec provenance et vocabulaire
ISO ; sa couverture partielle est mesurée, sans inférer les langues depuis les
alias IMDb. Aucune collecte de synopsis ou de biographies n'est exécutée.

Les sorties comprennent les Parquet par question, paramètres et statuts ainsi
qu'un CSV de films sélectionnés pour l'export de crédits existant. Le lanceur Windows
enchaîne préparation, exploration et export avec ressources bornées et surveillance
du disque. Un profil brut indépendant produit les 39 taux de valeurs manquantes,
sans être présenté comme une validation du nettoyage.

Validation : 33 tests contrôlés réussis sous Spark 4.2.0 (63,01 s), exécution CLI
sur le jeu contrôlé et réconciliation d'un instantané de 369 502 énoncés de langues.
Les résultats IMDb complets seront ajoutés après succès effectif des traitements.
Les branches de Benjamin et REPORT.md restent inchangés.

PR dépendante de #4 ; la cible sera ajustée après les approbations des PR parentes.
