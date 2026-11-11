Le profil des sept fichiers IMDb complets est désormais obtenu : 39 colonnes et
216 477 904 lignes de tables. Les fichiers compressés (2 003 620 065 octets) ont
été vérifiés, et la preuve Windows Docker versionne les taux de valeurs manquantes,
le top 5 et la provenance. Le nettoyage avec contrôles de références tourne
séparément et reste à valider jusqu'à son marqueur de succès.

Le module Spark implémente les huit questions et le top 10 année–genre. Les seuils
restent provisoires et explicitement non validés par les étudiants. Q1 inclut les
films sans note ; Q6 exploite une collecte Wikidata P364 avec provenance,
vocabulaire ISO et mesure de couverture. Aucune langue originale n'est déduite
des titres alternatifs. Aucune collecte de synopsis ou biographies n'est lancée.

Les sorties incluent les Parquet par question, neuf aperçus JSON bornés, le
classement CSV et une sélection de films dédupliquée. Le lanceur Windows enchaîne
préparation, exploration et export des crédits, avec ressources bornées et garde
disque. Le collecteur de preuves refuse les sorties incomplètes et compare les
39 profils aux résultats du profil brut séparé avant publication.

Validation : 34 tests Python contrôlés réussis sous Spark 4.2.0 (66,02 s), contrôle
CLI, réconciliation de 369 502 énoncés de langues et profil complet réel. Les
premières sorties réelles sont publiées : Q1 (13 640 lignes), Q2 (138), Q3 (92),
9 089 places de classement pour 6 257 films distincts. Les cinq autres analyses
attendent la fin du nettoyage de leurs tables. Les critères restent provisoires.
Les branches de Benjamin et REPORT.md restent inchangés.

PR dépendante de #4 ; la cible sera ajustée après les approbations des PR parentes.

Un export ciblé produit les couples tconst,nconst, les détails de rôles et la
couverture des seuls films sélectionnés à partir des tables parentes terminées.
Il conserve les contrôles de qualité en évitant de normaliser les crédits hors
sélection ; un test compare ce filtrage au calcul non filtré, avec Unicode et
clés conflictuelles. Son périmètre ne certifie pas le nettoyage global.
