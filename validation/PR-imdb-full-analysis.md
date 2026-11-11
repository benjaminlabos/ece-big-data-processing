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

Validation : 33 tests Python contrôlés réussis sous Spark 4.2.0 (61,96 s), contrôle
CLI, réconciliation de 369 502 énoncés de langues et profil complet réel. Les
résultats des huit analyses complètes seront disponibles après leur succès effectif.
Les branches de Benjamin et REPORT.md restent inchangés.

PR dépendante de #4 ; la cible sera ajustée après les approbations des PR parentes.
