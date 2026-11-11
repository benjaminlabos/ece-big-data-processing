Cette tâche prépare l'exécution des sept tables IMDb complètes, réduit les caches
simultanés du nettoyage et publie la progression par table. Le téléchargement
complet du 11 novembre a fourni 2 003 620 065 octets compressés, avec contrôle
d'intégrité des sept gzip ; cela ne constitue pas encore une validation du nettoyage.

Le module d'exploration implémente les huit questions et le top 10 année–genre.
Les seuils sont explicites et les valeurs de démonstration restent marquées
provisoires, sans validation méthodologique des étudiants. La question des langues
est bloquée sans source supplémentaire de langues originales avec provenance.
Aucune langue originale n'est déduite des titres alternatifs.

Les sorties incluent les Parquet par question, les paramètres, les statuts et un
CSV des films sélectionnés, utilisable pour l'export de crédits existant. Le
lanceur Windows enchaîne préparation, exploration et export, avec ressources
Docker bornées et surveillance du disque. Il n'exécute aucune collecte Wikipédia.

Les tests contrôlés couvrent les huit calculs, les saisons, les carrières,
les langues fournies, les multiples rôles et la limite de dix films par groupe.
Le guide distingue les essais de code des résultats complets réellement obtenus.
Les branches et pipelines de Benjamin ainsi que REPORT.md restent inchangés.

PR dépendante de #4 ; la cible sera ajustée après les approbations des PR parentes.
