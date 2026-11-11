Brouillon de message de Nicolas à Benjamin — point intermédiaire du 11 novembre.

Salut Ben, j’ai avancé sur notre partie dans la PR #5 :
https://github.com/benjaminlabos/ece-big-data-processing/pull/5

Le profil des valeurs manquantes est calculé sur les sept fichiers IMDb complets :
39 colonnes et 216,48 millions de lignes. Les trois premières questions sont
exécutées. Le top 10 année–genre donne 9 089 places pour 6 257 films distincts.
Les paramètres de travail sont 40 minutes minimum et 1 000 votes, puis note
décroissante, votes décroissants et tconst croissant. Ils restent provisoires :
on doit encore les valider ensemble.

Tu peux déjà récupérer dans cette branche :
- `validation/enrichment-input-full-provisional-2026-11-11.csv` : entrée directe
  de ton collecteur, exactement `tconst,nconst` ;
- `validation/enrichment-primary-full-provisional-2026-11-11.csv` et
  `validation/enrichment-reserves-full-provisional-2026-11-11.csv` si tu préfères
  traiter d’abord les trois personnes prioritaires ;
- `validation/enrichment-coverage-full-provisional-2026-11-11.csv` pour la couverture ;
- `validation/top10-full-provisional-2026-11-11.csv` pour le classement complet.

Le pool vérifié contient 113 273 couples. L’entrée bornée retient 18 764 couples
prioritaires et 12 466 réserves, soit 31 230 couples et 19 375 personnes distinctes.
La règle de travail privilégie un réalisateur et deux autres interprètes, puis
d’autres crédits disponibles quand ces rôles manquent. Une personne présente
sous plusieurs rôles n’est comptée qu’une fois par film. Cinq films ont moins de
trois personnes dans les données : la couverture les identifie pour conserver
un statut d’absence explicite.

De ton côté, tu peux brancher cette entrée sur l’enrichissement Wikipédia,
contrôler les synopsis et les biographies disponibles, et conserver les URL de
révision, licences et raisons d’absence. Tu gardes aussi le streaming et sa
démonstration complète. Pour la question libre, je te propose que tu la prennes ;
on confirme ensemble le sujet et la répartition.

De notre côté, les huit requêtes sont codées et 35 tests passent sous Spark 4.2.0.
Le nettoyage global tourne encore : cinq tables sont terminées ; les cinq autres
questions s’exécuteront ensuite automatiquement. Aucun changement n’a été fait
sur tes branches #2 et #3. Peux-tu relire nos PR #1, #4 et #5 avant intégration ?
On réunira aussi les sections de PROJECT.md lors de la fusion. Les explications
et choix méthodologiques de REPORT.md restent à rédiger par nous-mêmes.
