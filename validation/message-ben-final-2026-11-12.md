# Prompt de continuation pour Benjamin — Big Data gr-04

Tu es l’assistant de Benjamin Labos. Continue concrètement notre projet Big Data Processing ECE avec Nicolas Rihana. Développe, exécute, vérifie et prépare des livrables reproductibles. Appuie-toi sur le travail existant et poursuis les tâches indépendantes pendant les calculs et attentes réseau. L’objectif est de terminer les chantiers de Benjamin, puis de préparer l’intégration commune.

Dépôt commun : https://github.com/benjaminlabos/ece-big-data-processing
Consignes : https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/project_instructions.md
Échéance indiquée dans notre documentation : 15 novembre 2026 à 23 h 59, fuseau non précisé. Vérifie la version officielle actuelle.

## Réponse de Nicolas à ton bilan

Merci pour le bilan : les commits et résultats correspondent aux éléments publiés dans tes PR. La validation Docker du streaming, la reprise de l’enrichissement et la première exécution de la question libre sont de vrais jalons acquis. Préserve ces preuves et leur périmètre. Continue la collecte et finalise la livraison commune. Les calculs complets de Nicolas et de ta question libre sont maintenant exécutés et vérifiés, comme détaillé ci-dessous.

Ton relevé de 2 505 réponses sur 25 632 et l’estimation de 11 heures sont un état communiqué, pas une mesure actuelle. Ce relevé est historique ; le dernier état public et les entrées révisées sont détaillés plus bas. Actualise les compteurs vivants avant de programmer la suite. Les 25 632 identifiants attendus correspondent à 6 257 films plus 19 375 personnes distinctes ; vérifie cette réconciliation sur les entrées réellement utilisées.

Le dépôt commun existe déjà. Nous n’avons pas à créer un nouveau dépôt après les merges. Il faut réunir les branches approuvées dans ce dépôt, conserver nos contributions et livrer un état commun testé depuis un clone propre.

## Ce que Nicolas fait en parallèle

Nicolas prend en charge les sept tables IMDb : profil qualité des 39 colonnes, nettoyage, références, quarantaines et réconciliation ; les huit questions Spark ; le top 10 année–genre ; les interfaces pour ton enrichissement et ton streaming.

Branches/PR : préparation #1, exports #4, analyse complète #5 sur feat/imdb-full-analysis. Benjamin a indiqué avoir approuvé techniquement #1 après 21 tests Docker ; vérifier la review GitHub et le commit approuvé avant de s’en servir pour une fusion.

Les entrées déjà publiées contiennent 6 257 films distincts, 9 089 places dans 979 groupes année–genre, 31 230 couples film–personne (18 764 prioritaires, 12 466 réserves), 19 375 personnes distinctes. Cinq films ont moins de trois personnes candidates. Les CSV restent disponibles dans validation/ sur la branche de Nicolas. Critères provisoires : films movie, durée minimale 40 minutes, au moins 1 000 votes, période 1980–2025 ; classement note décroissante, votes décroissants, identifiant croissant. student_validated reste false.

Notre traitement nocturne tourne sur GitHub, sans dépendre du PC :
https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43093003953
Commit source : 321c07d578fc77daafdf3d2a4553899af40c4ea5.

État vérifié le 12 novembre : les sept tables complètes sont préparées et en cache immuable : 216 502 833 lignes brutes, 216 502 728 propres, 105 en quarantaine, aucun doublon normalisé retiré. Les mises à null sont des réparations distinctes. Manifeste source SHA256 aeb5effe20cab570e20fbd3a250309a0f081f7f3a203d02dfafca5da0c999cfd.

Le CSV de langues du premier run était tronqué, ce qui bloquait les analyses. Nicolas a restauré un instantané historique complet de 369 502 lignes, avec ses empreintes et métadonnées originales ; archive CC0 dans references/. La reprise ne dépend plus d'une nouvelle réponse réseau de Wikidata. Correctif de référence : 5049b384edc950f4152ce09d9f07cce9929a63de. Dernier commit publié : cce523689ec65dfd10904575abd0ad6a4ffa63a4.

Finition réussie :
https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43125611999

La finition a RÉUSSI. Les 18 tests ciblés passent (69,47 s), les huit analyses complètes et le classement sont contrôlés, Q2/Q3 et la sélection sont réconciliées, les neuf combinaisons durée/votes sont comparées. Les Parquet complets et VERIFIED.json sont dans l'artefact du run ; les marqueurs READY_FOR_REVIEW.json et QUERY_REVIEW.json ainsi que les entrées sont publiés sur la branche de Nicolas dans validation/overnight-43093003953/. Les aperçus des requêtes sont limités à 100 lignes, pas les Parquet.

NOUVELLES entrées à reprendre sans effacer ton cache : 6 256 films, 9 089 places, 31 225 couples (18 761 prioritaires + 12 464 réserves), 19 374 personnes distinctes. Critères toujours provisoires. Différence avec ton lot précédent : aucun film ajouté, tt5480782 retiré, un couple ajouté (tt11767484,nm19101446), six couples retirés ; une seule nouvelle personne nm19101446 à rechercher. Les fichiers films-added/removed, pairs-added/removed, people-added et leurs empreintes sont fournis. Garde les anciennes réponses même si leur identifiant n'est plus sélectionné ; change les entrées proprement et recalcule la couverture sur ce lot versionné. Cinq films restent structurellement sous trois candidats. Le total attendu titres+personnes est maintenant 25 630 identifiants distincts ; ne confonds pas le nombre attendu et le cache effectivement collecté.

Les artefacts finaux de cette reprise sont conservés 90 jours. Les caches intermédiaires GitHub ne sont pas des archives permanentes. Les deux calculs sont terminés et leurs résultats publiés. Ne relance pas le téléchargement/nettoyage complet. Si vous relancez un workflow, sa garde exige une branche stable pendant la publication.

Un candidat local d'intégration a déjà réuni les composants et passé un test depuis un clone Windows propre. PROJECT.md add/add a été résolu en conservant le guide streaming intégral sous PROJECT_STREAMING.md et les liens vers les autres guides. La chaîne synthétique préparation → huit requêtes → exports → tes lecteurs/enrichissement/question libre a passé sous Spark 4.2.0. Le rejeu des données enregistrées et les six tests du collecteur sont contrôlés séparément. Ce sont des contrôles techniques ; ils ne remplacent pas tes résultats réels, ta collecte ou les reviews.

Dernier état PUBLIC de ton enrichissement, commit d2498de : 4 247 réponses auditées, 821 synopsis, 2 761 biographies distinctes et 1 189 films avec trois biographies, dont 274 grâce aux réserves. Ce n'est pas une mesure de ton processus actuel : Nicolas n'a pas accès à l'état vivant de ton PC. Actualise ce relevé avant toute estimation.

Nicolas a exécuté avec succès le code immuable de ta question libre (8a0d75bb26d6437ead2c927d69da7b8bcff6592d) directement sur ses quatre tables complètes, avec dix seuils et ton audit DuckDB : https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43128551838 . Le run est réussi et les dix comparaisons ont passé l'audit indépendant. Récupère les résultats vérifiés pour ta PR #6 sans refaire ce calcul. Tu gardes la relecture méthodologique, l'interprétation personnelle, la documentation et la PR #6. Le marqueur VERIFIED_FOR_PEER_REVIEW.json est fourni ; le travail numérique est exécuté, la validation humaine et la PR restent à finaliser.

## 1. Priorité immédiate : sécuriser et terminer l’enrichissement

Point de départ : PR #3, cf4ae7020c81959305e82dc16ac6e0b23b21d590. Instantané audité de 1 401 réponses : 226 synopsis, 986 biographies distinctes, 331 films avec au moins trois biographies, dont 81 grâce aux réserves. Six tests et reprise de cache contrôlée déjà documentés. Capitalise dessus.

Travail à réaliser :

1. Vérifie que le collecteur tourne réellement : processus, progression du cache, dernières erreurs, capacité disque et emplacement durable. Vérifie si son exécution dépend du PC, de Docker et de la mise en veille ; donne à Benjamin les conditions exactes pour la laisser fonctionner. Ne promets pas une exécution PC éteint si elle est locale.
2. Mesure les identifiants attendus, les réponses valides, les absences confirmées, les fichiers endommagés et les demandes restantes. Actualise la cadence et une estimation avec marge à partir de plusieurs relevés horodatés. Distingue nombre de réponses et nombre de textes disponibles.
3. Préserve le cache existant. Un seul collecteur écrit dans un dossier. Respecte Retry-After, maxlag et les limites de Wikimedia. Ne parallélise pas les requêtes pour gagner artificiellement du temps. Ne relance pas une collecte entière.
4. Prépare une sauvegarde cohérente et une commande exacte de reprise. Les tests d’arrêt/reprise déjà réussis n’exigent pas d’interrompre de nouveau le gros lot en cours.
5. Utilise freeze_cache.py pour figer un instantané dans un nouveau fichier ; exécute enrich.py sous Docker/Spark 4.2.0 avec --titles, --candidates, --primary et --top sur cet instantané. Évite les gros traitements Spark simultanés si les ressources ne suffisent pas.
6. Relis les quatre Parquet et recalcule la couverture depuis les lignes détaillées : synopsis, personnes distinctes avec biographie, films ayant au moins trois biographies, apport des réserves, couverture année–genre et raisons d’absence. Conserve les films sans résultat.
7. Sépare not_collected, erreurs temporaires et absences durables. Le marqueur _COMPLETE signifie que chaque identifiant a une réponse, y compris les absences ; il ne signifie pas que tous les films ont un synopsis et trois biographies.
8. Contrôle un échantillon varié de sources : identité IMDb/Wikidata/article, langue, contenu réellement biographique ou synopsis, URL de révision, licence et date. Aucun texte ne doit être généré par IA.
9. Une fois la première passe finie, mesure le déficit. Pour les films sous trois biographies, utilise d’abord les réserves prévues. Si d’autres personnes du pool complet sont nécessaires, produis une liste dédupliquée et un plan de collecte additionnelle borné, fondé sur les manques observés. Préserve les critères et documente le changement. N’invente pas de biographies pour les cinq cas structurellement sous trois candidats.
10. Les nouveaux exports de Nicolas sont disponibles : compare les ensembles et leurs empreintes. Réutilise les réponses pour les identifiants communs et collecte uniquement les nouveaux identifiants. Ne remplace pas silencieusement les entrées d’un collecteur actif : termine ou arrête proprement son passage et trace la nouvelle version.

Livraison : preuve horodatée avec entrées/empreintes, compteurs réconciliés, manifestes, Parquet relus, provenance, absences et reprise ; petits résultats vérifiables versionnés, données volumineuses conservées hors Git avec instructions d’accès. Mettre à jour PR #3 et sa documentation quand il y a un résultat substantiel.

## 2. Question libre : le calcul complet est maintenant livré

Le code ORIGINAL de Benjamin, commit 8a0d75bb26d6437ead2c927d69da7b8bcff6592d, a été exécuté sans modification sur les quatre tables nettoyées du lot complet de Nicolas. Run réussi : https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43128551838 . Le contrôle DuckDB 1.4.3 a recalculé indépendamment tous les diagnostics et les dix comparaisons. Le marqueur VERIFIED_FOR_PEER_REVIEW.json est présent.

Résultats publiés dans validation/collaborations-full-43093003953/ sur la branche de Nicolas : comparisons.csv, summary.json, PROVENANCE.json, INDEPENDENT_AUDIT.json, VERIFIED_FOR_PEER_REVIEW.json. Commit de résultats : a8606bf. L'artefact imdb-collaborations-full-43128551838-1 conserve aussi le Parquet et le journal pendant 90 jours.

Au seuil de 1 000 votes et deux années distinctes : 16 884 binômes ; moyenne première année par binôme 6,5296913251 ; moyenne années suivantes 6,1819830188 ; différence moyenne appariée -0,3477083063. Les dix cellules croisent votes 0/100/1000/5000/10000 et historique 2/3 années. L'historique complet contient 4 379 203 triplets avant exclusion des 69 179 auto-collaborations ; les années inconnues et les films sans note sont comptés dans les diagnostics. Les paramètres restent provisoires et les étudiants écrivent leur interprétation.

Ce calcul conserve les films antérieurs à 1980 et ceux sans note pour établir la première année connue, puis mesure les notes dans 1980–2025. Les films d'une même première année restent groupés. Il ne définit pas les premières collaborations depuis le seul top 10. Le lot complet et ton essai précédent de 6 257 films ont des manifestes distincts : ne prétends pas qu'une comparaison entre eux isole un unique effet de sélection.

Tes tâches restantes pour PR #6 :

1. Récupère les fichiers du dossier ci-dessus et vérifie leurs empreintes, provenance et conclusion de run. Réutilise le résultat calculé ; ne relance pas la préparation ni le même calcul lourd simplement pour le recopier.
2. Mets ta documentation et la description de PR #6 à jour autour du résultat FULL. Conserve ton code et l'attribution de tes commits ; le workflow de Nicolas n'est qu'un lanceur et une preuve d'exécution. Si tu versions les petits résultats dans ta branche, copie-les avec leur provenance intacte.
3. Présente les dix seuils et les diagnostics de disponibilité aux étudiants pour qu'ils valident la question et les paramètres. Le contrôle numérique ne tranche pas ces choix.
4. Vérifie la commande documentée, le périmètre chronologique, l'unité binôme et l'absence de causalité revendiquée. Les raisonnements et conclusions de REPORT.md sont rédigés personnellement par les étudiants.
5. Ne refais une exécution que si ta relecture entraîne une correction de code ou un changement de paramètres. Dans ce cas, préserve le résultat actuel, donne un nouveau dossier et trace le commit et les entrées exactes.

Les quatre tables complètes utilisées sont dans le cache GitHub exact imdb-43093003953-prepared-v1, temporaire ; elles ne sont pas toutes incluses dans le petit ZIP des résultats. Le manifeste source a l'empreinte aeb5effe20cab570e20fbd3a250309a0f081f7f3a203d02dfafca5da0c999cfd et le résumé préparé c6ba4f0b9b77110079f65b91cea6b8da4066ba4c1f11eee8836ea823fc03ae74. Le workflow versionné explique comment la reprise utilise ce cache sans nettoyage redondant.

## 3. Streaming : finaliser la livraison intégrable

Point de départ : PR #2, 64c0e1e75c39a2c7fd00b81d393caae8ff1c20e8. La capture Docker de 15 min 25,948 s et son rejeu intégré constituent déjà des preuves : 1 845 événements, sept correspondances sur six articles, reprise des offsets sans doublon, délai médian 20,165 s. Préserve les preuves WSL et Docker séparément.

Ne refais pas une longue capture identique sans raison. Termine plutôt :

- Vérification des commandes depuis la racine du dépôt, des montages, dépendances, noms de services, catalogue et chemins de checkpoints.
- Documentation du catalogue intégré : 39 926 liens conservés sur 39 951, 25 absents signalés, provenance et normalisation cohérentes avec #4. Conserve un périmètre suffisamment large pour le flux vivant.
- Réconciliation des événements distincts, liens événement–IMDb, articles, partitions/offsets et éventuelles correspondances multiples ; métriques de latence et taille de l’échantillon concerné.
- Distinction entre preuve de retards observés dans le réel et test synthétique du watermark. Décris les paramètres réellement exécutés.
- Le rejeu de démonstration est déjà publié au commit e6e5933 et ses données enregistrées sont réconciliées côté Nicolas. Vérifier seulement sa commande dans le dépôt intégré et conserver la provenance de la capture.
- Après modification d’interface ou intégration, exécuter le contrôle ciblé pertinent. Une nouvelle capture vivante de 15–30 minutes est utile si elle valide un comportement changé ou une exigence encore sans preuve.

Livraison : PR #2 prête à relire, exemples partageables dans GitHub et commandes reproductibles. Les liens C:/Users/benja/... de ton bilan ne sont pas consultables depuis le PC de Nicolas : publie les petits documents/preuves appropriés dans le dépôt, sans secrets, gros caches ni données brutes.

## 4. Préparer l’intégration commune pendant les attentes

1. Inspecte les branches, dépendances et changements de PROJECT.md/requirements/Compose. Prépare un tableau PR → commit → tests → dépendances → review. Vérifie l’approbation réelle de #1 ; distingue approbation technique dans un message et review GitHub.
2. Tu peux préparer un worktree/une branche temporaire d’intégration à partir de l’état actuel de main, sans changer les branches de travail. Vérifie d’abord les AGENTS.md applicables et les modifications locales. Rassemble les changements pour tester et identifier les conflits. Ne pousse pas sur main et ne fusionne pas une PR sans la revue croisée requise.
3. Respecte la dépendance préparation #1 → exports #4 → analyse #5. #2/#3/#6 doivent être testées avec leurs vraies interfaces. Actualise les cibles et la liste des changements des PR après intégration des parents afin d’éviter de relire les mêmes changements.
4. Conserve dans PROJECT.md les instructions des deux membres : installation, versions, batch, streaming, enrichissement, question libre, entrées/sorties, limites, commandes testées et usage de l’IA. Vérifie les incompatibilités de dépendances plutôt que d’unifier les versions aveuglément.
5. Vérifie les chemins : aucune commande de lancement ne doit imposer C:/Users/benja ou C:/Users/nriha. Utilise chemins relatifs ou paramètres ; vérifie les montages Windows/Docker et les espaces dans les chemins si concernés.
6. Prépare un test d’intégration court : sortie IMDb contrôlée → interfaces → enrichissement avec cache figé → jointures/couverture ; catalogue → rejeu Kafka/Spark ; tables → question libre. Ce test vérifie les contrats et ne remplace pas les exécutions réelles complètes.
7. Depuis un clone propre de l’état intégré candidat, exécute les commandes documentées et les tests utiles. Vérifie que les résultats sont accessibles, les dossiers de sortie sont neufs, et les caches/données/secrets sont exclus du suivi Git. Conserve les gros fichiers hors Git avec instructions pour les récupérer ou les reconstruire.
8. Mets chaque PR de brouillon à prête à relire lorsque son périmètre annoncé est vérifié. Distingue code prêt à relire et collecte complète encore en cours. Nicolas doit relire tes PR ; toi, termine la relecture de #4 et #5 sur leurs commits actuels. Une correction après review peut nécessiter une nouvelle vérification.

Ne crée pas de nouveau dépôt et ne réécris pas l’historique partagé. La fusion finale attend les approbations et les contrôles d’intégration nécessaires. Nicolas a déjà préparé et sauvegardé le candidat local ; réutilisez-le plutôt que de résoudre de nouveau les mêmes conflits. Ses résultats full et tes résultats de collaborations sont désormais disponibles pour la relecture finale.

## 5. Critères, rapport et rendu

Prépare les éléments factuels pour la décision à deux : sensibilité durée/votes de Nicolas, couverture réelle de tes sources, cas sous trois biographies, conséquences sur le périmètre. N’adopte pas une définition uniquement pour masquer des informations manquantes. Une même sélection doit être identifiable dans classement, enrichissement et couverture.

Le rapport et ses interprétations sont rédigés personnellement par les étudiants, conformément aux consignes. L’assistant peut produire code, calculs, tableaux techniques, preuves et explications de fonctionnement pour aider à comprendre. Ne rédige pas les raisonnements ni conclusions de REPORT.md à leur place. Conserve les passages existants des étudiants.

Prépare une liste de preuves et de résultats pour qu’ils écrivent, ainsi qu’un déroulé technique de démonstration avec un rejeu disponible en cas d’absence d’événement pertinent dans le direct. Vérifie README, groupe, contributeurs, déclaration IA, accès au dépôt et consignes de soumission. Les étudiants gèrent l’envoi final ; ne l’envoie pas automatiquement.

## Ordre de travail et prochain bilan

Commence maintenant par un état réel : branche/commit, collecte active et progression, données complètes disponibles, ressources et blocages. Sécurise la collecte puis laisse-la progresser. Pendant les attentes, reprends les résultats complets de la question libre, la livraison streaming et l’intégration. Lance les gros travaux Spark successivement si la mémoire est limitée. Ne te contente pas d’un plan ; réalise les tâches indépendantes autorisées jusqu’à des résultats vérifiables.

Ton prochain bilan doit contenir :

- état actuel de la collecte, cadence mesurée, reste à traiter, absences et couverture du dernier instantané vérifié ;
- intégration des résultats complets déjà fournis dans ta PR #6, avec provenance et relecture des paramètres ;
- état des PR, commits et tests réellement exécutés ;
- conflits d’intégration résolus ou à décider et résultat du test depuis un clone propre ;
- ce qui exige une validation méthodologique du binôme ;
- demande précise à Nicolas (fichier, schéma, commit, format et usage), au lieu d’une attente générale.

Ne relance pas les workflows de Nicolas et ne remplace pas ton cache existant. Travaille sur tes branches et conserve une distinction nette entre terminé, vérifié sur échantillon, en cours et bloqué.
