# Vérification avant rendu — gr-04

Référence officielle : https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/project_instructions.md

## Vérifié techniquement

- [x] Lot complet Nicolas : sept tables, profils des 39 colonnes, Q1 à Q8, classement et exports ; run 43125611999.
- [x] Question libre complète, dix comparaisons et audit indépendant ; run 43128551838.
- [x] PR #6 : résultats complets repris par Benjamin.
- [x] Derniers commits #3/#6 réunis dans le candidat sans conflit.
- [x] Nouveau clone : chaîne synthétique, tests du collecteur, contrôleur des sorties et neuf comparaisons non vides/vide de la question libre.
- [x] Captures streaming existantes examinées ; fichier de démonstration et six identifiants du catalogue courant recoupés.
- [x] Guides des deux membres et historique des contributions conservés.

## À clôturer

- [x] Nicolas : répétition locale de l’ordre #1 → #4 → #5 → #8 → #2 → #3 → #6 ; seul conflit PROJECT.md résolu, code identique au candidat audité. Voir MERGE-PLAN-2026-11-13.md.
- [x] Nicolas : guide de démonstration, vérification hors ligne du jeu streaming, comptes des neuf CSV complets et dix comparaisons, trame du rapport personnel. Voir DEMONSTRATION-2026-11-13.md et REPORT-OUTLINE.md.

- [x] Benjamin : collecte initiale terminée et manifeste final publié dans #3 : 25 630 réponses, zéro en attente, zéro fichier endommagé signalé.
- [x] Benjamin : calcul Spark initial final publié ; deux CSV et 979 groupes recoupés indépendamment ici. Leurs octets ont ensuite été contrôlés sur la copie reçue par Nicolas, voir la ligne archive ci-dessous.
- [x] Nicolas : correctif Python 3.10 publié dans #8 (`0746647`) ; 16 tests sous 3.10/3.12 et huit CSV identiques après reconstruction complète.
- [x] Nicolas : sélection initiale #3 approuvée sur `9fcef57` ; APPROVE v2 également publié sur `409f167` (review 5888458936), avec périmètre et limites explicites.
- [x] Benjamin : Spark v2 exécuté et preuves publiées sur `409f167`. Trois empreintes CSV, huit empreintes d’entrées, 979 groupes et 140 franchissements recoupés ici.
- [x] Archive reçue sur le PC Nicolas et SHA-256 vérifié : `6a54dbed30efd4be905318e1fe053f0b75718dace5b33980d8bd57e546e23e81`. Les 41 entrées et 17 fichiers Parquet concordent avec leurs empreintes ; 25 630 réponses restaurées, audit PyArrow complet reproduit et résultats recoupés par DuckDB 1.4.3. Voir `peer-review-2026-11-13/received-v2-archive-audit.json`. Le transfert Drive n’est plus nécessaire pour cet audit.
- [x] Benjamin : #8 approuvée sur 0746647 ; la politique proposée est recommandée, accord final du binôme encore à enregistrer.
- [x] Paramètres de livraison retenus par Nicolas après délégation explicite : 40 min / 1 000 votes, question principale 1 000 votes / deux années, politique v2 ; les manifestes historiques conservent leur statut original. Voir DECISIONS-BINOME.md.
- [x] Benjamin : APPROVE #4/#5/#8 publiés le 13 novembre à 11 h 03 (Paris) et vérifiés sur GitHub, reviews 5889168032 / 5889168540 / 5889169011. #1/#2/#3/#6 ont aussi une approbation sur les têtes connues ; recontrôler immédiatement avant fusion.
- [ ] Ajouter au dépôt REPORT.md, qui n'est pas présent dans les fichiers suivis du candidat examiné. Inclure les résultats, métriques et explications méthodologiques personnelles requises pour chaque section.
- [x] PR #1/#4/#5/#8/#2/#3/#6 fusionnées avec merge commits, cibles dépendantes actualisées. Conflit PROJECT.md résolu dans a5584e9 et nouvelle approbation #2 enregistrée avant fusion. Main technique : 4781544fbd2282088a2bb51ec156be40cd0fdc3e.
- [x] Main technique 4781544 : clone neuf public, 69 tests réussis en 133,61 s ; six tests du collecteur et contrats croisés exécutés sur le code identique. Rejeu hors ligne du jeu démo vérifié.
- [ ] Après intégration des derniers documents et du rapport personnel, noter le SHA de soumission et vérifier une dernière fois ses liens et artefacts.
- [ ] Les étudiants effectuent la soumission prévue par le cours. Échéance : dimanche 15 novembre 2026, 23 h 59, fuseau non précisé par les consignes.

## Sources factuelles pour le rapport personnel

| Partie | Résultats / preuves à consulter |
| --- | --- |
| Préparation | validation/full-snapshot-43093003953/ |
| Q1 à Q8 et classement | validation/overnight-43093003953/query-review/QUERY_REVIEW.json ; manifeste des CSV et génération locale ; Parquet du run vérifié |
| Sélection et neuf seuils | validation/overnight-43093003953/threshold-summary.csv et READY_FOR_REVIEW.json |
| Enrichissement final initial | project/enrichment/evidence/final-enrichment-2026-11-12.json et final-enrichment-v2-2026-11-13.json |
| Streaming vivant, reprise, watermark et rejeu | PROJECT_STREAMING.md et project/streaming/evidence/ |
| Question libre complète | validation/collaborations-full-43093003953/ et collaborations-full-peer-check-2026-11-12.json |
| Relecture actuelle | validation/peer-review-2026-11-12/ |

Les consignes autorisent l'assistance au code mais interdisent les explications et raisonnements rédigés par IA dans REPORT.md. Ce document fournit les chemins et contrôles factuels ; il ne rédige pas ces sections à la place des étudiants.
