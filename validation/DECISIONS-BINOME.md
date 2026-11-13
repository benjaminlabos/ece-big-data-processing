# Paramètres retenus pour l’intégration — 13 novembre 2026

Nicolas a autorisé l’assistant à choisir et terminer l’intégration le 13 novembre :
« je te fais confiance chosisi et finit ce qu’il faut ». Benjamin a recommandé
ces trois propositions sans désaccord technique dans son bilan de clôture.

| Élément | Valeur retenue |
| --- | --- |
| Films du classement | movie, 1980–2025 |
| Durée minimale | 40 minutes |
| Votes minimum | 1 000 |
| Ordre du classement | note décroissante, votes décroissants, identifiant croissant |
| Question libre principale | 1 000 votes, deux années distinctes ; neuf autres cellules en sensibilité |
| Crédits d’enrichissement | trois prioritaires, deux réserves initiales, au plus trois compléments déjà en cache |
| Catégories v2 | self, archive_footage et archive_sound admises comme dans les entrées auditées |
| Responsable des fusions | Nicolas, intégration assistée par Codex |

Ces choix conservent exactement les entrées et sorties déjà calculées. Les
manifestes historiques sont conservés avec leur ancien student_validated=false ;
ils ne sont pas réécrits rétroactivement. Le présent document trace la décision
de livraison. Les justifications et interprétations du rapport restent à rédiger
personnellement par les étudiants selon les consignes du cours.

---

## Historique des paramètres et des calculs comparatifs

# Décisions à valider en binôme

Le code conserve `student_validated=false`. Cette fiche rassemble les paramètres
et les mesures disponibles ; les étudiants valident leurs choix et rédigent
leurs justifications personnelles.

## Classement et enrichissement

Configuration calculée actuellement : films `movie`, années 1980–2025, durée
au moins 40 minutes, au moins 1 000 votes ; classement par note décroissante,
votes décroissants, puis identifiant croissant. Collecte de trois personnes
prioritaires et jusqu'à deux réserves, sans doublon film–personne.

| Durée minimale | Votes minimum | Films admissibles | Films sélectionnés distincts | Sélectionnés avec trois candidats |
| --- | --- | --- | --- | --- |
| 40 | 100 | 113851 | 7165 | 7076 |
| 40 | 1000 | 40053 | 6256 | 6251 |
| 40 | 10000 | 11124 | 4707 | 4706 |
| 60 | 100 | 112245 | 7139 | 7068 |
| 60 | 1000 | 39876 | 6250 | 6245 |
| 60 | 10000 | 11109 | 4708 | 4707 |
| 90 | 100 | 82166 | 6977 | 6927 |
| 90 | 1000 | 32944 | 6073 | 6069 |
| 90 | 10000 | 9885 | 4528 | 4527 |

Ces comptes sont ceux des neuf calculs ; une différence d'effectif ne décrit
pas à elle seule les différences d'identifiants entre deux sélections.

- [ ] Durée minimale retenue et justification personnelle.
- [ ] Seuil de votes retenu et justification personnelle.
- [ ] Politique des personnes retenue ; cas sous trois candidats et absences de la source unique documentés.

## Question libre

Paramètres calculés : votes 0/100/1000/5000/10000, historique 2/3 années ;
chronologie complète jusqu'à 2025, comparaison des notes dans 1980–2025, même
poids par binôme. Les dix cellules sont dans
`validation/collaborations-full-43093003953/comparisons.csv`.

- [ ] Question et paramètres principaux retenus par les étudiants.
- [ ] Méthodologie, alternatives et interprétation personnelles prêtes pour REPORT.md.

Une modification retenue doit être tracée et ses sorties recalculées avec les
entrées correspondantes. Un accord humain n'est pas déduit de la réussite des tests.
