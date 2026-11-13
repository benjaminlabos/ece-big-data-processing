# Intégration technique terminée le 13 novembre 2026

Les sept PR techniques sont fusionnées dans main au commit
`4781544fbd2282088a2bb51ec156be40cd0fdc3e`, après approbations croisées.
La résolution du guide de #2 a été publiée dans `a5584e9` et approuvée dans
[la review 5889316722](https://github.com/benjaminlabos/ece-big-data-processing/pull/2#pullrequestreview-5889316722)
avant sa fusion. Les paramètres de livraison sont consignés dans
DECISIONS-BINOME.md après délégation explicite de Nicolas.

Les paragraphes ci-dessous conservent les états historiques. Le rapport
personnel, la relecture des documents de clôture et le contrôle de soumission
restent à terminer.

---

# Archive v2 reçue et auditée sur le PC Nicolas

SHA-256 de l’archive confirmé :
`6a54dbed30efd4be905318e1fe053f0b75718dace5b33980d8bd57e546e23e81`.
Les 41 entrées du manifeste, 17 fichiers Parquet et huit CSV d’entrée sont
vérifiés. Cache reconstitué : 25 630 réponses, empreinte identique et aucune
réponse manquante. Audit PyArrow complet avec `--require-complete` reproduit
exactement ; contrôle DuckDB indépendant conforme sur 5 369 / 4 710 films,
+140 / +102 gains v2 et 970 franchissements par réserves initiales.
Les anciennes limites de réception ci-dessous sont désormais levées pour
cette archive. Reçu : `peer-review-2026-11-13/received-v2-archive-audit.json`.

Les votes de Benjamin sur #4/#5/#8 restent attendus au dernier contrôle
GitHub. Aucun main fusionné. Le rapport personnel et les décisions finales
restent à terminer en binôme.

---

# Résultats v2 publiés et relus — 13 novembre

Nouvelle tête #3 `409f167` : production inchangée, ajout des preuves Spark v2.
[APPROVE publié par Nicolas](https://github.com/benjaminlabos/ece-big-data-processing/pull/3#pullrequestreview-5888458936) le 13 novembre à 08:58:11 UTC.
Trois CSV publiés et huit entrées vérifiés par SHA-256 ; reconstruction exacte
des 979 groupes et des 140 franchissements (102 avec synopsis) depuis les
listes v1/v2 et les 1 060 couples exacts. Statistiques : 5 369 films avec trois
biographies, 4 710 ayant aussi un synopsis, 1 546 incomplets. Voir le reçu
`peer-review-2026-11-13/pr3-v2-review-receipt.json`.

Le cache/ZIP et les quatre Parquet finaux restent à recevoir de Benjamin ;
aucun nouveau scan local de leurs octets n'est revendiqué. Les états plus
anciens ci-dessous sont conservés comme historique daté.

---

# Mise à jour du 13 novembre 2026

La sélection initiale finale de #3 (`9fcef57`) est approuvée par Nicolas :
[review publiée](https://github.com/benjaminlabos/ece-big-data-processing/pull/3#pullrequestreview-5887989948).
Les deux empreintes CSV et les 979 groupes ont été contrôlés indépendamment.
Voir `peer-review-2026-11-13/pr3-final-review-receipt.json` pour le périmètre et
la limite : les fichiers bruts et Parquet finaux locaux de Benjamin n'ont pas
été rescannés ici. La nouvelle tête ne modifie pas le code déjà relu.

Le correctif #8 `0746647` accepte les dates UTC Z sous Python 3.10 ; 16 tests
passent en 3.10.22 et 3.12.14. La reconstruction réelle en 3.10 reproduit les
huit CSV. Les résultats Spark de cette variante restent attendus. #4/#5 ont
encore besoin des votes de Benjamin ; #8 de sa review. Aucun main fusionné.

Les éléments ci-dessous conservent l'historique daté du 12 novembre.

---

# Relectures et préparation du rendu — 12 novembre 2026

| PR | Commit examiné | Conclusion technique | Suite |
| --- | --- | --- | --- |
| [#2](https://github.com/benjaminlabos/ece-big-data-processing/pull/2) | e6e5933 | Aucun blocage de code identifié dans le périmètre examiné | [Relecture préparée](peer-review-2026-11-12/PR-2.md), approbation du binôme |
| [#3](https://github.com/benjaminlabos/ece-big-data-processing/pull/3) | d870d97 | Contrats, collecte et jointures contrôlés ; instantané publié encore partiel | [Relecture préparée](peer-review-2026-11-12/PR-3.md), attendre les sorties finales |
| [#6](https://github.com/benjaminlabos/ece-big-data-processing/pull/6) | 4bfbf2a | Neuf cas contrôlés et audit DuckDB réussis ; résultat complet repris à l'identique | [Relecture préparée](peer-review-2026-11-12/PR-6.md), choix méthodologiques et approbation |

Les approbations techniques des PR #2 et #6 sont publiées par Nicolas le 12 novembre. La review COMMENT de #3 est publiée ; son approbation finale attend les dernières données. Les captures
streaming existantes sont examinées et leur payload est vérifié hors ligne ;
aucune nouvelle capture vivante n'est revendiquée. Le cache complet de Ben
n'est pas accessible dans cette copie et n'a pas été rescanné.

Le nouveau clone passe la chaîne synthétique de bout en bout sous Spark 4.2.0
et Java 21, six tests du collecteur, le contrôle des neuf sorties, les refus de
corruption et neuf comparaisons chronologiques recalculées dans DuckDB. Le
contrôle ciblé de l'enrichissement passe aussi avec BeautifulSoup 4.14.3, version
déclarée dans les dépendances. Les empreintes Windows diffèrent de celles de
Linux uniquement à cause de CRLF, pour les quatre fichiers vérifiés.

La [checklist finale](FINAL-RELEASE-CHECKLIST.md) sépare les contrôles terminés
des points à clôturer. REPORT.md est encore absent des fichiers suivis du
candidat : les étudiants doivent y ajouter leurs résultats et leurs explications
personnelles conformément aux consignes.

Mise à jour de l'enrichissement : le nouvel audit PyArrow de d870d97 a passé
trois contrôles ciblés indépendants du contrôle Spark initial. Les reviews
COMMENT de Benjamin sur #4 et #5 sont désormais publiées ; elles ne constituent
pas des votes APPROVE.

## Publication du 12 novembre, après reconnexion

- PR #2 : APPROVED, commit e6e5933, vote de nicohris.
- PR #6 : APPROVED, commit 4bfbf2a, vote de nicohris.
- PR #8 : extension bornée publiée et reconstruite sous Windows ; les empreintes
  des huit CSV correspondent à Linux. Sept tests ciblés passent aussi dans le
  candidat intégré. Les paramètres communs et la validation de Ben restent attendus.
- PR #4 et #5 : commentaires techniques de Ben publiés, votes APPROVE encore absents.
- La PR #7 de projection ponctuelle est fermée et n'est pas une dépendance à fusionner.
