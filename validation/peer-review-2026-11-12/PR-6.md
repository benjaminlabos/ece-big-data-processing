# Relecture technique — PR #6

Commit examiné : `4bfbf2a0b7c37e66a66b2c8709255cff07e1bdc7`.

Conclusion : aucun défaut de code bloquant identifié dans le périmètre examiné.

- Lecture de l'historique Spark et de l'audit SQL : déduplication des triplets, première année à égalité pour tous les films de cette année, antériorité calculée avant le filtrage des votes, films sans note et antérieurs à 1980 conservés pour la chronologie, exclusion explicite des auto-collaborations.
- Le clone neuf passe les neuf comparaisons contrôlées ; DuckDB recalcule indépendamment les neuf cellules et les diagnostics. Les cas couvrent notamment les années inconnues, les crédits répétés, la première collaboration avant période et les groupes vides.
- Le résumé complet intégré par Benjamin est identique au résultat complet publié par le workflow commun. Les six empreintes d'artefact intégrées concordent. Le calcul complet n'a pas été relancé.
- Au seuil provisoire 1 000 votes / deux années : 16 884 binômes, moyennes par binôme 6,5296913251 et 6,1819830188 ; dix comparaisons complètes vérifiées précédemment dans le run 43128551838.

Pour le rendu : valider la question et ses paramètres en binôme ; les étudiants écrivent leur interprétation. L'ancien essai top 10 et le lot complet gardent leurs périmètres et manifestes distincts.

Cette relecture technique est préparée localement ; aucune approbation GitHub n'est soumise par ce document. Les décisions méthodologiques et la validation finale du binôme restent distinctes des contrôles de code.

Approbation technique publiée sur GitHub par nicohris le 2026-11-12T18:28:34Z, sur 4bfbf2a.
