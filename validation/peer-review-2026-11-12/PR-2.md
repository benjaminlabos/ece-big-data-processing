# Relecture technique — PR #2

Commit examiné : `e6e5933c177ec612fa93538002e0169240b4548a`.

Conclusion : aucun défaut de code bloquant identifié dans le périmètre examiné.

- Lecture du producteur, de la jointure Structured Streaming, des compteurs, du rejeu et du filtre du catalogue. Les offsets Kafka et les identifiants Wikimedia sont comptés séparément ; les rapprochements multiples restent dans un tableau par événement.
- Le test d'intégration neuf accepte le CSV produit par la préparation, normalise les titres et écarte les références invalides.
- La vérification indépendante de la capture enregistrée retrouve 1 845 événements distincts, 1 311 modifications admissibles, sept correspondances et six pages. Les six identifiants du catalogue de démonstration existent dans le catalogue IMDb complet actuel.
- Les captures, reprises Kafka et contrôles de watermark Docker publiés ont été examinés. Aucun nouveau flux vivant ni nouveau rejeu Kafka n'a été exécuté dans cette session de relecture. Le catalogue complet historique de 39 926 liens conserve sa propre provenance ; le contrôle actuel ne prétend pas réauditer ces 39 926 lignes.

Pour le rendu : utiliser le guide PROJECT_STREAMING.md conservé dans le candidat ; distinguer les mesures de capture vivante, le rejeu et le test de watermark dans le travail personnel du rapport.

Cette relecture technique est préparée localement ; aucune approbation GitHub n'est soumise par ce document. Les décisions méthodologiques et la validation finale du binôme restent distinctes des contrôles de code.

Approbation technique publiée sur GitHub par nicohris le 2026-11-12T18:27:53Z, sur e6e5933.
