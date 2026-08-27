# Plan de monitoring

## 1. Objectif

Surveiller en continu que le pipeline (extraction => transformation => chargement) produit des données **exploitables**, dans un **délai raisonnable**, **sans dépasser les quotas** des API gratuites et être alerté rapidement si ce n'est plus le cas.

## 2. Ce qui est surveillé, par étape

| Étape          | Ce qu'on surveille                                                                   | Où c'est déjà journalisé aujourd'hui                                                  |
| -------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| Extraction     | Nombre de publications récupérées par source, échecs de clé/quota, images rejetées   | `02_extraction_scripts/logs/extraction.log`                                           |
| Transformation | Taux de rejet (champ manquant, texte vide), doublons supprimés, images mal associées | `03_transformation_pipeline/logs/transformation.log` + `data/processed/manifest.json` |
| Chargement     | Statut de chaque tâche du DAG, durée, retries                                        | UI Airflow (vue Graph/Logs) + `04_airflow_pipeline/logs/`                             |

## 3. KPI suivis

- **Précision** : taux d'exploitabilité (sortie/entrée), taux d'images valides, taux d'association texte-image correcte, taux de publications labellisées.
- **Rapidité** : temps par tâche, temps total, débit (publications/seconde).
- **Coût** : quota API consommé , volume de stockage (images + dataset).

## 4. Fréquence de vérification

| Vérification                         | Fréquence                                                               | Mécanisme                                                      |
| ------------------------------------ | ----------------------------------------------------------------------- | -------------------------------------------------------------- |
| Exécution du pipeline                | Quotidienne                                                             | `schedule="@daily"` déjà configuré dans `checkitai_etl_dag.py` |
| Statut du dernier run (succès/échec) | À chaque exécution, automatique                                         | Callback `on_failure_callback` + UI Airflow                    |
| Revue des KPI (dashboard)            | Hebdomadaire                                                            | Revue manuelle par le owner du pipeline                        |
| Revue du quota API consommé          | Hebdomadaire (ou avant toute augmentation de `--limit`/`EXTRACT_LIMIT`) | Dashboard, section 3                                           |

## 5. Seuils d'alerte

| Indicateur                                             | Seuil d'alerte                                                  | Sévérité | Action                                                                                      |
| ------------------------------------------------------ | --------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------- |
| Anomalie d'association texte-image                     | > 0 %                                                           | Critique | Ne pas utiliser le lot pour l'entraînement ; investiguer `valide_association_texte_image()` |
| Taux d'exploitabilité (sortie/entrée)                  | < 90 %                                                          | Élevée   | Vérifier les logs de transformation : motif de rejet dominant                               |
| Taux d'images valides                                  | < 85 %                                                          | Moyenne  | Vérifier la disponibilité des sources d'image (liens morts, blocages anti-bot.              |
| Échec d'une tâche Airflow après épuisement des retries | Toute occurrence                                                | Élevée   | Callback d'alerte automatique ; consulter les logs de la tâche dans l'UI                    |
| Quota API consommé                                     | > 80 % du quota journalier                                      | Moyenne  | Réduire `EXTRACT_LIMIT` ou étaler les runs, éviter un blocage total le lendemain            |
| Temps total du DAG                                     | > 2× le temps du dernier run réussi (visible dans le dashboard) | Basse    | Vérifier si `enrich_images=True` a été activé à plus grande échelle sur FakeNewsNet         |

## 6. Mécanismes d'alerte

```python
default_args = {
    "owner": "checkitai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}
```

`alert_on_failure()` se déclenche automatiquement quand une tâche épuise ses retries (1 nouvelle tentative après 2 minutes, donc alerte au plus tôt 2 minutes après le premier échec). Aujourd'hui elle journalise un message `ERROR` explicite (visible dans les logs de tâche et l'UI Airflow)

## 7. Gestion des erreurs

| Type d'erreur                           | Comportement actuel                                       | Où dans le code                                                                              |
| --------------------------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Clé API manquante (NewsAPI/NewsData.io) | Tâche en succès, source ignorée, avertissement journalisé | `extract_newsapi`/`extract_newsdata` dans le DAG, `NewsAPIConfigError`/`NewsDataConfigError` |
| Quota API dépassé (429)                 | Attente + jusqu'à 3 tentatives avant abandon propre       | `newsapi_extractor.fetch`, `newsdata_extractor.fetch`                                        |
| Lien mort / site bloquant le scraping   | Publication conservée sans image, écart journalisé        | `fakenewsnet_extractor._enrich_image`                                                        |
| `robots.txt` interdisant l'accès        | Enrichissement ignoré pour cette page uniquement          | `common/robots.py`                                                                           |
| Échec réseau générique sur une source   | Isolée : les autres tâches d'extraction continuent        | Un `try/except` par tâche dans le DAG (et par source dans `main.py` en usage hors-Airflow)   |
| Échec de la tâche de chargement         | Retry automatique (1×, délai 2 min), puis alerte          | `default_args` du DAG                                                                        |

## 8. Journalisation

- **Format** : horodatage, niveau, module, message (`%(asctime)s | %(levelname)-8s | %(name)s | %(message)s`), identique sur les étapes 2 et 3 (`logger_setup.py`).
- **Emplacement** : fichier local par étape (`logs/*.log`) + console + UI Airflow, qui centralise aussi les logs de tâche par exécution.
- **Rétention** : aucune purge automatique configurée à ce stade (volumétrie faible en usage local) à revoir si le pipeline tourne en continu sur une longue période (ex. rotation via `logging.handlers.RotatingFileHandler`, ou rétention Airflow standard si déployé au-delà du poste local).

## 9. Responsabilité

- **Déclenchement** : automatique (`@daily`) ou manuel (`airflow dags trigger checkitai_etl`).
- **Revue des KPI** : manuelle, via `streamlit run dashboard.py`, à effectuer après chaque run significatif ou au minimum chaque semaine.
