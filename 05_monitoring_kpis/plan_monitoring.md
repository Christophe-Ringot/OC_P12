# Plan de monitoring — pipeline ETL CheckItAI

## 1. Objectif

Surveiller en continu que le pipeline (extraction → transformation → chargement, étapes 2 à 4) produit des données **exploitables**, dans un **délai raisonnable**, **sans dépasser les quotas** des API gratuites — et être alerté rapidement si ce n'est plus le cas. Ce document décrit la stratégie ; les KPI eux-mêmes sont visualisés dans [dashboard.py](dashboard.py).

## 2. Ce qui est surveillé, par étape

| Étape | Ce qu'on surveille | Où c'est déjà journalisé aujourd'hui |
|---|---|---|
| Extraction (étape 2) | Nombre de publications récupérées par source, échecs de clé/quota, images rejetées | `02_extraction_scripts/logs/extraction.log` |
| Transformation (étape 3) | Taux de rejet (champ manquant, texte vide), doublons supprimés, images mal associées | `03_transformation_pipeline/logs/transformation.log` + `data/processed/manifest.json` |
| Chargement (étape 4) | Statut de chaque tâche du DAG, durée, retries | UI Airflow (vue Graph/Logs) + `04_airflow_pipeline/logs/` |

Ce tableau reprend uniquement des mécanismes qui existent déjà dans le code des étapes précédentes — rien n'est ajouté qui ne soit pas déjà en place.

## 3. KPI suivis (voir dashboard.py pour le détail visuel)

- **Précision** : taux d'exploitabilité (sortie/entrée), taux d'images valides, taux d'association texte-image correcte, taux de publications labellisées.
- **Rapidité** : temps par tâche, temps total, débit (publications/seconde).
- **Coût** : quota API consommé (% du quota gratuit journalier), volume de stockage (images + dataset).

## 4. Fréquence de vérification

| Vérification | Fréquence | Mécanisme |
|---|---|---|
| Exécution du pipeline | Quotidienne | `schedule="@daily"` déjà configuré dans `checkitai_etl_dag.py` |
| Statut du dernier run (succès/échec) | À chaque exécution, automatique | Callback `on_failure_callback` (voir §6) + UI Airflow |
| Revue des KPI (dashboard) | Hebdomadaire | Revue manuelle par le owner du pipeline (`christophe.ringot1996@gmail.com`) |
| Revue du quota API consommé | Hebdomadaire (ou avant toute augmentation de `--limit`/`EXTRACT_LIMIT`) | Dashboard, section 3 |

## 5. Seuils d'alerte

| Indicateur | Seuil d'alerte | Sévérité | Action |
|---|---|---|---|
| Anomalie d'association texte-image | > 0 % | Critique | Ne pas utiliser le lot pour l'entraînement ; investiguer `valide_association_texte_image()` (étape 3) |
| Taux d'exploitabilité (sortie/entrée) | < 90 % | Élevée | Vérifier les logs de transformation : motif de rejet dominant |
| Taux d'images valides | < 85 % | Moyenne | Vérifier la disponibilité des sources d'image (liens morts, blocages anti-bot — normal en partie pour FakeNewsNet, cf. étape 2) |
| Échec d'une tâche Airflow après épuisement des retries | Toute occurrence | Élevée | Callback d'alerte automatique (§6) ; consulter les logs de la tâche dans l'UI |
| Quota API consommé | > 80 % du quota journalier | Moyenne | Réduire `EXTRACT_LIMIT` ou étaler les runs ; éviter un blocage total le lendemain |
| Temps total du DAG | > 2× le temps du dernier run réussi (visible dans le dashboard) | Basse | Vérifier si `enrich_images=True` a été activé à plus grande échelle sur FakeNewsNet (cause connue de ralentissement, cf. étape 2) |

## 6. Mécanismes d'alerte — déjà branchés sur le DAG réel

Le DAG (`04_airflow_pipeline/dags/checkitai_etl_dag.py`) déclare :

```python
default_args = {
    "owner": "checkitai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}
```

`alert_on_failure()` se déclenche automatiquement quand une tâche épuise ses retries (1 nouvelle tentative après 2 minutes, donc alerte au plus tôt ~2 minutes après le premier échec). Aujourd'hui elle journalise un message `ERROR` explicite (visible dans les logs de tâche et l'UI Airflow) — **vérifié en conditions réelles** : en forçant l'échec de `load_to_postgres` (absence de connexion Postgres), le message `ALERTE pipeline : la tâche 'load_to_postgres' ...` apparaît bien dans les logs.

Passage à une alerte push (Slack/email) en production : une seule ligne à ajouter dans `alert_on_failure()`, par exemple :

```python
requests.post(SLACK_WEBHOOK_URL, json={"text": f"❌ {ti.dag_id}.{ti.task_id} a échoué"})
```

Pas de webhook configuré à ce stade (pas d'infrastructure Slack/email pour ce projet local) — le point d'extension est prêt et documenté plutôt que simulé.

## 7. Gestion des erreurs — déjà en place, résumée ici

| Type d'erreur | Comportement actuel | Où dans le code |
|---|---|---|
| Clé API manquante (NewsAPI/NewsData.io) | Tâche en succès, source ignorée, avertissement journalisé | `extract_newsapi`/`extract_newsdata` dans le DAG, `NewsAPIConfigError`/`NewsDataConfigError` |
| Quota API dépassé (429) | Attente + jusqu'à 3 tentatives avant abandon propre | `newsapi_extractor.fetch`, `newsdata_extractor.fetch` |
| Lien mort / site bloquant le scraping | Publication conservée sans image, écart journalisé | `fakenewsnet_extractor._enrich_image` |
| `robots.txt` interdisant l'accès | Enrichissement ignoré pour cette page uniquement | `common/robots.py` |
| Échec réseau générique sur une source | Isolée : les autres tâches d'extraction continuent | Un `try/except` par tâche dans le DAG (et par source dans `main.py` en usage hors-Airflow) |
| Échec de la tâche de chargement | Retry automatique (1×, délai 2 min), puis alerte (§6) | `default_args` du DAG |

## 8. Journalisation

- **Format** : horodatage, niveau, module, message (`%(asctime)s | %(levelname)-8s | %(name)s | %(message)s`), identique sur les étapes 2 et 3 (`logger_setup.py`).
- **Emplacement** : fichier local par étape (`logs/*.log`) + console + (étape 4) UI Airflow, qui centralise aussi les logs de tâche par exécution.
- **Rétention** : aucune purge automatique configurée à ce stade (volumétrie faible en usage local) — à revoir si le pipeline tourne en continu sur une longue période (ex. rotation via `logging.handlers.RotatingFileHandler`, ou rétention Airflow standard si déployé au-delà du poste local).

## 9. Responsabilité

- **Owner du pipeline** : `checkitai` (déclaré comme `owner` dans le DAG) — Christophe Ringot (christophe.ringot1996@gmail.com) pour ce projet.
- **Déclenchement** : automatique (`@daily`) ou manuel (`airflow dags trigger checkitai_etl`).
- **Revue des KPI** : manuelle, via `streamlit run dashboard.py`, à effectuer après chaque run significatif ou au minimum chaque semaine.

## 10. Cohérence avec les automatisations existantes — checklist

- [x] La fréquence annoncée (§4) correspond au `schedule="@daily"` réellement configuré dans le DAG.
- [x] Les seuils d'alerte (§5) portent sur des champs qui existent réellement dans `dataset_clean.jsonl`/`manifest.json` (pas de métrique fictive).
- [x] Le mécanisme d'alerte (§6) est le `on_failure_callback` réellement présent dans le DAG, testé en conditions réelles (voir README de l'étape 4).
- [x] La gestion des erreurs décrite (§7) correspond au code effectif de chaque extracteur (étape 2), pas à un comportement souhaité mais non implémenté.
- [x] Les quotas API (§3, §5) reprennent les valeurs documentées dans le rapport d'exploration (étape 1) : 100 requêtes/jour NewsAPI, 200 crédits/jour NewsData.io.
