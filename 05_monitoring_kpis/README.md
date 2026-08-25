# Étape 5 — KPI et monitoring du pipeline ETL

Deux livrables :

- [dashboard.py](dashboard.py) — tableau de bord Streamlit visualisant les KPI de précision, rapidité et coût du pipeline, à partir des **vraies données** produites par les étapes 2 à 4 (aucun chiffre inventé).
- [plan_monitoring.md](plan_monitoring.md) — stratégie de surveillance en production (seuils d'alerte, gestion des erreurs, fréquence), explicitement vérifiée contre les automatisations réelles du DAG (étape 4).

## Exécution du tableau de bord

```bash
cd 05_monitoring_kpis
python -m venv venv
venv\Scripts\activate          # ou source venv/bin/activate
pip install -r requirements.txt
streamlit run dashboard.py
```

Ouvre `http://localhost:8501`. Le tableau de bord lit directement :
- `03_transformation_pipeline/data/processed/manifest.json` et `dataset_clean.jsonl` (précision)
- `04_airflow_pipeline/benchmark_result.json` (rapidité)
- `02_extraction_scripts/data/images/` (volumétrie de stockage)

Si l'un de ces fichiers n'existe pas encore, relancer les étapes 2 et 3 (et éventuellement le script de benchmark ci-dessous) avant d'ouvrir le tableau de bord.

## Régénérer la mesure de rapidité

`benchmark_result.json` a été généré en chronométrant réellement les fonctions d'extraction et de transformation (pas de simulation) :

```bash
cd 02_extraction_scripts
python -c "
import sys, time, json, os
sys.path.insert(0, os.getcwd())
sys.path.insert(0, '../03_transformation_pipeline')
from pathlib import Path
from extractors import google_news_rss_extractor, newsapi_extractor, newsdata_extractor, fakenewsnet_extractor
import transform as transform_module

timings = {}
def timed(name, fn):
    t0 = time.perf_counter()
    result = fn()
    timings[name] = round(time.perf_counter() - t0, 3)
    return result

raw = []
raw += timed('extract_rss', lambda: google_news_rss_extractor.run(query='climat', language='fr', limit=10))
raw += timed('extract_newsapi', lambda: newsapi_extractor.run(query='climat', language='fr', limit=10))
raw += timed('extract_newsdata', lambda: newsdata_extractor.run(query='climat', language='fr', limit=10))
raw += timed('extract_fakenewsnet', lambda: fakenewsnet_extractor.run(limit_per_category=3, enrich_images=False))

t0 = time.perf_counter()
pubs, rejets = transform_module.traiter(raw, base_dir=Path(os.getcwd()))
pubs, doublons = transform_module.dedupliquer(pubs)
timings['transform'] = round(time.perf_counter() - t0, 3)
timings['total'] = round(sum(v for k, v in timings.items() if isinstance(v, (int, float))), 3)
timings['input_count'] = len(raw)
timings['output_count'] = len(pubs)
timings['rejets'] = rejets
timings['doublons'] = doublons

json.dump(timings, open('../04_airflow_pipeline/benchmark_result.json', 'w'), indent=2)
"
```

## Résultats mesurés (dernier run réel)

- **Précision** : 1129/1170 publications exploitables (96,5 %), 533/546 images valides (97,6 %), 0 anomalie d'association texte-image, 21,5 % de publications labellisées (fake/real).
- **Rapidité** : 3,4 s au total pour 42 publications (run à `--limit` bas, sans enrichissement d'image) — détail par tâche dans le dashboard.
- **Coût** : 785 images stockées (160 Mo), 1 requête API par source et par run (≈1 % du quota gratuit journalier NewsAPI, ≈0,5 % NewsData.io).
