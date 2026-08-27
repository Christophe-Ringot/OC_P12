import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "03_transformation_pipeline" / "data" / "processed"
IMAGES_DIR = BASE_DIR / "02_extraction_scripts" / "data" / "images"
BENCHMARK_FILE = BASE_DIR / "04_airflow_pipeline" / "benchmark_result.json"

# Quotas gratuits documentés dans le rapport d'exploration (étape 1)
NEWSAPI_DAILY_QUOTA = 100
NEWSDATA_DAILY_CREDITS = 200

TASK_LABELS = {
    "extract_rss": "Extraction RSS",
    "extract_newsapi": "Extraction NewsAPI",
    "extract_newsdata": "Extraction NewsData.io",
    "extract_fakenewsnet": "Extraction FakeNewsNet",
    "transform": "Transformation",
}

st.set_page_config(page_title="Suivi ETL CheckItAI", page_icon="📊", layout="wide")


@st.cache_data
def load_manifest() -> dict | None:
    path = PROCESSED_DIR / "manifest.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


@st.cache_data
def load_clean_dataset() -> pd.DataFrame:
    path = PROCESSED_DIR / "dataset_clean.jsonl"
    if not path.exists():
        return pd.DataFrame()
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return pd.DataFrame(rows)


@st.cache_data
def load_benchmark() -> dict | None:
    return json.loads(BENCHMARK_FILE.read_text(encoding="utf-8")) if BENCHMARK_FILE.exists() else None


def images_folder_size_mb() -> float:
    if not IMAGES_DIR.exists():
        return 0.0
    return sum(f.stat().st_size for f in IMAGES_DIR.glob("*") if f.is_file()) / (1024 * 1024)


manifest = load_manifest()
df = load_clean_dataset()
benchmark = load_benchmark()

st.title("Suivi du pipeline ETL — CheckItAI")
st.caption("Extraction (étape 2) → Transformation (étape 3) → Chargement Airflow (étape 4). Données lues directement depuis les fichiers produits par les runs réels du pipeline.")

if manifest is None or df.empty:
    st.warning(
        "Aucune donnée trouvée dans `03_transformation_pipeline/data/processed/`. "
        "Lancez au moins un run des étapes 2 et 3 avant d'ouvrir ce tableau de bord."
    )
    st.stop()

# --------------------------------------------------------------------------- #
# 1. Précision des données
# --------------------------------------------------------------------------- #
st.header("1. Précision des données")
st.caption("Est-ce que le pipeline produit des données propres et exploitables pour entraîner le détecteur de fake news ?")

input_count = manifest["input_count"]
output_count = manifest["output_count"]
exploitability_rate = output_count / input_count if input_count else 0
image_valid_rate = df["image_valid"].sum() / df["has_image"].sum() if df["has_image"].sum() else 0
alignment_ok_rate = df["text_image_aligned"].mean()
labeled_rate = (df["label"] != "unlabeled").mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Taux d'exploitabilité", f"{exploitability_rate:.0%}", help="Publications propres retenues / publications brutes ingérées")
col2.metric("Images valides", f"{image_valid_rate:.0%}", help="Parmi les publications qui annonçaient une image")
col3.metric("Association texte-image correcte", f"{alignment_ok_rate:.0%}", help="L'image sauvegardée correspond bien à la bonne publication")
col4.metric("Publications labellisées", f"{labeled_rate:.0%}", help="Label vrai/faux connu (vs 'non labellisé')")

if alignment_ok_rate < 1.0:
    st.error("⚠️ Des images sont mal associées à leur publication — à corriger avant tout entraînement de modèle.")
else:
    st.success("✅ Aucune anomalie d'association texte-image détectée sur ce jeu de données.")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Répartition des labels")
    label_counts = df["label"].value_counts().reset_index()
    label_counts.columns = ["label", "publications"]
    donut = (
        alt.Chart(label_counts)
        .mark_arc(innerRadius=70)
        .encode(
            theta="publications",
            color=alt.Color(
                "label",
                scale=alt.Scale(domain=["real", "fake", "unlabeled"], range=["#2ca02c", "#d62728", "#9e9e9e"]),
                legend=alt.Legend(title="Label"),
            ),
            tooltip=["label", "publications"],
        )
    )
    st.altair_chart(donut, use_container_width=True)

with chart_col2:
    st.subheader("Publications par type de source")
    source_counts = df["source_type"].value_counts().reset_index()
    source_counts.columns = ["source_type", "publications"]
    bar = (
        alt.Chart(source_counts)
        .mark_bar()
        .encode(x="publications", y=alt.Y("source_type", sort="-x"), tooltip=["source_type", "publications"])
    )
    st.altair_chart(bar, use_container_width=True)

with st.expander("Détail des rejets et doublons de la dernière transformation"):
    rejets = manifest["rejets"]
    st.write(
        f"- **{input_count}** publications brutes en entrée\n"
        f"- **{manifest['doublons_supprimes']}** doublons supprimés (textes identiques venus de plusieurs sources)\n"
        f"- **{rejets['champs_obligatoires_manquants']}** rejetées pour champ obligatoire manquant\n"
        f"- **{rejets['texte_vide_apres_nettoyage']}** rejetées pour texte vide après nettoyage\n"
        f"- **{output_count}** publications propres en sortie"
    )

# --------------------------------------------------------------------------- #
# 2. Rapidité
# --------------------------------------------------------------------------- #
st.header("2. Rapidité")
st.caption("Combien de temps prend chaque étape du pipeline ?")

if benchmark is None:
    st.info("Aucune mesure de temps disponible (`04_airflow_pipeline/benchmark_result.json` absent).")
else:
    total_time = benchmark["total"]
    total_records = benchmark["input_count"]
    throughput = total_records / total_time if total_time else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Temps total du run mesuré", f"{total_time:.1f} s")
    col2.metric("Publications traitées", total_records)
    col3.metric("Débit", f"{throughput:.1f} pub/s")

    timings = [(TASK_LABELS.get(k, k), v) for k, v in benchmark.items() if k in TASK_LABELS]
    timings_df = pd.DataFrame(timings, columns=["tâche", "secondes"])
    bar = (
        alt.Chart(timings_df)
        .mark_bar()
        .encode(x="secondes", y=alt.Y("tâche", sort="-x"), tooltip=["tâche", "secondes"])
    )
    st.altair_chart(bar, use_container_width=True)
    st.caption(
        "Mesuré directement sur les fonctions Python des étapes 2 et 3 (limite basse de test, "
        "sans enrichissement d'image FakeNewsNet). Un run planifié à plus grande échelle "
        "prendra davantage de temps, en particulier si l'enrichissement d'image est activé "
        "(délai de courtoisie d'1s par requête vers des sites tiers, cf. étape 2)."
    )

# --------------------------------------------------------------------------- #
# 3. Coût (ressources)
# --------------------------------------------------------------------------- #
st.header("3. Coût (ressources consommées)")
st.caption("Combien ce pipeline consomme-t-il de quota API et de stockage disque ?")

col1, col2, col3 = st.columns(3)
col1.metric("Images stockées", f"{len(list(IMAGES_DIR.glob('*'))) if IMAGES_DIR.exists() else 0}")
col2.metric("Volume d'images sur disque", f"{images_folder_size_mb():.0f} Mo")
col3.metric("Taille du dataset propre", f"{(PROCESSED_DIR / 'dataset_clean.jsonl').stat().st_size / 1024:.0f} Ko" if (PROCESSED_DIR / "dataset_clean.jsonl").exists() else "n/a")

st.subheader("Quota API utilisé (par run)")
quota_df = pd.DataFrame(
    [
        {"API": "NewsAPI.org", "utilisé": 1, "quota_jour": NEWSAPI_DAILY_QUOTA},
        {"API": "NewsData.io", "utilisé": 1, "quota_jour": NEWSDATA_DAILY_CREDITS},
    ]
)
quota_df["% du quota journalier"] = quota_df["utilisé"] / quota_df["quota_jour"] * 100
bar = (
    alt.Chart(quota_df)
    .mark_bar(color="#1f77b4")
    .encode(x=alt.X("% du quota journalier", scale=alt.Scale(domain=[0, 100])), y="API", tooltip=["API", "utilisé", "quota_jour"])
)
st.altair_chart(bar, use_container_width=True)
st.caption(
    "1 appel = 1 exécution de la tâche d'extraction (indépendamment du nombre d'articles "
    "récupérés dans cet appel). Avec un `schedule=@daily` (étape 4), le pipeline ne consomme "
    "donc qu'une fraction infime du quota gratuit journalier de chaque API."
)

st.divider()
st.caption("Sources des données : 03_transformation_pipeline/data/processed/, 04_airflow_pipeline/benchmark_result.json, 02_extraction_scripts/data/images/. Voir plan_monitoring.md pour la stratégie de surveillance en production.")
