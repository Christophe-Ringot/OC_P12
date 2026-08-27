import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


EXTRACTION_DIR = os.environ.get("CHECKITAI_EXTRACTION_DIR", "/opt/airflow/checkitai/02_extraction_scripts")
TRANSFORM_DIR = os.environ.get("CHECKITAI_TRANSFORM_DIR", "/opt/airflow/checkitai/03_transformation_pipeline")

DEFAULT_LANGUAGE = "fr"
DEFAULT_QUERY = "climat"
EXTRACT_LIMIT = 10 

def alert_on_failure(context):
    ti = context["task_instance"]
    logger.error(
        "ALERTE pipeline : la tâche '%s' du DAG '%s' a échoué (run_id=%s) après épuisement des retries.",
        ti.task_id, ti.dag_id, context["run_id"],
    )


default_args = {
    "owner": "checkitai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}


def _use_extraction_modules():
    if EXTRACTION_DIR not in sys.path:
        sys.path.insert(0, EXTRACTION_DIR)


def _use_transform_module():
    """Équivalent de _use_extraction_modules() pour l'étape 3 (voir sa docstring)."""
    if TRANSFORM_DIR not in sys.path:
        sys.path.insert(0, TRANSFORM_DIR)


def extract_rss(**context) -> list[dict]:
    _use_extraction_modules()
    from extractors import google_news_rss_extractor

    query = Variable.get("checkitai_query", default_var=DEFAULT_QUERY)
    return google_news_rss_extractor.run(query=query, language=DEFAULT_LANGUAGE, limit=EXTRACT_LIMIT)


def extract_newsapi(**context) -> list[dict]:
    _use_extraction_modules()
    from extractors import newsapi_extractor

    if not Variable.get("NEWSAPI_KEY", default_var=""):
        logger.warning("NEWSAPI_KEY non définie (Variable Airflow) : source ignorée")
        return []

    import config

    config.NEWSAPI_KEY = Variable.get("NEWSAPI_KEY")
    query = Variable.get("checkitai_query", default_var=DEFAULT_QUERY)
    try:
        return newsapi_extractor.run(query=query, language=DEFAULT_LANGUAGE, limit=EXTRACT_LIMIT)
    except newsapi_extractor.NewsAPIConfigError as exc:
        logger.warning("NewsAPI ignorée : %s", exc)
        return []


def extract_newsdata(**context) -> list[dict]:
    _use_extraction_modules()
    from extractors import newsdata_extractor

    if not Variable.get("NEWSDATA_KEY", default_var=""):
        logger.warning("NEWSDATA_KEY non définie (Variable Airflow) : source ignorée")
        return []

    import config

    config.NEWSDATA_KEY = Variable.get("NEWSDATA_KEY")
    query = Variable.get("checkitai_query", default_var=DEFAULT_QUERY)
    try:
        return newsdata_extractor.run(query=query, language=DEFAULT_LANGUAGE, limit=EXTRACT_LIMIT)
    except newsdata_extractor.NewsDataConfigError as exc:
        logger.warning("NewsData.io ignorée : %s", exc)
        return []


def extract_fakenewsnet(**context) -> list[dict]:
    _use_extraction_modules()
    from extractors import fakenewsnet_extractor

    return fakenewsnet_extractor.run(limit_per_category=3, enrich_images=False)


def transform(**context) -> list[dict]:
    _use_transform_module()
    import transform as transform_module

    ti = context["ti"]
    raw_records: list[dict] = []
    for task_id in ("extract_rss", "extract_newsapi", "extract_newsdata", "extract_fakenewsnet"):
        raw_records.extend(ti.xcom_pull(task_ids=task_id) or [])

    logger.info("%d enregistrement(s) bruts reçus des tâches d'extraction", len(raw_records))

    publications, rejets = transform_module.traiter(raw_records, base_dir=Path(EXTRACTION_DIR))
    publications, doublons = transform_module.dedupliquer(publications)
    logger.info("Après nettoyage : %d publication(s), rejets=%s, doublons=%d", len(publications), rejets, doublons)

    return [p.to_dict() for p in publications]


def _to_timestamp(value):
    if not value:
        return None
    from dateutil import parser as date_parser

    try:
        return date_parser.parse(value)
    except (ValueError, OverflowError):
        logger.warning("Date illisible ignorée : %r", value)
        return None


def load_to_postgres(**context) -> int:
    import psycopg2
    import psycopg2.extras
    from airflow.hooks.base import BaseHook

    ti = context["ti"]
    records: list[dict] = ti.xcom_pull(task_ids="transform") or []
    if not records:
        logger.info("Aucune publication à charger")
        return 0

    conn_info = BaseHook.get_connection("checkitai_postgres")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port or 5432,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )

    try:
        with conn, conn.cursor() as cur:
            sources_by_name = {r["source_name"]: (r["source_name"], r["source_domain"], r["source_type"]) for r in records}
            sources_rows = list(sources_by_name.values())

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO sources (source_name, source_domain, source_type)
                VALUES %s
                ON CONFLICT (source_name) DO UPDATE
                SET source_domain = EXCLUDED.source_domain, source_type = EXCLUDED.source_type
                """,
                sources_rows,
            )

            publication_rows = [
                (
                    r["id"], r["text_clean"], r["text_length"], r["word_count"], _to_timestamp(r["published_at"]),
                    r["source_name"], r["language"], r["label"], r["label_origin"],
                    r["image_url"], r["image_path"], r["has_image"], r["image_valid"],
                    r["text_image_aligned"], r["collection_method"], _to_timestamp(r["processed_at"]),
                )
                for r in records
            ]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO publications (
                    id, text_clean, text_length, word_count, published_at, source_name, language,
                    label, label_origin, image_url, image_path, has_image, image_valid,
                    text_image_aligned, collection_method, processed_at
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    text_clean = EXCLUDED.text_clean, label = EXCLUDED.label,
                    image_valid = EXCLUDED.image_valid, text_image_aligned = EXCLUDED.text_image_aligned,
                    processed_at = EXCLUDED.processed_at
                """,
                publication_rows,
            )
    finally:
        conn.close()

    logger.info("%d publication(s) chargée(s) dans Postgres (checkitai.publications)", len(records))
    return len(records)


with DAG(
    dag_id="checkitai_etl",
    description="ETL multimodal CheckItAI : extraction (5 sources) -> nettoyage -> chargement Postgres",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["checkitai", "etl", "multimodal"],
) as dag:
    t_extract_rss = PythonOperator(task_id="extract_rss", python_callable=extract_rss)
    t_extract_newsapi = PythonOperator(task_id="extract_newsapi", python_callable=extract_newsapi)
    t_extract_newsdata = PythonOperator(task_id="extract_newsdata", python_callable=extract_newsdata)
    t_extract_fakenewsnet = PythonOperator(task_id="extract_fakenewsnet", python_callable=extract_fakenewsnet)

    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    [t_extract_rss, t_extract_newsapi, t_extract_newsdata, t_extract_fakenewsnet] >> t_transform >> t_load
