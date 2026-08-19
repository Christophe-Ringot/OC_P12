import csv
import uuid
from pathlib import Path
from typing import Optional

import requests

import config
from common.schema import Publication
from common.storage import download_image
from common.text_clean import clean_text
from logger_setup import get_logger

logger = get_logger(__name__)


class FakedditFileError(RuntimeError):
    """Levée quand le fichier TSV Fakeddit est introuvable."""


def connect(tsv_path: Path) -> Path:
    if not tsv_path or not tsv_path.is_file():
        raise FakedditFileError(
            f"Fichier Fakeddit introuvable : {tsv_path}. "
            "Téléchargez le TSV officiel (cf. README) et passez son chemin via --fakeddit-path."
        )
    return tsv_path


def fetch(tsv_path: Path, limit: int) -> list[dict]:
    with open(tsv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        return [row for _, row in zip(range(limit), reader)]


def parse(rows: list[dict]) -> list[dict]:
    return [row for row in rows if row.get("clean_title")]


def clean(row: dict) -> Optional[Publication]:
    text = clean_text(row.get("clean_title", ""))
    subreddit = row.get("subreddit", "")
    submission_id = row.get("id")
    if not text or not submission_id:
        return None

    has_image = row.get("hasImage", "").strip().lower() in ("1", "true")
    image_url = row.get("image_url") or None

    return Publication(
        id=str(uuid.uuid4()),
        text=text,
        published_at=row.get("created_utc"),
        source_url=f"https://www.reddit.com/r/{subreddit}/comments/{submission_id}/",
        source_domain="reddit.com",
        source_name=f"r/{subreddit}" if subreddit else "reddit",
        language="en",
        collection_method="dataset",
        label="fake" if row.get("2_way_label", "").strip() == "1" else "real",
        label_origin="distant_supervision",
        image_url=image_url if has_image else None,
    )


def run(tsv_path: Optional[Path] = None, limit: int = 100) -> list[dict]:
    tsv_path = connect(tsv_path)
    rows = parse(fetch(tsv_path, limit))

    session = requests.Session()
    session.headers.update({"User-Agent": config.USER_AGENT})

    publications = []
    for row in rows:
        publication = clean(row)
        if publication is None:
            logger.debug("Ligne Fakeddit ignorée (titre ou id manquant)")
            continue

        if publication.image_url:
            publication.image_path = download_image(publication.image_url, publication.id, session)

        publications.append(publication.to_dict())

    logger.info(f"Fakeddit : {len(publications)}/{len(rows)} ligne(s) exploitable(s)")
    return publications
