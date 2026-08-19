import csv
import io
import time
import uuid
from typing import Optional
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import config
from common.robots import can_fetch
from common.schema import Publication
from common.storage import download_image
from common.text_clean import clean_text
from logger_setup import get_logger

logger = get_logger(__name__)

csv.field_size_limit(10_000_000)

def connect() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": config.USER_AGENT})
    return session


def fetch_csv(session: requests.Session, csv_url: str) -> list[dict]:
    response = session.get(csv_url, timeout=config.REQUEST_TIMEOUT)
    response.raise_for_status()
    return list(csv.DictReader(io.StringIO(response.text)))


def parse(rows: list[dict]) -> list[dict]:
    return [row for row in rows if row.get("news_url") and row.get("title")]


def _enrich_image(session: requests.Session, article_url: str) -> Optional[str]:
    if not article_url.startswith(("http://", "https://")):
        article_url = f"http://{article_url}"

    if not can_fetch(article_url):
        logger.info(f"robots.txt interdit l'accès à {article_url}, enrichissement ignoré")
        return None

    try:
        response = session.get(article_url, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.debug(f"Article inaccessible ({article_url}) : {exc}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    og_image = soup.find("meta", property="og:image")
    if not og_image or not og_image.get("content"):
        return None
    return urljoin(response.url, og_image["content"])


def clean(row: dict, category: str, verdict: str, session: requests.Session, enrich: bool) -> Optional[Publication]:
    text = clean_text(row["title"])
    source_url = row["news_url"]
    if not text or not source_url:
        return None

    publication_id = row.get("id") or str(uuid.uuid4())
    domain = urlparse(source_url if source_url.startswith("http") else f"http://{source_url}").netloc

    publication = Publication(
        id=publication_id,
        text=text,
        published_at=None, 
        source_url=source_url,
        source_domain=domain,
        source_name=category,
        language="en",
        collection_method="dataset",
        label="fake" if verdict == "fake" else "real",
        label_origin="factcheck",
    )

    if enrich:
        publication.image_url = _enrich_image(session, source_url)

    return publication


def run(limit_per_category: int = 10, enrich_images: bool = True) -> list[dict]:
    session = connect()
    publications = []

    for (category, verdict), csv_url in config.FAKENEWSNET_CSV_URLS.items():
        try:
            rows = fetch_csv(session, csv_url)
        except requests.RequestException as exc:
            logger.error(f"Impossible de récupérer {csv_url} : {exc}")
            continue

        rows = parse(rows)[:limit_per_category]
        logger.info(f"FakeNewsNet {category}/{verdict} : {len(rows)} ligne(s) à traiter",)

        images_trouvees = 0
        for row in rows:
            publication = clean(row, category, verdict, session, enrich_images)
            if publication is None:
                continue

            if publication.image_url:
                publication.image_path = download_image(publication.image_url, publication.id, session)
                if publication.image_path:
                    images_trouvees += 1

            publications.append(publication.to_dict())
            if enrich_images:
                time.sleep(config.MIN_DELAY_BETWEEN_REQUESTS)

        logger.info(
            f"FakeNewsNet {category}/{verdict} : {len(rows)} publication(s) retenue(s), {images_trouvees} avec image")

    logger.info(f"FakeNewsNet : {len(publications)} publication(s) au total")
    return publications
