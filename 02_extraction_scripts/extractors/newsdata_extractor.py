import time
import uuid
from typing import Optional
from urllib.parse import urlparse
import requests
import config
from common.schema import Publication
from common.storage import download_image
from common.text_clean import clean_text
from logger_setup import get_logger

logger = get_logger(__name__)

MAX_RETRIES = 3

class NewsDataConfigError(RuntimeError):
    """Levée quand la clé NEWSDATA_KEY est absente."""


def connect() -> requests.Session:
    if not config.NEWSDATA_KEY:
        raise NewsDataConfigError(
            "NEWSDATA_KEY manquante : définissez-la dans le fichier .env"
        )
    session = requests.Session()
    session.headers.update({"User-Agent": config.USER_AGENT})
    return session


def fetch(session: requests.Session, query: str, language: str, page_token: Optional[str] = None) -> dict:
    params = {"apikey": config.NEWSDATA_KEY, "language": language}
    if query:
        params["q"] = query
    if page_token:
        params["page"] = page_token

    for attempt in range(1, MAX_RETRIES + 1):
        response = session.get(config.NEWSDATA_BASE_URL, params=params, timeout=config.REQUEST_TIMEOUT)

        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 5))
            logger.warning("Quota NewsData.io atteint (tentative {attempt}/{MAX_RETRIES}), attente {wait}s")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError("Quota NewsData.io toujours dépassé après plusieurs tentatives")


def parse(raw_response: dict) -> list[dict]:
    return raw_response.get("results", [])


def clean(article: dict, language: str) -> Optional[Publication]:
    text = clean_text(f"{article.get('title', '')}. {article.get('description', '')}")
    source_url = article.get("link")
    if not text or not source_url:
        return None

    return Publication(
        id=str(uuid.uuid4()),
        text=text,
        published_at=article.get("pubDate"),
        source_url=source_url,
        source_domain=urlparse(source_url).netloc,
        source_name=article.get("source_id", "unknown"),
        language=language,
        collection_method="api",
        image_url=article.get("image_url"),
    )


def run(query: str = "", language: str = config.DEFAULT_LANGUAGE, limit: int = 10) -> list[dict]:
    session = connect()

    articles: list[dict] = []
    page_token = None
    while len(articles) < limit:
        raw_response = fetch(session, query=query, language=language, page_token=page_token)
        page_articles = parse(raw_response)
        if not page_articles:
            break
        articles.extend(page_articles)

        page_token = raw_response.get("nextPage")
        if not page_token:
            break

    articles = articles[:limit]

    publications = []
    for article in articles:
        publication = clean(article, language)
        if publication is None:
            logger.debug(f"Article ignoré (texte ou lien manquant) : {article.get('link')}")
            continue

        publication.image_path = download_image(publication.image_url, publication.id, session)
        publications.append(publication.to_dict())

    logger.info(f"NewsData.io : {len(publications)}/{len(articles)} article(s) exploitable(s)")
    return publications
