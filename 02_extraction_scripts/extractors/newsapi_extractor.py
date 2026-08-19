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


class NewsAPIConfigError(RuntimeError):
    """Levée quand la clé NEWSAPI_KEY est absente ou invalide."""


def connect() -> requests.Session:
    if not config.NEWSAPI_KEY:
        raise NewsAPIConfigError(
            "NEWSAPI_KEY manquante : définissez-la dans le fichier .env (voir .env.example)"
        )
    session = requests.Session()
    session.headers.update({"X-Api-Key": config.NEWSAPI_KEY, "User-Agent": config.USER_AGENT})
    return session


def fetch(session: requests.Session, query: str, language: str, page_size: int) -> dict:
    if query:
        url = config.NEWSAPI_EVERYTHING_URL
        params = {"q": query, "language": language, "pageSize": page_size, "sortBy": "publishedAt"}
    else:
        url = config.NEWSAPI_TOP_HEADLINES_URL
        params = {"country": config.DEFAULT_COUNTRY, "pageSize": page_size}

    for attempt in range(1, MAX_RETRIES + 1):
        response = session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)

        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 5))
            logger.warning(f"Quota NewsAPI atteint (tentative {attempt}/{MAX_RETRIES}), attente {wait}s")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError("Quota NewsAPI toujours dépassé après plusieurs tentatives")


def parse(raw_response: dict) -> list[dict]:
    return raw_response.get("articles", [])


def clean(article: dict, language: str) -> Optional[Publication]:
    text = clean_text(f"{article.get('title', '')}. {article.get('description', '')}")
    source_url = article.get("url")
    if not text or not source_url:
        return None

    return Publication(
        id=str(uuid.uuid4()),
        text=text,
        published_at=article.get("publishedAt"),
        source_url=source_url,
        source_domain=urlparse(source_url).netloc,
        source_name=(article.get("source") or {}).get("name", "unknown"),
        language=language,
        collection_method="api",
        image_url=article.get("urlToImage"),
    )


def run(query: str = "", language: str = config.DEFAULT_LANGUAGE, limit: int = 20) -> list[dict]:
    session = connect()
    raw_response = fetch(session, query=query, language=language, page_size=limit)
    articles = parse(raw_response)

    publications = []
    for article in articles:
        publication = clean(article, language)
        if publication is None:
            logger.debug(f"Article ignoré (texte ou URL manquant) : {article.get('url')}")
            continue

        publication.image_path = download_image(publication.image_url, publication.id, session)
        publications.append(publication.to_dict())

    logger.info("NewsAPI : {len(publications)}/{len(articles)} article(s) exploitable(s)")
    return publications
