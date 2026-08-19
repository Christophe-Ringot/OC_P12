import uuid
from typing import Optional
from urllib.parse import urlencode, urlparse
import feedparser
import requests
import config
from common.schema import Publication
from common.text_clean import clean_text
from logger_setup import get_logger

logger = get_logger(__name__)


def connect() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": config.USER_AGENT})
    return session


def build_feed_url(query: str, language: str, country: str) -> str:
    params = {"hl": language, "gl": country.upper(), "ceid": f"{country.upper()}:{language}"}
    if query:
        return f"{config.GOOGLE_NEWS_RSS_URL}/search?q={query}&{urlencode(params)}"
    return f"{config.GOOGLE_NEWS_RSS_URL}?{urlencode(params)}"


def fetch(session: requests.Session, feed_url: str) -> feedparser.FeedParserDict:
    response = session.get(feed_url, timeout=config.REQUEST_TIMEOUT)
    response.raise_for_status()
    return feedparser.parse(response.content)


def parse(feed: feedparser.FeedParserDict) -> list[dict]:
    return list(feed.get("entries", []))


def clean(entry: dict, language: str) -> Optional[Publication]:
    text = clean_text(f"{entry.get('title', '')}. {entry.get('summary', '')}")
    google_link = entry.get("link")
    if not text or not google_link:
        return None

    source = entry.get("source", {})
    source_href = source.get("href", "")

    return Publication(
        id=str(uuid.uuid4()),
        text=text,
        published_at=entry.get("published"),
        source_url=google_link,  
        source_domain=urlparse(source_href).netloc or "unknown",
        source_name=source.get("title", "unknown"),
        language=language,
        collection_method="rss",
    )


def run(
    query: str = "",
    language: str = config.DEFAULT_LANGUAGE,
    country: str = config.DEFAULT_COUNTRY,
    limit: int = 20,
) -> list[dict]:
    session = connect()
    feed_url = build_feed_url(query, language, country)
    feed = fetch(session, feed_url)
    entries = parse(feed)[:limit]

    publications = []
    for entry in entries:
        publication = clean(entry, language)
        if publication is None:
            logger.debug("Entrée RSS ignorée (texte ou lien manquant)")
            continue
        publications.append(publication.to_dict())

    logger.info(
        f"Google News RSS : {len(publications)}/{len(entries)} entrée(s) exploitable(s) (texte seul, pas d'image disponible sur cette source)"
    )
    return publications
