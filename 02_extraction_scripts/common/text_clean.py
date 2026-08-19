import re
from bs4 import BeautifulSoup

_WHITESPACE_RE = re.compile(r"\s+")


def strip_html(raw: str) -> str:
    if not raw:
        return ""
    return BeautifulSoup(raw, "html.parser").get_text(separator=" ")


def normalize_whitespace(raw: str) -> str:
    return _WHITESPACE_RE.sub(" ", raw).strip()


def clean_text(raw: str) -> str:
    return normalize_whitespace(strip_html(raw or ""))
