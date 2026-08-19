from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import config

_cache: dict[str, RobotFileParser] = {}


def _get_parser(domain: str) -> RobotFileParser:
    if domain not in _cache:
        parser = RobotFileParser()
        parser.set_url(f"https://{domain}/robots.txt")
        try:
            parser.read()
        except Exception:
            pass
        _cache[domain] = parser
    return _cache[domain]


def can_fetch(url: str) -> bool:
    domain = urlparse(url).netloc
    if not domain:
        return False
    return _get_parser(domain).can_fetch(config.USER_AGENT, url)
