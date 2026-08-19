"""
    python main.py --source all --query "élections" --limit 20
"""
import argparse
import sys
from pathlib import Path

import config
from common.storage import save_jsonl
from extractors import (
    fakeddit_extractor,
    fakenewsnet_extractor,
    google_news_rss_extractor,
    newsapi_extractor,
    newsdata_extractor,
)
from logger_setup import get_logger

logger = get_logger(__name__)

DEFAULT_FAKEDDIT_FIXTURE = config.BASE_DIR / "tests" / "fixtures" / "fakeddit_sample.tsv"

SOURCES = {
    "newsapi": {
        "run": lambda args: newsapi_extractor.run(query=args.query, language=args.language, limit=args.limit),
        "output": config.DATA_DIR / "newsapi.jsonl",
    },
    "newsdata": {
        "run": lambda args: newsdata_extractor.run(query=args.query, language=args.language, limit=args.limit),
        "output": config.DATA_DIR / "newsdata.jsonl",
    },
    "rss": {
        "run": lambda args: google_news_rss_extractor.run(
            query=args.query, language=args.language, country=args.country, limit=args.limit
        ),
        "output": config.DATA_DIR / "google_news_rss.jsonl",
    },
    "fakenewsnet": {
        "run": lambda args: fakenewsnet_extractor.run(limit_per_category=args.limit, enrich_images=True),
        "output": config.DATA_DIR / "fakenewsnet.jsonl",
    },
    "fakeddit": {
        "run": lambda args: _run_fakeddit(args),
        "output": config.DATA_DIR / "fakeddit.jsonl",
    },
}


def _run_fakeddit(args: argparse.Namespace) -> list[dict]:
    if args.fakeddit_path == DEFAULT_FAKEDDIT_FIXTURE:
        logger.warning(
            "Aucun --fakeddit-path fourni : utilisation de la fixture de démonstration "
            f"({DEFAULT_FAKEDDIT_FIXTURE}), pas du vrai dataset Fakeddit. Voir le README pour brancher le vrai TSV."
        )
    return fakeddit_extractor.run(tsv_path=args.fakeddit_path, limit=args.limit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline d'extraction multimodale CheckItAI")
    parser.add_argument("--source", choices=[*SOURCES.keys(), "all"], default="all")
    parser.add_argument("--query", default="", help="mot-clé de recherche (vide = actualité générale)")
    parser.add_argument("--language", default=config.DEFAULT_LANGUAGE)
    parser.add_argument("--country", default=config.DEFAULT_COUNTRY, help="utilisé par la source rss")
    parser.add_argument("--limit", type=int, default=20, help="nombre max de publications par source")
    parser.add_argument(
        "--fakeddit-path",
        type=Path,
        default=DEFAULT_FAKEDDIT_FIXTURE,
        help="chemin vers le TSV Fakeddit réel (défaut : fixture de démonstration)",
    )
    return parser.parse_args()


def run_source(name: str, args: argparse.Namespace) -> int:
    source = SOURCES[name]
    try:
        publications = source["run"](args)
    except Exception:
        logger.exception(f"Échec de la source '{name}', passage à la suivante")
        return 0

    if publications:
        save_jsonl(publications, source["output"])
    return len(publications)


def main() -> int:
    args = parse_args()
    targets = list(SOURCES.keys()) if args.source == "all" else [args.source]

    total = 0
    for name in targets:
        logger.info(f"--- Démarrage de la source '{name}' ---")
        total += run_source(name, args)

    logger.info(f"Extraction terminée : {total} publication(s) au total")
    return 0 if total > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
