import json
import mimetypes
from pathlib import Path
from typing import Optional

import requests

import config
from logger_setup import get_logger

logger = get_logger(__name__)

_ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}


def save_jsonl(records: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    logger.info(f"{len(records)} publication(s) écrite(s) dans {output_path}")


def download_image(url: str, publication_id: str, session: requests.Session) -> Optional[str]:
    if not url:
        return None

    try:
        response = session.get(url, timeout=config.REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning(f"Téléchargement image échoué pour {publication_id} : {exc}")
        return None

    content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
    if content_type not in _ALLOWED_IMAGE_TYPES:
        logger.warning(
            f"Image ignorée pour {publication_id} : type de contenu inattendu ({content_type})")
        return None

    content_length = int(response.headers.get("Content-Length", 0))
    if content_length and content_length > config.MAX_IMAGE_SIZE_BYTES:
        logger.warning(f"Image ignorée pour {publication_id} : trop volumineuse ({content_length} octets)")
        return None

    extension = mimetypes.guess_extension(content_type) or ".jpg"
    dest_path = config.IMAGES_DIR / f"{publication_id}{extension}"

    try:
        written = 0
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                written += len(chunk)
                if written > config.MAX_IMAGE_SIZE_BYTES:
                    logger.warning(f"Image ignorée pour {publication_id} : dépasse la taille max en cours de téléchargement")
                    f.close()
                    dest_path.unlink(missing_ok=True)
                    return None
                f.write(chunk)
    except OSError as exc:
        logger.warning(f"Écriture disque échouée pour {publication_id} : {exc}")
        return None

    return str(dest_path.relative_to(config.BASE_DIR))
