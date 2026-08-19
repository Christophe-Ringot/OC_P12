import argparse
import csv
import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

from logger_setup import get_logger

logger = get_logger(__name__)

REQUIRED_FIELDS = ("text", "source_url")
DUPLICATE_SEGMENT_THRESHOLD = 0.75


# Schéma de sortie 
@dataclass
class CleanPublication:
    id: str
    text_clean: str
    text_length: int
    word_count: int
    published_at: Optional[str]
    source_name: str
    source_domain: str
    source_type: str
    language: str
    label: str
    label_origin: str
    image_url: Optional[str]
    image_path: Optional[str]
    has_image: bool
    image_valid: bool
    text_image_aligned: bool
    collection_method: str
    processed_at: str

    def to_dict(self) -> dict:
        return asdict(self)


# 1. Lecture
def lire_jsonl(input_dir: Path, pattern: str = "*.jsonl") -> list[dict]:
    records = []
    files = sorted(input_dir.glob(pattern))
    if not files:
        logger.warning(f"Aucun fichier '{pattern}' trouvé dans {input_dir}")

    for file_path in files:
        count_before = len(records)
        with open(file_path, "r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(f"Ligne {line_number} illisible dans {file_path.name}, ignorée")
                    continue
                record["_source_file"] = file_path.name
                records.append(record)
        logger.info(f"{len(records) - count_before} enregistrement(s) lus depuis {file_path.name}")

    logger.info(f"Lecture terminée : {len(records)} enregistrement(s) au total")
    return records


# 2. Traitement
def _segments(text: str) -> list[str]:
    return [s.strip() for s in text.split(". ") if s.strip()]


def nettoie_texte(text_brut: str) -> str:
    # Normalise les espaces et supprime les segments quasi-dupliqués.
    segments = _segments(text_brut or "")
    if not segments:
        return ""

    deduplique = [segments[0]]
    for segment in segments[1:]:
        similarite = SequenceMatcher(None, deduplique[-1].lower(), segment.lower()).ratio()
        if similarite < DUPLICATE_SEGMENT_THRESHOLD:
            deduplique.append(segment)

    return " ".join(deduplique).strip()


def valide_champs_obligatoires(record: dict) -> bool:
    return all(record.get(champ) for champ in REQUIRED_FIELDS)


def valide_image(record: dict, base_dir: Path) -> tuple[bool, bool]:
    """Retourne (has_image, image_valid) : image annoncée vs. image réellement exploitable."""
    has_image = bool(record.get("image_url"))

    image_path = record.get("image_path")
    if not image_path:
        return has_image, False

    resolved = (base_dir / image_path).resolve()
    image_valid = resolved.is_file() and resolved.stat().st_size > 0
    return has_image, image_valid


def valide_association_texte_image(record: dict, image_valid: bool) -> bool:
    """Vérifie que l'image sauvegardée correspond bien à cette publication (même id)."""
    image_path = record.get("image_path")
    if not image_path:
        return True 

    if not image_valid:
        return False

    return Path(image_path).stem == record.get("id")


def _source_type(collection_method: str) -> str:
    return {
        "api": "editorial_api",
        "rss": "aggregator",
        "dataset": "academic_dataset",
    }.get(collection_method, "unknown")


def genere_colonnes(record: dict, text_clean: str, base_dir: Path) -> CleanPublication:
    has_image, image_valid = valide_image(record, base_dir)
    aligned = valide_association_texte_image(record, image_valid)

    return CleanPublication(
        id=record["id"],
        text_clean=text_clean,
        text_length=len(text_clean),
        word_count=len(text_clean.split()),
        published_at=record.get("published_at"),
        source_name=record.get("source_name", "unknown"),
        source_domain=record.get("source_domain", "unknown"),
        source_type=_source_type(record.get("collection_method", "")),
        language=record.get("language", "unknown"),
        label=record.get("label", "unlabeled"),
        label_origin=record.get("label_origin", "none"),
        image_url=record.get("image_url"),
        image_path=record.get("image_path"),
        has_image=has_image,
        image_valid=image_valid,
        text_image_aligned=aligned,
        collection_method=record.get("collection_method", "unknown"),
        processed_at=datetime.now(timezone.utc).isoformat(),
    )


def traiter(records: list[dict], base_dir: Path) -> tuple[list[CleanPublication], dict]:
    """Applique nettoyage, filtrage, validation image et génération de colonnes."""
    rejets = {"champs_obligatoires_manquants": 0, "texte_vide_apres_nettoyage": 0}
    publications: list[CleanPublication] = []

    for record in records:
        if not valide_champs_obligatoires(record):
            rejets["champs_obligatoires_manquants"] += 1
            logger.debug(f"Rejeté (champ obligatoire manquant) : id={record.get('id')}")
            continue

        text_clean = nettoie_texte(record["text"])
        if not text_clean:
            rejets["texte_vide_apres_nettoyage"] += 1
            logger.debug(f"Rejeté (texte vide après nettoyage) : id={record.get('id')}")
            continue

        publication = genere_colonnes(record, text_clean, base_dir)
        if not publication.text_image_aligned and publication.has_image:
            logger.warning(
                f"Association texte-image incohérente pour id={publication.id} "
                f"(image_path={publication.image_path})"
            )
        publications.append(publication)

    logger.info(
        f"Traitement terminé : {len(publications)} retenue(s), "
        f"{sum(rejets.values())} rejetée(s) ({rejets})"
    )
    return publications, rejets


def dedupliquer(publications: list[CleanPublication]) -> tuple[list[CleanPublication], int]:
    """Supprime les publications dont le texte nettoyé est identique (doublons inter-sources)."""
    vus = set()
    uniques = []
    for publication in publications:
        empreinte = hashlib.sha1(publication.text_clean.lower().encode("utf-8")).hexdigest()
        if empreinte in vus:
            continue
        vus.add(empreinte)
        uniques.append(publication)

    doublons = len(publications) - len(uniques)
    logger.info(f"Déduplication : {doublons} doublon(s) supprimé(s)")
    return uniques, doublons


# 3. Export
def exporter(publications: list[CleanPublication], output_dir: Path, rapport: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = [p.to_dict() for p in publications]

    jsonl_path = output_dir / "dataset_clean.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    logger.info(f"Export JSONL : {len(records)} ligne(s) -> {jsonl_path}")

    csv_path = output_dir / "dataset_clean.csv"
    if records:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)
        logger.info(f"Export CSV : {len(records)} ligne(s) -> {csv_path}")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "output_count": len(records),
        "rejets": rapport["rejets"],
        "doublons_supprimes": rapport["doublons"],
        "input_count": rapport["input_count"],
    }
    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    logger.info(f"Manifest -> {manifest_path}")


def run(input_dir: Path, output_dir: Path, image_root: Path) -> list[CleanPublication]:
    raw_records = lire_jsonl(input_dir)
    publications, rejets = traiter(raw_records, base_dir=image_root)
    publications, doublons = dedupliquer(publications)

    exporter(
        publications,
        output_dir,
        rapport={"rejets": rejets, "doublons": doublons, "input_count": len(raw_records)},
    )
    return publications


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline de transformation CheckItAI")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("../02_extraction_scripts/data"),
        help="dossier contenant les fichiers JSONL bruts de l'étape 2",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    parser.add_argument(
        "--image-root",
        type=Path,
        default=None,
        help="dossier depuis lequel résoudre les 'image_path' relatifs (défaut : parent de --input-dir, "
        "car l'étape 2 y stocke les chemins d'image relatifs à sa racine projet)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input_dir, args.output_dir, args.image_root or args.input_dir.parent)
