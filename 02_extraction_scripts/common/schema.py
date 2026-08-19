from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class Publication:
    id: str
    text: str
    published_at: Optional[str]
    source_url: str
    source_domain: str
    source_name: str
    language: str
    collection_method: str
    label: str = "unlabeled"
    label_origin: str = "none"
    image_url: Optional[str] = None
    image_path: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)
