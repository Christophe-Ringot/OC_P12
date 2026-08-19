import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
IMAGES_DIR = DATA_DIR / "images"
LOG_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
NEWSAPI_EVERYTHING_URL = "https://newsapi.org/v2/everything"
NEWSAPI_TOP_HEADLINES_URL = "https://newsapi.org/v2/top-headlines"

NEWSDATA_KEY = os.getenv("NEWSDATA_KEY", "")
NEWSDATA_BASE_URL = "https://newsdata.io/api/1/latest"

GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss"

FAKENEWSNET_CSV_URLS = {
    ("politifact", "fake"): "https://raw.githubusercontent.com/KaiDMML/FakeNewsNet/master/dataset/politifact_fake.csv",
    ("politifact", "real"): "https://raw.githubusercontent.com/KaiDMML/FakeNewsNet/master/dataset/politifact_real.csv",
    ("gossipcop", "fake"): "https://raw.githubusercontent.com/KaiDMML/FakeNewsNet/master/dataset/gossipcop_fake.csv",
    ("gossipcop", "real"): "https://raw.githubusercontent.com/KaiDMML/FakeNewsNet/master/dataset/gossipcop_real.csv",
}

USER_AGENT = "CheckItAI-DataPipeline/1.0 (+contact: christophe.ringot1996@gmail.com)"
REQUEST_TIMEOUT = 10  
DEFAULT_LANGUAGE = "fr"
DEFAULT_COUNTRY = "fr"
MIN_DELAY_BETWEEN_REQUESTS = 1.0  
MAX_IMAGE_SIZE_BYTES = 8 * 1024 * 1024 
