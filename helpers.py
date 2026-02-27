import json
import re
from pathlib import Path

CHECKPOINT_FILE = "output/checkpoint.json"

def extract_listing_id(url: str) -> str:
    match = re.search(r"/rooms/(\d+)", url)
    return match.group(1) if match else url

def load_checkpoint() -> dict:
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)

def load_urls(path: str) -> list[str]:
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]
