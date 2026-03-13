"""g-Harmony configuration."""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

# HuggingFace logging (secrets stay as env vars)
HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_LOG_REPO_ID = os.getenv("HF_LOG_REPO_ID", "")
HF_LOG_EVERY_MINUTES = int(os.getenv("HF_LOG_EVERY_MINUTES", "10"))

# ELO settings
DEFAULT_ELO = 1500
ELO_K_FACTOR = 32

# Load dataset config from YAML
_config_path = Path(__file__).resolve().parent.parent / "dataset_config.yaml"
with open(_config_path) as _f:
    _dataset_config = yaml.safe_load(_f)

DATASET_ID = _dataset_config["dataset_id"]
DATASET_CONFIG = _dataset_config.get("config", "default")
DATASET_SPLIT = _dataset_config.get("split", "train")
IMAGE_COLUMN = _dataset_config.get("image_column", "image")
ID_COLUMN = _dataset_config.get("id_column", "id_str")
POOL_SIZE = _dataset_config.get("pool_size", 3000)
MIN_COMPS_PER_ROUND = _dataset_config.get("min_comparisons_per_round", 3)
MAX_COMPS_PER_ROUND = _dataset_config.get("max_comparisons_per_round", 5)
ELIMINATION_FRACTION = _dataset_config.get("elimination_fraction", 0.5)
FINAL_POOL_SIZE = _dataset_config.get("final_pool_size", 100)
IMAGE_CACHE_DIR = _dataset_config.get("image_cache_dir", "cache/images")
IMAGE_CACHE_MAX_BYTES = _dataset_config.get("image_cache_max_bytes", 524288000)
CACHE_PREFETCH_COUNT = _dataset_config.get("cache_prefetch_count", 20)
