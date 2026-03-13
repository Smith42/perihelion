"""Perihelion configuration."""

import os
from dotenv import load_dotenv

load_dotenv()

# HuggingFace secrets
HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_LOG_REPO_ID = os.getenv("HF_LOG_REPO_ID", "")
HF_LOG_EVERY_MINUTES = int(os.getenv("HF_LOG_EVERY_MINUTES", "10"))

# ELO settings
DEFAULT_ELO = 1500
ELO_K_FACTOR = 32

# Dataset
DATASET_ID = os.getenv("DATASET_ID", "mwalmsley/gz_euclid")
DATASET_CONFIG = os.getenv("DATASET_CONFIG", "default")
DATASET_SPLIT = os.getenv("DATASET_SPLIT", "train")
IMAGE_COLUMN = os.getenv("IMAGE_COLUMN", "image")
ID_COLUMN = os.getenv("ID_COLUMN", "id_str")
POOL_SIZE = int(os.getenv("POOL_SIZE", "5000"))
POOL_SEED = int(os.getenv("POOL_SEED", "42"))
IMAGE_PREFETCH_COUNT = int(os.getenv("IMAGE_PREFETCH_COUNT", "100"))

# Image cache
IMAGE_CACHE_DIR = os.getenv("IMAGE_CACHE_DIR", "cache/images")
IMAGE_CACHE_MAX_BYTES = int(os.getenv("IMAGE_CACHE_MAX_BYTES", str(524288000)))
