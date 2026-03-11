"""Galaxy profiles loaded from HuggingFace datasets."""

import logging
from typing import Dict, List
from .galaxy_data_loader import load_galaxy_data

logger = logging.getLogger(__name__)

# Load galaxy profiles from HuggingFace datasets
_galaxy_data = load_galaxy_data()
GALAXY_PROFILES = _galaxy_data
GALAXY_IDS = list(GALAXY_PROFILES.keys())
NUM_GALAXIES = len(GALAXY_IDS)
TOTAL_PAIRS = NUM_GALAXIES * (NUM_GALAXIES - 1) // 2

logger.info(f"Loaded {NUM_GALAXIES} galaxies from datasets")
logger.info(f"Galaxy IDs: {GALAXY_IDS[:5]}{'...' if len(GALAXY_IDS) > 5 else ''}")
