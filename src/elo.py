"""ELO rating system with HuggingFace dataset persistence."""

from __future__ import annotations

import json
import random
import threading
import logging
from pathlib import Path
from itertools import combinations

from huggingface_hub import CommitScheduler, hf_hub_download

from src.config import DEFAULT_ELO, ELO_K_FACTOR, HF_LOG_REPO_ID, HF_TOKEN, HF_LOG_EVERY_MINUTES
from src.galaxy_profiles import GALAXY_IDS

logger = logging.getLogger(__name__)

STATE_DIR = Path("state")
STATE_FILE = STATE_DIR / "elo_state.json"

_lock = threading.Lock()
_elo_ratings: dict[str, float] = {}

# CommitScheduler for pushing state to HF
_state_scheduler = None


def _init_scheduler():
    """Initialize the CommitScheduler for ELO state persistence."""
    global _state_scheduler
    if not HF_LOG_REPO_ID:
        return
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    _state_scheduler = CommitScheduler(
        repo_id=HF_LOG_REPO_ID,
        repo_type="dataset",
        folder_path=STATE_DIR,
        path_in_repo="state",
        every=HF_LOG_EVERY_MINUTES,
        token=HF_TOKEN if HF_TOKEN else None,
    )
    logger.info("ELO state scheduler initialized (repo=%s)", HF_LOG_REPO_ID)


def load_elo_state():
    """Load ELO state from HF dataset, falling back to all-default."""
    global _elo_ratings

    # Try downloading from HF
    if HF_LOG_REPO_ID:
        try:
            local_path = hf_hub_download(
                repo_id=HF_LOG_REPO_ID,
                repo_type="dataset",
                filename="state/elo_state.json",
                token=HF_TOKEN if HF_TOKEN else None,
            )
            with open(local_path) as f:
                raw = json.load(f)
            # Only keep keys that match current galaxy IDs
            _elo_ratings = {k: v for k, v in raw.items() if k in GALAXY_IDS}
            logger.info("Loaded ELO state from HF dataset (%d galaxies)", len(_elo_ratings))
        except Exception as e:
            logger.warning("Could not load ELO state from HF: %s. Starting fresh.", e)
            _elo_ratings = {}

    # Ensure every galaxy has a rating
    for gid in GALAXY_IDS:
        if gid not in _elo_ratings:
            _elo_ratings[gid] = DEFAULT_ELO

    # Initialize scheduler after loading state
    _init_scheduler()

    # Write initial state file so scheduler has something to push
    _save_state()


def _save_state():
    """Write current ELO state to local JSON file."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(_elo_ratings, f, indent=2)


def _expected_score(rating_a: float, rating_b: float) -> float:
    """Calculate expected score for player A."""
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def record_comparison(winner_id: str, loser_id: str) -> dict:
    """Record a comparison result and update ELO ratings.

    Returns dict with before/after ratings for both galaxies.
    """
    with _lock:
        elo_w_before = _elo_ratings.get(winner_id, DEFAULT_ELO)
        elo_l_before = _elo_ratings.get(loser_id, DEFAULT_ELO)

        expected_w = _expected_score(elo_w_before, elo_l_before)
        expected_l = _expected_score(elo_l_before, elo_w_before)

        elo_w_after = elo_w_before + ELO_K_FACTOR * (1.0 - expected_w)
        elo_l_after = elo_l_before + ELO_K_FACTOR * (0.0 - expected_l)

        _elo_ratings[winner_id] = elo_w_after
        _elo_ratings[loser_id] = elo_l_after

        _save_state()

    return {
        "winner_elo_before": elo_w_before,
        "winner_elo_after": elo_w_after,
        "loser_elo_before": elo_l_before,
        "loser_elo_after": elo_l_after,
    }


def get_rating(galaxy_id: str) -> float:
    """Get current ELO rating for a galaxy."""
    with _lock:
        return _elo_ratings.get(galaxy_id, DEFAULT_ELO)


def get_leaderboard() -> list[dict]:
    """Get all galaxies sorted by ELO descending.

    Returns list of {id, elo} dicts.
    """
    with _lock:
        snapshot = dict(_elo_ratings)
    return sorted(
        [{"id": gid, "elo": elo} for gid, elo in snapshot.items()],
        key=lambda x: x["elo"],
        reverse=True,
    )


def select_pair(seen_pairs: set[tuple[str, str]], champion_id: str | None = None) -> tuple[str, str] | None:
    """Select a pair of galaxies for comparison.

    If champion_id is provided, returns (champion_id, challenger_id).
    Otherwise, prefers galaxies with close ELO ratings (70%) or random (30%).
    Skips pairs already seen in this session.
    Returns None if all pairs exhausted.
    """
    if champion_id is not None:
        # King of the hill mode: find challenger for champion
        challenger_candidates = [gid for gid in GALAXY_IDS if gid != champion_id]
        
        # Filter out seen challengers
        available_challengers = [
            gid for gid in challenger_candidates
            if (champion_id, gid) not in seen_pairs and (gid, champion_id) not in seen_pairs
        ]
        
        if not available_challengers:
            return None
        
        if random.random() < 0.3:
            # Pure random challenger
            challenger = random.choice(available_challengers)
        else:
            # Prefer challenger with close ELO to champion
            with _lock:
                champion_elo = _elo_ratings.get(champion_id, DEFAULT_ELO)
                rated_challengers = [
                    (gid, abs(_elo_ratings.get(gid, DEFAULT_ELO) - champion_elo))
                    for gid in available_challengers
                ]
            rated_challengers.sort(key=lambda x: x[1])
            # Pick from top 20% closest
            top_n = max(1, len(rated_challengers) // 5)
            challenger = random.choice(rated_challengers[:top_n])[0]
        
        return (champion_id, challenger)
    
    # Original random pair selection logic
    all_pairs = list(combinations(GALAXY_IDS, 2))
    # Normalize pair ordering for consistent comparison
    available = [
        p for p in all_pairs
        if (p[0], p[1]) not in seen_pairs and (p[1], p[0]) not in seen_pairs
    ]
    if not available:
        return None

    if random.random() < 0.3:
        # Pure random
        pair = random.choice(available)
    else:
        # Prefer close ELO ratings
        with _lock:
            rated = [(p, abs(_elo_ratings.get(p[0], DEFAULT_ELO) - _elo_ratings.get(p[1], DEFAULT_ELO)))
                     for p in available]
        rated.sort(key=lambda x: x[1])
        # Pick from top 20% closest
        top_n = max(1, len(rated) // 5)
        pair = random.choice(rated[:top_n])[0]

    # Randomize left/right for initial random pairs
    if random.random() < 0.5:
        return (pair[1], pair[0])
    return pair
