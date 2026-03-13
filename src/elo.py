"""ELO rating system for a persistent galaxy ranking."""

from __future__ import annotations

import json
import random
import threading
import logging
from pathlib import Path

from huggingface_hub import CommitScheduler, hf_hub_download

from src.config import (
    DATASET_ID,
    DEFAULT_ELO,
    ELO_K_FACTOR,
    HF_LOG_EVERY_MINUTES,
    HF_LOG_REPO_ID,
    HF_TOKEN,
)

# Imported lazily to avoid circular import at module load time
def _get_display_name(row_index: int) -> str:
    try:
        from src.galaxy_profiles import get_display_name
        return get_display_name(row_index)
    except Exception:
        return str(row_index)

logger = logging.getLogger(__name__)

STATE_DIR = Path("state")
STATE_FILE = STATE_DIR / "elo_state.json"

_lock = threading.Lock()
_state: EloState | None = None
_state_scheduler = None


class EloState:
    """ELO ratings for a fixed pool of galaxies."""

    def __init__(
        self,
        pool: list[int],
        elo_ratings: dict[int, float] | None = None,
        total_comparisons: int = 0,
        dataset_id: str = "",
    ):
        self.pool = list(pool)
        self.elo_ratings = elo_ratings or {idx: DEFAULT_ELO for idx in pool}
        self.total_comparisons = total_comparisons
        self.dataset_id = dataset_id

    def to_dict(self) -> dict:
        named_elo = {
            _get_display_name(idx): self.elo_ratings.get(idx, DEFAULT_ELO)
            for idx in self.pool
        }
        rankings = sorted(
            [{"galaxy_id": gid, "elo": elo} for gid, elo in named_elo.items()],
            key=lambda x: x["elo"],
            reverse=True,
        )
        return {
            "pool": [_get_display_name(idx) for idx in self.pool],
            "elo_ratings": named_elo,
            "total_comparisons": self.total_comparisons,
            "dataset_id": self.dataset_id,
            "rankings": rankings,
        }

    @classmethod
    def from_dict(cls, d: dict, id_to_row: dict[str, int] | None = None) -> EloState:
        """Restore from a saved dict.

        If *id_to_row* is provided (display-name → row-index map), pool entries
        and elo_ratings keys are treated as display names and converted back to
        row indices.  Entries that have no mapping are silently dropped.
        """
        if id_to_row is not None:
            pool = [id_to_row[gid] for gid in d["pool"] if gid in id_to_row]
            elo_ratings = {
                id_to_row[gid]: v
                for gid, v in d["elo_ratings"].items()
                if gid in id_to_row
            }
        else:
            pool = d["pool"]
            elo_ratings = {int(k): v for k, v in d["elo_ratings"].items()}
        return cls(
            pool=pool,
            elo_ratings=elo_ratings,
            total_comparisons=d.get("total_comparisons", 0),
            dataset_id=d.get("dataset_id", ""),
        )


def _init_scheduler():
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


def initialize_elo(pool_indices: list[int]):
    """Create fresh ELO state for the given pool."""
    global _state
    with _lock:
        _state = EloState(pool=pool_indices, dataset_id=DATASET_ID)
    _save_state()
    _init_scheduler()
    logger.info("ELO state initialized with %d galaxies", len(pool_indices))


def load_elo_state() -> bool:
    """Try to restore ELO state from HF Hub or local file.

    Discards saved state if it belongs to a different dataset.
    Returns True if state was loaded, False if starting fresh.
    """
    global _state

    raw = None

    if HF_LOG_REPO_ID:
        try:
            local_path = hf_hub_download(
                repo_id=HF_LOG_REPO_ID,
                repo_type="dataset",
                filename="state/elo_state.json",
                token=HF_TOKEN if HF_TOKEN else None,
                force_download=True,
            )
            with open(local_path) as f:
                raw = json.load(f)
            logger.info("Loaded state from HF Hub")
        except Exception as e:
            logger.warning("Could not load state from HF: %s", e)

    if raw is None and STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                raw = json.load(f)
            logger.info("Loaded state from local file")
        except Exception as e:
            logger.warning("Could not load local state: %s", e)

    if raw is None:
        return False

    # Validate dataset match
    saved_dataset = raw.get("dataset_id", "")
    if saved_dataset and saved_dataset != DATASET_ID:
        logger.info(
            "Saved state is for dataset '%s', current is '%s' — starting fresh",
            saved_dataset,
            DATASET_ID,
        )
        return False

    # Must have 'pool' key (new format); ignore old tournament-format files
    if "pool" not in raw:
        logger.info("Saved state is old format — starting fresh")
        return False

    # Build reverse map: display name → row index (requires metadata to be loaded first)
    id_to_row: dict[str, int] | None = None
    pool_sample = raw["pool"]
    if pool_sample and isinstance(pool_sample[0], str):
        # New format: pool contains display names — reverse-map via metadata cache
        from src.galaxy_profiles import get_row_index_by_id
        id_to_row = {}
        for gid in raw["pool"]:
            row = get_row_index_by_id(gid)
            if row is not None:
                id_to_row[gid] = row

    with _lock:
        _state = EloState.from_dict(raw, id_to_row=id_to_row)
    _init_scheduler()
    _save_state()
    logger.info("Restored ELO state: %d galaxies, %d comparisons",
                len(_state.pool), _state.total_comparisons)
    return True


def _save_state():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with _lock:
        if _state is None:
            return
        data = _state.to_dict()
    if _state_scheduler is not None:
        with _state_scheduler.lock:
            with open(STATE_FILE, "w") as f:
                json.dump(data, f, indent=2)
    else:
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)


def _expected_score(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def record_comparison(winner_idx: int, loser_idx: int) -> dict:
    """Record a comparison and update ELO ratings."""
    with _lock:
        if _state is None:
            raise RuntimeError("ELO state not initialized")

        elo_w_before = _state.elo_ratings.get(winner_idx, DEFAULT_ELO)
        elo_l_before = _state.elo_ratings.get(loser_idx, DEFAULT_ELO)

        expected_w = _expected_score(elo_w_before, elo_l_before)
        expected_l = _expected_score(elo_l_before, elo_w_before)

        elo_w_after = elo_w_before + ELO_K_FACTOR * (1.0 - expected_w)
        elo_l_after = elo_l_before + ELO_K_FACTOR * (0.0 - expected_l)

        _state.elo_ratings[winner_idx] = elo_w_after
        _state.elo_ratings[loser_idx] = elo_l_after
        _state.total_comparisons += 1

    _save_state()

    return {
        "winner_elo_before": elo_w_before,
        "winner_elo_after": elo_w_after,
        "loser_elo_before": elo_l_before,
        "loser_elo_after": elo_l_after,
    }


def select_pair() -> tuple[int, int] | None:
    """Select a pair to compare.

    70% close-ELO matchup, 30% random.
    """
    with _lock:
        if _state is None:
            return None
        pool = list(_state.pool)
        if len(pool) < 2:
            return None

        if random.random() < 0.3:
            pair = random.sample(pool, 2)
        else:
            rated = sorted(pool, key=lambda idx: _state.elo_ratings.get(idx, DEFAULT_ELO))
            start = random.randint(0, len(rated) - 2)
            pair = [rated[start], rated[start + 1]]

    if random.random() < 0.5:
        return (pair[1], pair[0])
    return (pair[0], pair[1])


def get_info() -> dict:
    """Return a snapshot of ELO state for the progress dashboard."""
    with _lock:
        if _state is None:
            return {"pool_size": 0, "total_comparisons": 0, "elo_values": []}
        return {
            "pool_size": len(_state.pool),
            "total_comparisons": _state.total_comparisons,
            "elo_values": [_state.elo_ratings.get(idx, DEFAULT_ELO) for idx in _state.pool],
        }


def get_leaderboard() -> list[dict]:
    """Return top 20 galaxies by ELO descending."""
    with _lock:
        if _state is None:
            return []
        return sorted(
            [{"id": idx, "elo": _state.elo_ratings.get(idx, DEFAULT_ELO)} for idx in _state.pool],
            key=lambda x: x["elo"],
            reverse=True,
        )[:20]


def get_rating(galaxy_idx: int) -> float:
    with _lock:
        if _state is None:
            return DEFAULT_ELO
        return _state.elo_ratings.get(galaxy_idx, DEFAULT_ELO)
