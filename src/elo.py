"""ELO rating system with successive-halving tournament engine."""

from __future__ import annotations

import json
import math
import random
import threading
import logging
from pathlib import Path

from huggingface_hub import CommitScheduler, hf_hub_download

from src.config import (
    DEFAULT_ELO,
    ELO_K_FACTOR,
    ELIMINATION_FRACTION,
    FINAL_POOL_SIZE,
    HF_LOG_EVERY_MINUTES,
    HF_LOG_REPO_ID,
    HF_TOKEN,
    MIN_COMPS_PER_ROUND,
)

logger = logging.getLogger(__name__)

STATE_DIR = Path("state")
STATE_FILE = STATE_DIR / "elo_state.json"

_lock = threading.Lock()
_state: TournamentState | None = None
_state_scheduler = None


class TournamentState:
    """Full tournament state for successive-halving rounds."""

    def __init__(
        self,
        active_pool: list[int],
        elo_ratings: dict[int, float] | None = None,
        round_comparisons: dict[int, int] | None = None,
        current_round: int = 1,
        eliminated: list[int] | None = None,
        total_comparisons: int = 0,
        tournament_complete: bool = False,
        pool_seed: int | None = None,
    ):
        self.active_pool = list(active_pool)
        self.elo_ratings = elo_ratings or {idx: DEFAULT_ELO for idx in active_pool}
        self.round_comparisons = round_comparisons or {idx: 0 for idx in active_pool}
        self.current_round = current_round
        self.eliminated = eliminated or []
        self.total_comparisons = total_comparisons
        self.tournament_complete = tournament_complete
        self.pool_seed = pool_seed

    def to_dict(self) -> dict:
        return {
            "active_pool": self.active_pool,
            "elo_ratings": {str(k): v for k, v in self.elo_ratings.items()},
            "round_comparisons": {str(k): v for k, v in self.round_comparisons.items()},
            "current_round": self.current_round,
            "eliminated": self.eliminated,
            "total_comparisons": self.total_comparisons,
            "tournament_complete": self.tournament_complete,
            "pool_seed": self.pool_seed,
        }

    @classmethod
    def from_dict(cls, d: dict) -> TournamentState:
        return cls(
            active_pool=d["active_pool"],
            elo_ratings={int(k): v for k, v in d["elo_ratings"].items()},
            round_comparisons={int(k): v for k, v in d["round_comparisons"].items()},
            current_round=d["current_round"],
            eliminated=d.get("eliminated", []),
            total_comparisons=d.get("total_comparisons", 0),
            tournament_complete=d.get("tournament_complete", False),
            pool_seed=d.get("pool_seed"),
        )


def _init_scheduler():
    """Initialize the CommitScheduler for state persistence."""
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


def initialize_tournament(pool_indices: list[int], pool_seed: int | None = None):
    """Create a fresh tournament with the given pool."""
    global _state
    with _lock:
        _state = TournamentState(active_pool=pool_indices, pool_seed=pool_seed)
    _save_state()
    _init_scheduler()
    logger.info("Tournament initialized with %d galaxies", len(pool_indices))


def load_tournament_state() -> bool:
    """Try to restore tournament state from HF or local file.

    Returns True if state was loaded, False if starting fresh.
    """
    global _state

    # Try HF first
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
            # Check if it's the new tournament format
            if "active_pool" in raw:
                with _lock:
                    _state = TournamentState.from_dict(raw)
                _init_scheduler()
                _save_state()
                logger.info(
                    "Loaded tournament state from HF: round %d, %d active galaxies",
                    _state.current_round,
                    len(_state.active_pool),
                )
                return True
            else:
                logger.info("Old-format state found on HF, ignoring")
        except Exception as e:
            logger.warning("Could not load state from HF: %s", e)

    # Try local file
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                raw = json.load(f)
            if "active_pool" in raw:
                with _lock:
                    _state = TournamentState.from_dict(raw)
                _init_scheduler()
                logger.info(
                    "Loaded tournament state from local file: round %d, %d active",
                    _state.current_round,
                    len(_state.active_pool),
                )
                return True
        except Exception as e:
            logger.warning("Could not load local state: %s", e)

    return False


def _save_state():
    """Write current tournament state to local JSON file."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with _lock:
        if _state is None:
            return
        data = _state.to_dict()
    with open(STATE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _expected_score(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def record_comparison(winner_idx: int, loser_idx: int) -> dict:
    """Record a comparison, update ELO, check round completion.

    Returns dict with before/after ratings and round info.
    """
    with _lock:
        if _state is None:
            raise RuntimeError("Tournament not initialized")

        elo_w_before = _state.elo_ratings.get(winner_idx, DEFAULT_ELO)
        elo_l_before = _state.elo_ratings.get(loser_idx, DEFAULT_ELO)

        expected_w = _expected_score(elo_w_before, elo_l_before)
        expected_l = _expected_score(elo_l_before, elo_w_before)

        elo_w_after = elo_w_before + ELO_K_FACTOR * (1.0 - expected_w)
        elo_l_after = elo_l_before + ELO_K_FACTOR * (0.0 - expected_l)

        _state.elo_ratings[winner_idx] = elo_w_after
        _state.elo_ratings[loser_idx] = elo_l_after

        _state.round_comparisons[winner_idx] = _state.round_comparisons.get(winner_idx, 0) + 1
        _state.round_comparisons[loser_idx] = _state.round_comparisons.get(loser_idx, 0) + 1
        _state.total_comparisons += 1

        round_before = _state.current_round
        advanced = _check_and_advance_round()

    _save_state()

    return {
        "winner_elo_before": elo_w_before,
        "winner_elo_after": elo_w_after,
        "loser_elo_before": elo_l_before,
        "loser_elo_after": elo_l_after,
        "round": round_before,
        "round_advanced": advanced,
    }


def _check_and_advance_round() -> bool:
    """Check if all active galaxies have enough comparisons; if so, advance.

    Caller must hold _lock.
    Returns True if a round was advanced.
    """
    if _state is None or _state.tournament_complete:
        return False

    for idx in _state.active_pool:
        if _state.round_comparisons.get(idx, 0) < MIN_COMPS_PER_ROUND:
            return False

    # All galaxies have enough comparisons — advance round
    _advance_round()
    return True


def _advance_round():
    """Eliminate bottom fraction, advance to next round. Caller holds _lock."""
    if _state is None:
        return

    # Sort active pool by ELO descending
    sorted_pool = sorted(
        _state.active_pool,
        key=lambda idx: _state.elo_ratings.get(idx, DEFAULT_ELO),
        reverse=True,
    )

    keep_count = max(
        FINAL_POOL_SIZE,
        int(math.ceil(len(sorted_pool) * (1 - ELIMINATION_FRACTION))),
    )

    survivors = sorted_pool[:keep_count]
    eliminated = sorted_pool[keep_count:]

    _state.eliminated.extend(eliminated)
    _state.active_pool = survivors
    _state.round_comparisons = {idx: 0 for idx in survivors}
    _state.current_round += 1

    if len(survivors) <= FINAL_POOL_SIZE:
        _state.tournament_complete = True
        logger.info("Tournament complete! %d galaxies in final pool.", len(survivors))
    else:
        logger.info(
            "Round %d: %d -> %d galaxies (eliminated %d)",
            _state.current_round - 1,
            len(sorted_pool),
            len(survivors),
            len(eliminated),
        )


def select_pair(seen_pairs: set[tuple[int, int]]) -> tuple[int, int] | None:
    """Swiss-style pair selection within the active pool.

    Prioritizes galaxies that need more comparisons in the current round.
    Returns None if tournament is complete or no pairs available.
    """
    with _lock:
        if _state is None or _state.tournament_complete:
            return None

        pool = list(_state.active_pool)
        if len(pool) < 2:
            return None

        # Prioritize galaxies needing more comparisons
        needs_more = [
            idx for idx in pool
            if _state.round_comparisons.get(idx, 0) < MIN_COMPS_PER_ROUND
        ]

        if not needs_more:
            # All have enough — round should advance soon, but pick a pair anyway
            needs_more = pool

        # Swiss-style: pair galaxies with similar ELO
        if random.random() < 0.3:
            # Pure random for exploration
            if len(needs_more) >= 2:
                pair = random.sample(needs_more, 2)
            else:
                pair = random.sample(pool, 2)
        else:
            # Sort by ELO and pair adjacent
            candidates = needs_more if len(needs_more) >= 2 else pool
            rated = sorted(
                candidates,
                key=lambda idx: _state.elo_ratings.get(idx, DEFAULT_ELO),
            )
            # Pick a random starting point, then take adjacent pair
            if len(rated) >= 2:
                start = random.randint(0, len(rated) - 2)
                pair = [rated[start], rated[start + 1]]
            else:
                pair = random.sample(pool, 2)

        # Check if already seen this session
        if (pair[0], pair[1]) in seen_pairs or (pair[1], pair[0]) in seen_pairs:
            # Try a few more random attempts
            for _ in range(50):
                pair = random.sample(pool, 2)
                if (pair[0], pair[1]) not in seen_pairs and (pair[1], pair[0]) not in seen_pairs:
                    break
            else:
                # All pairs exhausted for this session
                return None

    # Randomize left/right
    if random.random() < 0.5:
        return (pair[1], pair[0])
    return (pair[0], pair[1])


def get_pool_seed() -> int | None:
    """Return the shuffle seed used when the current pool was sampled."""
    with _lock:
        return _state.pool_seed if _state else None


def set_pool_seed(seed: int):
    """Store the pool seed into the current tournament state and save."""
    with _lock:
        if _state is not None:
            _state.pool_seed = seed
    _save_state()


def get_tournament_info() -> dict:
    """Return a snapshot of tournament state for the progress dashboard."""
    with _lock:
        if _state is None:
            return {
                "current_round": 0,
                "pool_size": 0,
                "total_comparisons": 0,
                "tournament_complete": False,
                "elo_values": [],
                "top_indices": [],
                "eliminated_count": 0,
            }

        elo_values = [_state.elo_ratings.get(idx, DEFAULT_ELO) for idx in _state.active_pool]

        # Top 100 by ELO
        sorted_pool = sorted(
            _state.active_pool,
            key=lambda idx: _state.elo_ratings.get(idx, DEFAULT_ELO),
            reverse=True,
        )
        top_indices = sorted_pool[:100]

        # Estimate remaining comparisons
        comps_needed_this_round = sum(
            max(0, MIN_COMPS_PER_ROUND - _state.round_comparisons.get(idx, 0))
            for idx in _state.active_pool
        )
        # Each comparison covers 2 galaxies
        est_remaining_this_round = max(0, comps_needed_this_round // 2)

        return {
            "current_round": _state.current_round,
            "pool_size": len(_state.active_pool),
            "total_comparisons": _state.total_comparisons,
            "tournament_complete": _state.tournament_complete,
            "elo_values": elo_values,
            "top_indices": top_indices,
            "eliminated_count": len(_state.eliminated),
            "est_remaining_this_round": est_remaining_this_round,
        }


def get_leaderboard() -> list[dict]:
    """Get active pool sorted by ELO descending."""
    with _lock:
        if _state is None:
            return []
        return sorted(
            [
                {"id": idx, "elo": _state.elo_ratings.get(idx, DEFAULT_ELO)}
                for idx in _state.active_pool
            ],
            key=lambda x: x["elo"],
            reverse=True,
        )


def get_rating(galaxy_idx: int) -> float:
    """Get current ELO rating for a galaxy."""
    with _lock:
        if _state is None:
            return DEFAULT_ELO
        return _state.elo_ratings.get(galaxy_idx, DEFAULT_ELO)
