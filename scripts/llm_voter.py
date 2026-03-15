# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "datasets",
#     "google-genai",
#     "pillow",
# ]
# ///

"""
Perihelion LLM Voter — automated galaxy interestingness tournament.

Mirrors the human experiment as closely as possible:
- Same images (JPEG-compressed, same as served to humans)
- Same pair selection logic (70% close-ELO, 30% random)
- Same minimal prompt (no morphological metadata, no definition of "interesting")
- Position bias control (randomised left/right, tracked for analysis)
- No confidence scores (humans don't express confidence)

Usage:
    export GOOGLE_API_KEY=...
    uv run scripts/llm_voter.py --pool-size 5000 --comparisons 50000

Output:
    llm_elo_rankings.json     — final ELO ratings
    llm_comparisons.jsonl     — one line per comparison (for analysis)
"""

import argparse
import io
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

from datasets import load_dataset
from google import genai
from PIL import Image
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — match the human app defaults
# ---------------------------------------------------------------------------

DATASET_ID = os.getenv("DATASET_ID", "mwalmsley/gz_euclid")
DATASET_CONFIG = os.getenv("DATASET_CONFIG", "default")
DATASET_SPLIT = os.getenv("DATASET_SPLIT", "train")
IMAGE_COLUMN = os.getenv("IMAGE_COLUMN", "image")
ID_COLUMN = os.getenv("ID_COLUMN", "id_str")

DEFAULT_ELO = 1500.0
K_FACTOR = 32.0

# The prompt humans see is just "Vote for the most interesting galaxy" with
# no elaboration. We keep the LLM prompt equally minimal.
PROMPT_TEMPLATE = (
    "You are a participant in a citizen science experiment called Perihelion.\n"
    "\n"
    "You are shown two galaxy images. Vote for the galaxy you find more interesting.\n"
    "\n"
    "Galaxy A: {id_a}\n"
    "Galaxy B: {id_b}\n"
    "\n"
    'Which is more interesting, A or B? Respond ONLY with JSON on a single line:\n'
    '{{"winner": "A" or "B", "reason": "one sentence"}}'
)

MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# Gemini Flash rate limits — be a good citizen
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.2"))  # seconds between calls


# ---------------------------------------------------------------------------
# Galaxy pool loader
# ---------------------------------------------------------------------------

def load_pool(pool_size: int, seed: int, jpeg_quality: int = 85) -> tuple[list[bytes], list[str]]:
    """Stream galaxies and convert to JPEG bytes (matching human presentation).

    Returns:
        images: list of JPEG bytes, indexed 0..N-1
        ids:    list of id_str display names, same order
    """
    logger.info("Loading %d galaxies (seed=%d)...", pool_size, seed)

    ds = load_dataset(
        DATASET_ID, DATASET_CONFIG, split=DATASET_SPLIT,
        streaming=True, token=os.getenv("HF_TOKEN") or None,
    )
    ds = ds.shuffle(seed=seed, buffer_size=200).take(pool_size)

    images: list[bytes] = []
    ids: list[str] = []

    for row in tqdm(ds, total=pool_size, desc="Loading galaxies"):
        # Convert to JPEG bytes — same as the app's image cache
        img = row[IMAGE_COLUMN]
        if not isinstance(img, Image.Image):
            img = Image.open(io.BytesIO(img["bytes"] if isinstance(img, dict) else img))
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=jpeg_quality)
        images.append(buf.getvalue())

        ids.append(str(row.get(ID_COLUMN, len(ids))))

    logger.info("Loaded %d galaxies", len(images))
    return images, ids


# ---------------------------------------------------------------------------
# ELO logic — identical to src/elo.py
# ---------------------------------------------------------------------------

def expected_score(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def select_pair(pool_size: int, elo: dict[int, float]) -> tuple[int, int]:
    """70% close-ELO, 30% random — same as the human app."""
    if random.random() < 0.3:
        return tuple(random.sample(range(pool_size), 2))
    else:
        rated = sorted(range(pool_size), key=lambda i: elo.get(i, DEFAULT_ELO))
        start = random.randint(0, len(rated) - 2)
        pair = (rated[start], rated[start + 1])
        # Randomise presentation order
        if random.random() < 0.5:
            pair = (pair[1], pair[0])
        return pair


# ---------------------------------------------------------------------------
# LLM comparison
# ---------------------------------------------------------------------------

def compare(
    client: genai.Client,
    img_a: bytes,
    img_b: bytes,
    id_a: str,
    id_b: str,
) -> dict | None:
    """Ask the LLM to pick the more interesting galaxy. Returns parsed result or None."""

    prompt = PROMPT_TEMPLATE.format(id_a=id_a, id_b=id_b)

    # Send JPEG bytes as inline images — same visual as humans see
    pil_a = Image.open(io.BytesIO(img_a))
    pil_b = Image.open(io.BytesIO(img_b))

    try:
        response = client.models.generate_content(
            contents=[pil_a, pil_b, prompt],
            model=MODEL,
            config={"temperature": 1.0},  # variability, like different humans
        )
        text = response.text.strip()
        # Strip markdown fences if present
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        result = json.loads(text)

        if result.get("winner") not in ("A", "B"):
            logger.warning("Invalid winner value: %s", result.get("winner"))
            return None

        return result

    except Exception as e:
        logger.warning("LLM call failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main tournament loop
# ---------------------------------------------------------------------------

def run_tournament(
    pool_size: int,
    n_comparisons: int,
    seed: int,
    output_dir: str,
    checkpoint_every: int = 500,
):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    rankings_file = output_path / "llm_elo_rankings.json"
    log_file = output_path / "llm_comparisons.jsonl"
    checkpoint_file = output_path / "llm_checkpoint.json"

    # Load galaxy pool
    images, ids = load_pool(pool_size, seed)
    actual_pool = len(images)

    # Resume from checkpoint if available
    elo: dict[int, float] = {i: DEFAULT_ELO for i in range(actual_pool)}
    start_comp = 0
    comparisons_log: list[dict] = []

    if checkpoint_file.exists():
        try:
            cp = json.loads(checkpoint_file.read_text())
            elo = {int(k): v for k, v in cp["elo"].items()}
            start_comp = cp["completed"]
            logger.info("Resumed from checkpoint: %d comparisons done", start_comp)
        except Exception as e:
            logger.warning("Could not load checkpoint: %s", e)

    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])

    errors = 0
    max_consecutive_errors = 20

    for comp in tqdm(range(start_comp, n_comparisons), desc="Comparisons", initial=start_comp, total=n_comparisons):
        idx_a, idx_b = select_pair(actual_pool, elo)

        result = compare(client, images[idx_a], images[idx_b], ids[idx_a], ids[idx_b])

        if result is None:
            errors += 1
            if errors >= max_consecutive_errors:
                logger.error("Too many consecutive errors — stopping")
                break
            time.sleep(1)
            continue

        errors = 0  # reset on success

        # Determine winner/loser indices
        winner_idx = idx_a if result["winner"] == "A" else idx_b
        loser_idx = idx_b if result["winner"] == "A" else idx_a

        # ELO update — identical to src/elo.py
        exp_w = expected_score(elo[winner_idx], elo[loser_idx])
        elo[winner_idx] += K_FACTOR * (1.0 - exp_w)
        elo[loser_idx] += K_FACTOR * (0.0 - expected_score(elo[loser_idx], elo[winner_idx]))

        # Log — track position for bias analysis
        log_entry = {
            "comparison": comp,
            "idx_a": idx_a,
            "idx_b": idx_b,
            "id_a": ids[idx_a],
            "id_b": ids[idx_b],
            "winner": result["winner"],  # "A" or "B" (positional)
            "winner_id": ids[winner_idx],
            "reason": result.get("reason", ""),
        }
        comparisons_log.append(log_entry)

        # Append to JSONL log
        with log_file.open("a") as f:
            f.write(json.dumps(log_entry) + "\n")

        # Checkpoint
        if (comp + 1) % checkpoint_every == 0:
            _save_checkpoint(checkpoint_file, elo, comp + 1)
            _save_rankings(rankings_file, elo, ids)
            logger.info(
                "Checkpoint at %d comparisons (top: %s %.0f, bottom: %s %.0f)",
                comp + 1,
                ids[max(elo, key=elo.get)], max(elo.values()),
                ids[min(elo, key=elo.get)], min(elo.values()),
            )

        time.sleep(REQUEST_DELAY)

    # Final save
    _save_checkpoint(checkpoint_file, elo, n_comparisons)
    _save_rankings(rankings_file, elo, ids)

    # Position bias summary
    a_wins = sum(1 for e in comparisons_log if e["winner"] == "A")
    b_wins = sum(1 for e in comparisons_log if e["winner"] == "B")
    total = a_wins + b_wins
    if total > 0:
        logger.info(
            "Position bias: A (first image) won %.1f%%, B won %.1f%% (n=%d)",
            100 * a_wins / total, 100 * b_wins / total, total,
        )

    logger.info("Done! Results in %s", output_path)


def _save_checkpoint(path: Path, elo: dict[int, float], completed: int):
    path.write_text(json.dumps({"elo": elo, "completed": completed}))


def _save_rankings(path: Path, elo: dict[int, float], ids: list[str]):
    rankings = sorted(
        [{"idx": i, "id": ids[i], "elo": round(elo[i], 2)} for i in elo],
        key=lambda x: x["elo"],
        reverse=True,
    )
    path.write_text(json.dumps({
        "rankings": rankings,
        "total_galaxies": len(elo),
        "elo_min": round(min(elo.values()), 2),
        "elo_max": round(max(elo.values()), 2),
        "elo_mean": round(sum(elo.values()) / len(elo), 2),
    }, indent=2))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Perihelion LLM Voter")
    parser.add_argument("--pool-size", type=int, default=5000)
    parser.add_argument("--comparisons", type=int, default=50000)
    parser.add_argument("--seed", type=int, default=42, help="Must match POOL_SEED in the human app")
    parser.add_argument("--output", type=str, default="llm_results")
    parser.add_argument("--checkpoint-every", type=int, default=500)
    args = parser.parse_args()

    run_tournament(
        pool_size=args.pool_size,
        n_comparisons=args.comparisons,
        seed=args.seed,
        output_dir=args.output,
        checkpoint_every=args.checkpoint_every,
    )
