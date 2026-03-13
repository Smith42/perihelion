"""Galaxy data loading: HF Datasets streaming sampler + disk-based LRU image cache."""

from __future__ import annotations

import logging
import random
import threading
import time
from pathlib import Path

from src.config import (
    DATASET_CONFIG,
    DATASET_ID,
    DATASET_SPLIT,
    HF_TOKEN,
    IMAGE_CACHE_DIR,
    IMAGE_CACHE_MAX_BYTES,
    IMAGE_COLUMN,
)

logger = logging.getLogger(__name__)

# Must be identical across both streaming passes so row order is reproducible.
_SHUFFLE_BUFFER = 1_000


def _make_dataset(seed: int, pool_size: int, with_images: bool):
    """Return a shuffled, length-limited streaming dataset iterator."""
    from datasets import load_dataset
    from datasets import Image as HFImage

    ds = load_dataset(
        DATASET_ID,
        DATASET_CONFIG,
        split=DATASET_SPLIT,
        streaming=True,
        token=HF_TOKEN if HF_TOKEN else None,
    )
    features = getattr(ds, "features", None)
    if with_images:
        if features and IMAGE_COLUMN in features:
            ds = ds.cast_column(IMAGE_COLUMN, HFImage(decode=False))
    else:
        if features and IMAGE_COLUMN in features:
            ds = ds.remove_columns([IMAGE_COLUMN])

    ds = ds.shuffle(seed=seed, buffer_size=_SHUFFLE_BUFFER)
    ds = ds.take(pool_size)
    return iter(ds)


# ---------------------------------------------------------------------------
# ImageCache — thread-safe, disk-based LRU
# ---------------------------------------------------------------------------

class ImageCache:
    """Disk-based LRU image cache keyed by sequential pool index."""

    def __init__(self, cache_dir: str = IMAGE_CACHE_DIR, max_bytes: int = IMAGE_CACHE_MAX_BYTES):
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._access_times: dict[int, float] = {}
        self._total_bytes = 0
        self._scan_existing()

    def _path_for(self, row_index: int) -> Path:
        return self._dir / f"{row_index}.jpg"

    def _scan_existing(self):
        total = 0
        for p in self._dir.glob("*.jpg"):
            try:
                idx = int(p.stem)
                size = p.stat().st_size
                total += size
                self._access_times[idx] = p.stat().st_mtime
            except (ValueError, OSError):
                continue
        self._total_bytes = total
        logger.info("Image cache: %d files, %.1f MB", len(self._access_times), total / 1e6)

    def get_path(self, row_index: int) -> Path | None:
        p = self._path_for(row_index)
        if p.exists():
            with self._lock:
                self._access_times[row_index] = time.monotonic()
            return p
        return None

    def put(self, row_index: int, image_bytes: bytes) -> Path:
        p = self._path_for(row_index)
        p.write_bytes(image_bytes)
        size = len(image_bytes)
        with self._lock:
            self._access_times[row_index] = time.monotonic()
            self._total_bytes += size
            self._evict_if_needed()
        return p

    def _evict_if_needed(self):
        while self._total_bytes > self._max_bytes and self._access_times:
            lru_idx = min(self._access_times, key=self._access_times.get)
            p = self._path_for(lru_idx)
            try:
                size = p.stat().st_size
                p.unlink()
                self._total_bytes -= size
            except OSError:
                pass
            del self._access_times[lru_idx]


# Module-level singleton
image_cache = ImageCache()


# ---------------------------------------------------------------------------
# Streaming pool sampler
# ---------------------------------------------------------------------------

def sample_pool_streaming(
    pool_size: int,
    seed: int | None = None,
    prefetch_images: int = 100,
) -> tuple[list[int], dict[int, dict], int]:
    """Build the galaxy pool with lazy image loading.

    Pass 1 (fast): streams metadata only — no image bytes downloaded.
    Pass 2a (sync): caches the first `prefetch_images` images before returning,
                    so the app can serve immediately.
    Pass 2b (async): background thread fills the rest of the image cache.

    Both passes use the same seed and shuffle buffer so row i in pass 1
    is the same galaxy as row i in pass 2.

    Returns:
        ids: sequential ints 0..N-1
        metadata_map: {id -> row_dict (no image column)}
        seed: seed used (fixed for reproducibility)
    """
    if seed is None:
        seed = random.randint(0, 2**32 - 1)

    # ------------------------------------------------------------------
    # Pass 1: metadata only — fast, no image bytes
    # ------------------------------------------------------------------
    logger.info("Streaming metadata for %d galaxies (seed=%d)...", pool_size, seed)
    ids: list[int] = []
    metadata_map: dict[int, dict] = {}

    for i, row in enumerate(_make_dataset(seed, pool_size, with_images=False)):
        metadata_map[i] = row
        ids.append(i)

    logger.info("Metadata ready: %d galaxies", len(ids))

    # ------------------------------------------------------------------
    # Pass 2: images — same seed/buffer so row order matches pass 1
    # ------------------------------------------------------------------
    sync_count = min(prefetch_images, pool_size)
    logger.info("Pre-caching first %d images...", sync_count)

    img_iter = _make_dataset(seed, pool_size, with_images=True)

    def _cache_row(i: int, row: dict):
        img_col = row.get(IMAGE_COLUMN)
        if isinstance(img_col, dict):
            img_bytes = img_col.get("bytes")
            if img_bytes:
                image_cache.put(i, img_bytes)
                return
        logger.warning("No image bytes for row %d", i)

    # Synchronous: first sync_count images
    for i in range(sync_count):
        _cache_row(i, next(img_iter))

    logger.info("Initial %d images cached — app ready", sync_count)

    # Asynchronous: remainder in background
    remaining = pool_size - sync_count
    if remaining > 0:
        def _bg_cache():
            logger.info("Background: caching %d remaining images...", remaining)
            for i in range(sync_count, pool_size):
                try:
                    _cache_row(i, next(img_iter))
                except StopIteration:
                    break
                except Exception as e:
                    logger.warning("Background cache error at row %d: %s", i, e)
            logger.info("Background image caching complete")

        threading.Thread(target=_bg_cache, daemon=True).start()

    return ids, metadata_map, seed
