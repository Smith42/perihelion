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

_SHUFFLE_BUFFER = 200


def _make_dataset(seed: int, pool_size: int):
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
    if features and IMAGE_COLUMN in features:
        ds = ds.cast_column(IMAGE_COLUMN, HFImage(decode=False))

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
    """Build the galaxy pool, caching a small batch of images before returning.

    Single streaming pass with cast_column(decode=False) to avoid Pillow.
    The shuffle buffer is small (200 rows) so only ~300 images are downloaded
    before the app starts serving. The rest are cached in a background thread.

    Returns:
        ids: sequential ints 0..N-1
        metadata_map: {id -> row_dict (no image column)}
        seed: seed used
    """
    if seed is None:
        seed = random.randint(0, 2**32 - 1)

    logger.info("Streaming %d galaxies (seed=%d)...", pool_size, seed)

    # Pool IDs are just 0..pool_size-1 — known upfront
    ids = list(range(pool_size))
    metadata_map: dict[int, dict] = {}

    it = _make_dataset(seed, pool_size)
    sync_count = min(prefetch_images, pool_size)

    # Synchronous: first sync_count rows — populate metadata + cache images
    for i in range(sync_count):
        row = next(it)
        img_col = row.get(IMAGE_COLUMN)
        if isinstance(img_col, dict) and img_col.get("bytes"):
            image_cache.put(i, img_col["bytes"])
        else:
            logger.warning("No image bytes for row %d", i)
        metadata_map[i] = {k: v for k, v in row.items() if k != IMAGE_COLUMN}

    logger.info("%d images cached — app ready, filling remaining %d in background",
                sync_count, pool_size - sync_count)

    # Asynchronous: rest of the pool
    if sync_count < pool_size:
        def _bg():
            for i in range(sync_count, pool_size):
                try:
                    row = next(it)
                    img_col = row.get(IMAGE_COLUMN)
                    if isinstance(img_col, dict) and img_col.get("bytes"):
                        image_cache.put(i, img_col["bytes"])
                    metadata_map[i] = {k: v for k, v in row.items() if k != IMAGE_COLUMN}
                except StopIteration:
                    break
                except Exception as e:
                    logger.warning("Background error at row %d: %s", i, e)
            logger.info("Background streaming complete")

        threading.Thread(target=_bg, daemon=True).start()

    return ids, metadata_map, seed
