"""Galaxy data loading: HF Datasets streaming sampler + disk-based LRU image cache."""

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


# ---------------------------------------------------------------------------
# ImageCache — thread-safe, disk-based LRU
# ---------------------------------------------------------------------------

class ImageCache:
    """Disk-based LRU image cache keyed by sequential pool index.

    Images are written at startup by sample_pool_streaming and served from
    disk thereafter.  There is no network fallback — if an image was not
    captured during streaming it simply won't be available.
    """

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
        """Return cached file path if present, updating access time."""
        p = self._path_for(row_index)
        if p.exists():
            with self._lock:
                self._access_times[row_index] = time.monotonic()
            return p
        return None

    def put(self, row_index: int, image_bytes: bytes) -> Path:
        """Write image bytes to cache, evicting LRU entries if needed."""
        p = self._path_for(row_index)
        p.write_bytes(image_bytes)
        size = len(image_bytes)
        with self._lock:
            self._access_times[row_index] = time.monotonic()
            self._total_bytes += size
            self._evict_if_needed()
        return p

    def _evict_if_needed(self):
        """Evict LRU entries until total size is within bounds. Caller holds lock."""
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

    def prefetch(self, row_indices: list[int]):
        """Log which requested indices are missing from cache (no-op fetch)."""
        missing = [idx for idx in row_indices if self.get_path(idx) is None]
        if missing:
            logger.debug("prefetch: %d indices not in cache (no refetch): %s", len(missing), missing[:5])


# Module-level singleton
image_cache = ImageCache()


# ---------------------------------------------------------------------------
# Streaming pool sampler
# ---------------------------------------------------------------------------

def sample_pool_streaming(
    pool_size: int, seed: int | None = None
) -> tuple[list[int], dict[int, dict], int]:
    """Stream pool_size shuffled galaxies from HF Datasets, pre-caching images.

    Args:
        pool_size: Number of galaxies to include in the pool.
        seed: Shuffle seed. Pass the same seed on subsequent startups to
              reproduce the exact same pool so saved ELO state stays valid.

    Returns:
        ids: sequential ints 0..N-1 used as galaxy IDs throughout the app
        metadata_map: {id -> row_dict (without image column)} for display names
        seed: the seed that was used (store in tournament state for reuse)
    """
    from datasets import load_dataset
    from datasets import Image as HFImage

    if seed is None:
        seed = random.randint(0, 2**32 - 1)

    logger.info(
        "Streaming %d galaxies from %s (shuffle seed=%d)...",
        pool_size,
        DATASET_ID,
        seed,
    )

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

    ds = ds.shuffle(seed=seed, buffer_size=10_000)
    ds = ds.take(pool_size)

    ids: list[int] = []
    metadata_map: dict[int, dict] = {}

    for i, row in enumerate(ds):
        img_col = row.get(IMAGE_COLUMN)
        img_bytes: bytes | None = None
        if isinstance(img_col, dict):
            img_bytes = img_col.get("bytes")

        if img_bytes:
            image_cache.put(i, img_bytes)
        else:
            logger.warning("No image bytes for streamed row %d", i)

        metadata_map[i] = {k: v for k, v in row.items() if k != IMAGE_COLUMN}
        ids.append(i)

        if (i + 1) % 100 == 0:
            logger.info("Streamed %d/%d galaxies", i + 1, pool_size)

    logger.info("Finished streaming %d galaxies", len(ids))
    return ids, metadata_map, seed
