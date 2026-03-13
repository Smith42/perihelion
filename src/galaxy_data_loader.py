"""Galaxy data loading: HF Datasets streaming sampler + disk-based LRU image cache."""

import logging
import os
import random
import threading
import time
from pathlib import Path

import requests

from src.config import (
    DATASET_CONFIG,
    DATASET_ID,
    DATASET_SPLIT,
    HF_TOKEN,
    ID_COLUMN,
    IMAGE_CACHE_DIR,
    IMAGE_CACHE_MAX_BYTES,
    IMAGE_COLUMN,
)

logger = logging.getLogger(__name__)

_HF_API_BASE = "https://datasets-server.huggingface.co"


def _hf_headers() -> dict:
    """Return auth headers if HF_TOKEN is set."""
    if HF_TOKEN:
        return {"Authorization": f"Bearer {HF_TOKEN}"}
    return {}


# ---------------------------------------------------------------------------
# Dataset metadata
# ---------------------------------------------------------------------------

def get_dataset_size() -> int:
    """Get total row count via the HF dataset-viewer /info endpoint."""
    url = f"{_HF_API_BASE}/info"
    params = {"dataset": DATASET_ID, "config": DATASET_CONFIG}
    resp = requests.get(url, params=params, headers=_hf_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    splits = data["dataset_info"]["splits"]
    return splits[DATASET_SPLIT]["num_examples"]


def sample_pool_indices(total: int, pool_size: int) -> list[int]:
    """Generate sorted random row indices for the tournament pool."""
    if pool_size >= total:
        return list(range(total))
    indices = random.sample(range(total), pool_size)
    indices.sort()
    return indices


# ---------------------------------------------------------------------------
# Row / image fetching
# ---------------------------------------------------------------------------

def fetch_rows(offsets: list[int]) -> dict[int, dict]:
    """Fetch rows by offset via the HF dataset-viewer /rows endpoint.

    Returns {offset: row_dict} for each successfully fetched offset.
    The image column value is replaced with its signed ``src`` URL (if present).
    """
    results: dict[int, dict] = {}
    # The /rows endpoint supports length param (max 100 rows at a time)
    # but requires a single offset — so we batch contiguous ranges where possible
    # For simplicity, fetch one-by-one or small batches
    for offset in offsets:
        try:
            url = f"{_HF_API_BASE}/rows"
            params = {
                "dataset": DATASET_ID,
                "config": DATASET_CONFIG,
                "split": DATASET_SPLIT,
                "offset": offset,
                "length": 1,
            }
            resp = requests.get(url, params=params, headers=_hf_headers(), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("rows", [])
            if rows:
                row = rows[0].get("row", {})
                results[offset] = row
        except Exception as e:
            logger.warning("Failed to fetch row at offset %d: %s", offset, e)
    return results


def _extract_image_url(row: dict) -> str | None:
    """Extract the image src URL from a row's image column."""
    img_val = row.get(IMAGE_COLUMN)
    if isinstance(img_val, dict):
        return img_val.get("src")
    if isinstance(img_val, str) and img_val.startswith("http"):
        return img_val
    return None


# ---------------------------------------------------------------------------
# ImageCache — thread-safe, disk-based LRU
# ---------------------------------------------------------------------------

class ImageCache:
    """Disk-based LRU image cache keyed by dataset row index."""

    def __init__(self, cache_dir: str = IMAGE_CACHE_DIR, max_bytes: int = IMAGE_CACHE_MAX_BYTES):
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        # access_times: row_index -> last-access monotonic time
        self._access_times: dict[int, float] = {}
        self._total_bytes = 0
        # Scan existing cache on init
        self._scan_existing()

    def _path_for(self, row_index: int) -> Path:
        return self._dir / f"{row_index}.jpg"

    def _scan_existing(self):
        """Scan cache dir and populate tracking state."""
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
        """Write image bytes to cache, evicting LRU if needed."""
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
            # Find LRU entry
            lru_idx = min(self._access_times, key=self._access_times.get)
            p = self._path_for(lru_idx)
            try:
                size = p.stat().st_size
                p.unlink()
                self._total_bytes -= size
            except OSError:
                pass
            del self._access_times[lru_idx]

    def ensure_cached(self, row_index: int) -> Path | None:
        """Get cached path or fetch + cache. Returns path or None on failure."""
        p = self.get_path(row_index)
        if p is not None:
            return p
        img_bytes = fetch_image_bytes(row_index)
        if img_bytes is None:
            return None
        return self.put(row_index, img_bytes)

    def prefetch(self, row_indices: list[int]):
        """Background-fetch a batch of images (skips already cached)."""
        to_fetch = [idx for idx in row_indices if self.get_path(idx) is None]
        if not to_fetch:
            return

        def _worker():
            for idx in to_fetch:
                try:
                    self.ensure_cached(idx)
                except Exception as e:
                    logger.debug("Prefetch failed for row %d: %s", idx, e)

        t = threading.Thread(target=_worker, daemon=True)
        t.start()


# Module-level singleton
image_cache = ImageCache()


# ---------------------------------------------------------------------------
# Streaming pool sampler
# ---------------------------------------------------------------------------

def sample_pool_streaming(pool_size: int) -> tuple[list[int], dict[int, dict]]:
    """Stream pool_size shuffled galaxies from HF Datasets, pre-caching images.

    Returns:
        ids: sequential ints 0..N-1 used as galaxy IDs throughout the app
        metadata_map: {id -> row_dict (without image column)} for display names
    """
    from datasets import load_dataset
    from datasets import Image as HFImage

    logger.info(
        "Streaming %d galaxies from %s (shuffle buffer=10000)...",
        pool_size,
        DATASET_ID,
    )

    ds = load_dataset(
        DATASET_ID,
        DATASET_CONFIG,
        split=DATASET_SPLIT,
        streaming=True,
        token=HF_TOKEN if HF_TOKEN else None,
    )

    # Request raw bytes instead of decoded PIL images to avoid pillow dependency
    features = getattr(ds, "features", None)
    if features and IMAGE_COLUMN in features:
        ds = ds.cast_column(IMAGE_COLUMN, HFImage(decode=False))

    ds = ds.shuffle(seed=random.randint(0, 2**32 - 1), buffer_size=10_000)
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
    return ids, metadata_map
