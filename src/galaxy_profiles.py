"""Lazy galaxy metadata accessors backed by HF dataset-viewer API."""

import logging
import threading

from src.config import ID_COLUMN
from src.galaxy_data_loader import fetch_rows, image_cache

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_metadata_cache: dict[int, dict] = {}


def get_display_name(row_index: int) -> str:
    """Return the display name for a galaxy by row index.

    Uses the dataset's ID column value if available, otherwise ``Galaxy #N``.
    """
    meta = _get_metadata(row_index)
    if meta is not None:
        id_val = meta.get(ID_COLUMN)
        if id_val is not None:
            return str(id_val)
    return f"Galaxy #{row_index}"


def get_image_path(row_index: int):
    """Return the cached image path, fetching if needed."""
    return image_cache.ensure_cached(row_index)


def register_metadata(metadata_map: dict[int, dict]):
    """Bulk-register row metadata from streaming init (keyed by sequential ID)."""
    with _lock:
        _metadata_cache.update(metadata_map)


def prefetch_metadata(row_indices: list[int]):
    """Batch-fetch and cache metadata for the given row indices."""
    to_fetch = []
    with _lock:
        for idx in row_indices:
            if idx not in _metadata_cache:
                to_fetch.append(idx)
    if not to_fetch:
        return
    rows = fetch_rows(to_fetch)
    with _lock:
        for idx, row in rows.items():
            _metadata_cache[idx] = row


def _get_metadata(row_index: int) -> dict | None:
    """Get metadata for a single row, fetching on demand if needed."""
    with _lock:
        if row_index in _metadata_cache:
            return _metadata_cache[row_index]
    # Fetch on demand
    rows = fetch_rows([row_index])
    row = rows.get(row_index)
    if row is not None:
        with _lock:
            _metadata_cache[row_index] = row
    return row
