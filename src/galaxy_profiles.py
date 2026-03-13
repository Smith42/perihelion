"""Galaxy metadata accessors backed by the in-memory cache populated at startup."""

import logging
import threading

from src.config import ID_COLUMN
from src.galaxy_data_loader import image_cache

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_metadata_cache: dict[int, dict] = {}


def get_display_name(row_index: int) -> str:
    """Return the display name for a galaxy by its sequential pool index."""
    meta = _get_metadata(row_index)
    if meta is not None:
        id_val = meta.get(ID_COLUMN)
        if id_val is not None:
            return str(id_val)
    return f"Galaxy #{row_index}"


def get_image_path(row_index: int):
    """Return the cached image path, or None if not available."""
    return image_cache.get_path(row_index)


def register_metadata(metadata_map: dict[int, dict]):
    """Bulk-register row metadata from streaming init (keyed by sequential ID)."""
    with _lock:
        _metadata_cache.update(metadata_map)


def _get_metadata(row_index: int) -> dict | None:
    with _lock:
        return _metadata_cache.get(row_index)
