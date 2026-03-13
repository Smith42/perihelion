"""g-Harmony - Galaxy Interestingness Tournament."""

import logging

import dash
import dash_bootstrap_components as dbc
from flask import send_file, abort

from src.components import get_app_theme, create_layout
from src.callbacks import register_callbacks
from src import elo
from src.galaxy_data_loader import sample_pool_streaming, image_cache
from src.galaxy_profiles import register_metadata
from src.config import POOL_SIZE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# Suppress noisy httpx request logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def create_app() -> dash.Dash:
    """Create and configure the Dash application."""
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )
    app.title = "g-Harmony"
    app.index_string = get_app_theme()

    server = app.server

    # Serve galaxy images from cache
    @server.route("/galaxy-images/<int:row_index>.jpg")
    def serve_galaxy_image(row_index):
        path = image_cache.ensure_cached(row_index)
        if path is None:
            abort(404)
        return send_file(path, mimetype="image/jpeg")

    # Initialize tournament
    logger.info("Loading tournament state...")
    loaded = elo.load_tournament_state()

    # Always re-stream the pool to populate the image + metadata caches.
    # On reload we reuse the saved seed so the same galaxies are sampled in the
    # same order, keeping ELO rankings consistent across restarts.
    seed = elo.get_pool_seed() if loaded else None
    logger.info(
        "Streaming pool of %d galaxies (seed=%s)...",
        POOL_SIZE,
        seed if seed is not None else "random",
    )
    try:
        pool, metadata_map, used_seed = sample_pool_streaming(POOL_SIZE, seed=seed)
        register_metadata(metadata_map)
        if not loaded:
            elo.initialize_tournament(pool, pool_seed=used_seed)
        else:
            # Persist seed into existing state so future reloads can reuse it
            elo.set_pool_seed(used_seed)
            logger.info(
                "Tournament state restored: round %d, %d active galaxies",
                elo.get_tournament_info().get("current_round", 1),
                len(pool),
            )
    except Exception as e:
        logger.error("Failed to stream galaxy pool: %s", e)
        raise

    # Layout and callbacks
    app.layout = create_layout()
    register_callbacks(app)

    logger.info("g-Harmony ready!")
    return app


app = create_app()
server = app.server

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=7860)
