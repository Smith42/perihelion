"""g-Harmony - Galaxy Interestingness Tournament."""

import logging

import dash
import dash_bootstrap_components as dbc
from flask import send_file, abort

from src.components import get_app_theme, create_layout
from src.callbacks import register_callbacks
from src import elo
from src.galaxy_data_loader import (
    get_dataset_size,
    sample_pool_indices,
    image_cache,
)
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
    if not loaded:
        logger.info("No existing tournament found. Sampling new pool...")
        try:
            total = get_dataset_size()
            logger.info("Dataset has %d rows. Sampling pool of %d...", total, POOL_SIZE)
            pool = sample_pool_indices(total, POOL_SIZE)
            elo.initialize_tournament(pool)
        except Exception as e:
            logger.error("Failed to initialize tournament: %s", e)
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
