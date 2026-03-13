"""Perihelion - Galaxy Interestingness Ranking."""

import logging

import dash
import dash_bootstrap_components as dbc
from flask import send_file, abort

from src.components import get_app_theme, create_layout
from src.callbacks import register_callbacks
from src import elo
from src.galaxy_data_loader import sample_pool_streaming, image_cache
from src.galaxy_profiles import register_metadata
from src.config import POOL_SIZE, POOL_SEED, IMAGE_PREFETCH_COUNT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def create_app() -> dash.Dash:
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )
    app.title = "Perihelion"
    app.index_string = get_app_theme()

    server = app.server

    @server.route("/galaxy-images/<int:row_index>.jpg")
    def serve_galaxy_image(row_index):
        path = image_cache.get_path(row_index)
        if path is None:
            abort(404)
        return send_file(path, mimetype="image/jpeg")

    # Always stream with the fixed seed so every participant sees the same pool
    logger.info("Streaming pool of %d galaxies (seed=%d)...", POOL_SIZE, POOL_SEED)
    pool, metadata_map, _ = sample_pool_streaming(POOL_SIZE, seed=POOL_SEED, prefetch_images=IMAGE_PREFETCH_COUNT)
    register_metadata(metadata_map)

    # Load persisted ELO state or start fresh
    if not elo.load_elo_state():
        logger.info("No saved state found — initializing fresh ELO rankings")
        elo.initialize_elo(pool)

    app.layout = create_layout()
    register_callbacks(app)

    logger.info("Perihelion ready!")
    return app


app = create_app()
server = app.server

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=7860)
