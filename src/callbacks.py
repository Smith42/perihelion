"""Dash callbacks for g-Harmony tournament."""

import uuid
import logging

import dash
from dash import Input, Output, State, ctx, html, no_update
from dash.exceptions import PreventUpdate

from src import elo
from src.hf_logging import log_query_event
from src.galaxy_data_loader import image_cache
from src.components import create_arena, create_leaderboard_rows, create_progress_dashboard

logger = logging.getLogger(__name__)


def register_callbacks(app):
    """Register all Dash callbacks."""

    # Initial load: populate the arena with the first pair
    @app.callback(
        [
            Output("arena-container", "children"),
            Output("current-pair", "data"),
            Output("leaderboard-body", "children"),
            Output("session-id", "data"),
            Output("tournament-info", "data"),
            Output("progress-dashboard-container", "children"),
        ],
        Input("arena-container", "id"),
    )
    def initial_load(_):
        session_id = uuid.uuid4().hex

        pair = elo.select_pair(set())
        if pair is None:
            arena = create_arena(None, None)
            current_pair_data = None
        else:
            # Ensure images are cached for the initial pair
            image_cache.ensure_cached(pair[0])
            image_cache.ensure_cached(pair[1])
            arena = create_arena(pair[0], pair[1])
            current_pair_data = list(pair)

            # Prefetch upcoming images
            info = elo.get_tournament_info()
            _prefetch_upcoming(info)

        leaderboard = create_leaderboard_rows(elo.get_leaderboard())
        info = elo.get_tournament_info()
        dashboard = create_progress_dashboard(info)

        return (
            arena,
            current_pair_data,
            leaderboard,
            session_id,
            info,
            dashboard,
        )

    # Card click: pick a winner, update ELO, load next pair
    @app.callback(
        [
            Output("arena-container", "children", allow_duplicate=True),
            Output("current-pair", "data", allow_duplicate=True),
            Output("seen-pairs", "data", allow_duplicate=True),
            Output("comparison-count", "data", allow_duplicate=True),
            Output("leaderboard-body", "children", allow_duplicate=True),
            Output("tournament-info", "data", allow_duplicate=True),
            Output("progress-dashboard-container", "children", allow_duplicate=True),
        ],
        [
            Input("left-card-btn", "n_clicks"),
            Input("right-card-btn", "n_clicks"),
        ],
        [
            State("current-pair", "data"),
            State("seen-pairs", "data"),
            State("comparison-count", "data"),
            State("session-id", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_card_click(left_clicks, right_clicks, current_pair, seen_pairs, comp_count, session_id):
        if not ctx.triggered_id:
            raise PreventUpdate

        if (left_clicks in [0, None]) and (right_clicks in [0, None]):
            raise PreventUpdate

        if current_pair is None:
            raise PreventUpdate

        if seen_pairs is None:
            seen_pairs = []
        if comp_count is None:
            comp_count = 0

        # Determine winner
        triggered = ctx.triggered_id
        if triggered == "left-card-btn":
            winner_side = "left"
        elif triggered == "right-card-btn":
            winner_side = "right"
        else:
            raise PreventUpdate

        left_idx = current_pair[0]
        right_idx = current_pair[1]

        if winner_side == "left":
            winner_idx, loser_idx = left_idx, right_idx
        else:
            winner_idx, loser_idx = right_idx, left_idx

        # Record comparison
        result = elo.record_comparison(winner_idx, loser_idx)

        # Log to HF
        log_query_event({
            "log_type": "comparison",
            "session_id": session_id,
            "galaxy_left": left_idx,
            "galaxy_right": right_idx,
            "winner": winner_idx,
            "round": result["round"],
            "round_advanced": result["round_advanced"],
            "elo_left_before": result["winner_elo_before"] if winner_side == "left" else result["loser_elo_before"],
            "elo_right_before": result["loser_elo_before"] if winner_side == "left" else result["winner_elo_before"],
            "elo_left_after": result["winner_elo_after"] if winner_side == "left" else result["loser_elo_after"],
            "elo_right_after": result["loser_elo_after"] if winner_side == "left" else result["winner_elo_after"],
        })

        # Update seen pairs and count
        seen_pairs.append([left_idx, right_idx])
        comp_count += 1

        # Select next pair
        seen_set = set()
        for p in seen_pairs:
            seen_set.add((p[0], p[1]))
            seen_set.add((p[1], p[0]))

        pair = elo.select_pair(seen_set)

        if pair is None:
            arena = create_arena(None, None)
            current_pair_data = None
        else:
            # Ensure images are cached
            image_cache.ensure_cached(pair[0])
            image_cache.ensure_cached(pair[1])
            arena = create_arena(pair[0], pair[1])
            current_pair_data = list(pair)

        info = elo.get_tournament_info()
        leaderboard = create_leaderboard_rows(elo.get_leaderboard())
        dashboard = create_progress_dashboard(info)

        # Prefetch upcoming images
        _prefetch_upcoming(info)

        return (
            arena,
            current_pair_data,
            seen_pairs,
            comp_count,
            leaderboard,
            info,
            dashboard,
        )

    # Progress dashboard update (interval-driven)
    @app.callback(
        [
            Output("tournament-info", "data", allow_duplicate=True),
            Output("progress-dashboard-container", "children", allow_duplicate=True),
        ],
        Input("progress-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def update_progress(n_intervals):
        info = elo.get_tournament_info()
        dashboard = create_progress_dashboard(info)
        return info, dashboard

    # Leaderboard toggle
    @app.callback(
        [
            Output("leaderboard-body", "style"),
            Output("leaderboard-arrow", "style"),
        ],
        Input("leaderboard-toggle", "n_clicks"),
        State("leaderboard-body", "style"),
        prevent_initial_call=True,
    )
    def toggle_leaderboard(n_clicks, current_style):
        if current_style and current_style.get("display") == "none":
            return (
                {"display": "block", "animation": "fadeSlideUp 0.3s ease"},
                {"transition": "transform 0.3s", "fontSize": "0.65rem", "transform": "rotate(180deg)"},
            )
        return (
            {"display": "none"},
            {"transition": "transform 0.3s", "fontSize": "0.65rem", "transform": "rotate(0deg)"},
        )

    # Reset session (client-side only — does NOT restart tournament)
    @app.callback(
        [
            Output("arena-container", "children", allow_duplicate=True),
            Output("current-pair", "data", allow_duplicate=True),
            Output("seen-pairs", "data", allow_duplicate=True),
            Output("comparison-count", "data", allow_duplicate=True),
            Output("leaderboard-body", "children", allow_duplicate=True),
        ],
        Input("reset-session", "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_session(n_clicks):
        if not n_clicks:
            raise PreventUpdate

        pair = elo.select_pair(set())
        if pair is None:
            arena = create_arena(None, None)
            current_pair_data = None
        else:
            image_cache.ensure_cached(pair[0])
            image_cache.ensure_cached(pair[1])
            arena = create_arena(pair[0], pair[1])
            current_pair_data = list(pair)

        leaderboard = create_leaderboard_rows(elo.get_leaderboard())

        return (
            arena,
            current_pair_data,
            [],
            0,
            leaderboard,
        )


def _prefetch_upcoming(info: dict):
    """Prefetch images for top-rated galaxies to reduce latency."""
    top = info.get("top_indices", [])
    if top:
        from src.config import CACHE_PREFETCH_COUNT
        image_cache.prefetch(top[:CACHE_PREFETCH_COUNT])
