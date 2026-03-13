"""Dash callbacks for Perihelion."""

import uuid
import logging

import dash
from dash import Input, Output, State, ctx
from dash.exceptions import PreventUpdate

from src import elo
from src.hf_logging import log_query_event
from src.components import create_arena, create_leaderboard_rows, create_progress_dashboard

logger = logging.getLogger(__name__)


def register_callbacks(app):
    """Register all Dash callbacks."""

    @app.callback(
        [
            Output("arena-container", "children"),
            Output("current-pair", "data"),
            Output("leaderboard-body", "children"),
            Output("session-id", "data"),
            Output("elo-info", "data"),
            Output("progress-dashboard-container", "children"),
        ],
        Input("arena-container", "id"),
    )
    def initial_load(_):
        session_id = uuid.uuid4().hex
        pair = elo.select_pair()
        info = elo.get_info()
        return (
            create_arena(pair[0], pair[1]),
            list(pair),
            create_leaderboard_rows(elo.get_leaderboard()),
            session_id,
            info,
            create_progress_dashboard(info),
        )

    @app.callback(
        [
            Output("arena-container", "children", allow_duplicate=True),
            Output("current-pair", "data", allow_duplicate=True),
            Output("comparison-count", "data", allow_duplicate=True),
            Output("leaderboard-body", "children", allow_duplicate=True),
            Output("elo-info", "data", allow_duplicate=True),
            Output("progress-dashboard-container", "children", allow_duplicate=True),
        ],
        [Input("left-card-btn", "n_clicks"), Input("right-card-btn", "n_clicks")],
        [
            State("current-pair", "data"),
            State("comparison-count", "data"),
            State("session-id", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_card_click(left_clicks, right_clicks, current_pair, comp_count, session_id):
        if not ctx.triggered_id:
            raise PreventUpdate
        if (left_clicks in [0, None]) and (right_clicks in [0, None]):
            raise PreventUpdate
        if current_pair is None:
            raise PreventUpdate

        if comp_count is None:
            comp_count = 0

        left_idx, right_idx = current_pair[0], current_pair[1]
        if ctx.triggered_id == "left-card-btn":
            winner_idx, loser_idx = left_idx, right_idx
        else:
            winner_idx, loser_idx = right_idx, left_idx

        result = elo.record_comparison(winner_idx, loser_idx)

        log_query_event({
            "log_type": "comparison",
            "session_id": session_id,
            "galaxy_left": left_idx,
            "galaxy_right": right_idx,
            "winner": winner_idx,
            "elo_left_before": result["winner_elo_before"] if winner_idx == left_idx else result["loser_elo_before"],
            "elo_right_before": result["loser_elo_before"] if winner_idx == left_idx else result["winner_elo_before"],
            "elo_left_after": result["winner_elo_after"] if winner_idx == left_idx else result["loser_elo_after"],
            "elo_right_after": result["loser_elo_after"] if winner_idx == left_idx else result["winner_elo_after"],
        })

        pair = elo.select_pair()
        info = elo.get_info()

        return (
            create_arena(pair[0], pair[1]),
            list(pair),
            comp_count + 1,
            create_leaderboard_rows(elo.get_leaderboard()),
            info,
            create_progress_dashboard(info),
        )

    @app.callback(
        [
            Output("elo-info", "data", allow_duplicate=True),
            Output("progress-dashboard-container", "children", allow_duplicate=True),
        ],
        Input("progress-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def update_progress(n_intervals):
        info = elo.get_info()
        return info, create_progress_dashboard(info)

    @app.callback(
        [Output("leaderboard-body", "style"), Output("leaderboard-arrow", "style")],
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

    @app.callback(
        [
            Output("arena-container", "children", allow_duplicate=True),
            Output("current-pair", "data", allow_duplicate=True),
            Output("comparison-count", "data", allow_duplicate=True),
            Output("leaderboard-body", "children", allow_duplicate=True),
        ],
        Input("reset-session", "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_session(n_clicks):
        if not n_clicks:
            raise PreventUpdate
        pair = elo.select_pair()
        return (
            create_arena(pair[0], pair[1]),
            list(pair),
            0,
            create_leaderboard_rows(elo.get_leaderboard()),
        )
