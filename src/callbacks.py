"""Dash callbacks for g-Harmony tournament."""

import uuid
import logging

import dash
from dash import Input, Output, State, ctx, html, no_update
from dash.exceptions import PreventUpdate

from src import elo
from src.hf_logging import log_query_event
from src.galaxy_profiles import TOTAL_PAIRS
from src.components import create_arena, create_leaderboard_rows

logger = logging.getLogger(__name__)


def register_callbacks(app):
    """Register all Dash callbacks."""

    # Initial load: populate the arena with the first champion vs challenger
    @app.callback(
        [
            Output("arena-container", "children"),
            Output("current-pair", "data"),
            Output("current-champion", "data"),
            Output("comparison-counter", "children"),
            Output("leaderboard-body", "children"),
            Output("session-id", "data"),
        ],
        Input("arena-container", "id"),
    )
    def initial_load(_):
        session_id = uuid.uuid4().hex
        # Select random starting champion
        from src.galaxy_profiles import GALAXY_IDS
        import random
        champion_id = random.choice(GALAXY_IDS)
        
        # Find first challenger
        pair = elo.select_pair(set(), champion_id=champion_id)
        arena = create_arena(pair[0], pair[1], champion_id=champion_id)
        leaderboard = create_leaderboard_rows(elo.get_leaderboard())
        return (
            arena,
            [pair[0], pair[1]],
            champion_id,
            f"0 / {TOTAL_PAIRS} comparisons",
            leaderboard,
            session_id,
        )

    # Card click: pick a winner, update ELO, load next pair
    @app.callback(
        [
            Output("arena-container", "children", allow_duplicate=True),
            Output("current-pair", "data", allow_duplicate=True),
            Output("current-champion", "data", allow_duplicate=True),
            Output("seen-pairs", "data", allow_duplicate=True),
            Output("comparison-count", "data", allow_duplicate=True),
            Output("comparison-counter", "children", allow_duplicate=True),
            Output("leaderboard-body", "children", allow_duplicate=True),
        ],
        [
            Input("left-card-btn", "n_clicks"),
            Input("right-card-btn", "n_clicks"),
        ],
        [
            State("current-pair", "data"),
            State("current-champion", "data"),
            State("seen-pairs", "data"),
            State("comparison-count", "data"),
            State("session-id", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_card_click(left_clicks, right_clicks, current_pair, current_champion, seen_pairs, comp_count, session_id):
        if not ctx.triggered_id:
            raise PreventUpdate

        if current_pair is None:
            raise PreventUpdate

        if seen_pairs is None:
            seen_pairs = []
        if comp_count is None:
            comp_count = 0

        # Determine winner based on which button was clicked
        triggered = ctx.triggered_id
        if triggered == "left-card-btn":
            winner_side = "left"
        elif triggered == "right-card-btn":
            winner_side = "right"
        else:
            raise PreventUpdate

        left_id = current_pair[0]
        right_id = current_pair[1]

        if winner_side == "left":
            winner_id, loser_id = left_id, right_id
        else:
            winner_id, loser_id = right_id, left_id

        # Record comparison
        result = elo.record_comparison(winner_id, loser_id)

        # Log to HF
        log_query_event({
            "log_type": "comparison",
            "session_id": session_id,
            "galaxy_left": left_id,
            "galaxy_right": right_id,
            "winner": winner_id,
            "elo_left_before": result["winner_elo_before"] if winner_side == "left" else result["loser_elo_before"],
            "elo_right_before": result["loser_elo_before"] if winner_side == "left" else result["winner_elo_before"],
            "elo_left_after": result["winner_elo_after"] if winner_side == "left" else result["loser_elo_after"],
            "elo_right_after": result["loser_elo_after"] if winner_side == "left" else result["winner_elo_after"],
        })

        # Update seen pairs and count
        seen_pairs.append([left_id, right_id])
        comp_count += 1
        
        # Update champion: winner becomes/stays champion
        new_champion = winner_id

        # Select next pair with champion logic
        seen_set = set()
        for p in seen_pairs:
            seen_set.add((p[0], p[1]))
            seen_set.add((p[1], p[0]))

        pair = elo.select_pair(seen_set, champion_id=new_champion)

        if pair is None:
            arena = create_arena(None, None, champion_id=new_champion)
            current_pair_data = None
        else:
            arena = create_arena(pair[0], pair[1], champion_id=new_champion)
            current_pair_data = [pair[0], pair[1]]

        counter_text = f"{comp_count} / {TOTAL_PAIRS} comparisons"
        leaderboard = create_leaderboard_rows(elo.get_leaderboard())

        return (
            arena,
            current_pair_data,
            new_champion,
            seen_pairs,
            comp_count,
            counter_text,
            leaderboard,
        )

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

    # Reset session
    @app.callback(
        [
            Output("arena-container", "children", allow_duplicate=True),
            Output("current-pair", "data", allow_duplicate=True),
            Output("seen-pairs", "data", allow_duplicate=True),
            Output("comparison-count", "data", allow_duplicate=True),
            Output("comparison-counter", "children", allow_duplicate=True),
            Output("leaderboard-body", "children", allow_duplicate=True),
        ],
        Input("reset-session", "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_session(n_clicks):
        if not n_clicks:
            raise PreventUpdate

        pair = elo.select_pair(set())
        arena = create_arena(pair[0], pair[1])
        leaderboard = create_leaderboard_rows(elo.get_leaderboard())

        return (
            arena,
            [pair[0], pair[1]],
            [],
            0,
            f"0 / {TOTAL_PAIRS} comparisons",
            leaderboard,
        )
