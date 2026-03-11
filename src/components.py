"""Dating-app themed tournament UI for g-Harmony."""

import random
from dash import dcc, html
import dash_bootstrap_components as dbc

from src.galaxy_profiles import GALAXY_PROFILES, GALAXY_IDS, NUM_GALAXIES


def get_app_theme() -> str:
    """Return the full HTML template with embedded CSS."""
    return '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>g-Harmony</title>
        {%favicon%}
        {%css%}
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Playfair+Display:wght@600;700;800&display=swap" rel="stylesheet">
        <style>
            * {
                box-sizing: border-box;
                -webkit-font-smoothing: antialiased;
                -moz-osx-font-smoothing: grayscale;
            }

            body {
                font-family: 'Outfit', sans-serif;
                background: radial-gradient(ellipse at 30% 20%, #0d0b2e 0%, #050510 50%, #020208 100%);
                color: #F5F5F7;
                min-height: 100vh;
                margin: 0;
                overflow-x: hidden;
            }

            .container-fluid {
                background-color: transparent !important;
                max-width: 960px;
                padding-left: 12px;
                padding-right: 12px;
            }

            /* Star twinkle animation */
            @keyframes twinkle {
                0%, 100% { opacity: 0.2; }
                50% { opacity: 1; }
            }

            @keyframes fadeIn {
                from { opacity: 0; }
                to { opacity: 1; }
            }

            @keyframes fadeSlideUp {
                from { opacity: 0; transform: translateY(12px); }
                to { opacity: 1; transform: translateY(0); }
            }

            /* Header gradient text */
            .gharmony-title {
                font-family: 'Playfair Display', serif;
                font-size: 2.8rem;
                font-weight: 800;
                background: linear-gradient(135deg, #a78bfa, #f472b6, #a78bfa);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                line-height: 1.1;
                letter-spacing: -0.5px;
            }

            .gharmony-tagline {
                font-family: 'Outfit', sans-serif;
                font-size: 0.7rem;
                font-weight: 600;
                color: rgba(255,255,255,0.35);
                letter-spacing: 4px;
                text-transform: uppercase;
            }

            /* Galaxy profile cards */
            .galaxy-card {
                position: relative;
                border-radius: 16px;
                overflow: hidden;
                cursor: pointer;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                background: #0a0a1a;
                border: 2px solid rgba(255,255,255,0.08);
            }

            .galaxy-card:hover {
                border-color: rgba(167,139,250,0.5);
                transform: translateY(-2px);
            }
            
            .galaxy-card:active {
                animation: galaxyClick 0.2s ease;
            }

            .galaxy-card-image {
                width: 100%;
                aspect-ratio: 1 / 1;
                object-fit: cover;
                display: block;
            }

            .galaxy-card-name {
                font-family: 'Playfair Display', serif;
                font-size: 1.2rem;
                font-weight: 700;
                color: #fff;
                letter-spacing: -0.3px;
                text-align: center;
                padding: 12px 12px 14px;
            }

            /* VS divider */
            .vs-divider {
                font-family: 'Playfair Display', serif;
                font-size: 1.4rem;
                font-weight: 800;
                color: rgba(255,255,255,0.25);
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 0 8px;
            }

            /* Leaderboard */
            .leaderboard-row {
                display: flex;
                align-items: center;
                padding: 10px 16px;
                border-bottom: 1px solid rgba(255,255,255,0.05);
                transition: background 0.2s;
            }

            .leaderboard-row:hover {
                background: rgba(255,255,255,0.03);
            }

            .leaderboard-rank {
                font-family: 'Outfit', sans-serif;
                font-size: 0.9rem;
                font-weight: 700;
                color: rgba(255,255,255,0.4);
                width: 36px;
                text-align: center;
            }

            .leaderboard-thumb {
                width: 40px;
                height: 40px;
                border-radius: 50%;
                object-fit: cover;
                margin: 0 12px;
                border: 2px solid rgba(255,255,255,0.1);
            }

            .leaderboard-name {
                font-family: 'Outfit', sans-serif;
                font-size: 0.9rem;
                font-weight: 600;
                color: #fff;
                flex: 1;
            }

            .leaderboard-elo {
                font-family: 'Outfit', sans-serif;
                font-size: 0.85rem;
                font-weight: 500;
                color: rgba(255,255,255,0.6);
            }

            .leaderboard-container {
                background: rgba(255,255,255,0.03);
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 16px;
                overflow: hidden;
                backdrop-filter: blur(20px);
            }

            .leaderboard-header {
                font-family: 'Outfit', sans-serif;
                font-size: 0.7rem;
                font-weight: 600;
                color: rgba(255,255,255,0.35);
                letter-spacing: 3px;
                text-transform: uppercase;
                padding: 16px;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: space-between;
                transition: color 0.2s;
            }

            .leaderboard-header:hover {
                color: rgba(255,255,255,0.6);
            }

            /* Counter */
            .comparison-counter {
                font-family: 'Outfit', sans-serif;
                font-size: 0.75rem;
                font-weight: 500;
                color: rgba(255,255,255,0.35);
            }

            /* All-done card */
            .all-done-card {
                background: rgba(255,255,255,0.03);
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 20px;
                padding: 48px 32px;
                text-align: center;
                backdrop-filter: blur(20px);
                animation: fadeIn 0.6s ease;
            }

            /* Scrollbar */
            ::-webkit-scrollbar { width: 6px; }
            ::-webkit-scrollbar-track { background: transparent; }
            ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.1); border-radius: 3px; }
            ::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.2); }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
        <script>
            document.addEventListener('keydown', function(e) {
                if (e.key === 'ArrowLeft') {
                    var btn = document.getElementById('left-card-btn');
                    if (btn) btn.click();
                } else if (e.key === 'ArrowRight') {
                    var btn = document.getElementById('right-card-btn');
                    if (btn) btn.click();
                }
            });
        </script>
    </body>
</html>
'''


def _create_star_field(n=80):
    """Generate CSS star-field background as inline-styled divs."""
    stars = []
    for i in range(n):
        x = random.random() * 100
        y = random.random() * 100
        size = random.random() * 2 + 0.5
        delay = round(random.random() * 4, 1)
        duration = round(random.random() * 3 + 2, 1)
        stars.append(html.Div(style={
            "position": "absolute",
            "left": f"{x:.1f}%",
            "top": f"{y:.1f}%",
            "width": f"{size:.1f}px",
            "height": f"{size:.1f}px",
            "borderRadius": "50%",
            "background": "#fff",
            "animation": f"twinkle {duration}s {delay}s infinite ease-in-out",
        }))
    return html.Div(
        stars,
        style={
            "position": "fixed", "top": "0", "left": "0", "right": "0", "bottom": "0",
            "pointerEvents": "none", "zIndex": "0",
        },
    )


def create_galaxy_card(galaxy_id, side="left", is_champion=False):
    """Build a single galaxy profile card -- image + name + description."""
    profile = GALAXY_PROFILES[galaxy_id]
    btn_id = f"{side}-card-btn"
    
    card_style = {
        "border": "none",
        "padding": "0",
        "textAlign": "left",
        "width": "100%",
    }
    
    # Add champion styling
    if is_champion:
        card_style["boxShadow"] = "0 0 20px rgba(255, 215, 0, 0.5)"
        card_style["border"] = "2px solid rgba(255, 215, 0, 0.7)"
        card_style["borderRadius"] = "12px"
        card_style["animation"] = "galaxyWin 1.5s ease-in-out"
    
    # Get description text
    description = profile.get('description', profile.get('bio', ''))
    
    card_contents = [
        html.Img(
            src=f"/galaxy-images/{galaxy_id}.jpg",
            className="galaxy-card-image",
        ),
        html.Div(
            [
                html.Span(profile["name"], style={"marginRight": "8px"}),
                html.I(
                    className="fas fa-crown",
                    style={
                        "color": "#FFD700",
                        "fontSize": "0.9rem",
                        "display": "inline" if is_champion else "none",
                    }
                ),
            ],
            className="galaxy-card-name",
        ),
        # Add description below the name
        html.Div(
            description,
            className="galaxy-card-description",
            style={
                "fontSize": "0.8rem",
                "color": "rgba(255,255,255,0.7)", 
                "padding": "8px 16px 16px",
                "lineHeight": "1.4",
                "fontFamily": "'Outfit', sans-serif",
            }
        ) if description else None,
    ]
    
    # Filter out None elements
    card_contents = [item for item in card_contents if item is not None]

    return html.Button(
        card_contents,
        className="galaxy-card",
        id=btn_id,
        n_clicks=0,
        style=card_style,
    )


def create_arena(left_id=None, right_id=None, champion_id=None):
    """Build the two-card arena with VS divider."""
    if left_id is None or right_id is None:
        # All done state
        return html.Div(
            [
                html.Div(
                    "Champion Reign Complete!",
                    style={
                        "fontFamily": "'Playfair Display', serif",
                        "fontSize": "1.8rem",
                        "fontWeight": "700",
                        "color": "#fff",
                        "marginBottom": "12px",
                    },
                ),
                html.P(
                    "The current champion has faced all possible challengers. "
                    "Check the leaderboard below for the final rankings!",
                    style={"color": "rgba(255,255,255,0.5)", "maxWidth": "400px", "margin": "0 auto 24px"},
                ),
                dbc.Button(
                    "Reset Session",
                    id="reset-session",
                    style={
                        "background": "linear-gradient(135deg, #a78bfa, #f472b6)",
                        "border": "none",
                        "color": "#fff",
                        "fontFamily": "'Outfit', sans-serif",
                        "fontWeight": "600",
                        "padding": "12px 36px",
                        "borderRadius": "30px",
                        "fontSize": "0.95rem",
                    },
                ),
            ],
            className="all-done-card",
        )

    # Determine champion status
    left_is_champion = champion_id is not None and left_id == champion_id
    right_is_champion = champion_id is not None and right_id == champion_id

    return dbc.Row(
        [
            dbc.Col(
                create_galaxy_card(left_id, side="left", is_champion=left_is_champion),
                width=5,
            ),
            dbc.Col(
                html.Div("VS", className="vs-divider"),
                width=2, className="d-flex align-items-center justify-content-center",
            ),
            dbc.Col(
                create_galaxy_card(right_id, side="right", is_champion=right_is_champion),
                width=5,
            ),
        ],
        className="g-0 align-items-stretch",
        style={"animation": "fadeSlideUp 0.4s ease"},
    )


def create_leaderboard_rows(leaderboard_data):
    """Build leaderboard row elements from sorted data."""
    rows = []
    for i, entry in enumerate(leaderboard_data):
        gid = entry["id"]
        profile = GALAXY_PROFILES.get(gid, {})
        rank = i + 1
        # Medal for top 3
        rank_display = {1: "1", 2: "2", 3: "3"}.get(rank, str(rank))
        rank_color = {1: "#FFD700", 2: "#C0C0C0", 3: "#CD7F32"}.get(rank, "rgba(255,255,255,0.4)")

        rows.append(
            html.Div(
                [
                    html.Span(rank_display, className="leaderboard-rank", style={"color": rank_color}),
                    html.Img(src=f"/galaxy-images/{gid}.jpg", className="leaderboard-thumb"),
                    html.Span(profile.get("name", gid[:8]), className="leaderboard-name"),
                    html.Span(f"{entry['elo']:.0f}", className="leaderboard-elo"),
                ],
                className="leaderboard-row",
            )
        )
    return rows


def create_layout():
    """Assemble the complete app layout."""
    return dbc.Container(
        [
            _create_star_field(80),

            # Header
            html.Div(
                [
                    html.Div("g-Harmony", className="gharmony-title text-center"),
                    html.Div("FIND YOUR GALAXY MATCH", className="gharmony-tagline text-center mt-1"),
                    html.Div(
                        "Left/Right arrow keys to choose",
                        style={
                            "fontFamily": "'Outfit', sans-serif",
                            "fontSize": "0.65rem",
                            "fontWeight": "400",
                            "color": "rgba(255,255,255,0.2)",
                            "letterSpacing": "1px",
                            "marginTop": "8px",
                        },
                    ),
                ],
                className="text-center pt-4 pb-3",
                style={"position": "relative", "zIndex": "10"},
            ),

            # Arena
            html.Div(id="arena-container", style={"position": "relative", "zIndex": "10"}),

            # Spacer
            html.Div(style={"height": "32px"}),

            # Leaderboard
            html.Div(
                [
                    html.Div(
                        [
                            html.Span("LEADERBOARD"),
                            html.I(className="fas fa-chevron-down", id="leaderboard-arrow",
                                   style={"transition": "transform 0.3s", "fontSize": "0.65rem"}),
                        ],
                        className="leaderboard-header",
                        id="leaderboard-toggle",
                        n_clicks=0,
                    ),
                    html.Div(id="leaderboard-body", style={"display": "none"}),
                ],
                className="leaderboard-container mb-4",
                style={"position": "relative", "zIndex": "10"},
            ),

            # Stores
            dcc.Store(id="seen-pairs", data=[]),
            dcc.Store(id="current-pair", data=None),
            dcc.Store(id="current-champion", data=None),
            dcc.Store(id="comparison-count", data=0),
            dcc.Store(id="session-id", data=""),
        ],
        fluid=True,
        className="py-0",
    )
