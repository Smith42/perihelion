"""Dating-app themed tournament UI for g-Harmony."""

import random
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

from src.galaxy_profiles import get_display_name


def get_app_theme() -> str:
    """Return the full HTML template with embedded CSS."""
    return '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Perihelion</title>
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

            /* Progress dashboard */
            .progress-dashboard {
                background: rgba(255,255,255,0.03);
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 16px;
                padding: 20px;
                backdrop-filter: blur(20px);
            }

            .progress-stat {
                text-align: center;
                padding: 8px;
            }

            .progress-stat-value {
                font-family: 'Playfair Display', serif;
                font-size: 1.6rem;
                font-weight: 700;
                color: #fff;
            }

            .progress-stat-label {
                font-family: 'Outfit', sans-serif;
                font-size: 0.65rem;
                font-weight: 600;
                color: rgba(255,255,255,0.35);
                letter-spacing: 2px;
                text-transform: uppercase;
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


def create_galaxy_card(row_index: int, side: str = "left"):
    """Build a single galaxy profile card — image + name."""
    name = get_display_name(row_index)
    btn_id = f"{side}-card-btn"

    return html.Button(
        [
            html.Img(
                src=f"/galaxy-images/{row_index}.jpg",
                className="galaxy-card-image",
            ),
            html.Div(name, className="galaxy-card-name"),
        ],
        className="galaxy-card",
        id=btn_id,
        n_clicks=0,
        style={
            "border": "none",
            "padding": "0",
            "textAlign": "left",
            "width": "100%",
        },
    )


def create_arena(left_idx, right_idx):
    """Build the two-card arena with VS divider."""
    return dbc.Row(
        [
            dbc.Col(
                create_galaxy_card(left_idx, side="left"),
                width=5,
            ),
            dbc.Col(
                html.Div("VS", className="vs-divider"),
                width=2, className="d-flex align-items-center justify-content-center",
            ),
            dbc.Col(
                create_galaxy_card(right_idx, side="right"),
                width=5,
            ),
        ],
        className="g-0 align-items-stretch",
        style={"animation": "fadeSlideUp 0.4s ease"},
    )


def create_progress_dashboard(info: dict):
    """Build the ELO ranking progress dashboard."""
    pool_size = info.get("pool_size", 0)
    total_comps = info.get("total_comparisons", 0)
    elo_values = info.get("elo_values", [])

    stats_row = dbc.Row(
        [
            dbc.Col(html.Div([
                html.Div(str(pool_size), className="progress-stat-value"),
                html.Div("GALAXIES", className="progress-stat-label"),
            ], className="progress-stat"), width=6),
            dbc.Col(html.Div([
                html.Div(str(total_comps), className="progress-stat-value"),
                html.Div("COMPARISONS", className="progress-stat-label"),
            ], className="progress-stat"), width=6),
        ],
        className="mb-3",
    )

    if elo_values:
        fig = go.Figure(data=[go.Histogram(
            x=elo_values,
            nbinsx=30,
            marker_color="rgba(167,139,250,0.6)",
            marker_line_color="rgba(167,139,250,0.8)",
            marker_line_width=1,
        )])
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="rgba(255,255,255,0.5)",
            font_family="Outfit",
            font_size=10,
            margin=dict(l=30, r=10, t=10, b=30),
            height=120,
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)", title_text="ELO Rating", title_font_size=9),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title_text="Count", title_font_size=9),
        )
        histogram = dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"height": "120px"})
    else:
        histogram = html.Div()

    return html.Div([stats_row, histogram], className="progress-dashboard")


def create_leaderboard_rows(leaderboard_data):
    """Build leaderboard row elements from sorted data."""
    rows = []
    for i, entry in enumerate(leaderboard_data):
        idx = entry["id"]
        name = get_display_name(idx)
        rank = i + 1
        rank_color = {1: "#FFD700", 2: "#C0C0C0", 3: "#CD7F32"}.get(rank, "rgba(255,255,255,0.4)")

        rows.append(
            html.Div(
                [
                    html.Span(str(rank), className="leaderboard-rank", style={"color": rank_color}),
                    html.Img(src=f"/galaxy-images/{idx}.jpg", className="leaderboard-thumb"),
                    html.Span(name, className="leaderboard-name"),
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
                    html.Div("Perihelion", className="gharmony-title text-center"),
                    html.Div("VOTE FOR THE MOST INTERESTING GALAXY", className="gharmony-tagline text-center mt-1"),
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
            html.Div(style={"height": "24px"}),

            # Progress dashboard
            html.Div(id="progress-dashboard-container", style={"position": "relative", "zIndex": "10"}),

            # Spacer
            html.Div(style={"height": "24px"}),

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
            dcc.Store(id="current-pair", data=None),
            dcc.Store(id="comparison-count", data=0),
            dcc.Store(id="elo-info", data={}),
            dcc.Store(id="session-id", data=""),

            # Interval for progress updates
            dcc.Interval(id="progress-interval", interval=10000, n_intervals=0),
        ],
        fluid=True,
        className="py-0",
    )
