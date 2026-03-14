"""
generate_dashboard.py — Market Intelligence Dashboard (Apple-Inspired Design)

Queries the mart_daily_metrics BigQuery table and produces a self-contained
interactive Plotly HTML dashboard saved to dashboard/dashboard.html.
Also exports PNG screenshots of the two primary tiles using kaleido.

Usage:
    python dashboard/generate_dashboard.py

Requirements:
    pip install -r dashboard/requirements.txt

Environment (loaded from .env at project root):
    GCP_PROJECT_ID                  — Google Cloud project ID
    GOOGLE_APPLICATION_CREDENTIALS  — optional; overridden by service account path below
"""

import os
import sys
import datetime

# ---------------------------------------------------------------------------
# Resolve paths relative to *this* file so the script can be run from anywhere
# ---------------------------------------------------------------------------
SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT     = os.path.dirname(SCRIPT_DIR)
ENV_FILE         = os.path.join(PROJECT_ROOT, ".env")
SA_FILE          = os.path.join(PROJECT_ROOT, "airflow", "credentials", "service_account.json")
OUTPUT_HTML      = os.path.join(SCRIPT_DIR, "dashboard.html")
SCREENSHOTS_DIR  = os.path.join(PROJECT_ROOT, "screenshots")

# ---------------------------------------------------------------------------
# Load .env before any GCP imports so env vars are set in time
# ---------------------------------------------------------------------------
from dotenv import load_dotenv  # noqa: E402

if os.path.exists(ENV_FILE):
    load_dotenv(ENV_FILE)
    print(f"[INFO] Loaded environment from {ENV_FILE}")
else:
    print(f"[WARN] .env not found at {ENV_FILE} — relying on existing environment variables")

# Set GOOGLE_APPLICATION_CREDENTIALS to the local service account file when it exists
if os.path.exists(SA_FILE):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_FILE
    print(f"[INFO] Using service account: {SA_FILE}")
elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    print(
        "[ERROR] Service account file not found and GOOGLE_APPLICATION_CREDENTIALS is not set.\n"
        f"        Expected: {SA_FILE}\n"
        "        Place your service_account.json there or set the env var."
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Validate required env vars
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
if not GCP_PROJECT_ID:
    print(
        "[ERROR] GCP_PROJECT_ID is not set.\n"
        "        Add it to your .env file or export it in your shell."
    )
    sys.exit(1)

BQ_DATASET = os.environ.get("BQ_DATASET", "market_analytics")
BQ_TABLE   = "mart_daily_metrics"
FULL_TABLE = f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"

print(f"[INFO] Target table: {FULL_TABLE}")

# ---------------------------------------------------------------------------
# Imports that depend on the env being ready
# ---------------------------------------------------------------------------
import pandas as pd                         # noqa: E402
import plotly.graph_objects as go          # noqa: E402
import plotly.io as pio                    # noqa: E402
from plotly.subplots import make_subplots  # noqa: E402
from google.cloud import bigquery          # noqa: E402

# ---------------------------------------------------------------------------
# Apple-inspired design system
# ---------------------------------------------------------------------------
COLOR = {
    "AAPL":       "#0071e3",                  # Apple blue
    "TSLA":       "#e31937",                  # Tesla red
    "SPY":        "#30d158",                  # Apple green
    "AAPL_sma20": "rgba(0,113,227,0.35)",     # lighter AAPL for SMA20
    "AAPL_sma50": "rgba(0,113,227,0.55)",     # mid AAPL for SMA50
    "TSLA_sma20": "rgba(227,25,55,0.35)",
    "TSLA_sma50": "rgba(227,25,55,0.55)",
    "SPY_sma20":  "rgba(48,209,88,0.35)",
    "SPY_sma50":  "rgba(48,209,88,0.55)",
    "bg":         "#0a0a0a",
    "paper":      "#000000",
    "card":       "#111111",
    "border":     "#2a2a2a",
    "text":       "#f5f5f7",
    "secondary":  "#86868b",
    "pos":        "#30d158",                  # Apple green for positive values
    "neg":        "#ff453a",                  # Apple red for negative values
    "zero_line":  "rgba(255,255,255,0.15)",
}

SYMBOLS   = ["AAPL", "TSLA", "SPY"]
FONT_FMLY = '-apple-system, "SF Pro Display", "Helvetica Neue", Arial, sans-serif'

# Axis style applied uniformly to all xy subplots
AXIS_STYLE = dict(
    gridcolor="#1a1a1a",
    linecolor="#2a2a2a",
    zerolinecolor="#2a2a2a",
    tickfont=dict(color="#86868b", size=11, family=FONT_FMLY),
    title_font=dict(color="#86868b", family=FONT_FMLY),
    showgrid=True,
    showline=True,
    mirror=False,
)


# ===========================================================================
# 1. Query BigQuery
# ===========================================================================
def fetch_data() -> pd.DataFrame:
    print("[INFO] Connecting to BigQuery ...")
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
        SELECT
            date,
            symbol,
            adjusted_close,
            volume,
            daily_return,
            cumulative_return,
            sma_20,
            sma_50,
            sma_200,
            rolling_volatility_20d,
            drawdown,
            excess_return_vs_spy
        FROM {FULL_TABLE}
        ORDER BY symbol, date
    """
    print("[INFO] Running BigQuery query ...")
    df = client.query(query).to_dataframe(create_bqstorage_client=False)

    if df.empty:
        print(
            "[ERROR] BigQuery returned no rows.\n"
            f"        Make sure {FULL_TABLE} exists and contains data.\n"
            "        Run: make dbt-run  to populate the table."
        )
        sys.exit(1)

    df["date"] = pd.to_datetime(df["date"])
    print(
        f"[INFO] Fetched {len(df):,} rows  |  "
        f"date range: {df['date'].min().date()} → {df['date'].max().date()}"
    )
    return df


# ===========================================================================
# 2. KPI summary table (row 1)
# ===========================================================================
def build_kpi_table(df: pd.DataFrame) -> go.Table:
    """Build a styled Plotly Table with latest KPIs for each symbol."""
    symbols_out, prices, ret1d, retcum, vols, drawdowns = [], [], [], [], [], []

    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date")
        if s.empty:
            continue
        last = s.iloc[-1]

        def fmt_pct(val, decimals=2):
            if pd.isna(val):
                return "N/A"
            return f"{val:+.{decimals}f}%"

        symbols_out.append(sym)
        prices.append(f"${last['adjusted_close']:.2f}" if pd.notna(last["adjusted_close"]) else "N/A")
        ret1d.append(fmt_pct(last["daily_return"]))
        retcum.append(fmt_pct(last["cumulative_return"]))
        vols.append(f"{last['rolling_volatility_20d']:.2f}%" if pd.notna(last["rolling_volatility_20d"]) else "N/A")
        drawdowns.append(fmt_pct(last["drawdown"]))

    # Color cells: symbol column uses symbol color; return cells use green/red
    def pct_color(val_str):
        if val_str == "N/A":
            return COLOR["secondary"]
        return COLOR["pos"] if val_str.startswith("+") else COLOR["neg"]

    sym_fill   = [COLOR[s] for s in symbols_out]
    ret1d_fill = [pct_color(v) for v in ret1d]
    cum_fill   = [pct_color(v) for v in retcum]
    dd_fill    = [pct_color(v) for v in drawdowns]
    neutral    = ["rgba(17,17,17,0.0)"] * len(symbols_out)

    header_vals = ["<b>Symbol</b>", "<b>Latest Price</b>", "<b>1-Day Return</b>",
                   "<b>Cumulative Return</b>", "<b>Volatility (20d)</b>", "<b>Drawdown</b>"]
    cell_vals   = [symbols_out, prices, ret1d, retcum, vols, drawdowns]
    cell_colors = [
        sym_fill,     # symbol column — brand color background
        neutral,
        ret1d_fill,
        cum_fill,
        neutral,
        dd_fill,
    ]
    cell_font_colors = [
        [COLOR["text"]] * len(symbols_out),
        [COLOR["text"]] * len(symbols_out),
        [COLOR["text"]] * len(symbols_out),
        [COLOR["text"]] * len(symbols_out),
        [COLOR["secondary"]] * len(symbols_out),
        [COLOR["text"]] * len(symbols_out),
    ]

    return go.Table(
        columnwidth=[70, 110, 110, 140, 130, 100],
        header=dict(
            values=header_vals,
            fill_color="rgba(42,42,42,0.6)",
            font=dict(color=COLOR["text"], size=12, family=FONT_FMLY),
            align="center",
            height=36,
            line_color=COLOR["border"],
        ),
        cells=dict(
            values=cell_vals,
            fill_color=cell_colors,
            font=dict(color=cell_font_colors, size=13, family=FONT_FMLY),
            align="center",
            height=34,
            line_color=COLOR["border"],
        ),
    )


# ===========================================================================
# 3. Tile 1 — Categorical Distribution: Performance Snapshot by Symbol
# ===========================================================================
def build_tile1_figure(df: pd.DataFrame) -> go.Figure:
    """
    Standalone grouped bar chart for Tile 1.
    Groups: Cumulative Return | Avg Daily Volatility | Current Drawdown
    One bar per symbol per group.
    """
    metrics = {
        "Cumulative Return (%)": [],
        "Avg Daily Volatility":  [],
        "Current Drawdown (%)":  [],
    }

    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date")
        if s.empty:
            metrics["Cumulative Return (%)"].append(0)
            metrics["Avg Daily Volatility"].append(0)
            metrics["Current Drawdown (%)"].append(0)
            continue
        last = s.iloc[-1]
        metrics["Cumulative Return (%)"].append(
            round(float(last["cumulative_return"]), 2) if pd.notna(last["cumulative_return"]) else 0
        )
        metrics["Avg Daily Volatility"].append(
            round(float(s["rolling_volatility_20d"].dropna().mean()), 2)
            if not s["rolling_volatility_20d"].dropna().empty else 0
        )
        metrics["Current Drawdown (%)"].append(
            round(float(last["drawdown"]), 2) if pd.notna(last["drawdown"]) else 0
        )

    fig = go.Figure()

    group_labels  = list(metrics.keys())
    group_x       = group_labels

    for sym in SYMBOLS:
        idx = SYMBOLS.index(sym)
        y_vals = [metrics[g][idx] for g in group_labels]
        text_vals = [f"{v:+.2f}%" for v in y_vals]

        fig.add_trace(go.Bar(
            name=sym,
            x=group_x,
            y=y_vals,
            marker=dict(
                color=COLOR[sym],
                opacity=0.85,
                line=dict(color=COLOR[sym], width=1),
            ),
            text=text_vals,
            textposition="outside",
            textfont=dict(color=COLOR["text"], size=11, family=FONT_FMLY),
            width=0.22,
        ))

    # Zero reference line
    fig.add_shape(
        type="line", x0=0, x1=1, xref="paper",
        y0=0, y1=0, yref="y",
        line=dict(color=COLOR["zero_line"], width=1, dash="dot"),
    )

    fig.update_layout(
        barmode="group",
        paper_bgcolor=COLOR["paper"],
        plot_bgcolor=COLOR["bg"],
        font=dict(family=FONT_FMLY, color=COLOR["text"]),
        title=dict(
            text=(
                "Performance Snapshot by Symbol"
                "<br><sup style='color:#86868b;font-size:13px'>"
                "Comparative metrics across AAPL, TSLA, and SPY"
                "</sup>"
            ),
            font=dict(size=20, color=COLOR["text"]),
            x=0.05, xanchor="left",
        ),
        legend=dict(
            bgcolor="rgba(17,17,17,0.8)",
            bordercolor=COLOR["border"],
            borderwidth=1,
            font=dict(color=COLOR["text"], size=12, family=FONT_FMLY),
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        xaxis=dict(
            **{k: v for k, v in AXIS_STYLE.items() if k != "showgrid"},
            title="Metric",
            showgrid=False,
        ),
        yaxis=dict(
            **{k: v for k, v in AXIS_STYLE.items() if k != "zerolinecolor"},
            title="Value (%)",
            zeroline=True,
            zerolinecolor=COLOR["zero_line"],
            zerolinewidth=1,
        ),
        margin=dict(l=60, r=60, t=100, b=60),
        hoverlabel=dict(
            bgcolor="#1a1a1a",
            bordercolor=COLOR["border"],
            font=dict(color=COLOR["text"], family=FONT_FMLY),
        ),
        height=600,
    )
    return fig


def add_tile1_traces(fig: go.Figure, df: pd.DataFrame, row: int):
    """Add Tile 1 traces to the main dashboard figure."""
    tile1 = build_tile1_figure(df)
    for trace in tile1.data:
        t = trace
        t.showlegend = True
        t.legendgroup = f"tile1_{t.name}"
        fig.add_trace(t, row=row, col=1)

    fig.add_shape(
        type="line", x0=0, x1=1, xref="paper",
        y0=0, y1=0, yref=f"y{row}",
        line=dict(color=COLOR["zero_line"], width=1, dash="dot"),
    )


# ===========================================================================
# 4. Tile 2 — Temporal Distribution: Price History & Moving Averages
# ===========================================================================
def build_tile2_figure(df: pd.DataFrame) -> go.Figure:
    """
    Standalone time-series chart for Tile 2.
    Solid lines: adjusted_close per symbol.
    Dashed lines: SMA20 per symbol (lighter shade of symbol color).
    Dotted lines: SMA50 per symbol (mid shade of symbol color).
    """
    fig = go.Figure()

    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date")
        if s.empty:
            continue

        col      = COLOR[sym]
        col_s20  = COLOR.get(f"{sym}_sma20", col)
        col_s50  = COLOR.get(f"{sym}_sma50", col)

        # Adjusted close — solid, full color
        fig.add_trace(go.Scatter(
            x=s["date"],
            y=s["adjusted_close"],
            name=sym,
            legendgroup=sym,
            mode="lines",
            line=dict(color=col, width=2),
            hovertemplate=(
                f"<b>{sym}</b><br>"
                "Date: %{x|%b %d, %Y}<br>"
                "Price: $%{y:.2f}<extra></extra>"
            ),
        ))

        # SMA 20 — dashed, lighter shade
        s_sma20 = s.dropna(subset=["sma_20"])
        fig.add_trace(go.Scatter(
            x=s_sma20["date"],
            y=s_sma20["sma_20"],
            name=f"{sym} SMA20",
            legendgroup=f"{sym}_sma",
            legendgrouptitle=dict(text="") if sym == "AAPL" else None,
            mode="lines",
            line=dict(color=col_s20, width=1, dash="dash"),
            hovertemplate=(
                f"<b>{sym} SMA20</b><br>"
                "Date: %{x|%b %d, %Y}<br>"
                "SMA20: $%{y:.2f}<extra></extra>"
            ),
        ))

        # SMA 50 — dotted, mid shade
        s_sma50 = s.dropna(subset=["sma_50"])
        fig.add_trace(go.Scatter(
            x=s_sma50["date"],
            y=s_sma50["sma_50"],
            name=f"{sym} SMA50",
            legendgroup=f"{sym}_sma",
            mode="lines",
            line=dict(color=col_s50, width=1, dash="dot"),
            hovertemplate=(
                f"<b>{sym} SMA50</b><br>"
                "Date: %{x|%b %d, %Y}<br>"
                "SMA50: $%{y:.2f}<extra></extra>"
            ),
        ))

    fig.update_layout(
        paper_bgcolor=COLOR["paper"],
        plot_bgcolor=COLOR["bg"],
        font=dict(family=FONT_FMLY, color=COLOR["text"]),
        title=dict(
            text=(
                "Price History & Moving Averages"
                "<br><sup style='color:#86868b;font-size:13px'>"
                "Adjusted close price with SMA 20 and SMA 50 trend lines"
                "</sup>"
            ),
            font=dict(size=20, color=COLOR["text"]),
            x=0.05, xanchor="left",
        ),
        legend=dict(
            bgcolor="rgba(17,17,17,0.8)",
            bordercolor=COLOR["border"],
            borderwidth=1,
            font=dict(color=COLOR["text"], size=11, family=FONT_FMLY),
            orientation="v",
            yanchor="top", y=1,
            xanchor="left", x=1.01,
        ),
        xaxis=dict(
            **AXIS_STYLE,
            title="Date",
            rangeselector=dict(
                buttons=[
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=3, label="3M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(step="all", label="All"),
                ],
                bgcolor=COLOR["card"],
                activecolor=COLOR["AAPL"],
                bordercolor=COLOR["border"],
                font=dict(color=COLOR["text"], size=11, family=FONT_FMLY),
            ),
            rangeslider=dict(visible=False),
            type="date",
        ),
        yaxis=dict(
            **AXIS_STYLE,
            title="Price (USD)",
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="#1a1a1a",
            bordercolor=COLOR["border"],
            font=dict(color=COLOR["text"], family=FONT_FMLY),
        ),
        margin=dict(l=60, r=140, t=100, b=60),
        height=600,
    )
    return fig


def add_tile2_traces(fig: go.Figure, df: pd.DataFrame, row: int):
    """Add Tile 2 traces to the main dashboard figure."""
    tile2 = build_tile2_figure(df)
    for trace in tile2.data:
        t = trace
        t.legendgroup = f"tile2_{t.legendgroup}"
        fig.add_trace(t, row=row, col=1)


# ===========================================================================
# 5. Tile 3 (Bonus) — Drawdown Over Time
# ===========================================================================
def build_drawdown_figure(df: pd.DataFrame) -> go.Figure:
    """Area chart of drawdown (%) over time for each symbol."""
    fig = go.Figure()

    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date").dropna(subset=["drawdown"])
        if s.empty:
            continue

        hex_col = COLOR[sym].lstrip("#")
        r_val   = int(hex_col[0:2], 16)
        g_val   = int(hex_col[2:4], 16)
        b_val   = int(hex_col[4:6], 16)
        fill    = f"rgba({r_val},{g_val},{b_val},0.15)"

        fig.add_trace(go.Scatter(
            x=s["date"],
            y=s["drawdown"],
            name=sym,
            legendgroup=f"dd_{sym}",
            mode="lines",
            line=dict(color=COLOR[sym], width=1.5),
            fill="tozeroy",
            fillcolor=fill,
            hovertemplate=(
                f"<b>{sym} Drawdown</b><br>"
                "Date: %{x|%b %d, %Y}<br>"
                "Drawdown: %{y:.2f}%<extra></extra>"
            ),
        ))

    fig.add_shape(
        type="line", x0=0, x1=1, xref="paper",
        y0=0, y1=0, yref="y",
        line=dict(color=COLOR["zero_line"], width=1, dash="dot"),
    )
    return fig


def add_tile3_traces(fig: go.Figure, df: pd.DataFrame, row: int):
    """Add Tile 3 (drawdown) traces to the main dashboard figure."""
    tile3 = build_drawdown_figure(df)
    for trace in tile3.data:
        t = trace
        t.legendgroup = f"tile3_{t.legendgroup}"
        fig.add_trace(t, row=row, col=1)

    fig.add_shape(
        type="line", x0=0, x1=1, xref="paper",
        y0=0, y1=0, yref=f"y{row}",
        line=dict(color=COLOR["zero_line"], width=1, dash="dot"),
    )


# ===========================================================================
# 6. Assemble full dashboard figure
# ===========================================================================
def build_dashboard(df: pd.DataFrame) -> go.Figure:
    date_min  = df["date"].min().strftime("%b %d, %Y")
    date_max  = df["date"].max().strftime("%b %d, %Y")
    generated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ROW_TABLE = 1
    ROW_TILE1 = 2   # Categorical bar chart
    ROW_TILE2 = 3   # Time-series price + SMAs
    ROW_TILE3 = 4   # Drawdown area chart

    fig = make_subplots(
        rows=4, cols=1,
        row_heights=[0.12, 0.28, 0.32, 0.28],
        subplot_titles=[
            "",
            "Performance Snapshot by Symbol",
            "Price History & Moving Averages",
            "Drawdown Over Time",
        ],
        vertical_spacing=0.06,
        specs=[
            [{"type": "table"}],
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "xy"}],
        ],
    )

    # ── KPI Summary Table ──────────────────────────────────────────────────
    print("[INFO] Building KPI summary table ...")
    fig.add_trace(build_kpi_table(df), row=ROW_TABLE, col=1)

    # ── Tile 1: Categorical bar chart ─────────────────────────────────────
    print("[INFO] Building Tile 1 — Performance Snapshot (categorical) ...")
    add_tile1_traces(fig, df, row=ROW_TILE1)

    # ── Tile 2: Time-series price + SMAs ──────────────────────────────────
    print("[INFO] Building Tile 2 — Price History & Moving Averages (temporal) ...")
    add_tile2_traces(fig, df, row=ROW_TILE2)

    # ── Tile 3: Drawdown area chart ───────────────────────────────────────
    print("[INFO] Building Tile 3 — Drawdown Over Time ...")
    add_tile3_traces(fig, df, row=ROW_TILE3)

    # ── Global layout ──────────────────────────────────────────────────────
    fig.update_layout(
        height=2000,
        template="plotly_dark",
        paper_bgcolor=COLOR["paper"],
        plot_bgcolor=COLOR["bg"],
        font=dict(
            family=FONT_FMLY,
            color=COLOR["text"],
            size=12,
        ),
        title=dict(
            text=(
                "Market Intelligence Dashboard"
                "<br><sup style='color:#86868b'>"
                f"AAPL · TSLA · SPY — Last 100 Trading Days"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;"
                f"Data: {date_min} – {date_max}"
                f"&nbsp;&nbsp;|&nbsp;&nbsp;"
                f"Generated: {generated}"
                "</sup>"
            ),
            font=dict(size=28, color=COLOR["text"], family=FONT_FMLY),
            x=0.05, xanchor="left", y=0.98,
        ),
        legend=dict(
            bgcolor="rgba(17,17,17,0.8)",
            bordercolor=COLOR["border"],
            borderwidth=1,
            font=dict(color=COLOR["text"], size=11, family=FONT_FMLY),
        ),
        margin=dict(l=60, r=60, t=100, b=60),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="#1a1a1a",
            bordercolor=COLOR["border"],
            font=dict(color=COLOR["text"], family=FONT_FMLY),
        ),
    )

    # ── Subplot title styling ──────────────────────────────────────────────
    for annotation in fig.layout.annotations:
        annotation.font.size    = 16
        annotation.font.color   = COLOR["text"]
        annotation.font.family  = FONT_FMLY
        annotation.x            = 0.05
        annotation.xanchor      = "left"

    # ── Axis styling — Tile 1 (row 2) ─────────────────────────────────────
    fig.update_xaxes(
        row=ROW_TILE1, col=1,
        showgrid=False,
        **{k: v for k, v in AXIS_STYLE.items() if k not in ("showgrid",)},
        title_text="Metric",
    )
    fig.update_yaxes(
        row=ROW_TILE1, col=1,
        **{k: v for k, v in AXIS_STYLE.items() if k not in ("zerolinecolor",)},
        title_text="Value (%)",
        zeroline=True,
        zerolinecolor=COLOR["zero_line"],
        zerolinewidth=1,
    )

    # ── Axis styling — Tile 2 (row 3) ─────────────────────────────────────
    fig.update_xaxes(
        row=ROW_TILE2, col=1,
        **AXIS_STYLE,
        title_text="Date",
        rangeselector=dict(
            buttons=[
                dict(count=1, label="1M", step="month", stepmode="backward"),
                dict(count=3, label="3M", step="month", stepmode="backward"),
                dict(count=6, label="6M", step="month", stepmode="backward"),
                dict(step="all", label="All"),
            ],
            bgcolor=COLOR["card"],
            activecolor=COLOR["AAPL"],
            bordercolor=COLOR["border"],
            font=dict(color=COLOR["text"], size=11, family=FONT_FMLY),
        ),
        type="date",
    )
    fig.update_yaxes(
        row=ROW_TILE2, col=1,
        **AXIS_STYLE,
        title_text="Price (USD)",
    )

    # ── Axis styling — Tile 3 (row 4) ─────────────────────────────────────
    fig.update_xaxes(
        row=ROW_TILE3, col=1,
        **AXIS_STYLE,
        title_text="Date",
        type="date",
    )
    fig.update_yaxes(
        row=ROW_TILE3, col=1,
        **{k: v for k, v in AXIS_STYLE.items() if k not in ("zerolinecolor",)},
        title_text="Drawdown (%)",
        zeroline=True,
        zerolinecolor=COLOR["zero_line"],
        zerolinewidth=1,
    )

    return fig


# ===========================================================================
# 7. Export standalone tile PNGs
# ===========================================================================
def export_tile_screenshots(df: pd.DataFrame):
    """Export Tile 1, Tile 2, and Tile 3 as standalone PNG screenshots using kaleido."""
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

    tile1_path = os.path.join(SCREENSHOTS_DIR, "tile1_categorical.png")
    tile2_path = os.path.join(SCREENSHOTS_DIR, "tile2_temporal.png")
    tile3_path = os.path.join(SCREENSHOTS_DIR, "tile3_drawdown.png")

    print("[INFO] Exporting Tile 1 screenshot ...")
    try:
        fig_tile1 = build_tile1_figure(df)
        fig_tile1.write_image(tile1_path, width=1200, height=600, scale=2)
        print(f"[INFO] Tile 1 saved to {tile1_path}")
    except Exception as exc:
        print(f"[WARN] Could not export Tile 1 PNG: {exc}")
        print("[WARN] Ensure kaleido is installed: pip install kaleido>=0.2.1")

    print("[INFO] Exporting Tile 2 screenshot ...")
    try:
        fig_tile2 = build_tile2_figure(df)
        fig_tile2.write_image(tile2_path, width=1200, height=600, scale=2)
        print(f"[INFO] Tile 2 saved to {tile2_path}")
    except Exception as exc:
        print(f"[WARN] Could not export Tile 2 PNG: {exc}")
        print("[WARN] Ensure kaleido is installed: pip install kaleido>=0.2.1")

    print("[INFO] Exporting Tile 3 screenshot ...")
    try:
        fig_tile3 = build_drawdown_figure(df)
        fig_tile3.write_image(tile3_path, width=1200, height=600, scale=2)
        print(f"[INFO] Tile 3 saved to {tile3_path}")
    except Exception as exc:
        print(f"[WARN] Could not export Tile 3 PNG: {exc}")
        print("[WARN] Ensure kaleido is installed: pip install kaleido>=0.2.1")


# ===========================================================================
# 8. Save main dashboard HTML
# ===========================================================================
def save_dashboard(fig: go.Figure):
    os.makedirs(SCRIPT_DIR, exist_ok=True)
    print(f"[INFO] Writing dashboard to {OUTPUT_HTML} ...")
    fig.write_html(
        OUTPUT_HTML,
        include_plotlyjs=True,
        full_html=True,
        config={
            "displayModeBar": True,
            "displaylogo": False,
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],
            "toImageButtonOptions": {
                "format": "png",
                "filename": "market_intelligence_dashboard",
                "height": 2000,
                "width": 1600,
                "scale": 2,
            },
        },
    )
    size_mb = os.path.getsize(OUTPUT_HTML) / 1_048_576
    print(f"[INFO] Dashboard saved  ({size_mb:.1f} MB)")


# ===========================================================================
# 9. Entry point
# ===========================================================================
def main():
    print("=" * 65)
    print("  Market Intelligence Dashboard Generator  —  Apple Design")
    print("=" * 65)

    df  = fetch_data()
    fig = build_dashboard(df)
    save_dashboard(fig)
    export_tile_screenshots(df)

    print("=" * 65)
    print(f"[DONE] Dashboard → {OUTPUT_HTML}")
    print(f"[DONE] Screenshots → {SCREENSHOTS_DIR}/")
    print("       Open dashboard/dashboard.html in your browser.")
    print("=" * 65)


if __name__ == "__main__":
    main()
