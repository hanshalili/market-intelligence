"""
generate_dashboard.py — Apple & Tesla Market Intelligence Dashboard

Queries the mart_daily_metrics BigQuery table and produces a self-contained
interactive Plotly HTML dashboard saved to dashboard/dashboard.html.

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
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
ENV_FILE     = os.path.join(PROJECT_ROOT, ".env")
SA_FILE      = os.path.join(PROJECT_ROOT, "airflow", "credentials", "service_account.json")
OUTPUT_HTML  = os.path.join(SCRIPT_DIR, "dashboard.html")

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
import pandas as pd                          # noqa: E402
import plotly.graph_objects as go           # noqa: E402
from plotly.subplots import make_subplots   # noqa: E402
from google.cloud import bigquery           # noqa: E402

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
COLOR = {
    "AAPL":    "#00D4FF",              # cyan
    "TSLA":    "#FF6B35",              # orange
    "SPY":     "#7FFF00",              # chartreuse
    "SMA20":   "rgba(255,255,255,0.5)",
    "SMA50":   "rgba(255,200,0,0.7)",
    "SMA200":  "rgba(255,100,100,0.7)",
    "pos":     "#26A69A",              # teal-green for positive bars
    "neg":     "#EF5350",              # red for negative bars
    "bg":      "#0a0a0a",
    "paper":   "#111111",
    "text":    "#E0E0E0",
    "grid":    "rgba(255,255,255,0.06)",
    "border":  "rgba(255,255,255,0.12)",
}

SYMBOLS   = ["AAPL", "TSLA", "SPY"]
FONT_FMLY = "Inter, Helvetica Neue, Arial, sans-serif"


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
    print(f"[INFO] Fetched {len(df):,} rows  |  date range: {df['date'].min().date()} → {df['date'].max().date()}")
    return df


# ===========================================================================
# 2. Build summary stats table
# ===========================================================================
def build_summary_table(df: pd.DataFrame) -> go.Table:
    rows = []
    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date")
        if s.empty:
            continue
        last = s.iloc[-1]
        rows.append(
            {
                "Symbol":          sym,
                "Latest Price":    f"${last['adjusted_close']:.2f}",
                "1-Day Return":    f"{last['daily_return']:+.2f}%" if pd.notna(last["daily_return"]) else "N/A",
                "Cumul. Return":   f"{last['cumulative_return']:+.2f}%" if pd.notna(last["cumulative_return"]) else "N/A",
                "Drawdown":        f"{last['drawdown']:.2f}%"           if pd.notna(last["drawdown"]) else "N/A",
                "Volatility (20d)": f"{last['rolling_volatility_20d']:.2f}%" if pd.notna(last["rolling_volatility_20d"]) else "N/A",
            }
        )

    if not rows:
        return go.Table()

    import itertools
    keys   = list(rows[0].keys())
    vals   = [[r[k] for r in rows] for k in keys]

    # Colour the symbol column
    sym_colors = [COLOR.get(sym, COLOR["text"]) for sym in [r["Symbol"] for r in rows]]
    cell_colors = [sym_colors] + [["rgba(30,30,30,0.9)"] * len(rows)] * (len(keys) - 1)

    return go.Table(
        columnwidth=[80, 110, 110, 120, 100, 130],
        header=dict(
            values=[f"<b>{k}</b>" for k in keys],
            fill_color="rgba(255,255,255,0.08)",
            font=dict(color=COLOR["text"], size=12, family=FONT_FMLY),
            align="center",
            height=36,
            line_color=COLOR["border"],
        ),
        cells=dict(
            values=vals,
            fill_color=cell_colors,
            font=dict(color=COLOR["text"], size=12, family=FONT_FMLY),
            align="center",
            height=32,
            line_color=COLOR["border"],
        ),
    )


# ===========================================================================
# 3. Chart helpers
# ===========================================================================
def _axis_style(title: str = "", show_grid: bool = True) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=11, color="rgba(255,255,255,0.55)", family=FONT_FMLY)),
        showgrid=show_grid,
        gridcolor=COLOR["grid"],
        gridwidth=1,
        zeroline=False,
        color="rgba(255,255,255,0.45)",
        tickfont=dict(size=10, family=FONT_FMLY),
        linecolor=COLOR["border"],
        showline=True,
        mirror=False,
    )


def add_price_traces(fig, df: pd.DataFrame, row_map: dict):
    """Chart 1: Adjusted close + SMA20/50/200, one subplot per symbol."""
    for sym in SYMBOLS:
        r   = row_map[sym]
        s   = df[df["symbol"] == sym].sort_values("date")
        col = COLOR[sym]

        # Adjusted close
        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["adjusted_close"],
                name=f"{sym}",
                line=dict(color=col, width=2),
                legendgroup=sym,
                showlegend=True,
            ),
            row=r, col=1,
        )
        # SMA20
        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["sma_20"],
                name="SMA 20" if sym == "AAPL" else None,
                line=dict(color=COLOR["SMA20"], width=1, dash="dot"),
                legendgroup="SMA20",
                showlegend=(sym == "AAPL"),
            ),
            row=r, col=1,
        )
        # SMA50
        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["sma_50"],
                name="SMA 50" if sym == "AAPL" else None,
                line=dict(color=COLOR["SMA50"], width=1, dash="dot"),
                legendgroup="SMA50",
                showlegend=(sym == "AAPL"),
            ),
            row=r, col=1,
        )
        # SMA200
        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["sma_200"],
                name="SMA 200" if sym == "AAPL" else None,
                line=dict(color=COLOR["SMA200"], width=1, dash="dash"),
                legendgroup="SMA200",
                showlegend=(sym == "AAPL"),
            ),
            row=r, col=1,
        )


def add_daily_return_traces(fig, df: pd.DataFrame, row_map: dict):
    """Chart 2: Daily return % bar chart, green/red, one subplot per symbol."""
    for sym in SYMBOLS:
        r = row_map[sym]
        s = df[df["symbol"] == sym].sort_values("date").dropna(subset=["daily_return"])

        bar_colors = [COLOR["pos"] if v >= 0 else COLOR["neg"] for v in s["daily_return"]]
        fig.add_trace(
            go.Bar(
                x=s["date"], y=s["daily_return"],
                name=sym,
                marker_color=bar_colors,
                marker_line_width=0,
                legendgroup=f"dr_{sym}",
                showlegend=True,
            ),
            row=r, col=1,
        )


def add_cumulative_return_trace(fig, df: pd.DataFrame, row: int, col: int = 1):
    """Chart 3: Cumulative return %, all symbols on same chart."""
    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date").dropna(subset=["cumulative_return"])
        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["cumulative_return"],
                name=sym,
                line=dict(color=COLOR[sym], width=2),
                legendgroup=f"cr_{sym}",
                showlegend=True,
            ),
            row=row, col=col,
        )
    # Zero reference line — use add_shape to avoid conflict with Table subplot
    fig.add_shape(type="line", x0=0, x1=1, xref="paper",
                  y0=0, y1=0, yref=f"y{row}",
                  line=dict(color="rgba(255,255,255,0.2)", width=1, dash="dot"))


def add_volatility_trace(fig, df: pd.DataFrame, row: int, col: int = 1):
    """Chart 4: Rolling 20-day volatility, all symbols on same chart."""
    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date").dropna(subset=["rolling_volatility_20d"])
        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["rolling_volatility_20d"],
                name=sym,
                line=dict(color=COLOR[sym], width=2),
                legendgroup=f"vol_{sym}",
                showlegend=True,
            ),
            row=row, col=col,
        )


def add_drawdown_trace(fig, df: pd.DataFrame, row: int, col: int = 1):
    """Chart 5: Drawdown % area chart filled below zero."""
    for sym in SYMBOLS:
        s = df[df["symbol"] == sym].sort_values("date").dropna(subset=["drawdown"])
        fill_color = COLOR[sym].replace("#", "")
        # Convert hex to rgba for fill
        hex_col = COLOR[sym].lstrip("#")
        r_val   = int(hex_col[0:2], 16)
        g_val   = int(hex_col[2:4], 16)
        b_val   = int(hex_col[4:6], 16)
        fill    = f"rgba({r_val},{g_val},{b_val},0.18)"

        fig.add_trace(
            go.Scatter(
                x=s["date"], y=s["drawdown"],
                name=sym,
                line=dict(color=COLOR[sym], width=1.5),
                fill="tozeroy",
                fillcolor=fill,
                legendgroup=f"dd_{sym}",
                showlegend=True,
            ),
            row=row, col=col,
        )
    fig.add_shape(type="line", x0=0, x1=1, xref="paper",
                  y0=0, y1=0, yref=f"y{row}",
                  line=dict(color="rgba(255,255,255,0.2)", width=1, dash="dot"))


def add_excess_return_trace(fig, df: pd.DataFrame, row: int, col: int = 1):
    """Chart 6: Excess return vs SPY bar chart (AAPL & TSLA only)."""
    for sym in ["AAPL", "TSLA"]:
        s = df[df["symbol"] == sym].sort_values("date").dropna(subset=["excess_return_vs_spy"])
        bar_colors = [COLOR["pos"] if v >= 0 else COLOR["neg"] for v in s["excess_return_vs_spy"]]
        fig.add_trace(
            go.Bar(
                x=s["date"], y=s["excess_return_vs_spy"],
                name=sym,
                marker_color=bar_colors,
                marker_line_width=0,
                legendgroup=f"er_{sym}",
                showlegend=True,
            ),
            row=row, col=col,
        )


# ===========================================================================
# 4. Assemble full dashboard figure
# ===========================================================================
def build_dashboard(df: pd.DataFrame) -> go.Figure:
    date_min = df["date"].min().strftime("%b %d, %Y")
    date_max = df["date"].max().strftime("%b %d, %Y")
    generated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------
    # Row layout (row indices, 1-based):
    #   1      — summary table
    #   2,3,4  — price chart (one sub-row per symbol AAPL/TSLA/SPY)
    #   5,6,7  — daily return bars (one sub-row per symbol)
    #   8      — cumulative return
    #   9      — rolling volatility
    #   10     — drawdown
    #   11     — excess return vs SPY
    # ------------------------------------------------------------------
    ROW_TABLE   = 1
    ROW_PRICE   = {sym: i for i, sym in enumerate(SYMBOLS, start=2)}   # rows 2,3,4
    ROW_DRET    = {sym: i for i, sym in enumerate(SYMBOLS, start=5)}   # rows 5,6,7
    ROW_CUMRET  = 8
    ROW_VOL     = 9
    ROW_DD      = 10
    ROW_EXCESS  = 11
    N_ROWS      = 11

    # Row heights — table short, price rows taller, rest medium
    row_heights = (
        [0.06]           # table
        + [0.085] * 3    # price subplots
        + [0.065] * 3    # daily return subplots
        + [0.095]        # cumulative
        + [0.085]        # volatility
        + [0.085]        # drawdown
        + [0.085]        # excess return
    )

    subplot_titles = (
        [""]                                         # table (no title above it)
        + [f"<b>{sym}</b> — Adjusted Close & Moving Averages" for sym in SYMBOLS]
        + [f"<b>{sym}</b> — Daily Return (%)" for sym in SYMBOLS]
        + ["<b>Cumulative Return (%)</b> — All Symbols"]
        + ["<b>Rolling 20-Day Volatility (Annualised %)</b> — All Symbols"]
        + ["<b>Drawdown (%) from 52-Week High</b> — All Symbols"]
        + ["<b>Excess Return vs SPY (%)</b> — AAPL & TSLA"]
    )

    specs = [[{"type": "table"}]] + [[{"type": "scatter"}]] * (N_ROWS - 1)

    fig = make_subplots(
        rows=N_ROWS,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.022,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
        specs=specs,
    )

    # ── Table ──────────────────────────────────────────────────────────
    print("[INFO] Building summary table ...")
    fig.add_trace(build_summary_table(df), row=ROW_TABLE, col=1)

    # ── Chart 1: Price + SMAs ──────────────────────────────────────────
    print("[INFO] Building price & moving average charts ...")
    add_price_traces(fig, df, ROW_PRICE)

    # ── Chart 2: Daily return bars ─────────────────────────────────────
    print("[INFO] Building daily return charts ...")
    add_daily_return_traces(fig, df, ROW_DRET)

    # ── Chart 3: Cumulative return ─────────────────────────────────────
    print("[INFO] Building cumulative return chart ...")
    add_cumulative_return_trace(fig, df, row=ROW_CUMRET)

    # ── Chart 4: Rolling volatility ────────────────────────────────────
    print("[INFO] Building rolling volatility chart ...")
    add_volatility_trace(fig, df, row=ROW_VOL)

    # ── Chart 5: Drawdown ──────────────────────────────────────────────
    print("[INFO] Building drawdown chart ...")
    add_drawdown_trace(fig, df, row=ROW_DD)

    # ── Chart 6: Excess return vs SPY ──────────────────────────────────
    print("[INFO] Building excess return chart ...")
    add_excess_return_trace(fig, df, row=ROW_EXCESS)

    # ------------------------------------------------------------------
    # Global layout
    # ------------------------------------------------------------------
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=COLOR["paper"],
        plot_bgcolor=COLOR["bg"],
        font=dict(family=FONT_FMLY, color=COLOR["text"], size=12),
        title=dict(
            text=(
                "<b style='font-size:22px'>Apple & Tesla Market Intelligence Dashboard</b>"
                f"<br><span style='font-size:12px;color:rgba(255,255,255,0.55)'>"
                f"Data: {date_min} — {date_max} &nbsp;|&nbsp; Generated: {generated}</span>"
            ),
            x=0.5,
            xanchor="center",
            y=0.995,
            yanchor="top",
            font=dict(family=FONT_FMLY, color=COLOR["text"]),
        ),
        legend=dict(
            bgcolor="rgba(17,17,17,0.85)",
            bordercolor=COLOR["border"],
            borderwidth=1,
            font=dict(size=11, family=FONT_FMLY, color=COLOR["text"]),
            orientation="h",
            yanchor="bottom",
            y=1.01,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=60, r=40, t=80, b=40),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="rgba(17,17,17,0.92)",
            bordercolor=COLOR["border"],
            font=dict(family=FONT_FMLY, size=11, color=COLOR["text"]),
        ),
        height=3800,
    )

    # ── Per-axis styling ───────────────────────────────────────────────
    # Price subplots y-axes
    for sym, r in ROW_PRICE.items():
        fig.update_yaxes(_axis_style(title="USD ($)"), row=r, col=1)
        fig.update_xaxes(
            dict(showgrid=False, showticklabels=(r == max(ROW_PRICE.values())),
                 color="rgba(255,255,255,0.45)", tickfont=dict(size=10, family=FONT_FMLY),
                 linecolor=COLOR["border"], showline=True),
            row=r, col=1,
        )

    # Daily return subplots
    for sym, r in ROW_DRET.items():
        fig.update_yaxes(_axis_style(title="Return (%)"), row=r, col=1)
        fig.update_xaxes(
            dict(showgrid=False, showticklabels=(r == max(ROW_DRET.values())),
                 color="rgba(255,255,255,0.45)", tickfont=dict(size=10, family=FONT_FMLY),
                 linecolor=COLOR["border"], showline=True),
            row=r, col=1,
        )

    # Cumulative return
    fig.update_yaxes(_axis_style(title="Cumul. Return (%)"), row=ROW_CUMRET, col=1)
    fig.update_xaxes(showgrid=False, row=ROW_CUMRET, col=1)

    # Volatility
    fig.update_yaxes(_axis_style(title="Volatility (%)"), row=ROW_VOL, col=1)
    fig.update_xaxes(showgrid=False, row=ROW_VOL, col=1)

    # Drawdown
    fig.update_yaxes(_axis_style(title="Drawdown (%)"), row=ROW_DD, col=1)
    fig.update_xaxes(showgrid=False, row=ROW_DD, col=1)

    # Excess return
    fig.update_yaxes(_axis_style(title="Excess Ret. (%)"), row=ROW_EXCESS, col=1)
    fig.update_xaxes(showgrid=False, row=ROW_EXCESS, col=1)

    # Subplot title styling (annotation colour)
    for ann in fig.layout.annotations:
        ann.font.update(size=12, color="rgba(255,255,255,0.70)", family=FONT_FMLY)

    return fig


# ===========================================================================
# 5. Save to HTML
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
                "height": 3800,
                "width": 1600,
                "scale": 2,
            },
        },
    )
    size_mb = os.path.getsize(OUTPUT_HTML) / 1_048_576
    print(f"[INFO] Dashboard saved  ({size_mb:.1f} MB)")


# ===========================================================================
# 6. Entry point
# ===========================================================================
def main():
    print("=" * 60)
    print("  Apple & Tesla Market Intelligence Dashboard Generator")
    print("=" * 60)

    df  = fetch_data()
    fig = build_dashboard(df)
    save_dashboard(fig)

    print("=" * 60)
    print("[DONE] Open dashboard/dashboard.html in your browser.")
    print("=" * 60)


if __name__ == "__main__":
    main()
