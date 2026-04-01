"""
generate_index.py — Read dashboard/data.json and write dashboard/index.html
with real BigQuery data embedded directly in the page.
"""
import json, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(ROOT, "dashboard", "data.json")) as f:
    D = json.load(f)

T   = D["tickers"]
CL  = D["close"]
OP  = D["open"]
DTS = D["dates"]

COLORS = {"NVDA":"#76b900","AAPL":"#0071e3","GOOGL":"#d97706","TSLA":"#b91c1c","VOO":"#7c3aed"}
SYMS   = ["NVDA","AAPL","GOOGL","TSLA","VOO"]

def fmt_ret(v):
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"

def ret_class(v):
    return "pos" if v >= 0 else "neg"

# KPI cards
kpi_cards = ""
for sym in SYMS:
    t   = T[sym]
    col = COLORS[sym]
    rc  = ret_class(t["cumulative_return"])
    kpi_cards += f"""
      <div class="kpi-card">
        <div class="kpi-top">
          <span class="kpi-ticker"><span class="kpi-dot" style="background:{col}"></span>{sym}</span>
          <span class="kpi-ret {rc}">{fmt_ret(t["cumulative_return"])}</span>
        </div>
        <div class="kpi-close">${t['close']:,.2f}</div>
        <div class="kpi-ohlc">O&nbsp;${t['open']:,.2f}&nbsp;&nbsp;·&nbsp;&nbsp;C&nbsp;${t['close']:,.2f}</div>
        <div class="kpi-meta">Vol&nbsp;{t['volatility']:.1f}%&nbsp;&nbsp;·&nbsp;&nbsp;DD&nbsp;{t['drawdown']:.1f}%</div>
      </div>"""

# Stats table rows
stats_rows = ""
for sym in SYMS:
    t   = T[sym]
    col = COLORS[sym]
    rc  = ret_class(t["cumulative_return"])
    drc = ret_class(t["daily_return"])
    stats_rows += f"""
            <tr>
              <td><span class="kpi-dot" style="background:{col}"></span>{sym}</td>
              <td>${t['close']:,.2f}</td>
              <td class="{rc}-val">{fmt_ret(t["cumulative_return"])}</td>
              <td class="{drc}-val">{fmt_ret(t["daily_return"])}</td>
              <td>{t['volatility']:.1f}%</td>
              <td class="neg-val">{t['drawdown']:.1f}%</td>
            </tr>"""

# Determine best/worst
best  = max(SYMS, key=lambda s: T[s]["cumulative_return"])
worst = min(SYMS, key=lambda s: T[s]["cumulative_return"])
most_vol = max(SYMS, key=lambda s: T[s]["volatility"])
bench = T["VOO"]
date_label = T["NVDA"]["date"]

# Summary paragraph
pos_syms = [s for s in SYMS if T[s]["cumulative_return"] > 0]
neg_syms = [s for s in SYMS if T[s]["cumulative_return"] <= 0]
pos_str  = ", ".join(pos_syms) if pos_syms else "none"
neg_str  = ", ".join(neg_syms) if neg_syms else "none"

summary_p1 = (
    f"Over the 100 trading days ending {date_label}, "
    f"{'all five instruments declined' if not pos_syms else f'{pos_str} gained while {neg_str} declined'}. "
    f"{worst} was the weakest performer at {fmt_ret(T[worst]['cumulative_return'])} with a "
    f"peak drawdown of {T[worst]['drawdown']:.1f}% and annualised volatility of {T[worst]['volatility']:.1f}%. "
    f"{best} led the group at {fmt_ret(T[best]['cumulative_return'])}."
)
summary_p2 = (
    f"{most_vol} carried the highest annualised volatility at {T[most_vol]['volatility']:.1f}%, "
    f"reflecting elevated single-name risk. "
    f"VOO — the Vanguard S&amp;P 500 benchmark — posted {fmt_ret(bench['cumulative_return'])} "
    f"with a drawdown of {bench['drawdown']:.1f}% and volatility of {bench['volatility']:.1f}%, "
    f"underscoring the diversification benefit relative to individual names. "
    f"Data is ingested daily after NYSE close via Alpha Vantage and transformed in BigQuery through dbt."
)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Market Intelligence</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
    :root{{
      --bg:#fff;--border:#e3e3e3;--text-1:#0a0a0a;--text-2:#4a4a4a;--text-3:#9a9a9a;
      --neg:#b91c1c;--pos:#15803d;
      --nvda:#76b900;--aapl:#0071e3;--googl:#d97706;--tsla:#b91c1c;--voo:#7c3aed;
      --mono:'JetBrains Mono','Fira Code',ui-monospace,monospace;
      --sans:'Inter',system-ui,-apple-system,sans-serif;
      --r:6px;--pad:48px;--max:1280px;
    }}
    html{{font-size:16px}}
    body{{background:var(--bg);color:var(--text-1);font-family:var(--sans);line-height:1.6;-webkit-font-smoothing:antialiased}}

    /* Header */
    header{{border-bottom:1px solid var(--border);padding:18px var(--pad)}}
    .header-inner{{max-width:var(--max);margin:0 auto;display:flex;align-items:center;justify-content:space-between}}
    .wordmark-label{{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--text-3);margin-bottom:2px}}
    .wordmark-title{{font-size:17px;font-weight:600;letter-spacing:-.02em}}
    .header-right{{display:flex;align-items:center;gap:20px}}
    .badge{{font-family:var(--mono);font-size:10px;color:var(--text-3);letter-spacing:.05em}}
    .live{{display:inline-flex;align-items:center;gap:6px;font-family:var(--mono);font-size:10px;color:var(--text-2)}}
    .live::before{{content:'';width:6px;height:6px;border-radius:50%;background:#16a34a;animation:pulse 2s ease-in-out infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.35}}}}

    /* KPI Strip */
    .kpi-strip{{border-bottom:1px solid var(--border)}}
    .kpi-inner{{max-width:var(--max);margin:0 auto;display:grid;grid-template-columns:repeat(5,1fr)}}
    .kpi-card{{padding:14px var(--pad) 14px 0;margin-left:var(--pad);border-right:1px solid var(--border);display:flex;flex-direction:column;gap:3px}}
    .kpi-card:last-child{{border-right:none}}
    .kpi-top{{display:flex;align-items:center;justify-content:space-between}}
    .kpi-ticker{{display:flex;align-items:center;gap:6px;font-family:var(--mono);font-size:10px;letter-spacing:.12em;color:var(--text-3)}}
    .kpi-dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0}}
    .kpi-ret{{font-family:var(--mono);font-size:11px;font-weight:600}}
    .neg{{color:var(--neg)}}.pos{{color:var(--pos)}}
    .kpi-close{{font-size:21px;font-weight:600;letter-spacing:-.03em;line-height:1.1}}
    .kpi-ohlc{{font-family:var(--mono);font-size:10px;color:var(--text-3)}}
    .kpi-meta{{font-family:var(--mono);font-size:10px;color:var(--text-3)}}

    /* Main */
    main{{max-width:var(--max);margin:0 auto;padding:32px var(--pad)}}
    .section-label{{font-family:var(--mono);font-size:10px;letter-spacing:.16em;text-transform:uppercase;color:var(--text-3);margin-bottom:14px}}

    /* Charts */
    .charts-grid{{display:grid;grid-template-columns:3fr 2fr;gap:10px;margin-bottom:44px}}
    .chart-card{{border:1px solid var(--border);border-radius:var(--r);padding:16px 18px;display:flex;flex-direction:column;gap:10px}}
    .chart-card--full{{grid-column:1/3}}
    .card-header{{display:flex;align-items:baseline;justify-content:space-between}}
    .card-title{{font-size:12px;font-weight:600;letter-spacing:-.01em}}
    .card-sub{{font-family:var(--mono);font-size:10px;color:var(--text-3)}}
    .legend{{display:flex;gap:12px;flex-wrap:wrap}}
    .legend-item{{display:inline-flex;align-items:center;gap:5px;font-family:var(--mono);font-size:10px;color:var(--text-2)}}
    .legend-dot{{width:7px;height:7px;border-radius:50%}}
    .chart-wrap{{position:relative}}
    .h280{{height:280px}}.h200{{height:200px}}.h180{{height:180px}}

    /* Summary */
    .summary{{border-top:1px solid var(--border);padding-top:32px}}
    .summary-grid{{display:grid;grid-template-columns:3fr 2fr;gap:48px;align-items:start}}
    .stats-table{{width:100%;border-collapse:collapse}}
    .stats-table th{{font-family:var(--mono);font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--text-3);text-align:right;padding:0 0 10px;font-weight:400}}
    .stats-table th:first-child{{text-align:left}}
    .stats-table td{{font-family:var(--mono);font-size:11px;padding:8px 0;border-top:1px solid var(--border);text-align:right;color:var(--text-2)}}
    .stats-table td:first-child{{text-align:left;display:flex;align-items:center;gap:7px;font-weight:600;color:var(--text-1)}}
    .neg-val{{color:var(--neg)}}.pos-val{{color:var(--pos)}}
    .summary-text h2{{font-size:14px;font-weight:600;letter-spacing:-.02em;margin-bottom:10px}}
    .summary-text p{{font-size:13px;color:var(--text-2);line-height:1.8;max-width:520px}}
    .summary-text p+p{{margin-top:8px}}

    /* Footer */
    footer{{border-top:1px solid var(--border);margin-top:52px;padding:18px var(--pad)}}
    .footer-inner{{max-width:var(--max);margin:0 auto;display:flex;justify-content:space-between}}
    .footer-copy{{font-family:var(--mono);font-size:10px;color:var(--text-3)}}

    /* Responsive */
    @media(max-width:900px){{
      :root{{--pad:20px}}
      .kpi-inner,.summary-grid{{grid-template-columns:1fr 1fr}}
      .charts-grid{{grid-template-columns:1fr}}
      .chart-card--full{{grid-column:1}}
      .header-right{{display:none}}
    }}
    @media(max-width:480px){{.kpi-inner{{grid-template-columns:1fr 1fr}}}}
    @media(prefers-reduced-motion:reduce){{.live::before{{animation:none}}}}
  </style>
</head>
<body>

<header>
  <div class="header-inner">
    <div>
      <div class="wordmark-label">Intelligence Platform</div>
      <div class="wordmark-title">Market Intelligence</div>
    </div>
    <div class="header-right">
      <span class="badge">NVDA · AAPL · GOOGL · TSLA · VOO</span>
      <span class="badge">Nov 2025 – Mar 2026 · 100 trading days</span>
      <span class="live">BigQuery Live</span>
    </div>
  </div>
</header>

<div class="kpi-strip">
  <div class="kpi-inner">{kpi_cards}</div>
</div>

<main>
  <p class="section-label">Charts · Nov 5 2025 – Mar 31 2026 · Source: Alpha Vantage → BigQuery mart_daily_metrics</p>

  <div class="charts-grid">

    <!-- Chart 1: Daily Close -->
    <div class="chart-card">
      <div class="card-header">
        <span class="card-title">Adjusted Close Price</span>
        <span class="card-sub">Daily · USD · 100 trading days</span>
      </div>
      <div class="legend">
        {"".join(f'<span class="legend-item"><span class="legend-dot" style="background:{COLORS[s]}"></span>{s}</span>' for s in SYMS)}
      </div>
      <div class="chart-wrap h280"><canvas id="chartClose"></canvas></div>
    </div>

    <!-- Chart 2: Cumulative Return -->
    <div class="chart-card">
      <div class="card-header">
        <span class="card-title">Cumulative Return %</span>
        <span class="card-sub">From Nov 5 2025</span>
      </div>
      <div class="legend">
        {"".join(f'<span class="legend-item"><span class="legend-dot" style="background:{COLORS[s]}"></span>{s}</span>' for s in SYMS)}
      </div>
      <div class="chart-wrap h280"><canvas id="chartCumRet"></canvas></div>
    </div>

    <!-- Chart 3: Open vs Close (full history) -->
    <div class="chart-card chart-card--full">
      <div class="card-header">
        <span class="card-title">Daily Open vs Close</span>
        <span class="card-sub">Dashed = open · Solid = close · Last 100 trading days</span>
      </div>
      <div class="legend" id="ocLegend"></div>
      <div class="chart-wrap h180"><canvas id="chartOC"></canvas></div>
    </div>

  </div>

  <!-- Summary -->
  <section class="summary">
    <p class="section-label">Analysis Summary · As of {date_label}</p>
    <div class="summary-grid">
      <table class="stats-table">
        <thead>
          <tr>
            <th>Ticker</th><th>Close</th><th>Cumul.</th><th>1-Day</th><th>Volatility</th><th>Drawdown</th>
          </tr>
        </thead>
        <tbody>{stats_rows}</tbody>
      </table>
      <div class="summary-text">
        <h2>Market Summary · Nov 2025 – Mar 2026</h2>
        <p>{summary_p1}</p>
        <p>{summary_p2}</p>
      </div>
    </div>
  </section>
</main>

<footer>
  <div class="footer-inner">
    <span class="footer-copy">© 2026 Market Intelligence · {" · ".join(SYMS)}</span>
    <span class="footer-copy">Alpha Vantage → GCS → BigQuery → dbt · Runs daily 21:00 UTC</span>
  </div>
</footer>

<script>
  Chart.defaults.font.family = "'Inter',system-ui,sans-serif";
  Chart.defaults.font.size   = 11;
  Chart.defaults.color       = '#9a9a9a';

  const G  = '#ebebeb';
  const TK = '#9a9a9a';
  const C  = {json.dumps(COLORS)};
  const SYMS = {json.dumps(SYMS)};

  const TT = {{
    backgroundColor:'#fff',borderColor:'#e3e3e3',borderWidth:1,
    titleColor:'#0a0a0a',bodyColor:'#4a4a4a',padding:10,cornerRadius:4
  }};

  const DATES = {json.dumps(DTS)};
  const CLOSE = {json.dumps(CL)};
  const OPEN  = {json.dumps(OP)};

  // ── 1. Close Price ──────────────────────────────
  // NVDA/AAPL/GOOGL on left axis, TSLA/VOO on right (different price scale)
  new Chart(document.getElementById('chartClose'), {{
    type:'line',
    data:{{
      labels:DATES,
      datasets: SYMS.map(s => ({{
        label:s, data:CLOSE[s],
        borderColor:C[s], backgroundColor:'transparent',
        borderWidth:1.5, pointRadius:0, tension:0.2,
        yAxisID:(['TSLA','VOO'].includes(s)?'y2':'y')
      }}))
    }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{display:false}},
        tooltip:{{...TT, callbacks:{{label:c=>` ${{c.dataset.label}}: $${{c.parsed.y.toFixed(2)}}`}}}}
      }},
      scales:{{
        x:{{grid:{{color:G}},ticks:{{color:TK,maxTicksLimit:8}}}},
        y:{{position:'left',grid:{{color:G}},ticks:{{color:TK,callback:v=>`$${{v}}`}},
            title:{{display:true,text:'NVDA · AAPL · GOOGL',color:TK,font:{{size:10}}}}}},
        y2:{{position:'right',grid:{{display:false}},ticks:{{color:TK,callback:v=>`$${{v}}`}},
             title:{{display:true,text:'TSLA · VOO',color:TK,font:{{size:10}}}}}}
      }}
    }}
  }});

  // ── 2. Cumulative Return % ──────────────────────
  const cumRet = sym => CLOSE[sym].map(v => +((v/CLOSE[sym][0]-1)*100).toFixed(2));
  new Chart(document.getElementById('chartCumRet'), {{
    type:'line',
    data:{{
      labels:DATES,
      datasets: SYMS.map(s => ({{
        label:s, data:cumRet(s),
        borderColor:C[s], backgroundColor:'transparent',
        borderWidth:1.5, pointRadius:0, tension:0.2
      }}))
    }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{display:false}},
        tooltip:{{...TT, callbacks:{{label:c=>` ${{c.dataset.label}}: ${{c.parsed.y>0?'+':''}}${{c.parsed.y.toFixed(2)}}%`}}}}
      }},
      scales:{{
        x:{{grid:{{color:G}},ticks:{{color:TK,maxTicksLimit:8}}}},
        y:{{grid:{{color:G}},ticks:{{color:TK,callback:v=>`${{v}}%`}}}}
      }}
    }}
  }});

  // ── 3. Open vs Close ────────────────────────────
  const ocLegend = document.getElementById('ocLegend');
  SYMS.forEach(s => {{
    const el = document.createElement('span');
    el.className = 'legend-item';
    el.innerHTML = `<span class="legend-dot" style="background:${{C[s]}}"></span>${{s}} open/close`;
    ocLegend.appendChild(el);
  }});

  const ocDs = [];
  SYMS.forEach(s => {{
    const yId = ['TSLA','VOO'].includes(s)?'y2':'y';
    ocDs.push({{label:`${{s}} Open`,data:OPEN[s],borderColor:C[s],borderWidth:1,
      borderDash:[4,3],pointRadius:0,tension:0.2,backgroundColor:'transparent',yAxisID:yId}});
    ocDs.push({{label:`${{s}} Close`,data:CLOSE[s],borderColor:C[s],borderWidth:2,
      pointRadius:0,tension:0.2,backgroundColor:'transparent',yAxisID:yId}});
  }});

  new Chart(document.getElementById('chartOC'), {{
    type:'line',
    data:{{labels:DATES, datasets:ocDs}},
    options:{{
      responsive:true, maintainAspectRatio:false,
      interaction:{{mode:'index',intersect:false}},
      plugins:{{
        legend:{{display:false}},
        tooltip:{{...TT, callbacks:{{label:c=>` ${{c.dataset.label}}: $${{c.parsed.y.toFixed(2)}}`}}}}
      }},
      scales:{{
        x:{{grid:{{color:G}},ticks:{{color:TK,maxTicksLimit:8}}}},
        y:{{position:'left',grid:{{color:G}},ticks:{{color:TK,callback:v=>`$${{v}}`}},
            title:{{display:true,text:'NVDA · AAPL · GOOGL',color:TK,font:{{size:10}}}}}},
        y2:{{position:'right',grid:{{display:false}},ticks:{{color:TK,callback:v=>`$${{v}}`}},
             title:{{display:true,text:'TSLA · VOO',color:TK,font:{{size:10}}}}}}
      }}
    }}
  }});
</script>
</body>
</html>"""

out = os.path.join(ROOT, "dashboard", "index.html")
with open(out, "w") as f:
    f.write(html)
print(f"Written → {out}")
