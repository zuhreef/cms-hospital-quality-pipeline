"""Shared data access + chart styling for the dashboard.

The dashboard reads only the published gold Parquet snapshot (data/gold), via an
in-memory DuckDB connection. It never touches the warehouse file, so it keeps
serving the last good publish while the pipeline runs.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

GOLD_DIR = Path(os.getenv("GOLD_DIR", Path(__file__).resolve().parents[1] / "data" / "gold"))

# ---- design tokens (validated reference palette, light mode) ----------------
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
# diverging semantics for "vs national": better = blue pole, worse = red pole, neutral gray midpoint
BETTER, WORSE, NEUTRAL = "#2a78d6", "#e34948", "#c3c2b7"
STATUS = {"good": "#0ca30c", "warning": "#fab219", "critical": "#d03b3b"}

pio.templates["hq"] = go.layout.Template(
    layout=go.Layout(
        font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', size=13, color=INK_2),
        title=dict(font=dict(size=15, color=INK), x=0, xanchor="left", y=0.985, yanchor="top", yref="container"),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        colorway=SERIES,
        margin=dict(l=12, r=16, t=72, b=12),
        xaxis=dict(gridcolor=GRID, linecolor=AXIS, zeroline=False, automargin=True,
                   tickfont=dict(color=MUTED), title_font=dict(color=INK_2)),
        yaxis=dict(gridcolor=GRID, linecolor=AXIS, zeroline=False, automargin=True,
                   tickfont=dict(color=MUTED), title_font=dict(color=INK_2)),
        hoverlabel=dict(bgcolor="#ffffff", bordercolor=GRID, font=dict(color=INK, size=12)),
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0, font=dict(color=INK_2)),
        bargap=0.25,
    )
)
pio.templates.default = "hq"
PLOTLY_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    # map shapes are bundled in dashboard/static (Streamlit static serving) instead of fetched from a CDN
    "topojsonURL": "./app/static/",
}


def chart(fig: go.Figure, height: int = 360) -> None:
    # Set explicitly: importing Streamlit's plotly support overrides pio.templates.default.
    fig.layout.template = pio.templates["hq"]  # assignment replaces (update_layout would merge)
    fig.update_layout(height=height, plot_bgcolor=SURFACE, paper_bgcolor=SURFACE)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG, theme=None)


# ---- data access -------------------------------------------------------------
def _publish_stamp() -> str:
    parts = []
    for sub in ("marts", "ops"):
        f = GOLD_DIR / sub / "_published.json"
        parts.append(f.read_text() if f.exists() else "")
    return "|".join(parts)


@st.cache_data(show_spinner=False)
def _query(sql: str, stamp: str) -> pd.DataFrame:
    con = duckdb.connect()
    for sub in ("marts", "ops"):
        for f in (GOLD_DIR / sub).glob("*.parquet"):
            con.execute(f"create or replace view {f.stem} as select * from read_parquet('{f}')")
    return con.execute(sql).df()


def query(sql: str) -> pd.DataFrame:
    """Run SQL against the gold views; cache invalidates automatically on each publish."""
    return _query(sql, _publish_stamp())


def published_meta() -> dict:
    f = GOLD_DIR / "marts" / "_published.json"
    return json.loads(f.read_text()) if f.exists() else {}


def require_data() -> None:
    if not (GOLD_DIR / "marts" / "mart_hospital_scorecard.parquet").exists():
        st.warning("No published data yet. Run the pipeline first: `make run` (or trigger the DAG in Airflow).")
        st.stop()


def metric(col, label: str, value: str, note: str | None = None, help: str | None = None) -> None:
    """KPI tile with a neutral caption instead of Streamlit's delta arrow (arrows imply direction/goodness)."""
    col.metric(label, value, help=help)
    if note:
        col.caption(note)


def stars(n) -> str:
    return "—" if pd.isna(n) else "★" * int(n) + "☆" * (5 - int(n))


def fmt_int(n) -> str:
    return f"{int(n):,}" if pd.notna(n) else "—"


def page_header(title: str, subtitle: str) -> None:
    st.markdown(f"## {title}")
    meta = published_meta()
    release = query("select max(release_date) d from dim_hospital")["d"].iloc[0]
    stamp = meta.get("published_at", "")[:16].replace("T", " ")
    st.caption(f"{subtitle}  ·  CMS release **{pd.to_datetime(release):%b %d, %Y}**  ·  published {stamp} UTC")
    src = query("select source from pipeline_runs where status = 'success' order by finished_at desc limit 1")
    if not src.empty and src.source.iloc[0] == "sample":
        st.info("Demo mode: showing **synthetic sample data** shaped like the CMS feeds. "
                "Run the pipeline with `--source api` for live CMS data.", icon=":material/science:")
