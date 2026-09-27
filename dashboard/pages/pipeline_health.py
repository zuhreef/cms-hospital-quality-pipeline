import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from lib import SERIES, STATUS, chart, fmt_int, metric, published_meta, query, require_data

require_data()
st.markdown("## Pipeline health")
st.caption("Observability for the pipeline itself: runs, task timings, data volumes per layer and data-quality gate results.")

runs = query("select * from pipeline_runs order by started_at desc")
tasks = query("select * from task_runs order by started_at")
dq = query("select * from dq_results where run_id = (select max(run_id) from dq_results)")
manifest = query("select * from ingest_manifest order by dataset, release_date")

last = runs.iloc[0]
last_ok = runs[runs.status == "success"].head(1)
icon = {"success": "✅", "failed": "❌", "running": "⏳"}
k1, k2, k3, k4 = st.columns(4)
k1.metric("Last run", f"{icon.get(last.status, '•')} {last.status.title()}", help=f"run_id {last.run_id}")
metric(k2, "Last successful run", "—" if last_ok.empty else f"{last_ok.finished_at.iloc[0]:%b %d %H:%M}",
       note="UTC")
passed = int(dq.passed.sum())
metric(k3, "DQ checks passed (latest)", f"{passed} / {len(dq)}",
       note=f"{int((~dq.passed & (dq.severity == 'warn')).sum())} warnings · "
            f"{int((~dq.passed & (dq.severity == 'error')).sum())} blocking failures")
k4.metric("Latest CMS release loaded", manifest.release_date.max())

st.divider()

# ---- lineage ---------------------------------------------------------------------------
counts = published_meta().get("tables", {})
bronze_rows = manifest.groupby("release_date").rows.sum()
latest_silver = tasks[tasks.task == "spark.bronze_to_silver"]
silver_tables = {}
for d in latest_silver.details:
    for t in json.loads(d).get("tables", []):
        if t["rows_out"]:
            silver_tables[t["table"]] = t
silver_rows = sum(t["rows_out"] for t in silver_tables.values())
quarantined = sum(t["quarantined"] for t in silver_tables.values())
dupes = sum(t["duplicates_removed"] for t in silver_tables.values())

st.markdown("**Data flow**")
stages = [
    ("CMS Provider Data API", "4 datasets · paginated, retried", "#f3f2ee", "#c3c2b7"),
    ("Bronze", f"raw Parquet · {fmt_int(bronze_rows.sum())} rows · {len(bronze_rows)} releases", "#ffffff", "#c3c2b7"),
    ("Silver · PySpark", f"{fmt_int(silver_rows)} typed rows · {fmt_int(dupes)} dupes removed · "
                         f"{fmt_int(quarantined)} quarantined", "#ffffff", "#c3c2b7"),
    ("DQ gate", f"{passed}/{len(dq)} checks passed", "#eef7ee", "#0ca30c"),
    ("Gold · dbt + DuckDB", f"{len(counts)} marts · {fmt_int(sum(counts.values()))} rows", "#ffffff", "#c3c2b7"),
    ("Dashboard", "Parquet snapshot → Streamlit", "#e8f1fc", "#2a78d6"),
]
cards = "<span class='arrow'>→</span>".join(
    f"<div class='stage' style='background:{bg};border-color:{bd}'><b>{t}</b><span>{sub}</span></div>"
    for t, sub, bg, bd in stages)
st.markdown(f"""
<style>
.flow {{display:flex;align-items:stretch;gap:6px;flex-wrap:wrap;margin:4px 0 12px}}
.flow .stage {{flex:1 1 140px;border:1px solid;border-radius:10px;padding:10px 12px;display:flex;flex-direction:column;gap:4px}}
.flow .stage b {{font-size:14px;color:#0b0b0b}}
.flow .stage span {{font-size:12.5px;color:#52514e;line-height:1.35}}
.flow .arrow {{align-self:center;color:#898781;font-size:18px}}
</style>
<div class='flow'>{cards}</div>""", unsafe_allow_html=True)

left, right = st.columns([3, 2], gap="large")
with left:
    run_pick = st.selectbox("Run", runs.run_id.tolist(), index=0,
                            format_func=lambda r: f"{r}  ·  {runs.set_index('run_id').status[r]}")
    lr = tasks[tasks.run_id == run_pick].copy()
    lr["label"] = lr.task
    fig = go.Figure(go.Bar(
        y=lr.label, x=lr.duration_s, orientation="h", marker_cornerradius=4,
        marker_color=[STATUS["critical"] if s == "failed" else SERIES[0] for s in lr.status],
        text=[f"{d:.1f}s" for d in lr.duration_s], textposition="outside", textfont=dict(color="#52514e"),
        hovertemplate="%{y}: %{x:.2f}s<extra></extra>",
    ))
    fig.update_layout(title=f"Task durations ({lr.duration_s.sum():.0f}s total)",
                      xaxis=dict(title="seconds", range=[0, lr.duration_s.max() * 1.2]),
                      yaxis=dict(autorange="reversed"))
    chart(fig, 340)

with right:
    hist = runs.head(15).copy()
    hist["duration_s"] = (hist.finished_at - hist.started_at).dt.total_seconds()
    st.markdown("**Recent runs**")
    st.dataframe(
        hist.assign(status=hist.status.map(lambda s: f"{icon.get(s, '•')} {s}"))[
            ["run_id", "source", "trigger", "status", "duration_s", "error"]],
        hide_index=True, use_container_width=True,
        column_config={"duration_s": st.column_config.NumberColumn("Duration (s)", format="%.0f")},
    )

st.markdown("**Data quality gate - latest run**")
dq_view = dq.assign(result=[
    "✅ pass" if p else ("⚠️ warn" if s == "warn" else "❌ fail") for p, s in zip(dq.passed, dq.severity, strict=True)
])[["result", "dataset", "check_name", "severity", "observed", "threshold", "message"]]
order = {"❌ fail": 0, "⚠️ warn": 1, "✅ pass": 2}
st.dataframe(dq_view.sort_values(["result", "dataset"], key=lambda c: c.map(order) if c.name == "result" else c),
             hide_index=True, use_container_width=True)

c1, c2 = st.columns(2, gap="large")
with c1:
    st.markdown("**Ingestion manifest (bronze)**")
    st.dataframe(manifest[["dataset", "release_date", "rows", "content_sha256", "ingested_at"]]
                 .assign(content_sha256=manifest.content_sha256.str[:12]),
                 hide_index=True, use_container_width=True)
with c2:
    st.markdown("**Silver layer - per-table cleaning stats**")
    st.dataframe(pd.DataFrame(silver_tables.values()).drop(columns=["releases"]), hide_index=True,
                 use_container_width=True,
                 column_config={"table": "silver <- bronze", "rows_in": "Rows in", "rows_out": "Rows out",
                                "quarantined": "Quarantined", "duplicates_removed": "Dupes removed",
                                "unparseable_numerics": "Bad numerics"})
