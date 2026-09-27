import plotly.graph_objects as go
import streamlit as st
from lib import SERIES, chart, metric, page_header, query, require_data

require_data()
page_header("Measure benchmarks", "What good looks like for each outcome measure, nationally and by state")

bm = query("""
    select * from mart_measure_benchmarks
    where release_date = (select max(release_date) from mart_measure_benchmarks)
    order by domain, measure_short_name
""")

f1, f2 = st.columns([3, 2])
label = (bm.domain + " · " + bm.measure_short_name).tolist()
choice = f1.selectbox("Measure", label, index=label.index(next(x for x in label if "Heart failure 30-day readmission" in x))
                      if any("Heart failure 30-day readmission" in x for x in label) else 0)
row = bm.iloc[label.index(choice)]
mid = row.measure_id
states = query(f"select distinct h.state from fct_measure_scores f join dim_hospital h using (facility_id) "
               f"where measure_id = '{mid}' order by 1").state.tolist()
focus = f2.selectbox("Highlight a state", ["None", *states], index=states.index("CA") + 1 if "CA" in states else 0)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Hospitals reporting", f"{int(row.hospitals_reported):,}")
k2.metric("National median", f"{row['median']:g}", help=row.unit)
k3.metric("Best 10% at or below", f"{row.p10:g}", help="Lower is better for every measure in this set")
metric(k4, "Flagged worse than national", f"{row.pct_worse:.1f}%", note=f"{row.pct_better:.1f}% flagged better")

scores = query(f"""
    select f.score, h.state from fct_measure_scores f join dim_hospital h using (facility_id)
    where f.measure_id = '{mid}' and f.score is not null
      and f.release_date = (select max(release_date) from fct_measure_scores)
""")

left, right = st.columns(2, gap="large")
with left:
    fig = go.Figure(go.Histogram(x=scores.score, nbinsx=40, marker_color=SERIES[0], marker_line_color="#fcfcfb",
                                 marker_line_width=2, hovertemplate="%{x}: %{y} hospitals<extra></extra>"))
    for q, name in [(row.p10, "P10"), (row["median"], "Median"), (row.p90, "P90")]:
        fig.add_vline(x=q, line_color="#52514e" if name == "Median" else "#898781", line_width=1,
                      annotation_text=name, annotation_position="top",
                      annotation_font=dict(size=11, color="#52514e"))
    fig.update_layout(title=f"Distribution of hospital scores ({row.unit})", xaxis_title=row.unit,
                      yaxis_title="Hospitals", bargap=0)
    chart(fig, 400)

with right:
    by_state = (scores.groupby("state").score.agg(["median", "count"]).query("count >= 5")
                .sort_values("median", ascending=False).reset_index())
    colors = [SERIES[0] if s == focus else "#c3c2b7" for s in by_state.state] if focus != "None" \
        else [SERIES[0]] * len(by_state)
    # dot plot, not bars: medians don't share a zero baseline (some measures are negative)
    fig = go.Figure(go.Scatter(x=by_state.state, y=by_state["median"], mode="markers",
                               marker=dict(color=colors, size=[13 if s == focus else 9 for s in by_state.state],
                                           line=dict(color="#fcfcfb", width=2)),
                               customdata=by_state["count"],
                               hovertemplate="%{x}: median %{y:.2f} (%{customdata} hospitals)<extra></extra>"))
    fig.add_hline(y=row["median"], line_color="#52514e", line_width=1, annotation_text="national median",
                  annotation_position="top right", annotation_font=dict(size=11, color="#52514e"))
    fig.update_layout(title="State median score (≥ 5 reporting hospitals; lower = better, right = best)",
                      yaxis=dict(title=row.unit), xaxis=dict(tickangle=-90, tickfont=dict(size=10), showgrid=False))
    chart(fig, 400)

st.markdown("**All measures - latest release**")
st.dataframe(
    bm[["domain", "measure_short_name", "unit", "hospitals_reported", "p10", "p25", "median", "p75", "p90",
        "pct_better", "pct_worse"]],
    hide_index=True, use_container_width=True,
    column_config={"domain": "Domain", "measure_short_name": "Measure", "unit": "Unit",
                   "hospitals_reported": "Hospitals", "pct_better": "% better", "pct_worse": "% worse"},
)
