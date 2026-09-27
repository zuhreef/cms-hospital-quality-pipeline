import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from lib import BETTER, NEUTRAL, SERIES, WORSE, chart, page_header, query, require_data, stars

require_data()
page_header("Hospital explorer", "Filter, rank and drill into individual hospitals")

sc = query("select * from mart_hospital_scorecard")

# ---- one filter row scoping everything below --------------------------------------------
f1, f2, f3, f4 = st.columns([2, 3, 2, 2])
state = f1.selectbox("State", ["All states", *sorted(sc.state.dropna().unique())])
types = f2.multiselect("Hospital type", sorted(sc.hospital_type.unique()), placeholder="All types")
owners = f3.multiselect("Ownership", sorted(sc.ownership_group.unique()), placeholder="All")
min_star = f4.select_slider("Min. CMS rating", options=["Any", 1, 2, 3, 4, 5], value="Any")

df = sc.copy()
if state != "All states":
    df = df[df.state == state]
if types:
    df = df[df.hospital_type.isin(types)]
if owners:
    df = df[df.ownership_group.isin(owners)]
if min_star != "Any":
    df = df[df.overall_rating >= min_star]
df = df.sort_values(["outcome_composite", "overall_rating"], ascending=False, na_position="last")

st.caption(f"{len(df):,} hospitals match. Click a row to see its detail.")
def _delta(v) -> str:
    return "" if pd.isna(v) or v == 0 else (f"▲ {int(v)}" if v > 0 else f"▼ {abs(int(v))}")


table = df.assign(rating=df.overall_rating.map(stars), px_star=df.patient_experience_star.map(stars),
                  rating_change=df.rating_change.map(_delta))[[
    "facility_name", "state", "hospital_type", "ownership_group", "rating", "rating_change",
    "px_star", "outcome_composite", "measures_better", "measures_worse",
]]
event = st.dataframe(
    table, hide_index=True, use_container_width=True, height=360,
    on_select="rerun", selection_mode="single-row",
    column_config={
        "facility_name": st.column_config.TextColumn("Hospital", width="medium"),
        "state": st.column_config.TextColumn("State", width="small"),
        "hospital_type": "Type", "ownership_group": "Ownership",
        "rating": "CMS rating",
        "rating_change": st.column_config.TextColumn("Δ", help="Rating change vs. previous CMS release", width="small"),
        "px_star": "Patient experience",
        "outcome_composite": st.column_config.ProgressColumn(
            "Outcome composite", min_value=0, max_value=100, format="%.0f",
            help="Average national percentile across rated outcome measures (100 = best). Needs ≥ 5 rated measures."),
        "measures_better": st.column_config.NumberColumn("Better", help="# measures better than national rate", width="small"),
        "measures_worse": st.column_config.NumberColumn("Worse", help="# measures worse than national rate", width="small"),
    },
)

rows = event.selection.rows if event and event.selection else []
if not rows:
    if df.empty:
        st.stop()
    st.info("Select a hospital in the table to see its measure-level detail. "
            "Showing the top-ranked match with full survey and outcome data.",
            icon=":material/touch_app:")
if rows:
    h = df.iloc[rows[0]]
else:  # default to the best-ranked hospital with a full data profile
    rich = df[(df.measures_rated >= 15) & df.patient_experience_star.notna()]
    h = (rich if not rich.empty else df).iloc[0]
fid = h.facility_id

st.divider()
st.markdown(f"### {h.facility_name}")
st.caption(f"{h.city}, {h.state}  ·  {h.hospital_type}  ·  {h.ownership}  ·  CCN {fid}")

m1, m2, m3, m4 = st.columns(4)
m1.metric("CMS overall rating", stars(h.overall_rating),
          delta=None if pd.isna(h.rating_change) or h.rating_change == 0 else f"{int(h.rating_change):+d} vs prior release")
m2.metric("Patient experience", stars(h.patient_experience_star))
m3.metric("Outcome composite", "—" if pd.isna(h.outcome_composite) else f"{h.outcome_composite:.0f} / 100",
          help=h.outcome_tier)
m4.metric("Would definitely recommend", "—" if pd.isna(h.pct_definitely_recommend) else f"{h.pct_definitely_recommend:.0f}%")

left, right = st.columns([3, 2], gap="large")
with left:
    ms = query(f"""
        select f.measure_id, m.measure_short_name, f.domain, f.comparison_category, f.score, f.lower_estimate,
               f.higher_estimate, f.national_median, f.performance_percentile, m.unit
        from fct_measure_scores f join dim_measure m using (measure_id)
        where f.facility_id = '{fid}'
          and f.release_date = (select max(release_date) from fct_measure_scores)
          and f.performance_percentile is not null
        order by f.domain desc, f.performance_percentile
    """)
    if ms.empty:
        st.info("CMS reports no rated outcome measures for this hospital (often too few cases).")
    else:
        colors = {"better": BETTER, "worse": WORSE, "no_different": NEUTRAL}
        names = {"better": "Better than national", "no_different": "No different", "worse": "Worse than national"}
        fig = go.Figure()
        for cat in ["better", "no_different", "worse"]:
            d = ms[ms.comparison_category == cat]
            if d.empty:
                continue
            fig.add_bar(
                y=d.domain + " · " + d.measure_short_name, x=d.performance_percentile, orientation="h",
                name=names[cat], marker_color=colors[cat], marker_cornerradius=4,
                customdata=d[["score", "national_median", "lower_estimate", "higher_estimate", "unit"]],
                hovertemplate="<b>%{y}</b><br>Percentile %{x:.0f}<br>Score %{customdata[0]} %{customdata[4]}"
                              " (95% CI %{customdata[2]}–%{customdata[3]})<br>National median %{customdata[1]}<extra></extra>",
            )
        fig.add_vline(x=50, line_color="#898781", line_width=1)
        fig.update_layout(title="National percentile by measure (100 = best)", barmode="overlay",
                          margin=dict(t=90), legend=dict(y=1.02, yanchor="bottom", orientation="h"),
                          xaxis=dict(range=[0, 100], title="Percentile"),
                          yaxis=dict(categoryorder="array", categoryarray=list(ms.domain + " · " + ms.measure_short_name)))
        chart(fig, max(320, 26 * len(ms) + 110))
        st.caption("Vertical line = national median (50th percentile). Hover a bar for the score and its 95% CI.")

with right:
    pe = query(f"""
        select * from fct_patient_experience
        where facility_id = '{fid}' and release_date = (select max(release_date) from fct_patient_experience)
    """)
    dims = {"nurse_communication_star": "Nurse communication", "doctor_communication_star": "Doctor communication",
            "staff_responsiveness_star": "Staff responsiveness", "medicine_communication_star": "Medicine info",
            "discharge_info_star": "Discharge info", "care_transition_star": "Care transition",
            "cleanliness_star": "Cleanliness", "quietness_star": "Quietness", "recommend_star": "Recommend"}
    if pe.empty or pe[list(dims)].isna().all(axis=1).iloc[0]:
        st.info("No HCAHPS patient survey results for this hospital.")
    else:
        vals = pe.iloc[0][list(dims)].astype(float)
        fig = go.Figure(go.Bar(y=list(dims.values()), x=vals.values, orientation="h", marker_color=SERIES[0],
                               marker_cornerradius=4, hovertemplate="%{y}: %{x} ★<extra></extra>",
                               text=[f"{v:.0f} ★" for v in vals.values], textposition="outside",
                               textfont=dict(color="#52514e")))
        fig.update_layout(title="Patient survey (HCAHPS) star ratings",
                          xaxis=dict(range=[0, 5.8], tickvals=[1, 2, 3, 4, 5]),
                          yaxis=dict(autorange="reversed"))
        chart(fig, 360)
        st.caption(f"{int(pe.completed_surveys.iloc[0]):,} completed surveys · "
                   f"{int(pe.response_rate_pct.iloc[0])}% response rate")

    hist = query(f"""
        select valid_from, valid_to, overall_rating, ownership, hospital_type, is_current
        from dim_hospital_history where facility_id = '{fid}' order by valid_from
    """)
    st.markdown("**Attribute history (SCD Type 2)**")
    st.dataframe(hist.assign(overall_rating=hist.overall_rating.map(stars),
                             valid_to=hist.valid_to.dt.strftime("%Y-%m-%d").fillna("open (current)"),
                             valid_from=hist.valid_from.dt.strftime("%Y-%m-%d")),
                 hide_index=True, use_container_width=True,
                 column_config={"valid_from": "Valid from", "valid_to": "Valid to", "overall_rating": "Rating",
                                "ownership": "Ownership", "hospital_type": "Type", "is_current": "Current"})
