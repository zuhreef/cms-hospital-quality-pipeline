import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from lib import BETTER, BLUE_RAMP, SERIES, WORSE, chart, fmt_int, metric, page_header, query, require_data

require_data()
page_header("National overview", "How U.S. hospitals compare on CMS quality ratings and outcomes")

kpi = query("""
    select count(*) hospitals,
           count(overall_rating) rated,
           avg(overall_rating) avg_rating,
           100.0 * count(*) filter (where overall_rating >= 4) / count(overall_rating) pct_top,
           count(*) filter (where rating_change > 0) upgrades,
           count(*) filter (where rating_change < 0) downgrades,
           100.0 * sum(measures_worse) / sum(measures_rated) pct_worse
    from mart_hospital_scorecard
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Hospitals", fmt_int(kpi.hospitals), help="Active hospitals in the latest CMS release")
c2.metric("Avg. CMS star rating", f"{kpi.avg_rating:.2f} ★", help=f"Across {fmt_int(kpi.rated)} rated hospitals")
c3.metric("Rated 4–5 stars", f"{kpi.pct_top:.1f}%")
metric(c4, "Rating changes vs. prior release", f"{int(kpi.upgrades + kpi.downgrades):,}",
       note=f"▲ {int(kpi.upgrades)} upgraded · ▼ {int(kpi.downgrades)} downgraded")

st.divider()

# ---- map --------------------------------------------------------------------------------
MIN_RATED = 5
states = query("select * from mart_state_summary")
left, right = st.columns([3, 2], gap="large")
with left:
    metric = st.segmented_control(
        "Color states by",
        ["Avg. star rating", "% rated 4–5 stars", "Patient experience"],
        default="Avg. star rating",
    ) or "Avg. star rating"
    col = {"Avg. star rating": "avg_overall_rating", "% rated 4–5 stars": "pct_4_or_5_star",
           "Patient experience": "avg_patient_experience_star"}[metric]
    shown = states[states.rated_hospitals >= MIN_RATED]
    fig = px.choropleth(
        shown, locations="state", locationmode="USA-states", scope="usa", color=col,
        color_continuous_scale=BLUE_RAMP,
        custom_data=["state_name", "hospitals", "avg_overall_rating", "pct_4_or_5_star", "avg_patient_experience_star"],
    )
    fig.update_traces(
        marker_line_color="#fcfcfb", marker_line_width=1,
        hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]} hospitals"
                      "<br>Avg. rating %{customdata[2]:.2f} ★<br>Rated 4–5★: %{customdata[3]:.1f}%"
                      "<br>Patient experience %{customdata[4]:.2f} ★<extra></extra>",
    )
    fig.update_layout(title=f"{metric} by state", geo=dict(bgcolor="#fcfcfb", lakecolor="#fcfcfb", showlakes=False),
                      coloraxis_colorbar=dict(title="", thickness=10, len=0.6, tickfont=dict(color="#898781")))
    chart(fig, 420)
    st.caption(f"States with fewer than {MIN_RATED} rated hospitals are left blank. "
               "Territories are listed in the table below.")

with right:
    dist = query("""
        select coalesce(overall_rating::varchar || ' ★', 'Not rated') as label, count(*) as n,
               coalesce(overall_rating, 0) ord
        from mart_hospital_scorecard group by all order by ord
    """)
    fig = go.Figure(go.Bar(
        x=dist.label, y=dist.n,
        marker_color=[SERIES[0] if lbl != "Not rated" else "#c3c2b7" for lbl in dist.label],
        marker_cornerradius=4,
        hovertemplate="%{x}: %{y:,} hospitals<extra></extra>",
        text=dist.n, textposition="outside", textfont=dict(color="#52514e"),
    ))
    fig.update_layout(title="Distribution of CMS overall star ratings", yaxis_title="Hospitals",
                      yaxis_range=[0, dist.n.max() * 1.15])
    chart(fig, 420)

st.divider()

# ---- ownership -------------------------------------------------------------------------
own = query("""
    select ownership_group,
           count(*) hospitals,
           100.0 * sum(measures_better) / nullif(sum(measures_rated), 0) pct_better,
           100.0 * sum(measures_worse)  / nullif(sum(measures_rated), 0) pct_worse,
           avg(overall_rating) avg_rating
    from mart_hospital_scorecard
    group by 1 having sum(measures_rated) > 0
    order by pct_better - pct_worse
""")
left, right = st.columns([3, 2], gap="large")
with left:
    fig = go.Figure([
        go.Bar(y=own.ownership_group, x=-own.pct_worse, name="Worse than national", orientation="h",
               marker_color=WORSE, marker_cornerradius=4, customdata=own.pct_worse,
               hovertemplate="%{y}: %{customdata:.1f}% of rated measures worse<extra></extra>"),
        go.Bar(y=own.ownership_group, x=own.pct_better, name="Better than national", orientation="h",
               marker_color=BETTER, marker_cornerradius=4,
               hovertemplate="%{y}: %{x:.1f}% of rated measures better<extra></extra>"),
    ])
    lim = max(own.pct_better.max(), own.pct_worse.max()) * 1.15
    ticks = [-10, -5, 0, 5, 10] if lim <= 12 else [-20, -10, 0, 10, 20]
    fig.update_layout(title="Outcome measures better / worse than the national rate, by ownership",
                      barmode="relative", xaxis=dict(range=[-lim, lim], tickvals=ticks,
                                                     ticktext=[f"{abs(t)}%" for t in ticks], title="% of rated measures"),
                      legend_traceorder="reversed")
    fig.add_vline(x=0, line_color="#c3c2b7", line_width=1)
    chart(fig, 340)
with right:
    st.markdown("**How to read this**")
    st.markdown(
        "CMS flags each outcome measure (e.g. 30-day heart-failure mortality) as *better*, *no different* "
        "or *worse* than the national rate when the hospital's confidence interval clears it. "
        "Bars show what share of each ownership group's rated measures landed on either side."
    )
    st.dataframe(
        own.rename(columns={"ownership_group": "Ownership", "hospitals": "Hospitals", "avg_rating": "Avg ★",
                            "pct_better": "% better", "pct_worse": "% worse"}).iloc[::-1],
        hide_index=True, use_container_width=True,
        column_config={"Avg ★": st.column_config.NumberColumn(format="%.2f"),
                       "% better": st.column_config.NumberColumn(format="%.1f"),
                       "% worse": st.column_config.NumberColumn(format="%.1f")},
    )

with st.expander("State table"):
    st.dataframe(states.sort_values("state"), hide_index=True, use_container_width=True)
