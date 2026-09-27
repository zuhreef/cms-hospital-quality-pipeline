import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))  # make `lib` importable from pages

st.set_page_config(page_title="Hospital Quality", page_icon="🏥", layout="wide")

pages = [
    st.Page("pages/overview.py", title="National overview", icon=":material/map:", default=True),
    st.Page("pages/explorer.py", title="Hospital explorer", icon=":material/local_hospital:"),
    st.Page("pages/benchmarks.py", title="Measure benchmarks", icon=":material/insights:"),
    st.Page("pages/pipeline_health.py", title="Pipeline health", icon=":material/monitor_heart:"),
]
nav = st.navigation(pages)

with st.sidebar:
    st.markdown("**CMS Hospital Quality**")
    st.caption(
        "Built from CMS Care Compare data: hospital star ratings, 30+ outcome measures "
        "(mortality, complications, readmissions) and the HCAHPS patient survey."
    )
    st.caption("Pipeline: CMS API → bronze → PySpark silver → DQ gate → dbt/DuckDB gold → this app.")

nav.run()
