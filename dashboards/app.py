# ======================================================
# AI Job Market Intelligence Dashboard (FINAL STABLE)
# ======================================================

# --------------------------------------------------
# 1. PATH FIX — MUST COME FIRST
# --------------------------------------------------
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --------------------------------------------------
# 2. IMPORTS
# --------------------------------------------------
from config.settings import DEFAULT_TOP_N
from typing import Dict
import streamlit as st
import pandas as pd
st.caption(f"Last dashboard refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
import plotly.express as px

from analytics.load_metrics import load_all_metrics
from analytics.salary_insights import (
    prepare_salary_dataframe,
    highest_paying_roles,
    highest_paying_locations,
    salary_distribution,
)

# --------------------------------------------------
# 3. PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="AI Job Market Intelligence",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🤖 AI Job Market Intelligence Dashboard")
st.caption("Real-time analytics for AI hiring trends")

# --------------------------------------------------
# 4. LOAD DATA (CACHED)
# --------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_data() -> Dict[str, pd.DataFrame]:
    return load_all_metrics()

metrics = get_data()

skills = metrics.get("skills", pd.DataFrame())
locations = metrics.get("locations", pd.DataFrame())
jobs = metrics.get("jobs", pd.DataFrame())
clean_df = metrics.get("clean", pd.DataFrame())

# Salary prep (SAFE)
salary_df = prepare_salary_dataframe(clean_df)

# --------------------------------------------------
# 5. SIDEBAR
# --------------------------------------------------
st.sidebar.header("⚙️ Controls")

top_n = st.sidebar.slider(
    "Top N Results",
    5,
    50,
    DEFAULT_TOP_N
)

skill_search = st.sidebar.text_input("Search Skill")
location_search = st.sidebar.text_input("Search Location")

st.sidebar.divider()

def dataset_status(df: pd.DataFrame, name: str):
    if df.empty:
        st.sidebar.error(f"{name}: Missing")
    else:
        st.sidebar.success(f"{name}: {len(df):,} rows")

dataset_status(skills, "Skills")
dataset_status(locations, "Locations")
dataset_status(jobs, "Jobs")

# --------------------------------------------------
# 6. HELPERS
# --------------------------------------------------
def safe_search(df: pd.DataFrame, column: str, term: str):
    if df.empty or column not in df.columns or not term:
        return df
    return df[df[column].astype(str).str.contains(term, case=False, na=False)]

def safe_top(df: pd.DataFrame, value_col: str):
    if df.empty or value_col not in df.columns:
        return pd.DataFrame()
    return df.sort_values(value_col, ascending=False).head(top_n)

def safe_plot_bar(df, x, y, title, height=600, color=None):
    if df.empty:
        st.warning(f"No data available for {title}")
        return

    fig = px.bar(
        df,
        x=x,
        y=y,
        orientation="h",
        height=height,
        color=color
    )

    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")

# --------------------------------------------------
# 7. FILTER DATA
# --------------------------------------------------
skills = safe_search(skills, "skill", skill_search)
locations = safe_search(locations, "location", location_search)

top_skills = safe_top(skills, "demand")
top_locations = safe_top(locations, "job_count")
top_jobs = safe_top(jobs, "openings")

# --------------------------------------------------
# 8. KPI SNAPSHOT
# --------------------------------------------------
st.subheader("📊 Market Snapshot")

k1, k2, k3 = st.columns(3)
k1.metric("Unique Skills", f"{len(skills):,}")
k2.metric("Hiring Locations", f"{len(locations):,}")
k3.metric("Distinct Job Titles", f"{len(jobs):,}")

st.divider()

# --------------------------------------------------
# 9. CORE CHARTS
# --------------------------------------------------
c1, c2 = st.columns(2)

with c1:
    st.subheader("🔥 Most In-Demand Skills")
    safe_plot_bar(top_skills, "demand", "skill", "Skills")

with c2:
    st.subheader("📍 Hiring Hotspots")
    safe_plot_bar(top_locations, "job_count", "location", "Locations")

st.subheader("💼 Job Title Demand")
safe_plot_bar(
    top_jobs,
    "openings",
    "job_title",
    "Job Titles",
    height=700,
    color="openings"
)

# --------------------------------------------------
# 10. SALARY INTELLIGENCE
# --------------------------------------------------
st.divider()
st.header("💰 Salary Intelligence Engine")

if salary_df.empty:
    st.warning("No reliable salary data available.")
else:

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("🏆 Highest Paying Roles")
        top_roles = highest_paying_roles(salary_df)
        safe_plot_bar(
            top_roles,
            "avg_salary",
            "job_title",
            "Top Paying Roles",
            color="avg_salary"
        )

    with c2:
        st.subheader("🌎 Highest Paying Locations")
        top_salary_locations = highest_paying_locations(salary_df)
        safe_plot_bar(
            top_salary_locations,
            "avg_salary",
            "location",
            "Top Paying Locations",
            color="avg_salary"
        )

    st.subheader("📊 Salary Distribution")
    fig = px.histogram(
        salary_distribution(salary_df),
        nbins=40,
        height=500
    )
    st.plotly_chart(fig, width="stretch")

# --------------------------------------------------
# 11. DOWNLOADS
# --------------------------------------------------
st.divider()
st.subheader("⬇️ Export Analytics")

def download_button(df, name):
    if df.empty:
        return
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        f"Download {name}",
        csv,
        file_name=f"{name}.csv",
        mime="text/csv"
    )

d1, d2, d3 = st.columns(3)
with d1:
    download_button(skills, "skills")
with d2:
    download_button(locations, "locations")
with d3:
    download_button(jobs, "jobs")

# --------------------------------------------------
# 12. RAW DATA INSPECTION
# --------------------------------------------------
with st.expander("🔍 Inspect Raw Data"):

    t1, t2, t3 = st.tabs(["Skills", "Locations", "Jobs"])

    t1.dataframe(skills, width="stretch")
    t2.dataframe(locations, width="stretch")
    t3.dataframe(jobs, width="stretch")
