# Import Python packages
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd
from snowflake.snowpark.functions import col

# ─── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Hamburg — Weather & Sales Intelligence",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap');

:root {
    --bg:        #0a0c10;
    --surface:   #111318;
    --border:    #1e2230;
    --accent:    #4fc3f7;
    --accent2:   #f06292;
    --accent3:   #a5d6a7;
    --accent4:   #ffcc80;
    --muted:     #5a6480;
    --text:      #dce3f0;
    --text-dim:  #8896b3;
}

html, body, [data-testid="stAppViewContainer"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
}

[data-testid="stAppViewContainer"] {
    font-family: 'Syne', sans-serif;
}

/* Hide default Streamlit header/footer */
header[data-testid="stHeader"], footer { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }

/* Main container */
.block-container {
    padding: 2.5rem 3rem !important;
    max-width: 1400px !important;
}

/* ── Hero ── */
.hero {
    border-bottom: 1px solid var(--border);
    padding-bottom: 2rem;
    margin-bottom: 2.5rem;
}
.hero-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    letter-spacing: 0.25em;
    color: var(--muted);
    text-transform: uppercase;
    margin-bottom: 0.5rem;
}
.hero-title {
    font-family: 'DM Serif Display', serif;
    font-size: clamp(2.2rem, 4vw, 3.8rem);
    line-height: 1.05;
    color: var(--text);
    margin: 0;
}
.hero-title em {
    font-style: italic;
    color: var(--accent);
}
.hero-sub {
    font-family: 'DM Mono', monospace;
    font-size: 0.75rem;
    color: var(--text-dim);
    margin-top: 0.75rem;
    letter-spacing: 0.05em;
}

/* ── KPI Cards ── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 2.5rem;
}
.kpi-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1.25rem 1.5rem;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 3px; height: 100%;
    background: var(--card-accent, var(--accent));
}
.kpi-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    color: var(--muted);
    text-transform: uppercase;
    margin-bottom: 0.6rem;
}
.kpi-value {
    font-family: 'DM Serif Display', serif;
    font-size: 2rem;
    color: var(--text);
    line-height: 1;
}
.kpi-delta {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    color: var(--text-dim);
    margin-top: 0.4rem;
}
.kpi-delta.up   { color: var(--accent3); }
.kpi-delta.down { color: var(--accent2); }

/* ── Section headers ── */
.section-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.25em;
    color: var(--muted);
    text-transform: uppercase;
    margin-bottom: 0.4rem;
}
.section-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.4rem;
    color: var(--text);
    margin-bottom: 1.25rem;
}

/* ── Insight box ── */
.insight-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    border-radius: 4px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.75rem;
    font-family: 'Syne', sans-serif;
    font-size: 0.82rem;
    color: var(--text-dim);
    line-height: 1.6;
}
.insight-box strong { color: var(--accent); font-weight: 600; }

/* ── Divider ── */
.divider {
    border: none;
    border-top: 1px solid var(--border);
    margin: 2rem 0;
}

/* ── Chart containers ── */
.chart-wrap {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1.25rem;
}

/* Altair / Vega overrides */
.vega-embed { background: transparent !important; }
</style>
""", unsafe_allow_html=True)

# ─── Session & data ──────────────────────────────────────────────────────────
session = get_active_session()
env = "STAGING"

df = session.table(f"{env}_TASTY_BYTES.HARMONIZED.WEATHER_HAMBURG").select(
    col("DATE"),
    col("DAILY_SALES"),
    col("AVG_TEMPERATURE_FAHRENHEIT"),
    col("AVG_TEMPERATURE_CELSIUS"),
    col("AVG_PRECIPITATION_INCHES"),
    col("AVG_PRECIPITATION_MILLIMETERS"),
    col("MAX_WIND_SPEED_100M_MPH")
).to_pandas()

df["DATE"] = pd.to_datetime(df["DATE"])
df = df.sort_values("DATE")

# ─── Derived metrics ─────────────────────────────────────────────────────────
total_sales     = df["DAILY_SALES"].sum()
avg_sales       = df["DAILY_SALES"].mean()
max_sales       = df["DAILY_SALES"].max()
max_sales_date  = df.loc[df["DAILY_SALES"].idxmax(), "DATE"].strftime("%b %d")
avg_temp_c      = df["AVG_TEMPERATURE_CELSIUS"].mean()
max_wind        = df["MAX_WIND_SPEED_100M_MPH"].max()
max_wind_date   = df.loc[df["MAX_WIND_SPEED_100M_MPH"].idxmax(), "DATE"].strftime("%b %d")
avg_precip_mm   = df["AVG_PRECIPITATION_MILLIMETERS"].mean()

# Correlation: temperature vs sales
corr_temp_sales = df["AVG_TEMPERATURE_CELSIUS"].corr(df["DAILY_SALES"])
corr_wind_sales = df["MAX_WIND_SPEED_100M_MPH"].corr(df["DAILY_SALES"])
corr_prec_sales = df["AVG_PRECIPITATION_MILLIMETERS"].corr(df["DAILY_SALES"])

cold_days  = df[df["AVG_TEMPERATURE_CELSIUS"] < 5]
warm_days  = df[df["AVG_TEMPERATURE_CELSIUS"] >= 5]
sales_cold = cold_days["DAILY_SALES"].mean()
sales_warm = warm_days["DAILY_SALES"].mean()

# ─── Hero ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="hero-label">Tasty Bytes — Operations Intelligence / Feb 2022</div>
    <h1 class="hero-title">Hamburg<br><em>Weather & Sales</em></h1>
    <div class="hero-sub">STAGING environment &nbsp;·&nbsp; February 2022 &nbsp;·&nbsp; Daily granularity</div>
</div>
""", unsafe_allow_html=True)

# ─── KPI Cards ───────────────────────────────────────────────────────────────
sales_delta_dir = "up" if sales_warm > sales_cold else "down"
st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi-card" style="--card-accent:#4fc3f7">
        <div class="kpi-label">Total Revenue</div>
        <div class="kpi-value">${total_sales:,.0f}</div>
        <div class="kpi-delta">February 2022</div>
    </div>
    <div class="kpi-card" style="--card-accent:#f06292">
        <div class="kpi-label">Peak Day</div>
        <div class="kpi-value">${max_sales:,.0f}</div>
        <div class="kpi-delta">{max_sales_date}</div>
    </div>
    <div class="kpi-card" style="--card-accent:#a5d6a7">
        <div class="kpi-label">Avg Temperature</div>
        <div class="kpi-value">{avg_temp_c:.1f}&deg;C</div>
        <div class="kpi-delta">Monthly average</div>
    </div>
    <div class="kpi-card" style="--card-accent:#ffcc80">
        <div class="kpi-label">Peak Wind</div>
        <div class="kpi-value">{max_wind:.0f} mph</div>
        <div class="kpi-delta">{max_wind_date}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─── Chart config shared ─────────────────────────────────────────────────────
CHART_CONFIG = {
    "background": "#111318",
    "axis": {
        "gridColor": "#1e2230",
        "labelColor": "#5a6480",
        "titleColor": "#5a6480",
        "labelFont": "DM Mono",
        "titleFont": "DM Mono",
        "labelFontSize": 10,
        "titleFontSize": 10,
    },
    "legend": {
        "labelColor": "#8896b3",
        "titleColor": "#5a6480",
        "labelFont": "DM Mono",
        "titleFont": "DM Mono",
        "labelFontSize": 10,
    },
    "view": {"stroke": "transparent"},
}

# ─── Row 1: Sales + Temp ──────────────────────────────────────────────────────
st.markdown('<div class="section-label">Performance over time</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Daily Revenue vs Temperature</div>', unsafe_allow_html=True)

col1, col2 = st.columns([3, 2])

with col1:
    base = alt.Chart(df).encode(x=alt.X("DATE:T", title=None, axis=alt.Axis(format="%b %d", labelAngle=-30)))

    sales_line = base.mark_area(
        line={"color": "#4fc3f7", "strokeWidth": 1.5},
        color=alt.Gradient(gradient="linear", stops=[
            alt.GradientStop(color="#4fc3f7", offset=0),
            alt.GradientStop(color="#0a0c10", offset=1)
        ], x1=0, x2=0, y1=0, y2=1),
        opacity=0.5
    ).encode(
        y=alt.Y("DAILY_SALES:Q", title="Sales (USD)", axis=alt.Axis(format="$,.0f")),
        tooltip=[
            alt.Tooltip("DATE:T", title="Date", format="%b %d"),
            alt.Tooltip("DAILY_SALES:Q", title="Sales", format="$,.2f"),
        ]
    )

    temp_line = base.mark_line(strokeWidth=1.5, strokeDash=[4, 3], color="#f06292").encode(
        y=alt.Y("AVG_TEMPERATURE_CELSIUS:Q", title="Temperature (°C)"),
        tooltip=[alt.Tooltip("AVG_TEMPERATURE_CELSIUS:Q", title="Temp °C", format=".1f")]
    )

    chart1 = alt.layer(sales_line, temp_line).resolve_scale(y="independent").properties(
        height=280
    ).configure(**CHART_CONFIG)

    st.altair_chart(chart1, use_container_width=True)

with col2:
    st.markdown('<div class="section-label">Correlation insight</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Temp vs Sales</div>', unsafe_allow_html=True)

    scatter = alt.Chart(df).mark_point(
        size=60, opacity=0.8, filled=True
    ).encode(
        x=alt.X("AVG_TEMPERATURE_CELSIUS:Q", title="Temperature (°C)"),
        y=alt.Y("DAILY_SALES:Q", title="Sales (USD)", axis=alt.Axis(format="$,.0f")),
        color=alt.Color("DAILY_SALES:Q", scale=alt.Scale(scheme="blues"), legend=None),
        tooltip=[
            alt.Tooltip("DATE:T", title="Date", format="%b %d"),
            alt.Tooltip("AVG_TEMPERATURE_CELSIUS:Q", title="Temp °C", format=".1f"),
            alt.Tooltip("DAILY_SALES:Q", title="Sales", format="$,.2f"),
        ]
    )

    trend = scatter.transform_regression(
        "AVG_TEMPERATURE_CELSIUS", "DAILY_SALES"
    ).mark_line(color="#f06292", strokeWidth=1.5, strokeDash=[4, 3])

    st.altair_chart(
        (scatter + trend).properties(height=280).configure(**CHART_CONFIG),
        use_container_width=True
    )

# ─── Insights ────────────────────────────────────────────────────────────────
direction = "positive" if corr_temp_sales > 0 else "negative"
direction_wind = "positive" if corr_wind_sales > 0 else "negative"

st.markdown(f"""
<div class="insight-box">
    <strong>Temperature correlation:</strong> r = {corr_temp_sales:.2f} — 
    A {direction} relationship between temperature and daily sales. 
    Cold days (&lt;5°C) averaged <strong>${sales_cold:,.0f}</strong> vs warm days at <strong>${sales_warm:,.0f}</strong>.
</div>
<div class="insight-box">
    <strong>Wind speed correlation:</strong> r = {corr_wind_sales:.2f} — 
    Wind speed shows a {direction_wind} correlation with revenue. 
    Peak wind of {max_wind:.0f} mph occurred on {max_wind_date}.
</div>
<div class="insight-box">
    <strong>Precipitation correlation:</strong> r = {corr_prec_sales:.2f} — 
    Average precipitation was {avg_precip_mm:.1f} mm/day throughout February 2022.
</div>
""", unsafe_allow_html=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)

# ─── Row 2: Wind + Precipitation ─────────────────────────────────────────────
st.markdown('<div class="section-label">Environmental factors</div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">Wind Speed & Precipitation</div>', unsafe_allow_html=True)

col3, col4 = st.columns(2)

with col3:
    wind_chart = alt.Chart(df).mark_bar(
        cornerRadiusTopLeft=2, cornerRadiusTopRight=2, color="#ffcc80", opacity=0.85
    ).encode(
        x=alt.X("DATE:T", title=None, axis=alt.Axis(format="%b %d", labelAngle=-30)),
        y=alt.Y("MAX_WIND_SPEED_100M_MPH:Q", title="Max Wind Speed (mph)"),
        tooltip=[
            alt.Tooltip("DATE:T", title="Date", format="%b %d"),
            alt.Tooltip("MAX_WIND_SPEED_100M_MPH:Q", title="Wind (mph)", format=".1f"),
        ]
    ).properties(height=220).configure(**CHART_CONFIG)

    st.altair_chart(wind_chart, use_container_width=True)

with col4:
    precip_chart = alt.Chart(df).mark_bar(
        cornerRadiusTopLeft=2, cornerRadiusTopRight=2, color="#4fc3f7", opacity=0.6
    ).encode(
        x=alt.X("DATE:T", title=None, axis=alt.Axis(format="%b %d", labelAngle=-30)),
        y=alt.Y("AVG_PRECIPITATION_MILLIMETERS:Q", title="Precipitation (mm)"),
        tooltip=[
            alt.Tooltip("DATE:T", title="Date", format="%b %d"),
            alt.Tooltip("AVG_PRECIPITATION_MILLIMETERS:Q", title="Precip (mm)", format=".2f"),
        ]
    ).properties(height=220).configure(**CHART_CONFIG)

    st.altair_chart(precip_chart, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)

# ─── Raw data toggle ─────────────────────────────────────────────────────────
with st.expander("View raw data"):
    display_df = df.copy()
    display_df["DATE"] = display_df["DATE"].dt.strftime("%b %d, %Y")
    display_df["DAILY_SALES"] = display_df["DAILY_SALES"].map("${:,.2f}".format)
    display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]
    st.dataframe(display_df, use_container_width=True, hide_index=True)