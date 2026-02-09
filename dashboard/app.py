import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ---------------- Page Config ----------------
st.set_page_config(
    page_title="Product Feature Quality & Performance Analytics",
    layout="wide"
)

# ---------------- Global Styling ----------------
st.markdown(
    """
    <style>
    /* GLOBAL BACKGROUND */
    body, .stApp {
        background-color: #f0fdf4;
    }

    section.main {
        background-color: #f0fdf4;
    }

    .block-container {
        background-color: #f0fdf4;
        padding: 2rem 2.5rem;
    }

    /* STREAMLIT TOP HEADER (DEPLOY BAR) */
header[data-testid="stHeader"] {
    background-color: #EAF1E7;
}

header[data-testid="stHeader"]::after {
    background: none;
}

div[data-testid="stToolbar"] {
    background-color: #EAF1E7;
}


/* Ensure text/icons stay readable */
header[data-testid="stHeader"] *,
div[data-testid="stToolbar"] * {
    color: #064e3b !important;
    fill: #064e3b !important;
}

    /* METRIC CARDS */
    div[data-testid="metric-container"] {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.04);
    }

    /* ALERT CARDS */
    div[data-testid="stAlert"] {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.04);
        border-left: 6px solid transparent;
    }

    div[data-testid="stAlert"][class*="error"] {
        background-color: #fdecec;
        color: #7f1d1d;
        border-left-color: #dc2626;
    }

    div[data-testid="stAlert"][class*="warning"] {
        background-color: #fff4e5;
        color: #92400e;
        border-left-color: #f59e0b;
    }

    div[data-testid="stAlert"][class*="success"] {
        background-color: #ecfdf5;
        color: #065f46;
        border-left-color: #10b981;
    }

    /* DATAFRAME */
    .stDataFrame {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 12px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.04);
    }

    /* DIVIDERS */
    hr {
        border: none;
        height: 1px;
        background-color: #bbf7d0;
        margin: 2.5rem 0;
    }

    /* HEADINGS */
    h1, h2, h3 {
        color: #064e3b;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# ---------------- Cached Data Load ----------------
@st.cache_data
def load_data():
    metrics = pd.read_csv("data/processed/feature_metrics_with_risk.csv")
    trends = pd.read_csv("data/processed/feature_daily_trends.csv")
    metrics["severity_score"] = metrics["risk_probability"] * metrics["usage_count"]
    return metrics, trends

metrics, trends = load_data()

# ---------------- Header ----------------
st.title("📊 Product Feature Quality & Performance Analytics")
st.caption(
    "ML-powered analytics for monitoring feature reliability, performance trends, "
    "and proactive risk detection at scale."
)

# ---------------- Executive Overview ----------------
st.markdown("### 📌 Executive Overview")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Features", metrics.shape[0])
c2.metric("Avg Quality Score", round(metrics["quality_score"].mean(), 2))
c3.metric("High-Risk Features", (metrics["risk_probability"] > 0.6).sum())
c4.metric("Avg Latency (ms)", int(metrics["avg_latency_ms"].mean()))

st.divider()

# ---------------- Feature Drill-Down ----------------
st.markdown("### 🎯 Feature Drill-Down")

feature = st.selectbox(
    "Select a product feature to analyze",
    metrics["feature_name"]
)

feature_row = metrics[metrics["feature_name"] == feature].iloc[0]

# ---------------- Risk Assessment ----------------
st.markdown("### ⚠️ Predictive Risk Assessment")

risk_pct = round(feature_row["risk_probability"] * 100, 1)

c1, c2 = st.columns([1, 3])
c1.metric("Risk Probability", f"{risk_pct}%")

if risk_pct >= 70:
    c2.error(
        "High likelihood of quality degradation. "
        "Immediate investigation and mitigation recommended."
    )
elif risk_pct >= 40:
    c2.warning(
        "Moderate risk detected. Monitor closely for early signs of regression."
    )
else:
    c2.success(
        "Feature is operating within healthy quality and performance limits."
    )

st.divider()

# ---------------- Decision Summary ----------------
st.markdown("### 🧠 Decision Summary")

if risk_pct >= 70 and feature_row["usage_count"] > metrics["usage_count"].median():
    st.error(
        "🚨 Immediate Action Recommended\n\n"
        "High predicted risk combined with high user exposure. "
        "Prioritize investigation in the next release cycle."
    )
elif risk_pct >= 70:
    st.warning(
        "⚠️ Risk Detected\n\n"
        "Elevated risk with limited exposure. Schedule investigation without blocking releases."
    )
else:
    st.success(
        "✅ No Immediate Action Required\n\n"
        "Current risk levels are acceptable. Continue monitoring."
    )

st.divider()

# ---------------- Performance Trends ----------------
st.markdown("### 📈 Performance & User Experience Trends")

trend_data = trends[trends["feature_name"] == feature].copy()
trend_data["date"] = pd.to_datetime(trend_data["date"])

if trend_data.empty:
    st.info(
        "Not enough historical data is available yet to display trends for this feature."
    )
else:
    fig, ax1 = plt.subplots(figsize=(12, 4))

    ax1.plot(
        trend_data["date"],
        trend_data["latency_trend"],
        color="#1f77b4",
        linewidth=2.5
    )
    ax1.set_ylabel("Latency (ms)", color="#1f77b4")
    ax1.tick_params(axis="y", labelcolor="#1f77b4")

    ax2 = ax1.twinx()
    ax2.plot(
        trend_data["date"],
        trend_data["feedback_trend"],
        color="#ff7f0e",
        linewidth=2.5
    )
    ax2.set_ylabel("Feedback Score", color="#ff7f0e")
    ax2.tick_params(axis="y", labelcolor="#ff7f0e")

    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")

    ax1.set_title(
        "Smoothed Performance vs User Experience Trend",
        fontsize=13,
        fontweight="bold"
    )

    fig.tight_layout()
    st.pyplot(fig)

    latency_delta = trend_data["latency_trend"].iloc[-1] - trend_data["latency_trend"].iloc[0]
    feedback_delta = trend_data["feedback_trend"].iloc[-1] - trend_data["feedback_trend"].iloc[0]

    if latency_delta > 0 and feedback_delta < 0:
        st.warning(
            "Latency is increasing while user feedback is declining — "
            "an early indicator of experience degradation."
        )
    elif latency_delta < 0 and feedback_delta > 0:
        st.success(
            "Performance improvements are aligned with rising user feedback."
        )
    else:
        st.info(
            "Performance and feedback trends are stable with no strong degradation signals."
        )

st.divider()

# ---------------- Risk Prioritization ----------------
st.markdown("### 🚨 Feature Risk Prioritization")

ranked = metrics.sort_values("severity_score", ascending=False)

display_ranked = ranked.rename(columns={
    "severity_score": "Impact Score"
})

st.dataframe(
    display_ranked[[
        "feature_name",
        "risk_probability",
        "usage_count",
        "Impact Score",
        "quality_score"
    ]]
    .style
    .background_gradient(subset=["Impact Score"], cmap="Reds")
    .format({
        "risk_probability": "{:.2f}",
        "Impact Score": "{:.0f}",
        "quality_score": "{:.2f}",
        "usage_count": "{:,}"
    }),
    use_container_width=True
)

st.caption(
    "This dashboard helps product and engineering teams identify early quality regressions, "
    "prioritize mitigation based on user impact, and prevent negative member experiences "
    "before they scale."
)
