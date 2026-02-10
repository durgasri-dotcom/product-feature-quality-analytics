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
    .stApp {
        background-color: #f0fdf4;
    }

    header[data-testid="stHeader"] {
        background-color: #cbfad0;
        box-shadow: none;
    }

    section.main > div {
        padding-top: 1.5rem;
    }

    h1 {
        color: #1f2933;
        font-weight: 600;
    }

    h2 {
        font-size: 1.7rem;
        font-weight: 700;
        margin-top: 2.8rem;
        margin-bottom: 1rem;
        color: #0f172a;
    }

    h3 {
        font-size: 1.45rem;
        font-weight: 700;
        margin-top: 2.2rem;
        margin-bottom: 0.75rem;
        color: #1f2933;
    }

    p, span, label {
        color: #374151;
        font-size: 0.95rem;
    }

    div[data-testid="metric-container"] {
        background-color: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 10px;
        padding: 1rem;
    }

    div[data-testid="stAlert"] {
        border-radius: 10px;
        font-size: 0.95rem;
        border: none;
    }

    div[data-testid="stAlert"][class*="error"] {
        background-color: #fdecec;
        color: #7f1d1d;
    }

    div[data-testid="stAlert"][class*="warning"] {
        background-color: #fff4e5;
        color: #92400e;
    }

    div[data-testid="stAlert"][class*="success"] {
        background-color: #ecfdf5;
        color: #065f46;
    }

    hr {
        border: none;
        height: 1px;
        background-color: #d1fae5;
        margin: 2rem 0;
    }

    section[data-testid="stSidebar"] {
        background-color: #ecfdf5;
        border-right: 1px solid #d1fae5;
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
st.title(" Product Feature Quality & Performance Analytics")
st.caption(
    "ML-powered analytics for monitoring feature reliability, performance trends, "
    "and proactive risk detection at scale."
)

# ---------------- Executive Overview ----------------
st.markdown("##  Executive Overview")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Features", metrics.shape[0])
c2.metric("Avg Quality Score", round(metrics["quality_score"].mean(), 2))
c3.metric("High-Risk Features", (metrics["risk_probability"] > 0.6).sum())
c4.metric("Avg Latency (ms)", int(metrics["avg_latency_ms"].mean()))

st.divider()

# ---------------- Feature Drill-Down ----------------
st.markdown("##  Feature Drill-Down")

feature = st.selectbox(
    "Select a product feature to analyze",
    metrics["feature_name"]
)

feature_row = metrics[metrics["feature_name"] == feature].iloc[0]

# ---------------- Risk Assessment ----------------
st.markdown("##  Predictive Risk Assessment")

risk_pct = round(feature_row["risk_probability"] * 100, 1)

c1, c2 = st.columns([1, 3])
c1.metric("Risk Probability", f"{risk_pct}%")

# ------------- Prediction Confidence ---------------
trend_data_for_confidence = trends[trends["feature_name"] == feature]

if trend_data_for_confidence.shape[0] >= 21:
    confidence = "High"
elif trend_data_for_confidence.shape[0] >= 10:
    confidence = "Medium"
else:
    confidence = "Low"

st.caption(
    f"Prediction confidence: {confidence} "
    "(based on historical data coverage)"
)

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
        "Feature is operating within acceptable quality and performance thresholds."

    )


st.divider()

# ---------------- Model Explainability ----------------
st.markdown("###  Model Explanation")

explain_factors = {
    "Usage Volume": feature_row["usage_count"],
    "Average Latency (ms)": feature_row["avg_latency_ms"],
    "Average Feedback Score": feature_row["avg_feedback_score"],
    "Crash Rate": feature_row["crash_rate"]
}

# Convert to clean two-column dataframe
explain_df = (
    pd.DataFrame(
        explain_factors.items(),
        columns=["Signal", "Value"]
    )
    .sort_values("Value", ascending=False)
)

# relative contribution
explain_df["Relative Contribution"] = (
    explain_df["Value"] / explain_df["Value"].sum()
)

st.dataframe(
    explain_df.style
        .format({
            "Value": "{:.3f}",
            "Relative Contribution": "{:.0%}"
        })
        .set_properties(**{"text-align": "left"})
        .set_table_styles([
            {"selector": "th", "props": [("text-align", "left")]}
        ]),
    use_container_width=True
)

st.caption(
    "This table highlights the primary operational signals contributing "
    "to the predicted risk score for the selected feature."
)

# ---------------- Operational Decision Playbook ----------------

st.markdown("##  Operational Recommendation")

if risk_pct >= 70 and feature_row["usage_count"] > metrics["usage_count"].median():
    st.error(
        "Immediate Action Required\n\n"
        "- Trigger on-call alert\n"
        "- Enable rollback or traffic throttling\n"
        "- Increase monitoring resolution\n"
        "- Schedule root cause analysis within 24 hours"
    )

elif risk_pct >= 40:
    st.warning(
        "Preventive Action Recommended\n\n"
        "- Add feature to monitoring watchlist\n"
        "- Enable enhanced telemetry\n"
        "- Review trends in next release cycle"
    )

else:
    st.success(
        "No Immediate Action Required\n\n"
        "- Continue standard monitoring\n"
        "- Reassess during scheduled reliability review"
    )


# ---------------- Performance Trends ----------------
st.markdown("##  Performance & User Experience Trends")

trend_data = trends[trends["feature_name"] == feature].copy()
trend_data["date"] = pd.to_datetime(trend_data["date"])

if trend_data.empty:
    st.info(
        "Not enough historical data is available yet to display trends for this feature."
    )
else:
    # ---------------- Trend-Based Risk Signal ----------------
    recent_window = trend_data.tail(7)
    baseline_window = trend_data.head(7)

    latency_change_pct = (
        recent_window["latency_trend"].mean() -
        baseline_window["latency_trend"].mean()
    ) / baseline_window["latency_trend"].mean() * 100

    feedback_change_pct = (
        recent_window["feedback_trend"].mean() -
        baseline_window["feedback_trend"].mean()
    ) / baseline_window["feedback_trend"].mean() * 100

    st.markdown("#### Trend-Based Risk Signal")

    if latency_change_pct > 5 and feedback_change_pct < -3:
        st.error(
            f"Latency increased by {latency_change_pct:.1f}% while feedback declined by "
            f"{abs(feedback_change_pct):.1f}% relative to baseline."
        )
    elif latency_change_pct < -5 and feedback_change_pct > 3:
        st.success(
            f"Latency improved by {abs(latency_change_pct):.1f}% and feedback increased by "
            f"{feedback_change_pct:.1f}% compared to baseline."
        )
    else:
        st.info(
            "No significant deviation detected relative to historical baseline."
        )

    # ---------------- Trend Chart ----------------
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

    # ---------------- Directional Insight ----------------
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


# ---------------- Risk Prioritization ----------------
st.markdown("##  Feature Risk Prioritization")

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
    "Features are ranked by Impact Score, combining predicted risk "
    "with usage volume to prioritize mitigation based on potential user impact."
)

