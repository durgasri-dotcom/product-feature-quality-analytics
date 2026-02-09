import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Product Feature Quality & Performance Analytics",
    layout="wide"
)

# Load data
metrics = pd.read_csv("data/processed/feature_metrics_with_risk.csv")
trends = pd.read_csv("data/processed/feature_daily_trends.csv")

st.title("📊 Product Feature Quality & Performance Analytics")
st.caption("ML-powered insights for product reliability, performance & user experience")

# ---------------- KPIs ----------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Features", metrics.shape[0])
c2.metric("Avg Quality Score", round(metrics["quality_score"].mean(), 2))
c3.metric("High-Risk Features", (metrics["risk_probability"] > 0.6).sum())
c4.metric("Avg Latency (ms)", int(metrics["avg_latency_ms"].mean()))

st.divider()

# ---------------- Feature Selector ----------------
feature = st.selectbox(
    "Select Feature",
    metrics["feature_name"]
)

feature_row = metrics[metrics["feature_name"] == feature].iloc[0]

# ---------------- Risk Insight ----------------
st.subheader("⚠️ ML Risk Assessment")

st.metric(
    "Risk Probability",
    f"{round(feature_row['risk_probability'] * 100, 1)}%"
)

if feature_row["risk_probability"] > 0.6:
    st.error("High probability of quality degradation")
else:
    st.success("Feature is operating within safe quality limits")

st.divider()

# ---------------- Time Trend ----------------
st.subheader("📈 Feature Performance & Quality Trends")

trend_data = trends[trends["feature_name"] == feature]

if trend_data.shape[0] < 2:
    st.info("Not enough historical data to display trend yet.")
else:
    fig, ax1 = plt.subplots(figsize=(10, 4))

    ax1.plot(
        trend_data["date"],
        trend_data["latency_trend"],
        color="tab:blue",
        linewidth=2,
        label="Latency (ms)"
    )
    ax1.set_ylabel("Latency (ms)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    ax2.plot(
        trend_data["date"],
        trend_data["feedback_trend"],
        color="tab:orange",
        linewidth=2,
        label="Feedback Score"
    )
    ax2.set_ylabel("Feedback Score", color="tab:orange")
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    ax1.set_xlabel("Date")
    ax1.set_title("Smoothed Performance vs User Experience Trend")

    st.pyplot(fig)


# ---------------- Risk Ranking ----------------
st.subheader("🚨 Feature Risk Ranking")

ranked = metrics.sort_values("risk_probability", ascending=False)

st.dataframe(
    ranked[[
        "feature_name",
        "quality_score",
        "risk_probability",
        "avg_latency_ms",
        "crash_rate"
    ]],
    use_container_width=True
)

st.caption(
    "This dashboard enables proactive identification of feature-level "
    "performance regressions using machine learning and time-series analysis."
)

