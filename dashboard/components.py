"""
Reusable visual UI components and Plotly chart generators for Streamlit dashboard.
Adheres to modern data visualization best practices with consistent styling.
"""

from typing import Any, Dict, Optional
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# Color palette definition (Zinc / Slate / Indigo corporate palette)
COLORS = {
    "primary": "#3B82F6",       # Blue 500
    "primary_dark": "#1D4ED8",  # Blue 700
    "secondary": "#10B981",     # Emerald 500
    "accent": "#8B5CF6",        # Purple 500
    "warning": "#F59E0B",       # Amber 500
    "danger": "#EF4444",        # Red 500
    "neutral_dark": "#1E293B",  # Slate 800
    "neutral_light": "#F8FAFC", # Slate 50
    "card_bg": "#FFFFFF",
    "card_border": "#E2E8F0",
}


def inject_custom_css():
    """Injects custom CSS to style Streamlit elements with modern card designs."""
    st.markdown("""
        <style>
        /* Base page styling */
        .main .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 1400px;
        }
        
        /* Metric Card Container */
        .metric-card {
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 1.1rem 1.25rem;
            box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
            margin-bottom: 0.75rem;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }
        .metric-card:hover {
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.08);
        }
        .metric-title {
            font-size: 0.825rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #64748B;
            margin-bottom: 0.35rem;
        }
        .metric-value {
            font-size: 1.65rem;
            font-weight: 700;
            color: #0F172A;
            line-height: 1.2;
            margin-bottom: 0.25rem;
        }
        .metric-sub {
            font-size: 0.8rem;
            color: #64748B;
        }
        .metric-sub-pos {
            color: #10B981;
            font-weight: 600;
        }
        .metric-sub-neg {
            color: #EF4444;
            font-weight: 600;
        }

        /* Pipeline Step Card */
        .pipeline-step {
            background: #F8FAFC;
            border-left: 4px solid #3B82F6;
            border-radius: 4px;
            padding: 0.75rem 1rem;
            margin-bottom: 0.5rem;
        }
        .pipeline-step-title {
            font-weight: 600;
            color: #1E293B;
            font-size: 0.95rem;
        }
        .pipeline-step-desc {
            font-size: 0.8rem;
            color: #64748B;
        }
        
        /* Badges */
        .tech-badge {
            display: inline-block;
            background-color: #EFF6FF;
            color: #1D4ED8;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.2rem 0.6rem;
            border-radius: 9999px;
            border: 1px solid #BFDBFE;
            margin-right: 0.35rem;
            margin-bottom: 0.35rem;
        }
        </style>
    """, unsafe_allow_html=True)


def render_kpi_card(title: str, value: str, subtitle: Optional[str] = None, is_positive: Optional[bool] = None):
    """Renders a responsive KPI card with modern typography and optional delta indicator."""
    sub_class = "metric-sub"
    if is_positive is True:
        sub_class += " metric-sub-pos"
    elif is_positive is False:
        sub_class += " metric-sub-neg"

    sub_html = f'<div class="{sub_class}">{subtitle}</div>' if subtitle else ""

    html = f"""
    <div class="metric-card">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{value}</div>
        {sub_html}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def plot_monthly_sales_trend(df: pd.DataFrame) -> go.Figure:
    """
    Generates dual-axis monthly revenue and MoM growth chart.

    Visualization Treatment:
    To ensure normal operational percentage movements are readable, months with an 
    insufficient or near-zero comparison baseline (prior month GMV < R$ 1,000, such as 
    2016-10 and 2017-01 during pilot ramp-up) are excluded from the MoM percentage line.
    This prevents extreme arithmetic spikes (+32,307% and +649,657%) from compressing 
    the Y-axis. The exact SQL values remain completely unaltered in the underlying data 
    and detail tables.
    """
    # Identify months with sufficient comparison baseline (prior month GMV >= R$ 1,000)
    has_prev_rev = "prev_month_revenue" in df.columns
    if has_prev_rev:
        has_sufficient_base = df["prev_month_revenue"].fillna(0) >= 1000.0
    else:
        has_sufficient_base = df["mom_growth_pct"].abs() <= 500.0

    mom_growth_display = df["mom_growth_pct"].where(has_sufficient_base, None)
    excluded_outliers = df[~has_sufficient_base & df["mom_growth_pct"].notna()]

    subplot_title_mom = (
        "Month-over-Month Growth Rate (%) [Established Baseline: Prior GMV ≥ R$ 1k]"
        if not excluded_outliers.empty
        else "Month-over-Month Growth Rate (%)"
    )

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.10,
        row_heights=[0.68, 0.32],
        subplot_titles=("Monthly Gross Merchandise Value (BRL)", subplot_title_mom)
    )

    # GMV Bar Trace
    fig.add_trace(
        go.Bar(
            x=df["year_month"],
            y=df["total_order_value"],
            name="GMV (BRL)",
            marker_color=COLORS["primary"],
            hovertemplate="<b>%{x}</b><br>GMV: R$ %{y:,.2f}<br>Orders: %{customdata[0]:,}<extra></extra>",
            customdata=df[["number_of_orders"]].values,
        ),
        row=1, col=1
    )

    # MoM Growth Line Trace
    colors_mom = [
        COLORS["secondary"] if (x is not None and x >= 0) else COLORS["danger"]
        for x in mom_growth_display
    ]
    fig.add_trace(
        go.Scatter(
            x=df["year_month"],
            y=mom_growth_display,
            name="MoM Growth %",
            mode="lines+markers",
            line=dict(color=COLORS["accent"], width=2.5),
            marker=dict(size=6, color=colors_mom),
            hovertemplate="<b>%{x}</b><br>MoM Growth: %{y:.2f}%<extra></extra>",
            connectgaps=False,
        ),
        row=2, col=1
    )

    # Annotate excluded early pilot outliers cleanly on the chart
    if not excluded_outliers.empty:
        outlier_text_items = [
            f"<b>{r['year_month']}</b>: +{r['mom_growth_pct']:,.1f}% (prior base: R$ {r['prev_month_revenue']:.2f})"
            for _, r in excluded_outliers.iterrows()
        ]
        annotation_str = (
            "📌 <b>Early Pilot Baseline Outliers Excluded from Line:</b> "
            + "; ".join(outlier_text_items)
            + " — <i>Exact unfiltered values preserved in detail table.</i>"
        )
        fig.add_annotation(
            text=annotation_str,
            xref="paper", yref="paper",
            x=0.0, y=-0.24,
            showarrow=False,
            font=dict(size=11, color="#64748B"),
            align="left",
        )

    fig.update_layout(
        height=520,
        margin=dict(l=20, r=20, t=40, b=55 if not excluded_outliers.empty else 20),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        showlegend=False,
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#F1F5F9")
    fig.update_yaxes(showgrid=True, gridcolor="#F1F5F9", row=1, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="#F1F5F9", row=2, col=1, zeroline=True, zerolinecolor="#94A3B8")
    return fig


def plot_sales_by_state(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    """Renders ranked horizontal bar chart for top customer states."""
    sub_df = df.head(top_n).sort_values("total_order_value", ascending=True)

    fig = go.Figure(go.Bar(
        x=sub_df["total_order_value"],
        y=sub_df["customer_state"],
        orientation="h",
        marker=dict(
            color=sub_df["total_order_value"],
            colorscale="Blues",
            showscale=False
        ),
        hovertemplate="<b>State: %{y}</b><br>Revenue: R$ %{x:,.2f}<br>Orders: %{customdata[0]:,}<br>Share: %{customdata[1]:.1f}%<extra></extra>",
        customdata=sub_df[["orders_count", "revenue_share_pct"]].values,
    ))

    fig.update_layout(
        height=380,
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(title="Delivered GMV (BRL)", showgrid=True, gridcolor="#F1F5F9"),
        yaxis=dict(title="Customer State", showgrid=False),
    )
    return fig


def plot_top_categories(df: pd.DataFrame) -> go.Figure:
    """Renders ranked horizontal bar chart of top product categories."""
    sub_df = df.sort_values("total_category_revenue", ascending=True)

    fig = go.Figure(go.Bar(
        x=sub_df["total_category_revenue"],
        y=sub_df["category_name"],
        orientation="h",
        marker=dict(color=COLORS["primary"]),
        hovertemplate="<b>%{y}</b><br>Revenue: R$ %{x:,.2f}<br>Units Sold: %{customdata[0]:,}<br>Avg Price: R$ %{customdata[1]:,.2f}<extra></extra>",
        customdata=sub_df[["units_sold", "average_item_price"]].values,
    ))

    fig.update_layout(
        height=450,
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(title="Revenue (BRL)", showgrid=True, gridcolor="#F1F5F9"),
        yaxis=dict(title="", showgrid=False),
    )
    return fig


def plot_rfm_distribution(df: pd.DataFrame) -> go.Figure:
    """Generates an intuitive dual-bar chart showing customer share vs revenue share per segment."""
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df["customer_segment"],
        y=df["customer_share_pct"],
        name="% of Total Customers",
        marker_color="#94A3B8",
        hovertemplate="<b>%{x}</b><br>Customer Share: %{y:.1f}%<br>Count: %{customdata:,}<extra></extra>",
        customdata=df["customer_count"],
    ))

    fig.add_trace(go.Bar(
        x=df["customer_segment"],
        y=df["revenue_share_pct"],
        name="% of Total Revenue",
        marker_color=COLORS["primary"],
        hovertemplate="<b>%{x}</b><br>Revenue Share: %{y:.1f}%<br>Total Spend: R$ %{customdata:,.2f}<extra></extra>",
        customdata=df["total_segment_revenue"],
    ))

    fig.update_layout(
        barmode="group",
        height=400,
        margin=dict(l=20, r=20, t=30, b=20),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(title="", showgrid=False),
        yaxis=dict(title="Percentage (%)", showgrid=True, gridcolor="#F1F5F9"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def plot_logistics_performance_bars(df: pd.DataFrame, top_n: int = 10, ascending: bool = True) -> go.Figure:
    """Displays fastest or slowest delivery states."""
    sub_df = df.sort_values("avg_delivery_days", ascending=ascending).head(top_n)
    if not ascending:
        sub_df = sub_df.sort_values("avg_delivery_days", ascending=True)

    color = COLORS["secondary"] if ascending else COLORS["danger"]

    fig = go.Figure(go.Bar(
        x=sub_df["avg_delivery_days"],
        y=sub_df["customer_state"],
        orientation="h",
        marker_color=color,
        hovertemplate="<b>State: %{y}</b><br>Avg Delivery: %{x:.1f} days<br>Late Rate: %{customdata[0]:.1f}%<br>Avg Rating: %{customdata[1]:.2f} ★<extra></extra>",
        customdata=sub_df[["late_delivery_rate_pct", "avg_review_score"]].values,
    ))

    fig.update_layout(
        height=360,
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(title="Average Delivery Time (Days)", showgrid=True, gridcolor="#F1F5F9"),
        yaxis=dict(title="", showgrid=False),
    )
    return fig


def plot_seller_pareto(df: pd.DataFrame) -> go.Figure:
    """Plots the empirical Lorenz/Pareto curve comparing cumulative sellers vs revenue."""
    fig = go.Figure()

    # Empirical cumulative curve
    fig.add_trace(go.Scatter(
        x=df["cumulative_seller_pct"],
        y=df["cumulative_revenue_pct"],
        mode="lines",
        name="Actual Marketplace Cumulative Curve",
        line=dict(color=COLORS["primary"], width=3),
        hovertemplate="Top %{x:.1f}% of Sellers<br>Generate %{y:.1f}% of Revenue<extra></extra>",
    ))

    # Baseline 45-degree parity line
    fig.add_trace(go.Scatter(
        x=[0, 100],
        y=[0, 100],
        mode="lines",
        name="Equal Distribution Baseline",
        line=dict(color="#CBD5E1", width=1.5, dash="dash"),
        hoverinfo="skip",
    ))

    # Add 20% vertical and 80% horizontal reference lines
    fig.add_shape(type="line", x0=20, y0=0, x1=20, y1=100, line=dict(color="#F59E0B", width=1, dash="dot"))
    fig.add_shape(type="line", x0=0, y0=80, x1=100, y1=80, line=dict(color="#F59E0B", width=1, dash="dot"))

    fig.add_annotation(
        x=20, y=82,
        text="Pareto Threshold (~20% sellers -> ~82% revenue)",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowcolor="#F59E0B",
        ax=50, ay=-30,
        font=dict(size=11, color="#1E293B"),
        bgcolor="#FEF3C7",
        bordercolor="#F59E0B",
        borderwidth=1,
    )

    fig.update_layout(
        height=450,
        margin=dict(l=20, r=20, t=30, b=20),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(title="Cumulative % of Active Sellers (Ranked by Revenue)", showgrid=True, gridcolor="#F1F5F9", range=[0, 100]),
        yaxis=dict(title="Cumulative % of Total Marketplace Revenue", showgrid=True, gridcolor="#F1F5F9", range=[0, 105]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def render_pipeline_flow():
    """Renders the end-to-end data pipeline architectural stage cards."""
    stages = [
        ("1. Raw CSV Ingestion", "Kaggle Olist Source Dump (9 CSV files, 1.0M+ rows)", "Python, Pandas, OS IO"),
        ("2. Raw Data Quality Gate", "Validates file presence, schemas, encodings, non-empty bounds", "Pydantic, Python"),
        ("3. PySpark Transformation", "Distributed cleansing, geolocation centroid dedup, timestamp parsing", "Apache Spark 3.5, PySpark"),
        ("4. Processed Quality Gate", "12 automated assertions, referential integrity, Parquet schema gate", "PySpark, Parquet"),
        ("5. Kimball Star Schema", "Surrogate keys, 4 dimension tables + 2 fact tables", "PySpark, Parquet"),
        ("6. PostgreSQL Warehouse", "COPY streaming, PK/FK constraints, indexes, 349,405 records", "PostgreSQL 15, DWH Schema"),
        ("7. SQL Analytics & Marts", "7 analytical queries (MoM growth, RFM segmentation, Pareto)", "PostgreSQL, SQL Views"),
        ("8. Apache Airflow (Docker)", "Automated DAG orchestration with LocalExecutor & healthchecks", "Airflow 2.7, Docker Compose"),
        ("9. Streamlit Dashboard", "Interactive presentation layer querying live PostgreSQL warehouse", "Streamlit, Plotly, Pandas"),
    ]

    for title, desc, tech in stages:
        st.markdown(f"""
        <div class="pipeline-step">
            <div class="pipeline-step-title">{title}</div>
            <div class="pipeline-step-desc">{desc}</div>
            <div style="margin-top: 0.35rem;"><span class="tech-badge">{tech}</span></div>
        </div>
        """, unsafe_allow_html=True)
