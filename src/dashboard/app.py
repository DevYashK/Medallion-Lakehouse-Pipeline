"""Interactive Streamlit dashboard backed by Gold star-schema tables."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st


@st.cache_data(ttl=300)
def load_sales(gold_path: str, file_format: str) -> pd.DataFrame:
    """Load the Gold fact table into pandas for the interactive dashboard."""
    try:
        if file_format == "parquet":
            return pd.read_parquet(f"{gold_path}/fact_sales")
        from lakehouse_pipeline.spark import get_spark
        return get_spark("sales-dashboard").read.format(file_format).load(f"{gold_path}/fact_sales").toPandas()
    except Exception as exc:
        raise RuntimeError(f"Unable to load Gold sales data: {exc}") from exc


def main() -> None:
    """Render executive KPIs and sales trends."""
    st.set_page_config(page_title="Sales Lakehouse", page_icon="📊", layout="wide")
    st.title("Sales Lakehouse Dashboard")
    gold_path, file_format = os.getenv("GOLD_PATH", "data/gold"), os.getenv("FILE_FORMAT", "parquet")
    try:
        sales = load_sales(gold_path, file_format)
    except RuntimeError as exc:
        st.error(str(exc))
        st.info("Run the pipeline first, then set GOLD_PATH if the Gold layer is elsewhere.")
        return
    if sales.empty:
        st.warning("The fact_sales table is empty.")
        return
    sales["net_amount"] = pd.to_numeric(sales["net_amount"])
    a, b, c = st.columns(3)
    a.metric("Net sales", f"${sales.net_amount.sum():,.2f}")
    b.metric("Orders", f"{sales.order_id.nunique():,}")
    c.metric("Units sold", f"{sales.quantity.sum():,}")
    st.subheader("Sales by date")
    trend = sales.groupby("date_key", as_index=False)["net_amount"].sum().sort_values("date_key")
    st.line_chart(trend, x="date_key", y="net_amount")
    st.subheader("Top products")
    st.bar_chart(sales.groupby("product_key")["net_amount"].sum().sort_values(ascending=False).head(10))
    st.dataframe(sales.sort_values("net_amount", ascending=False), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
