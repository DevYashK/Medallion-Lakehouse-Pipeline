"""Create a small Gold fact table for dashboard demonstrations without Spark.

This utility is intentionally separate from the production PySpark pipeline.
It is useful on developer machines where Java/Spark networking is unavailable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def main() -> None:
    """Transform the bundled landing sample into a dashboard-ready Parquet file."""
    root = Path(__file__).resolve().parents[1]
    source = root / "data" / "landing" / "orders" / "sample_orders.csv"
    destination = root / "data" / "gold" / "fact_sales"
    destination.mkdir(parents=True, exist_ok=True)
    orders = pd.read_csv(source, parse_dates=["order_timestamp"])
    orders["quantity"] = orders["quantity"].astype("int64")
    orders["unit_price"] = orders["unit_price"].astype(float)
    orders["discount_amount"] = orders["discount_amount"].astype(float)
    orders["gross_amount"] = (orders["quantity"] * orders["unit_price"]).round(2)
    orders["net_amount"] = (orders["gross_amount"] - orders["discount_amount"]).round(2)
    orders["date_key"] = orders["order_timestamp"].dt.strftime("%Y%m%d").astype("int64")
    orders["customer_key"] = pd.factorize(orders["customer_id"])[0].astype("int64") + 1
    orders["product_key"] = pd.factorize(orders["product_id"])[0].astype("int64") + 1
    fact = orders[[
        "order_id", "date_key", "customer_key", "product_key", "quantity",
        "unit_price", "discount_amount", "gross_amount", "net_amount",
    ]]
    fact.to_parquet(destination / "part-00000.parquet", index=False)
    print(f"Created {len(fact)} demo fact rows at {destination}")


if __name__ == "__main__":
    main()
