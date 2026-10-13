"""
Local (pandas) prototype of the Contoso Retail sales-cleaning logic.

Purpose: iterate fast on cleaning rules against claude/mock-data/sales_raw.json
before porting the same logic to PySpark in
claude/notebooks/clean_sales_data_fabric.ipynb, where it runs against the
Lakehouse bronze/silver tables over OneLake.

Rules applied (bronze -> silver):
1. Drop exact-duplicate rows (same sale_id + payload).
2. Trim whitespace on sale_id / customer_id / payment_method; normalize
   payment_method casing.
3. Cast unit_price to float (handles "19.99" string rows); cast quantity
   and discount_pct to int.
4. Parse sale_date from mixed formats (ISO, ISO w/ time, MM/DD/YYYY,
   DD-Mon-YYYY) into a single date type.
5. Drop rows with missing/blank customer_id, missing unit_price, or
   missing sale_date -- these can't be reliably reconciled.
6. Drop rows with non-positive quantity or out-of-range discount_pct
   (0-100) -- treat as bad data, not real transactions.
7. Flag (not drop) orphan customer_id / product_id that don't exist in
   the reference dimension files, so downstream can decide.
8. Recompute line_total = quantity * unit_price * (1 - discount_pct/100).
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

MOCK_DATA_DIR = Path(__file__).resolve().parent.parent / "mock-data"

DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%d-%b-%Y"]


def parse_sale_date(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def load_json(filename: str) -> pd.DataFrame:
    with open(MOCK_DATA_DIR / filename) as f:
        return pd.DataFrame(json.load(f))


def clean_sales(sales: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    df = sales.copy()

    # 1. drop exact duplicates
    df = df.drop_duplicates()

    # 2. trim/normalize string fields
    for col in ("sale_id", "customer_id", "product_id", "store_id"):
        df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)
    df["payment_method"] = (
        df["payment_method"].apply(lambda v: v.strip().title() if isinstance(v, str) else v)
    )

    # 3. cast numeric types
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    # 4. parse dates
    df["sale_date"] = df["sale_date"].apply(parse_sale_date)

    # 5. drop rows missing required fields
    df["customer_id"] = df["customer_id"].replace("", None)
    df = df.dropna(subset=["customer_id", "unit_price", "sale_date"])

    # 6. drop invalid business values
    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]
    df = df[(df["discount_pct"] >= 0) & (df["discount_pct"] <= 100)]

    # 7. flag orphans instead of dropping
    known_customers = set(customers["customer_id"])
    known_products = set(products["product_id"])
    df["is_orphan_customer"] = ~df["customer_id"].isin(known_customers)
    df["is_orphan_product"] = ~df["product_id"].isin(known_products)

    # 8. recompute line_total
    df["line_total"] = (
        df["quantity"] * df["unit_price"] * (1 - df["discount_pct"] / 100)
    ).round(2)

    return df.reset_index(drop=True)


def main() -> None:
    sales = load_json("sales_raw.json")
    customers = load_json("customers.json")
    products = load_json("products.json")

    print(f"Raw rows:     {len(sales)}")

    cleaned = clean_sales(sales, customers, products)

    print(f"Cleaned rows: {len(cleaned)}")
    print(f"Orphan customer rows: {cleaned['is_orphan_customer'].sum()}")
    print(f"Orphan product rows:  {cleaned['is_orphan_product'].sum()}")
    print(cleaned.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
