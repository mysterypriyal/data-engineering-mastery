"""
Local (pandas) prototype of the Contoso Retail sales-cleaning logic,
matching fabric/01-fabric-fundamentals/notebooks/clean_sales_data_fabric.ipynb
column-for-column (SalesID/Date/Region/Amount/ProductID/CustomerID/
LoadTimestamp). Same six steps, run against the mock JSON directly with
pandas instead of Spark -- useful for iterating on the logic fast before
spending Fabric Spark-session time on it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

MOCK_DATA_DIR = Path(__file__).resolve().parent.parent / "mock-data"


def load_json(filename: str) -> pd.DataFrame:
    with open(MOCK_DATA_DIR / filename) as f:
        return pd.DataFrame(json.load(f))


def parse_date(series: pd.Series) -> pd.Series:
    a = pd.to_datetime(series, format="%Y/%m/%d", errors="coerce")
    b = pd.to_datetime(series, format="%m-%d-%Y", errors="coerce")
    return a.fillna(b)


def clean_sales(sales_raw: pd.DataFrame) -> pd.DataFrame:
    # Step 2: keep latest row per SalesID (by LoadTimestamp)
    sales_raw = sales_raw.copy()
    sales_raw["LoadTimestamp"] = pd.to_datetime(sales_raw["LoadTimestamp"])
    deduped = (
        sales_raw.sort_values("LoadTimestamp", ascending=False)
        .drop_duplicates(subset="SalesID", keep="first")
        .reset_index(drop=True)
    )
    print(f"Raw rows: {len(sales_raw)}  ->  Deduplicated rows: {len(deduped)}")

    # Step 3: standardize dates, casing, types
    cleaned = deduped.copy()
    cleaned["SaleDate"] = parse_date(cleaned["Date"])
    cleaned["Region"] = cleaned["Region"].str.title()
    cleaned["Amount"] = pd.to_numeric(cleaned["Amount"], errors="coerce")
    cleaned = cleaned[["SalesID", "SaleDate", "ProductID", "CustomerID", "Region", "Amount"]]

    missing_product = cleaned[cleaned["ProductID"].isna()]
    print(f"Rows with missing ProductID (flagged, not dropped): {missing_product['SalesID'].tolist()}")

    return deduped, cleaned


def late_arriving(deduped: pd.DataFrame) -> pd.DataFrame:
    # Step 4: SaleDate vs LoadTimestamp gap > 1 day
    df = deduped.copy()
    df["SaleDate2"] = parse_date(df["Date"])
    df["LoadDate"] = df["LoadTimestamp"].dt.normalize()
    df["GapDays"] = (df["LoadDate"] - df["SaleDate2"]).dt.days
    return df[df["GapDays"] > 1][["SalesID", "SaleDate2", "LoadDate", "GapDays"]]


def sales_by_region(cleaned: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    # Step 6: join to customers, group by the customer's (clean) Region
    joined = cleaned.merge(customers, on="CustomerID", suffixes=("", "_cust"))
    result = (
        joined.groupby("Region_cust")
        .agg(TotalSales=("Amount", "sum"), OrderCount=("SalesID", "count"))
        .reset_index()
        .rename(columns={"Region_cust": "Region"})
        .sort_values("TotalSales", ascending=False)
    )
    return result


def main() -> None:
    sales_raw = load_json("sales_raw.json")
    products = load_json("products.json")
    customers = load_json("customers.json")

    deduped, cleaned = clean_sales(sales_raw)
    print(cleaned.to_string(index=False))

    print("\nLate-arriving rows:")
    print(late_arriving(deduped).to_string(index=False))

    print("\nSales by region:")
    print(sales_by_region(cleaned, customers).to_string(index=False))


if __name__ == "__main__":
    main()
