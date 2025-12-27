import duckdb
import pandas as pd
import matplotlib.pyplot as plt


def fetch_revenue_by_service(
    db_path: str = "data/staging/staging.db",
    table: str = "main.fact_revenue_by_internet_service",
) -> pd.DataFrame:
    """
    Query DuckDB for revenue & churn metrics grouped by internet_service + contract_type.
    """
    con = duckdb.connect(db_path)
    query = f"""
        SELECT
            internet_service,
            sum(total_lifetime_revenue) as total_lifetime_revenue,
        FROM {table}
        GROUP BY total_lifetime_revenue
        ORDER BY internet_service
    """
    df = con.execute(query).df()  # DuckDB → pandas DataFrame[web:342][web:348]
    con.close()
    return df


def plot_revenue_by_service(df: pd.DataFrame) -> None:
    """
    Create bar charts : total_lifetime_revenue by internet_service
    """
    # Build combined label like "Fiber Optic\nMonth-to-Month"
    df = df.copy()
    df["label"] = df["internet_service"]

    # --- Revenue bar chart ---
    plt.figure(figsize=(12, 6))
    plt.bar(df["label"], df["total_lifetime_revenue"], color="steelblue")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Total lifetime revenue")
    plt.title("Total Lifetime Revenue by Internet Service")
    plt.tight_layout()
    plt.show()  # matplotlib bar chart[web:346][web:355]

def main():
    df = fetch_revenue_by_service()
    print(df.head())
    plot_revenue_by_service(df)


if __name__ == "__main__":
    main()
