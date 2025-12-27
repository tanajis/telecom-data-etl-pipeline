# file: scripts/staging.py

import duckdb

def load_raw_churn_to_duckdb(
    db_path: str = "data/staging/staging.db",
    csv_path: str = "data/raw/customer_churn_data.csv",
):
    """
    Load raw churn CSV into DuckDB as raw.raw_churn.
    Creates schema 'raw' if it does not exist.
    """
    con = duckdb.connect(db_path)

    # Create FLAT schema (no nesting)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Load raw data to raw.raw_churn
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.raw_churn AS 
        SELECT * FROM read_csv_auto('{csv_path}')
    """)

    rows = con.execute("SELECT COUNT(*) FROM raw.raw_churn").fetchone()[0]
    tables = con.execute("SHOW TABLES FROM raw").fetchall()

    print(f"Loaded {rows} rows to raw.raw_churn")
    print("Raw tables:", tables)

    con.close()
    return rows, tables

if __name__ == "__main__":
    load_raw_churn_to_duckdb()