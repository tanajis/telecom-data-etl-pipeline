
1. Create Python Envionment

python3 -m venv .venv
.venv\Scripts\Activate.ps1

2. install all dependencies
pip install -r requirements.txt

3.install  duckdb

4. Create Databases "staging.db" and load raw data using python script staging.py
py .\scripts\staging.py

Check tables loaded:
C:\Users\tanaj\Downloads\duckdb_cli-windows-amd64\duckdb.exe data/staging/staging.db
select * from information_schema.tables;
select * from raw.raw_churn limit 5;

┌────────────┬───────┬─────────┬────────┬────────────────┬────────────────┬─────────────────┬──────────────┬─────────────┬─────────┐
│ CustomerID │  Age  │ Gender  │ Tenure │ MonthlyCharges │  ContractType  │ InternetService │ TotalCharges │ TechSupport │  Churn  │
│   int64    │ int64 │ varchar │ int64  │     double     │    varchar     │     varchar     │    double    │   boolean   │ boolean │
├────────────┼───────┼─────────┼────────┼────────────────┼────────────────┼─────────────────┼──────────────┼─────────────┼─────────┤
│          1 │    49 │ Male    │      4 │          88.35 │ Month-to-Month │ Fiber Optic     │        353.4 │ true        │ true    │
│          2 │    43 │ Male    │      0 │          36.67 │ Month-to-Month │ Fiber Optic     │          0.0 │ true        │ true    │
│          3 │    51 │ Female  │      2 │          63.79 │ Month-to-Month │ Fiber Optic     │       127.58 │ false       │ true    │
│          4 │    60 │ Female  │      8 │         102.34 │ One-Year       │ DSL             │       818.72 │ true        │ true    │
│          5 │    42 │ Male    │     32 │          69.01 │ Month-to-Month │ None            │      2208.32 │ false       │ true    │
└────────────┴───────┴─────────┴────────┴────────────────┴────────────────┴─────────────────┴──────────────┴─────────────┴─────────┘

5. go to folder dbt/telecom_data_etl

 run dbt models
    dbt deps
    dbt run


6. Check tables loaded:
C:\Users\tanaj\Downloads\duckdb_cli-windows-amd64\duckdb.exe data/staging/staging.db
select * from information_schema.tables;
┌───────────────┬──────────────┬──────────────────────┬────────────┬───┬────────────────────┬──────────┬───────────────┬───────────────┐
│ table_catalog │ table_schema │      table_name      │ table_type │ . │ is_insertable_into │ is_typed │ commit_action │ TABLE_COMMENT │
│    varchar    │   varchar    │       varchar        │  varchar   │   │      varchar       │ varchar  │    varchar    │    varchar    │
├───────────────┼──────────────┼──────────────────────┼────────────┼───┼────────────────────┼──────────┼───────────────┼───────────────┤
│ staging       │ main         │ dim_contract_type    │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
│ staging       │ main         │ dim_customer         │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
│ staging       │ main         │ dim_internet_service │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
│ staging       │ main         │ fact_customer_reve.  │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
│ staging       │ main         │ fact_revenue_by_in.  │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
│ staging       │ main         │ telecom_churn_stg    │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
│ staging       │ raw          │ raw_churn            │ BASE TABLE │ . │ YES                │ NO       │ NULL          │ NULL          │
├───────────────┴──────────────┴──────────────────────┴────────────┴───┴────────────────────┴──────────┴───────────────┴───────────────┤
│ 7 rows                                                                                                          13 columns (8 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘


select * from main.dim_contract_type limit 5;
select * from main.dim_customer limit 5;
select * from main.dim_internet_service limit 5;
select * from main.fact_customer_revenue limit 5;
select * from main.fact_revenue_by_internet_service limit 5;

D select * from main.fact_customer_revenue limit 5;
┌──────────────────────────────────┬─────────────────┬─────────────────┬───────────────┐
│           customer_key           │ tenure_in_month │ monthly_charges │ total_charges │
│             varchar              │      int32      │     double      │    double     │
├──────────────────────────────────┼─────────────────┼─────────────────┼───────────────┤
│ ddf55f1bf1e2edf05232e268211f9bcd │               4 │           88.35 │         353.4 │
│ 506cd8d4605ab25caf236baccf63d8b0 │               0 │           36.67 │           0.0 │
│ 282b5b0967ea931ce9d6be6b895c24e6 │               2 │           63.79 │        127.58 │
│ 6bbb83b41053f6bacd126f8132914083 │               8 │          102.34 │        818.72 │
│ 62e5d2b779e51361bec18520e075af19 │              32 │           69.01 │       2208.32 │
└──────────────────────────────────┴─────────────────┴─────────────────┴───────────────┘
D select * from main.fact_revenue_by_internet_service limit 5;
┌──────────────────┬────────────────┬────────────────┬──────────────────────┬─────────────────────────┬───────────────────┬───────────────────┐
│ internet_service │ contract_type  │ customer_count │ total_lifetime_rev.  │ avg_lifetime_revenue_.  │ churned_customers │  churn_rate_pct   │
│     varchar      │    varchar     │     int64      │        double        │         double          │      int128       │      double       │
├──────────────────┼────────────────┼────────────────┼──────────────────────┼─────────────────────────┼───────────────────┼───────────────────┤
│ DSL              │ Month-to-Month │            154 │   231000.58999999994 │      1500.0038311688309 │               154 │             100.0 │
│ DSL              │ One-Year       │             82 │   114692.04000000002 │      1398.6834146341466 │                53 │ 64.63414634146342 │
│ DSL              │ Two-Year       │             72 │    82488.05000000002 │      1145.6673611111114 │                53 │ 73.61111111111111 │
│ Fiber Optic      │ Month-to-Month │            199 │    283341.2800000001 │      1423.8255276381915 │               199 │             100.0 │
│ Fiber Optic      │ One-Year       │            117 │            165634.07 │       1415.675811965812 │                75 │  64.1025641025641 │
└──────────────────┴────────────────┴────────────────┴──────────────────────┴─────────────────────────┴───────────────────┴───────────────────┘
D

=========================


pip install apache-airflow
$env:AIRFLOW_HOME = "C:\Users\tanaj\PycharmProjects\telecom-data-etl-pipeline"
airflow standalone
localhost:8080