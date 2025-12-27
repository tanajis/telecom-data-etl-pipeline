-- models/fact/fact_customer_revenue.sql
{{ config(materialized='table') }}

WITH src AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['anon_customer_id']) }} as customer_key,
        tenure_in_month,
        monthly_charges,
        total_charges
    FROM {{ ref('telecom_churn_stg') }}
)

SELECT *
FROM src
