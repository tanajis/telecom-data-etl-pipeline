-- models/fact/fact_revenue_by_service.sql
{{ config(materialized='table') }}

WITH base AS (
    SELECT
        internet_service,
        contract_type,
        monthly_charges,
        total_charges,
        tenure_in_month,
        is_churned
    FROM {{ ref('telecom_churn_stg') }}
),

agg AS (
    SELECT
        internet_service,
        contract_type,
        COUNT(*)                               AS customer_count,
        SUM(total_charges)                     AS total_lifetime_revenue,
        AVG(total_charges)                     AS avg_lifetime_revenue_per_customer,
        SUM(CASE WHEN is_churned = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
        100.0 * SUM(CASE WHEN is_churned = 'Yes' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0)           AS churn_rate_pct
    FROM base
    GROUP BY
        internet_service,
        contract_type
)

SELECT
    internet_service,
    contract_type,
    customer_count,
    total_lifetime_revenue,
    avg_lifetime_revenue_per_customer,
    churned_customers,
    churn_rate_pct
FROM agg
ORDER BY
    internet_service,
    contract_type
