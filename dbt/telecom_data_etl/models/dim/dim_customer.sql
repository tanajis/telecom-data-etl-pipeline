-- models/dim/customer.sql
{{ config(materialized='table') }}

WITH src AS (
    SELECT
        anon_customer_id,        -- business key
        age,
        gender,
        is_churned
    FROM {{ ref('telecom_churn_stg') }}
)

SELECT
    anon_customer_id,
    {{ dbt_utils.generate_surrogate_key(['anon_customer_id']) }} as customer_key,
    age,
    gender,
    is_churned
FROM src
