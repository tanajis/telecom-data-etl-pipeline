-- models/dim/contract_type.sql
{{ config(materialized='table') }}

WITH src AS (
    SELECT DISTINCT
        contract_type,
    FROM {{ ref('telecom_churn_stg') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['contract_type']) }} as contract_type_key,
    contract_type
FROM src
