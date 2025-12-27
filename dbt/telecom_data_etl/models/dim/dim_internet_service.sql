-- models/dim/internet_service.sql
{{ config(materialized='table') }}

WITH src AS (
    SELECT DISTINCT
        internet_service,
    FROM {{ ref('telecom_churn_stg') }}               -- or your staging model name
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['internet_service']) }} as internet_service_key,
    internet_service
FROM src
