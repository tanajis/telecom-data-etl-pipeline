{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'raw_churn') }}
)
SELECT
    sha256(CAST(customerID AS VARCHAR)) as anon_customer_id, ---Anonymised.
    TRY_CAST(COALESCE(age, '0') AS INTEGER) as age,
    CASE WHEN gender = 'Male' then 'M'
         WHEN gender = 'Female' then 'F'
         ELSE 'U'        ---- Unknown
        END  as gender,
    TRY_CAST(COALESCE(MonthlyCharges, '0') AS DOUBLE) as monthly_charges,
    COALESCE(ContractType, 'Unknown')  as contract_type,
    COALESCE(InternetService, 'Unknown')  as internet_service,
    TRY_CAST(COALESCE(tenure, '0') AS INTEGER) as tenure_in_month, -- Default Tenure 0  If comes NULL
    TRY_CAST(COALESCE(TotalCharges, '0') AS DOUBLE) as total_charges,-- Default Tenure 0  If comes NULL
    COALESCE(TechSupport, False)  as is_tech_support_opted,
    churn as is_churned,
FROM {{ source('raw', 'raw_churn') }}
WHERE customerID IS NOT NULL