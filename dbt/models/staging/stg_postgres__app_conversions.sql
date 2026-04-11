{{
    config(
        materialized = 'view',
        schema = 'bronze'
    )
}}

SELECT  
 {{ dbt_utils.generate_surrogate_key(['conversion_id']) }} AS stg_postgres__conversions_id,
  conversion_id,
  usr_id AS user_id, 
  cmpgn_id AS campaign_id, 
  conv_type_cd AS conversion_type_code, 
  CASE
    WHEN conv_type_cd = 1 THEN 'purchase'
    WHEN conv_type_cd = 2 THEN 'signup'
    WHEN conv_type_cd = 3 THEN 'trial_start'
    WHEN conv_type_cd = 4 THEN 'demo_request'
  END AS conversion_type,
  revenue_amt AS revenue_amount,
  conv_ts::TIMESTAMPTZ AT TIME ZONE 'UTC'  AS conversion_timestamp 
FROM {{source('marketing_raw', 'app_conversions')}}