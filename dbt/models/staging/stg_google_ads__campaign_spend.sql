{{
    config(
        materialized = 'view',
        schema = 'bronze'
    )
}}

select 
    COALESCE(
        TRY_STRPTIME(date, '%Y-%m-%d'),    -- 2025-03-24
        TRY_STRPTIME(date, '%m/%d/%Y'),    -- 03/24/2025
        TRY_STRPTIME(date, '%d-%b-%Y')     -- 24-Mar-2025
    )::DATE AS spend_date,

    campaign_id::INT AS campaign_id, 

    LOWER(TRIM(campaign_name)) AS campaign_name

    
from {{source('marketing_raw', 'google_ads')}}