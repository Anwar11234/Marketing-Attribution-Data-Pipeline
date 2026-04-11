{{
    config(
        materialized = 'view',
        schema = 'bronze'
    )
}}


WITH source_data AS (

    SELECT      
        COALESCE(
            TRY_STRPTIME(date, '%Y-%m-%d'),    -- 2025-03-24
            TRY_STRPTIME(date, '%m/%d/%Y'),    -- 03/24/2025
            TRY_STRPTIME(date, '%d-%b-%Y')     -- 24-Mar-2025
        )::DATE AS spend_date,

        campaign_id::INT AS campaign_id, 

        LOWER(TRIM(campaign_name)) AS campaign_name,

        CASE lower(trim(channel))
            WHEN 'search'           THEN 'search'
            WHEN 'paid search'      THEN 'search'
            WHEN 'paid_search'      THEN 'search'
            WHEN 'display'          THEN 'display'
            WHEN 'display_network'  THEN 'display'
            WHEN 'video'            THEN 'video'
            WHEN 'youtube_video'    THEN 'video'
            WHEN 'shopping'         THEN 'shopping'
            WHEN 'product_shopping' THEN 'shopping'
            ELSE NULL
        END AS channel,

        spend_usd AS spend_amount,
        impressions,
        clicks
    FROM {{source('marketing_raw', 'google_ads')}}
    WHERE soft_deleted = false
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['spend_date', 'campaign_id'])}} AS stg_google_ads__campaign_spend_id,
    *

FROM source_data