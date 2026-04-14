{{
    config(
        materialized = 'view',
        schema = 'bronze'
    )
}}

SELECT 
 {{ dbt_utils.generate_surrogate_key(['message_id']) }} AS stg_segment__tracks_id,

  message_id, 
  type, 
  "event",
  COALESCE(
        try_strptime("timestamp", '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime("timestamp", '%Y-%m-%d %H:%M:%S UTC')
    )  as timestamp ,
  case when user_id = 'N/A' or user_id = '' or user_id = 'null'
  then null else user_id end as user_id,
  anonymous_id,
  (properties ->> 'campaign_id')::INT AS campaign_id,
  properties ->> 'channel' AS channel,
  properties ->> 'page_url' AS page_url,
  properties ->> 'product_id' AS product_id,
  properties ->> 'time_on_page' AS time_on_page,
  properties ->> 'product_name' AS product_name,
  (properties ->> 'price_usd')::FLOAT AS price_usd,
  (properties ->> 'quantity')::INT AS quantity,
  (properties ->> 'cart_total')::FLOAT AS cart_total,
  (properties ->> 'item_count')::INT AS item_count,
  properties ->> 'query' AS query,
  (properties ->> 'results_count')::INT AS results_count,
  
  coalesce((context ->> 'library' ->> 'name'), (context ->> 'library_name')) as library_name, 
  coalesce((context ->> 'library' ->> 'version'), (context ->> 'library_version')) as library_version,
  coalesce((context ->> 'userAgent'), (context ->> 'user_agent')) as user_agent,
  context ->> 'ip' as ip,
  context ->> 'locale' as locale,
  session_id
from {{source('marketing_raw', 'segment_tracks')}}
