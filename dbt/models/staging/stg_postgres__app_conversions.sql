{{
    config(
        materialized = 'view',
        schema = 'bronze'
    )
}}

select *
from {{source('marketing_raw', 'app_conversions')}}
limit 12