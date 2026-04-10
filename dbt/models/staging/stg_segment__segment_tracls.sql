{{
    config(
        materialized = 'view',
        schema = 'bronze'
    )
}}

select *
from {{source('marketing_raw', 'segment_tracks')}}
limit 12