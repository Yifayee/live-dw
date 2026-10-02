{{ config(materialized='table') }}

-- Grain: one row per ad impression.
-- Natural key (timestamp, uid, campaign) verified unique in scripts/criteo_check_grain.py.

with src as (
    select * from {{ source('criteo', 'criteo_attribution') }}
)

select
    md5(concat_ws('|',
        cast(timestamp as varchar),
        cast(uid as varchar),
        cast(campaign as varchar)))            as impression_id,

    -- time (relative seconds from dataset start)
    timestamp                                  as impression_ts_sec,
    cast(timestamp // 86400 as integer)        as impression_day,

    -- entities
    uid                                        as user_id,
    campaign                                   as campaign_id,

    -- engagement and spend
    click = 1                                  as is_click,
    cost,
    cpo,

    -- conversion journey (sentinel -1 converted to NULL)
    conversion = 1                             as is_conversion_journey,
    nullif(conversion_id, -1)                  as conversion_id,
    nullif(conversion_timestamp, -1)           as conversion_ts_sec,
    attribution = 1                            as is_attributed,
    nullif(click_pos, -1)                      as click_pos,
    nullif(click_nb, -1)                       as click_nb,
    nullif(time_since_last_click, -1)          as time_since_last_click_sec,

    -- anonymized context features
    cat1, cat2, cat3, cat4, cat5, cat6, cat7, cat8, cat9

from src