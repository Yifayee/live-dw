{{ config(materialized='table') }}

-- Grain: one row per (conversion_id, user_id).
-- conversion_id alone is NOT unique: 2,910 IDs are shared by different users with
-- different conversion times (source ID collision). Within (conversion_id, user_id),
-- campaign, conversion time and attribution are constant, so they are part of the
-- GROUP BY. If that ever stops holding, the unique test on conversion_key fails.

with journey as (
    select *
    from {{ ref('dwd_ad_impression') }}
    where is_conversion_journey
),

shared_ids as (
    select conversion_id
    from journey
    group by conversion_id
    having count(distinct user_id) > 1
)

select
    md5(concat_ws('|',
        cast(j.conversion_id as varchar),
        cast(j.user_id as varchar)))                     as conversion_key,

    j.conversion_id,
    j.user_id,
    j.campaign_id,
    j.conversion_ts_sec,
    cast(j.conversion_ts_sec // 86400 as integer)        as conversion_day,
    j.is_attributed,

    -- journey shape
    count(*)                                             as n_journey_clicks,
    min(j.impression_ts_sec)                             as first_click_ts_sec,
    max(j.impression_ts_sec)                             as last_click_ts_sec,
    j.conversion_ts_sec - max(j.impression_ts_sec)       as conversion_lag_sec,

    -- data quality flag: this source ID is also used by another user
    j.conversion_id in (select conversion_id from shared_ids) as is_shared_conversion_id

from journey j
group by
    j.conversion_id,
    j.user_id,
    j.campaign_id,
    j.conversion_ts_sec,
    j.is_attributed