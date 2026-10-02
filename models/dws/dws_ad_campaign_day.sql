{{ config(materialized='table') }}

-- Grain: one row per (campaign_id, day_index).
-- Additive measures only; ratios (CTR, CVR, CPA) are computed in ADS.
-- Conversions are assigned to the day of the last click on the journey (click-date
-- attribution), so clicks and conversions on the same row come from the same traffic.
-- Window rules (full day, mature day) come from dim_ad_data_window.

with impressions as (
    select
        campaign_id,
        impression_day               as day_index,
        count(*)                     as impressions,
        count_if(is_click)           as clicks,
        sum(cost)                    as cost,
        count(distinct user_id)      as reach_users
    from {{ ref('dwd_ad_impression') }}
    group by all
),

conversions as (
    select
        campaign_id,
        cast(last_click_ts_sec // 86400 as integer)   as day_index,
        count(*)                                      as conversions,
        count_if(is_attributed)                       as attributed_conversions
    from {{ ref('dwd_ad_conversion') }}
    group by all
)

select
    i.campaign_id,
    i.day_index,
    i.impressions,
    i.clicks,
    i.cost,
    i.reach_users,
    coalesce(c.conversions, 0)               as conversions,
    coalesce(c.attributed_conversions, 0)    as attributed_conversions,
    i.day_index <= w.last_full_day           as is_full_day,
    i.day_index <= w.last_mature_day         as is_mature_day

from impressions i
left join conversions c
    on  i.campaign_id = c.campaign_id
    and i.day_index   = c.day_index
cross join {{ ref('dim_ad_data_window') }} w