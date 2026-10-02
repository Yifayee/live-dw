{{ config(materialized='table') }}

-- Grain: one row per day_index. Platform-level ad metrics.
-- Ratios are computed from summed numerators and denominators, never averaged.
-- Daily reach is omitted on purpose: reach_users in DWS is per campaign, and a user
-- can see several campaigns on the same day, so summing it would double count.
-- Conversion metrics on immature days are biased upward (see dim_ad_data_window).

select
    day_index,
    count(*)                                                  as active_campaigns,
    sum(impressions)                                          as impressions,
    sum(clicks)                                               as clicks,
    sum(cost)                                                 as cost,
    sum(conversions)                                          as conversions,
    sum(attributed_conversions)                               as attributed_conversions,

    sum(clicks)      / nullif(sum(impressions), 0)            as ctr,
    sum(conversions) / nullif(sum(clicks), 0)                 as cvr,
    sum(cost)        / nullif(sum(conversions), 0)            as cpa,
    1000 * sum(cost) / nullif(sum(impressions), 0)            as cpm,
    sum(attributed_conversions) / nullif(sum(conversions), 0) as attribution_rate,

    bool_and(is_full_day)                                     as is_full_day,
    bool_and(is_mature_day)                                   as is_mature_day

from {{ ref('dws_ad_campaign_day') }}
group by day_index