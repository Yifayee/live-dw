{{ config(materialized='table') }}

-- Grain: one row per campaign_id, over mature days only (see dim_ad_data_window).
-- Campaigns below the conversion threshold get a NULL cpa_rank because their CPA
-- is too noisy to compare. Threshold is configurable via --vars.

{% set min_conv = var('ad_min_conversions_for_rank', 30) %}

with totals as (
    select
        campaign_id,
        count(*)                       as active_days,
        sum(impressions)               as impressions,
        sum(clicks)                    as clicks,
        sum(cost)                      as cost,
        sum(conversions)               as conversions,
        sum(attributed_conversions)    as attributed_conversions
    from {{ ref('dws_ad_campaign_day') }}
    where is_mature_day
    group by campaign_id
),

metrics as (
    select
        *,
        clicks      / nullif(impressions, 0)               as ctr,
        conversions / nullif(clicks, 0)                    as cvr,
        cost        / nullif(conversions, 0)               as cpa,
        1000 * cost / nullif(impressions, 0)               as cpm,
        attributed_conversions / nullif(conversions, 0)    as attribution_rate,
        conversions >= {{ min_conv }}                      as is_rankable
    from totals
)

select
    *,
    case when is_rankable
         then rank() over (partition by is_rankable order by cpa asc)
    end                                                    as cpa_rank,
    cost / sum(cost) over ()                               as share_of_cost
from metrics