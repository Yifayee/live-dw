-- Totals in dws_ad_campaign_day must match the DWD tables:
-- no impressions, clicks, cost or conversions lost or duplicated by the aggregation or join.

with dws as (
    select
        sum(impressions)   as impressions,
        sum(clicks)        as clicks,
        sum(cost)          as cost,
        sum(conversions)   as conversions
    from {{ ref('dws_ad_campaign_day') }}
),

dwd as (
    select
        (select count(*)           from {{ ref('dwd_ad_impression') }}) as impressions,
        (select count_if(is_click) from {{ ref('dwd_ad_impression') }}) as clicks,
        (select sum(cost)          from {{ ref('dwd_ad_impression') }}) as cost,
        (select count(*)           from {{ ref('dwd_ad_conversion') }}) as conversions
)

select
    dws.impressions  as dws_impressions,  dwd.impressions  as dwd_impressions,
    dws.clicks       as dws_clicks,       dwd.clicks       as dwd_clicks,
    dws.cost         as dws_cost,         dwd.cost         as dwd_cost,
    dws.conversions  as dws_conversions,  dwd.conversions  as dwd_conversions
from dws, dwd
where dws.impressions != dwd.impressions
   or dws.clicks      != dwd.clicks
   or abs(dws.cost - dwd.cost) > 1e-6
   or dws.conversions != dwd.conversions