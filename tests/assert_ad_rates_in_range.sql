-- CTR, CVR and attribution rate must lie in [0, 1] in both ADS ad models.
-- NULL rates (zero denominators) are allowed. Returns offending rows.

select
    'ads_ad_daily'                as model,
    cast(day_index as varchar)    as row_key,
    ctr, cvr, attribution_rate
from {{ ref('ads_ad_daily') }}
where ctr not between 0 and 1
   or cvr not between 0 and 1
   or attribution_rate not between 0 and 1

union all

select
    'ads_ad_campaign_performance',
    cast(campaign_id as varchar),
    ctr, cvr, attribution_rate
from {{ ref('ads_ad_campaign_performance') }}
where ctr not between 0 and 1
   or cvr not between 0 and 1
   or attribution_rate not between 0 and 1