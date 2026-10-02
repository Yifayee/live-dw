-- Each (campaign_id, day_index) must appear exactly once in dws_ad_campaign_day.

select
    campaign_id,
    day_index,
    count(*) as n
from {{ ref('dws_ad_campaign_day') }}
group by all
having count(*) > 1