-- Fails if concurrency is ever negative, or does not return to 0 after the last step
select 'negative' as issue, step, concurrent_users
from {{ ref('ads_platform_concurrency') }}
where concurrent_users < 0

union all

select 'not_zero_at_end' as issue, step, concurrent_users
from (
    select step, concurrent_users
    from {{ ref('ads_platform_concurrency') }}
    order by step desc
    limit 1
)
where concurrent_users <> 0