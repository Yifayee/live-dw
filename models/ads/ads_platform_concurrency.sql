-- ADS grain: one row per time step that has at least one start or stop event
-- Assumption: a user counts as online during [start_step, stop_step)
-- Steps with no events are omitted; concurrency is unchanged across them

with events as (
    select start_step as step,  1 as delta from {{ ref('dwd_watch_session') }}
    union all
    select stop_step  as step, -1 as delta from {{ ref('dwd_watch_session') }}
),

per_step as (
    -- Net change in online users at each step
    select step, sum(delta) as net_change
    from events
    group by step
),

running as (
    -- Running total of net changes = users online at that step
    select
        step,
        sum(net_change) over (
            order by step
            rows between unbounded preceding and current row
        ) as concurrent_users
    from per_step
)

select
    step,
    step // 144 as day_index,
    concurrent_users
from running