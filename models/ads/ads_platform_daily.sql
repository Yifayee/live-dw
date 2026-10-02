-- ADS grain: one row per day_index
-- DAU = users with at least one session starting that day (chat presence, 100k sample)
-- Day 42 is partial (data ends at step 6148); flagged by is_full_day

with bounds as (
    -- The day containing the last recorded step is incomplete
    select max(stop_step) // 144 as partial_day
    from {{ ref('dwd_watch_session') }}
),

user_side as (
    select
        day_index,
        count(*)          as dau,          -- dws_user_day has one row per user per day
        sum(n_sessions)   as sessions,
        sum(watch_min)    as watch_min
    from {{ ref('dws_user_day') }}
    group by day_index
),

streamer_side as (
    select day_index, count(*) as active_streamers
    from {{ ref('dws_streamer_day') }}
    group by day_index
),

pcu as (
    select day_index, max(concurrent_users) as pcu
    from {{ ref('ads_platform_concurrency') }}
    group by day_index
)

select
    u.day_index,
    u.dau,
    s.active_streamers,
    p.pcu,
    u.sessions,
    u.watch_min,
    round(u.watch_min * 1.0 / u.dau, 1)  as avg_watch_min_per_dau,
    u.day_index < b.partial_day          as is_full_day
from user_side u
join streamer_side s using (day_index)
left join pcu p using (day_index)
cross join bounds b
order by u.day_index