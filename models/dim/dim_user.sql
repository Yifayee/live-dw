-- DIM grain: one row per user_id
-- first_day is the first day observed in the 43-day window, not a signup date
-- (users active on day 0 may have existed before the window: left-censored)

select
    user_id,
    min(day_index)             as first_day,
    max(day_index)             as last_day,
    count(distinct day_index)  as active_days,
    count(*)                   as n_sessions,           -- one row per user x stream in DWD
    count(distinct streamer)   as n_streamers_watched
from {{ ref('dwd_watch_session') }}
group by user_id