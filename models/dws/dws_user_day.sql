-- DWS grain: one row per (user_id, day_index)
-- A session is assigned to the day it started
-- watch_min may double count if a user is in several chats at the same time

select
    user_id,
    day_index,
    count(*)                   as n_sessions,
    count(distinct streamer)   as n_streamers,
    sum(duration_min)          as watch_min
from {{ ref('dwd_watch_session') }}
group by user_id, day_index