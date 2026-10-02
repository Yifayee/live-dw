-- DWS grain: one row per (streamer, day_index)

select
    streamer,
    day_index,
    count(distinct stream_id)  as n_streams,
    count(distinct user_id)    as n_viewers,
    count(*)                   as n_sessions,
    sum(duration_min)          as watch_min
from {{ ref('dwd_watch_session') }}
group by streamer, day_index