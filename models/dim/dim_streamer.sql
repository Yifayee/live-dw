-- DIM grain: one row per streamer
-- Viewer counts reflect the 100k sampled users only, not the streamer's full audience

select
    streamer,
    min(day_index)             as first_day,
    max(day_index)             as last_day,
    count(distinct stream_id)  as n_streams,
    count(distinct user_id)    as n_unique_viewers,
    sum(duration_min)          as total_watch_min
from {{ ref('dwd_watch_session') }}
group by streamer