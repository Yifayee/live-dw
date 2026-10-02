-- DWD: one row per recorded viewing record (user x stream x start/stop steps)
-- Time unit: 1 step = 10 minutes (per dataset documentation)
-- Note: "viewer" means a user present in the stream's chat, not every viewer

select
    user_id,
    stream_id,
    streamer,
    time_start                        as start_step,
    time_stop                         as stop_step,
    time_stop - time_start            as duration_steps,
    (time_stop - time_start) * 10     as duration_min,   -- 10-minute resolution
    time_start // 144                 as day_index       -- 144 steps = 1 day; relative day, not calendar date
from {{ source('ods', 'raw_twitch') }}