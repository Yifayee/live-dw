-- DIM grain: exactly one row describing the observed data window
-- 1 step = 10 minutes; 144 steps = 1 day

select
    max(stop_step)              as last_step,
    max(stop_step) // 144       as partial_day,     -- the day containing the last step is incomplete
    max(stop_step) // 144 - 1   as last_full_day
from {{ ref('dwd_watch_session') }}