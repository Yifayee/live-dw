-- Fails if any (user_id, stream_id) pair appears more than once in DWD
select user_id, stream_id, count(*) as n_rows
from {{ ref('dwd_watch_session') }}
group by user_id, stream_id
having count(*) > 1