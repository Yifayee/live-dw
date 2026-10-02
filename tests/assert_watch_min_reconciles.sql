-- Fails if total watch minutes differ between DWD and either DWS table
with totals as (
    select
        (select sum(duration_min) from {{ ref('dwd_watch_session') }}) as dwd_total,
        (select sum(watch_min)    from {{ ref('dws_user_day') }})      as user_day_total,
        (select sum(watch_min)    from {{ ref('dws_streamer_day') }})  as streamer_day_total
)
select *
from totals
where dwd_total <> user_day_total
   or dwd_total <> streamer_day_total