-- ADS grain: one row per cohort_day (users grouped by first observed day)
-- Day 0 excluded (left-censored); D1/D7 set to NULL when the target day
-- is outside the last full day, so partial data never lowers the rate
-- Caveat: first observed day is a proxy for "new user", not signup date

with bounds as (
    select last_full_day
    from {{ ref('dim_data_window') }}
),

cohort as (
    select user_id, first_day
    from {{ ref('dim_user') }}
    where first_day >= 1
),

activity as (
    select user_id, day_index
    from {{ ref('dws_user_day') }}
)

select
    c.first_day                   as cohort_day,
    count(*)                      as cohort_size,
    case when c.first_day + 1 <= b.last_full_day
         then round(count(a1.user_id) * 1.0 / count(*), 4) end  as retention_d1,
    case when c.first_day + 7 <= b.last_full_day
         then round(count(a7.user_id) * 1.0 / count(*), 4) end  as retention_d7
from cohort c
cross join bounds b
left join activity a1
       on a1.user_id = c.user_id and a1.day_index = c.first_day + 1
left join activity a7
       on a7.user_id = c.user_id and a7.day_index = c.first_day + 7
group by c.first_day, b.last_full_day
order by c.first_day