-- Every (conversion_id, user_id) pair in dwd_ad_impression must appear exactly once
-- in dwd_ad_conversion. Returns a row (test fails) only if the counts differ.

with expected as (
    select count(*) as n
    from (
        select distinct conversion_id, user_id
        from {{ ref('dwd_ad_impression') }}
        where conversion_id is not null
    )
),

actual as (
    select count(*) as n
    from {{ ref('dwd_ad_conversion') }}
)

select
    expected.n as expected_n,
    actual.n   as actual_n
from expected, actual
where expected.n != actual.n