{{ config(materialized='table') }}

-- Single-row table describing the observed Criteo ad data window.
-- Shared by ad DWS and ADS models so the partial-day and maturity rules are defined once.
--
-- Maturity: conversions are credited to the last in-window click. For journeys that
-- continue after the window ends, later clicks are not observed, so recent click days
-- collect extra conversions (right truncation). Click days after last_mature_day are
-- therefore biased upward in conversion metrics.

{% set maturity_days = var('ad_maturity_days', 7) %}

with bounds as (
    select max(impression_ts_sec) as last_ts
    from {{ ref('dwd_ad_impression') }}
),

days as (
    select
        last_ts,
        cast(last_ts // 86400 as integer)                 as last_day,
        case
            when (last_ts + 1) % 86400 = 0 then cast(last_ts // 86400 as integer)
            else cast(last_ts // 86400 as integer) - 1
        end                                               as last_full_day
    from bounds
)

select
    last_ts,
    last_day,
    last_full_day,
    {{ maturity_days }}                                   as maturity_days,
    last_full_day - {{ maturity_days }}                   as last_mature_day
from days