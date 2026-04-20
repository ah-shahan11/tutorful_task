{{ config(
    materialized='incremental',
    unique_key=['tutor_id', 'event_type']
) }}

-- Materialises the February 2026 tutor-level churn and reactivation view used in the business readout.
with february_tutors as (
    select
        tutor_id,
        event_type,
        event_count,
        student_count,
        row_number() over (
            partition by event_type
            order by event_count desc, tutor_id asc
        ) as event_rank
    from {{ ref('mart_lifecycle_by_tutor_monthly') }}
    where event_month = date '2026-02-01'
)
select *
from february_tutors
