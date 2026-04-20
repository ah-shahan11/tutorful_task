{{ config(
    materialized='incremental',
    unique_key=['subject_id', 'event_type']
) }}

-- Materialises the February 2026 subject-level churn and reactivation view used in the business readout.
with february_subjects as (
    select
        subject_id,
        subject_name,
        event_type,
        event_count,
        student_count,
        row_number() over (
            partition by event_type
            order by event_count desc, subject_id asc
        ) as event_rank
    from {{ ref('mart_lifecycle_by_subject_monthly') }}
    where event_month = date '2026-02-01'
)
select *
from february_subjects
