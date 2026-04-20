{{ config(
    materialized='incremental',
    unique_key=['student_tenure_segment', 'event_type']
) }}

-- Materialises the February 2026 tenure-level churn and reactivation view used in the business readout.
select
    student_tenure_segment,
    event_type,
    event_count,
    student_count,
    avg_student_tenure_days
from {{ ref('mart_lifecycle_by_tenure_monthly') }}
where event_month = date '2026-02-01'
