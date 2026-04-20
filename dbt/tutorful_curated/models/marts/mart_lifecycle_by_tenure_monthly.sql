{{ config(
    materialized='incremental',
    unique_key=['event_month', 'event_type', 'student_tenure_segment']
) }}

-- Aggregates churn and reactivation events by month and student tenure segment.
select
    date_trunc(date(event_at), month) as event_month,
    event_type,
    student_tenure_segment,
    count(*) as event_count,
    count(distinct student_id) as student_count,
    avg(student_tenure_days) as avg_student_tenure_days
from {{ ref('int_student_tenure_events') }}
group by 1, 2, 3
