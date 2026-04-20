{{ config(
    materialized='incremental',
    unique_key=['event_month', 'event_type', 'tutor_id']
) }}

-- Aggregates churn and reactivation events by month and tutor.
select
    date_trunc(date(event_at), month) as event_month,
    event_type,
    tutor_id,
    count(*) as event_count,
    count(distinct student_id) as student_count
from {{ ref('fct_student_lifecycle_events') }}
group by 1, 2, 3
