{{ config(
    materialized='incremental',
    unique_key=['event_month', 'event_type']
) }}

-- Aggregates lifecycle events into monthly churn and reactivation counts.
select
    date_trunc(date(event_at), month) as event_month,
    event_type,
    count(*) as event_count,
    count(distinct student_id) as student_count
from {{ ref('fct_student_lifecycle_events') }}
group by 1, 2
