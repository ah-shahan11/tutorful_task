{{ config(
    materialized='incremental',
    unique_key=['event_month', 'event_type', 'subject_id']
) }}

-- Aggregates churn and reactivation events by month and subject.
select
    date_trunc(date(e.event_at), month) as event_month,
    e.event_type,
    e.subject_id,
    s.subject_name,
    count(*) as event_count,
    count(distinct e.student_id) as student_count
from {{ ref('fct_student_lifecycle_events') }} as e
left join {{ source('staging_data', 'subjects') }} as s
    on e.subject_id = s.subject_id
group by 1, 2, 3, 4
