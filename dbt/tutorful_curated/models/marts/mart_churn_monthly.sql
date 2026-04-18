{{ config(materialized='table') }}

select
    date_trunc(date(event_at), month) as event_month,
    event_type,
    count(*) as event_count,
    count(distinct student_id) as student_count
from {{ ref('fct_student_lifecycle_events') }}
group by 1, 2
