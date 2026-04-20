{{ config(
    materialized='incremental',
    unique_key=['student_id', 'anchor_lesson_id', 'event_type', 'event_at']
) }}

-- Enriches lifecycle events with student tenure so new and established cohorts can be analysed separately.
with first_completed_lesson as (
    select
        student_id,
        min(lesson_finished_at) as first_completed_lesson_at
    from {{ ref('int_completed_lessons') }}
    group by 1
)
select
    e.student_id,
    e.relationship_id,
    e.tutor_id,
    e.anchor_lesson_id,
    e.subject_id,
    e.event_type,
    e.event_at,
    e.previous_lesson_finished_at,
    e.current_lesson_finished_at,
    e.days_since_previous_completed_lesson,
    f.first_completed_lesson_at,
    timestamp_diff(e.event_at, f.first_completed_lesson_at, day) as student_tenure_days,
    case
        when timestamp_diff(e.event_at, f.first_completed_lesson_at, day) < 90 then 'new'
        else 'established'
    end as student_tenure_segment
from {{ ref('fct_student_lifecycle_events') }} as e
inner join first_completed_lesson as f
    on e.student_id = f.student_id
