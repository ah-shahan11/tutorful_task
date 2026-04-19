{{ config(
    materialized='incremental',
    unique_key=['student_id', 'anchor_lesson_id', 'event_type', 'event_at']
) }}

-- Filters the lifecycle event fact table down to reactivation events that occur in February 2026.
select
    student_id,
    relationship_id,
    tutor_id,
    anchor_lesson_id,
    subject_id,
    event_type,
    event_at,
    previous_lesson_finished_at,
    current_lesson_finished_at,
    days_since_previous_completed_lesson
from {{ ref('fct_student_lifecycle_events') }}
where event_type = 'reactivation'
  and date(event_at) >= date '2026-02-01'
  and date(event_at) < date '2026-03-01'
