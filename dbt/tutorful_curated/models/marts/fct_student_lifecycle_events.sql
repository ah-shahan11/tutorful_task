{{ config(
    materialized='incremental',
    unique_key=['student_id', 'anchor_lesson_id', 'event_type', 'event_at']
) }}

-- Produces the reusable lifecycle event table containing churn and reactivation events per student.
with student_lesson_gaps as (
    select *
    from {{ ref('int_student_lesson_gaps') }}
),
reactivation_events as (
    -- A reactivation happens when a completed lesson follows a gap of 30 days or more.
    select
        student_id,
        relationship_id,
        tutor_id,
        lesson_id as anchor_lesson_id,
        subject_id,
        'reactivation' as event_type,
        lesson_finished_at as event_at,
        previous_lesson_finished_at,
        lesson_finished_at as current_lesson_finished_at,
        days_since_previous_completed_lesson
    from student_lesson_gaps
    where days_since_previous_completed_lesson >= 30
),
churn_events_between_lessons as (
    -- A churn is recorded 30 days after the previous completed lesson if the next lesson arrives after that threshold.
    select
        student_id,
        relationship_id,
        tutor_id,
        lesson_id as anchor_lesson_id,
        subject_id,
        'churn' as event_type,
        timestamp_add(previous_lesson_finished_at, interval 30 day) as event_at,
        previous_lesson_finished_at,
        lesson_finished_at as current_lesson_finished_at,
        days_since_previous_completed_lesson
    from student_lesson_gaps
    where days_since_previous_completed_lesson >= 30
      and previous_lesson_finished_at is not null
),
latest_completed_lesson as (
    -- Keep the latest completed lesson per student to identify students currently in an open churn period.
    select
        student_id,
        relationship_id,
        tutor_id,
        lesson_id,
        subject_id,
        lesson_finished_at,
        row_number() over (
            partition by student_id
            order by lesson_finished_at desc, lesson_id desc
        ) as row_num
    from student_lesson_gaps
),
open_churn_events as (
    -- If no new lesson arrives, emit a churn event once 30 days have passed since the latest completed lesson.
    select
        student_id,
        relationship_id,
        tutor_id,
        lesson_id as anchor_lesson_id,
        subject_id,
        'churn' as event_type,
        timestamp_add(lesson_finished_at, interval 30 day) as event_at,
        lesson_finished_at as previous_lesson_finished_at,
        cast(null as timestamp) as current_lesson_finished_at,
        timestamp_diff(current_timestamp(), lesson_finished_at, day) as days_since_previous_completed_lesson
    from latest_completed_lesson
    where row_num = 1
      and timestamp_add(lesson_finished_at, interval 30 day) <= current_timestamp()
)
select * from churn_events_between_lessons
union all
select * from reactivation_events
union all
select * from open_churn_events
