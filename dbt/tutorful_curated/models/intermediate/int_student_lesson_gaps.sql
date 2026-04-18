{{ config(materialized='table') }}

with completed_lessons as (
    select *
    from {{ ref('int_completed_lessons') }}
),
ordered_lessons as (
    select
        student_id,
        relationship_id,
        tutor_id,
        lesson_id,
        subject_id,
        booking_id,
        lesson_start_at,
        lesson_finished_at,
        lag(lesson_finished_at) over (
            partition by student_id
            order by lesson_finished_at, lesson_id
        ) as previous_lesson_finished_at
    from completed_lessons
)
select
    student_id,
    relationship_id,
    tutor_id,
    lesson_id,
    subject_id,
    booking_id,
    lesson_start_at,
    lesson_finished_at,
    previous_lesson_finished_at,
    timestamp_diff(
        lesson_finished_at,
        previous_lesson_finished_at,
        day
    ) as days_since_previous_completed_lesson
from ordered_lessons
