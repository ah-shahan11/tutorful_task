{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'lesson_id', 'lesson_start_at', 'lesson_finished_at']
) }}

-- Builds the base student activity set by keeping only completed lessons with student context attached.
select
    r.student_id as student_id,
    r.relationship_id as relationship_id,
    r.tutor_id as tutor_id,
    l.lesson_id as lesson_id,
    l.subject_id as subject_id,
    b.booking_id as booking_id,
    b.start_at as lesson_start_at,
    b.finish_at as lesson_finished_at,
    b.status as booking_status
from {{ source('staging_data', 'bookings') }} as b
inner join {{ source('staging_data', 'lessons') }} as l
    on b.lesson_id = l.lesson_id
inner join {{ source('staging_data', 'relationship') }} as r
    on l.relationship_id = r.relationship_id
where lower(b.status) = 'completed'
  and b.finish_at is not null
