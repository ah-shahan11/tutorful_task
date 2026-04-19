{{ config(
    materialized='incremental',
    unique_key=['relationship_id', 'tutor_id', 'student_id']
) }}

-- Stages the tutor and student relationship keys used to connect lessons back to students.
SELECT relationship_id            as relationship_id,
       tutor_id                   as tutor_id,
       student_id                 as student_id
FROM {{ source('raw_data', 'relationship') }}  
