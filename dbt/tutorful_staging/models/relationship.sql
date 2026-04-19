{{ config(
    materialized='incremental',
    unique_key=['relationship_id', 'tutor_id', 'student_id']
) }}

-- Stages the tutor and student relationship keys used to connect lessons back to students.
SELECT CAST(relationship_id AS INT64)            as relationship_id,
       CAST(tutor_id AS INT64)                   as tutor_id,
       CAST(student_id AS INT64)                 as student_id
FROM {{ source('raw_data', 'relationship') }}  
