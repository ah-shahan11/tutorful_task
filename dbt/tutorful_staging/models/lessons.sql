{{ config(
    materialized='incremental',
    unique_key=['lesson_id', 'relationship_id', 'subject_id']
) }}

-- Brings raw lesson identifiers into the staging layer at one row per lesson-to-relationship mapping.
SELECT CAST(lesson_id AS INT64)                    as lesson_id,
       CAST(relationship_id AS INT64)              as relationship_id,
       CAST(subject_id AS INT64)                   as subject_id
FROM {{ source('raw_data', 'lessons') }}  
