{{ config(
    materialized='incremental',
    unique_key=['lesson_id', 'relationship_id', 'subject_id']
) }}

-- Brings raw lesson identifiers into the staging layer at one row per lesson-to-relationship mapping.
SELECT lesson_id                    as lesson_id,
       relationship_id              as relationship_id,
       subject_id                   as subject_id,
FROM {{ source('raw_data', 'lessons') }}  
