{{ config(
    materialized='incremental',
    unique_key=['lesson_id', 'relationship_id', 'subject_id']
) }}

SELECT lesson_id                    as lesson_id,
       relationship_id              as relationship_id,
       subject_id                   as subject_id,
FROM {{ source('raw_data', 'lessons') }}  
