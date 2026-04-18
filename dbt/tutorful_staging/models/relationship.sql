{{ config(
    materialized='incremental',
    unique_key=['relationship_id', 'tutor_id', 'student_id']
) }}

SELECT relationship_id            as relationship_id,
       tutor_id                   as tutor_id,
       student_id                 as student_id
FROM {{ source('raw_data', 'relationship') }}  
