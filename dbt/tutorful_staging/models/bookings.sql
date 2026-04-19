{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'lesson_id', 'start_at', 'finish_at']
) }}

-- Standardises raw booking records and parses the lesson timestamps into BigQuery timestamps.
SELECT CAST(booking_id AS INT64)                   as booking_id,
       CAST(lesson_id AS INT64)                    as lesson_id,
       PARSE_TIMESTAMP('%d/%m/%Y %H:%M', start_at)  as start_at,
       PARSE_TIMESTAMP('%d/%m/%Y %H:%M', finish_at) as finish_at,
       status                       as status
FROM {{ source('raw_data', 'bookings') }}  
