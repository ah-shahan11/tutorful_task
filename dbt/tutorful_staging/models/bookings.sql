{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'lesson_id', 'start_at', 'finish_at']
) }}

SELECT booking_id                   as booking_id,
       lesson_id                    as lesson_id,
       PARSE_TIMESTAMP('%d/%m/%Y %H:%M', start_at)  as start_at,
       PARSE_TIMESTAMP('%d/%m/%Y %H:%M', finish_at) as finish_at,
       status                       as status
FROM {{ source('raw_data', 'bookings') }}  
