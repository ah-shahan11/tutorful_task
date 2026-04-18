{{ config(
    materialized='incremental',
    unique_key=['subject_id', 'subject_name', 'left', 'right', 'roll_up_to']
) }}

SELECT subject_id                       as subject_id,
       subject_name                     as subject_name,
       CAST(`left` AS INT64)            as `left`,
       CAST(`right` AS INT64)           as `right`,
       CAST(roll_up_to AS INT64)        as roll_up_to

FROM {{ source('raw_data', 'subjects') }}  
