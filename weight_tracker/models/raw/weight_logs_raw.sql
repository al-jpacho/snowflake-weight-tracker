{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id'
) }}

select
    id,
    date,
    weight_kg,
    time_of_day,
    loaded_at
from weight_db.weight_tracker_raw.weight_logs_raw_temp