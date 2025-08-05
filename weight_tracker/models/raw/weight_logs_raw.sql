{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select
    id,
    date,
    weight_kg,
    time_of_day,
    loaded_at
from {{ ref('weight_logs_raw_temp') }} 

{% if is_incremental() %}
where id not in (select id from {{ this }})
{% endif %}