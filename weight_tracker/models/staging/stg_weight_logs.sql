select 
    id, 
    cast(date as date) as weigh_in_date,
    weight_kg,
    lower(time_of_day) as time_of_day,
    loaded_at
from {{ ref('weight_logs_raw') }} 