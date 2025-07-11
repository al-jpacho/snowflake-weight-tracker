select 
    id, 
    cast(date as date) as weight_in_date,
    weight_kg,
    lower(time_of_day) as time_of_day,
    loaded_at
from weight_db.raw.weight_logs_raw