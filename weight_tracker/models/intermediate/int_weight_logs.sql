with source as (
    select * from {{ ref ('stg_weight_logs') }}
)
select 
    id, 
    weigh_in_date,
    weight_kg,
    time_of_day,
    -- dervied columns
    date_trunc('week', weigh_in_date) as week_start_date,
    date_trunc('month', weigh_in_date) as month_start_date,
    date_trunc('year', weigh_in_date) as year_start_date,
    date_trunc('quarter', weigh_in_date) as quarter_start_date,
from source