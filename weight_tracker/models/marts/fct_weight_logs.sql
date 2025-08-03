with source as (
    select * from {{ ref('int_weight_logs') }}
)
select
    id,
    weigh_in_date as date,
    time_of_day,
    day_of_week,
    week_start_date as week,
    month_start_date as month,
    year_start_date as year,
    quarter_start_date as quarter,
    weight_kg
from source