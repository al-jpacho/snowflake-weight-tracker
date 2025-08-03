with source as (
    select * from {{ ref('fct_weight_logs') }}
)
select 
    weigh_in_date,
    avg(weight_kg) as avg_weight,
    avg(weight_kg) over (order by weigh_in_date rows between 6 preceding and current row) as rolling_7_day_avg,
    min(weight_kg) as min_weight,
    max(weight_kg) as max_weight,
    lag(avg(weight_kg)) over (order by weigh_in_date) as weight_delta
from source