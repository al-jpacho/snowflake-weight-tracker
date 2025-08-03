with source as (
    select * from {{ ref('int_weight_logs') }}
),
aggregated as (
    select 
        weigh_in_date as date,
        avg(weight_kg) as avg_weight,
        min(weight_kg) as min_weight,
        max(weight_kg) as max_weight
    from source
    group by weigh_in_date
)
select 
    date,
    avg_weight,
    min_weight,
    max_weight,
    avg(avg_weight) over (order by date rows between 6 preceding and current row) as rolling_7_day_avg,
    avg_weight - lag(avg_weight) over (order by date) as weight_delta
from aggregated
