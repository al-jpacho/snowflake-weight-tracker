with aggregated as (
  select * from {{ ref('daily_weight_summary') }}
)
select
  date,
  avg_weight,
  min_weight,
  max_weight,
  avg(avg_weight) over (order by date rows between 6 preceding and current row) as rolling_7_day_avg,
  avg_weight - lag(avg_weight) over (order by date) as weight_delta
from aggregated
