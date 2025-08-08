with source as (
  select * from {{ ref('int_weight_logs') }}
)
select
  weigh_in_date as date,
  avg(weight_kg) as avg_weight,
  min(weight_kg) as min_weight,
  max(weight_kg) as max_weight
from source
group by weigh_in_date
