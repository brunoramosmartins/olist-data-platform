-- delay_rate must lie in [0, 1] for all daily rows
select *
from {{ ref('met_daily_delay_rate') }}
where delay_rate < 0
   or delay_rate > 1
