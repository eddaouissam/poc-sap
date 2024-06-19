select *
from {{ source('utils', 'calendar_date_dim') }} as calendar_date_dim