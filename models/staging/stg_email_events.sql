-- One row per email event. The email platform's webhook export re-delivers
-- some events; a re-delivery has the same event_id and a later _loaded_at.
-- Keep the FIRST load: the original arrival time is what freshness and
-- lateness metrics should see.
with source as (
    select * from {{ source('raw', 'email_events') }}
),

typed as (
    select
        trim(event_id)                     as event_id,
        trim(campaign_id)                  as campaign_id,
        trim(user_id)                      as user_id,
        lower(trim(event_type))            as event_type,   -- upstream casing drifts ("OPEN")
        cast(event_ts as timestamp)        as event_at,
        cast(_loaded_at as timestamp)      as _loaded_at
    from source
)

select
    *,
    cast(event_at as date)                                         as event_date,
    date_diff('minute', event_at, _loaded_at) / 60.0               as load_lag_hours
from typed
qualify row_number() over (partition by event_id order by _loaded_at asc) = 1
