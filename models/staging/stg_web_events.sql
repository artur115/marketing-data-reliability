with source as (
    select * from {{ source('raw', 'web_events') }}
),

typed as (
    select
        trim(event_id)                          as event_id,
        trim(anonymous_id)                      as anonymous_id,
        nullif(trim(user_id), '')               as user_id,
        trim(session_id)                        as session_id,
        lower(trim(event_name))                 as event_name,
        cast(event_ts as timestamp)             as event_at,
        nullif(trim(utm_campaign), '')          as utm_campaign,
        cast(_loaded_at as timestamp)           as _loaded_at
    from source
)

select
    *,
    cast(event_at as date) as event_date
from typed
qualify row_number() over (partition by event_id order by _loaded_at asc) = 1
