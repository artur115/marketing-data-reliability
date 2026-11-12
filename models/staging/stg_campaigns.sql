with source as (
    select * from {{ source('raw', 'campaigns') }}
)

select
    trim(campaign_id)                                  as campaign_id,
    trim(campaign_name)                                as campaign_name,
    lower(trim(campaign_type))                         as campaign_type,
    lower(trim(channel))                               as channel,
    -- Triggered (always-on) campaigns have no launch date.
    cast(nullif(trim(launched_at), '') as timestamp)   as launched_at,
    nullif(trim(launched_at), '') is null              as is_triggered,
    cast(_loaded_at as timestamp)                      as _loaded_at
from source
qualify row_number() over (partition by campaign_id order by _loaded_at desc) = 1
