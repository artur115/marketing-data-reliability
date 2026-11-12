-- One row per user. Profiles are sometimes re-exported (e.g. a corrected
-- country), so keep the most recently loaded version.
with source as (
    select * from {{ source('raw', 'users') }}
),

typed as (
    select
        trim(user_id)                          as user_id,
        cast(registered_at as timestamp)       as registered_at,
        lower(trim(signup_channel))            as signup_channel,
        upper(trim(country))                   as country_code,
        cast(_loaded_at as timestamp)          as _loaded_at
    from source
)

select *
from typed
qualify row_number() over (partition by user_id order by _loaded_at desc) = 1
