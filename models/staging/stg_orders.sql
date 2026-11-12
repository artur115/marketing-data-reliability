-- Orders arrive as versions: a refund writes a new row for the same order_id
-- with a later updated_at. Staging exposes the current version only.
with source as (
    select * from {{ source('raw', 'orders') }}
),

typed as (
    select
        trim(order_id)                              as order_id,
        trim(user_id)                               as user_id,
        cast(ordered_at as timestamp)               as ordered_at,
        cast(amount_usd as decimal(18, 2))          as amount_usd,
        lower(trim(status))                         as order_status,
        cast(updated_at as timestamp)               as updated_at,
        cast(_loaded_at as timestamp)               as _loaded_at
    from source
)

select
    *,
    order_status = 'completed' as is_revenue
from typed
qualify row_number() over (partition by order_id order by updated_at desc, _loaded_at desc) = 1
