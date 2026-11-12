-- Grain: one row per user. The three lifecycle milestones the funnel is built on.
with users as (
    select * from {{ ref('stg_users') }}
),

first_action as (
    select user_id, min(event_at) as first_action_at
    from {{ ref('stg_web_events') }}
    where event_name = 'create_project'
      and user_id is not null
    group by user_id
),

order_stats as (
    select
        user_id,
        min(ordered_at) filter (where order_status <> 'cancelled')   as first_order_at,
        count(*) filter (where is_revenue)                           as completed_orders,
        coalesce(sum(amount_usd) filter (where is_revenue), 0)       as lifetime_revenue_usd
    from {{ ref('stg_orders') }}
    group by user_id
)

select
    users.user_id,
    users.registered_at,
    users.signup_channel,
    users.country_code,
    first_action.first_action_at,
    order_stats.first_order_at,
    coalesce(order_stats.completed_orders, 0)        as completed_orders,
    coalesce(order_stats.lifetime_revenue_usd, 0)    as lifetime_revenue_usd
from users
left join first_action using (user_id)
left join order_stats using (user_id)
