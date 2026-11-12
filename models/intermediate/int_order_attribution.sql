-- Grain: one row per order. Last-click attribution: the most recent email
-- click by the same user in the N days before the order gets the credit.
-- Orders with no qualifying click keep a null campaign_id (unattributed).
with orders as (
    select * from {{ ref('stg_orders') }}
),

clicks as (
    select user_id, campaign_id, event_at as clicked_at
    from {{ ref('stg_email_events') }}
    where event_type = 'click'
),

candidates as (
    select
        orders.order_id,
        clicks.campaign_id,
        clicks.clicked_at,
        row_number() over (partition by orders.order_id order by clicks.clicked_at desc) as click_rank
    from orders
    inner join clicks
        on  clicks.user_id = orders.user_id
        and clicks.clicked_at <= orders.ordered_at
        and clicks.clicked_at > orders.ordered_at - interval '{{ var("attribution_window_days") }} days'
)

select
    orders.order_id,
    orders.user_id,
    orders.ordered_at,
    orders.amount_usd,
    orders.order_status,
    orders.is_revenue,
    candidates.campaign_id      as attributed_campaign_id,
    candidates.clicked_at       as attributed_click_at
from orders
left join candidates
    on candidates.order_id = orders.order_id
    and candidates.click_rank = 1
