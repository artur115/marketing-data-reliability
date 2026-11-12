-- Revenue credited to campaigns in the mart must equal the attributed revenue
-- at order grain. Catches double counting when an order could match several
-- clicks (the attribution model must pick exactly one).
with mart as (
    select coalesce(sum(attributed_revenue_usd), 0) as revenue
    from {{ ref('fct_campaign_performance') }}
),

orders as (
    select coalesce(sum(amount_usd), 0) as revenue
    from {{ ref('int_order_attribution') }}
    where attributed_campaign_id is not null
      and is_revenue
)

select mart.revenue as mart_revenue, orders.revenue as order_grain_revenue
from mart, orders
where abs(mart.revenue - orders.revenue) > 0.01
