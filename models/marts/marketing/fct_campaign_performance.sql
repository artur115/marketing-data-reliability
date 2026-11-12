-- Grain: one row per email campaign (lifetime-to-date).
-- Engagement metrics count USERS, not events: a user who opens twice is one
-- unique open. Revenue uses last-click attribution (int_order_attribution).
with campaigns as (
    select * from {{ ref('stg_campaigns') }}
),

engagement as (
    select
        campaign_id,
        count(*) filter (where send_count > 0)                              as recipients,
        count(*) filter (where send_count > 0 and not is_bounced)           as delivered,
        count(*) filter (where first_open_at is not null)                   as unique_opens,
        count(*) filter (where first_click_at is not null)                  as unique_clicks,
        count(*) filter (where is_unsubscribed)                             as unsubscribes
    from {{ ref('int_campaign_user_engagement') }}
    group by campaign_id
),

attributed as (
    select
        attributed_campaign_id                                              as campaign_id,
        count(*) filter (where is_revenue)                                  as attributed_orders,
        coalesce(sum(amount_usd) filter (where is_revenue), 0)              as attributed_revenue_usd
    from {{ ref('int_order_attribution') }}
    where attributed_campaign_id is not null
    group by attributed_campaign_id
),

joined as (
    select
        campaigns.campaign_id,
        campaigns.campaign_name,
        campaigns.campaign_type,
        campaigns.is_triggered,
        campaigns.launched_at,
        coalesce(engagement.recipients, 0)              as recipients,
        coalesce(engagement.delivered, 0)               as delivered,
        coalesce(engagement.unique_opens, 0)            as unique_opens,
        coalesce(engagement.unique_clicks, 0)           as unique_clicks,
        coalesce(engagement.unsubscribes, 0)            as unsubscribes,
        coalesce(attributed.attributed_orders, 0)       as attributed_orders,
        coalesce(attributed.attributed_revenue_usd, 0)  as attributed_revenue_usd
    from campaigns
    left join engagement using (campaign_id)
    left join attributed using (campaign_id)
)

select
    cast(campaign_id as varchar)                                as campaign_id,
    cast(campaign_name as varchar)                              as campaign_name,
    cast(campaign_type as varchar)                              as campaign_type,
    cast(is_triggered as boolean)                               as is_triggered,
    cast(launched_at as timestamp)                              as launched_at,
    cast(recipients as bigint)                                  as recipients,
    cast(delivered as bigint)                                   as delivered,
    cast(unique_opens as bigint)                                as unique_opens,
    cast(unique_clicks as bigint)                               as unique_clicks,
    cast(unsubscribes as bigint)                                as unsubscribes,
    cast(round(unique_opens  / nullif(delivered, 0), 4) as double)     as open_rate,
    cast(round(unique_clicks / nullif(delivered, 0), 4) as double)     as click_rate,
    cast(round(unique_clicks / nullif(unique_opens, 0), 4) as double)  as click_to_open_rate,
    cast(attributed_orders as bigint)                           as attributed_orders,
    cast(attributed_revenue_usd as decimal(18, 2))              as attributed_revenue_usd,
    cast(round(attributed_revenue_usd / nullif(delivered, 0), 4) as double) as revenue_per_delivered_usd
from joined
