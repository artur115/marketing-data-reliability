-- Grain: one row per registered user.
with milestones as (
    select * from {{ ref('int_user_milestones') }}
),

email as (
    select
        user_id,
        count(*) filter (where send_count > 0)          as campaigns_received,
        count(*) filter (where first_click_at is not null) as campaigns_clicked,
        bool_or(is_unsubscribed)                        as is_unsubscribed
    from {{ ref('int_campaign_user_engagement') }}
    group by user_id
)

select
    milestones.user_id,
    milestones.registered_at,
    cast(milestones.registered_at as date)                   as registration_date,
    milestones.signup_channel,
    milestones.country_code,
    milestones.first_action_at,
    milestones.first_order_at,
    case
        when milestones.first_order_at is not null then 'customer'
        when milestones.first_action_at is not null then 'activated'
        else 'registered'
    end                                                      as lifecycle_stage,
    milestones.completed_orders,
    milestones.lifetime_revenue_usd,
    coalesce(email.campaigns_received, 0)                    as campaigns_received,
    coalesce(email.campaigns_clicked, 0)                     as campaigns_clicked,
    coalesce(email.is_unsubscribed, false)                   as is_unsubscribed
from milestones
left join email using (user_id)
