-- Grain: one row per registration date x signup channel (a daily cohort).
--   registration -> first key action (create_project) within N days
--                -> first order ("hire") within M days
-- Cohort-based, not event-date-based, so conversion rates are comparable across
-- days. Recent cohorts have not had their full window yet: is_cohort_complete
-- flags them so dashboards don't read an immature cohort as a drop in conversion.
with milestones as (
    select * from {{ ref('int_user_milestones') }}
),

as_of as (
    select max(event_at) as as_of_at from {{ ref('stg_web_events') }}
),

flagged as (
    select
        cast(registered_at as date)                                          as cohort_date,
        signup_channel,
        first_action_at is not null
            and first_action_at < registered_at + interval '{{ var("activation_window_days") }} days' as is_activated,
        first_order_at is not null
            and first_order_at < registered_at + interval '{{ var("conversion_window_days") }} days'  as is_converted,
        date_diff('hour', registered_at, first_action_at)                     as hours_to_action,
        date_diff('hour', registered_at, first_order_at)                      as hours_to_order
    from milestones
)

select
    cohort_date,
    signup_channel,
    count(*)                                                    as registrations,
    count(*) filter (where is_activated)                        as activated_users,
    count(*) filter (where is_converted)                        as converted_users,
    round(count(*) filter (where is_activated) / count(*), 4)   as activation_rate,
    round(count(*) filter (where is_converted) / count(*), 4)   as conversion_rate,
    median(hours_to_action) filter (where is_activated)         as median_hours_to_action,
    median(hours_to_order) filter (where is_converted)          as median_hours_to_order,
    cohort_date + interval '{{ var("conversion_window_days") }} days'
        <= (select as_of_at from as_of)                          as is_cohort_complete
from flagged
group by cohort_date, signup_channel
