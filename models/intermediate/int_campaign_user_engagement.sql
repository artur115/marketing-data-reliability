{#-
  Grain: one row per (campaign_id, user_id) that was sent the campaign or
  interacted with it.

  Incremental strategy: "recompute affected keys".
    1. Find the high-water mark: the newest _loaded_at already in this table.
    2. Pick every (campaign, user) key that has ANY event loaded after
       (high-water mark - lookback). The lookback catches rows a slow loader
       committed with a timestamp just behind the mark.
    3. Rebuild those keys from ALL of their events (not just the new ones), so
       a late open that arrives days after the send still updates first_open_at,
       open counts etc. correctly.
    4. delete+insert on the surrogate key replaces the old rows.

  Rows that arrive later than the lookback but with a _loaded_at AFTER the mark
  (the normal "late event" case) are always picked up, because the filter is on
  load time, not event time. Event-time filters are what silently drop late data.
-#}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'campaign_user_key',
        on_schema_change = 'fail'
    )
}}

with events as (
    select
        *,
        campaign_id || '|' || user_id as campaign_user_key
    from {{ ref('stg_email_events') }}
),

{% if is_incremental() %}
changed_keys as (
    select distinct campaign_user_key
    from events
    where _loaded_at > (
        select max(_max_loaded_at) - interval '{{ var("late_data_lookback_hours") }} hours'
        from {{ this }}
    )
),
{% endif %}

scoped as (
    select events.*
    from events
    {% if is_incremental() %}
    where campaign_user_key in (select campaign_user_key from changed_keys)
    {% endif %}
)

select
    campaign_user_key,
    campaign_id,
    user_id,
    min(event_at) filter (where event_type = 'send')          as first_sent_at,
    min(event_at) filter (where event_type = 'open')          as first_open_at,
    min(event_at) filter (where event_type = 'click')         as first_click_at,
    max(event_at) filter (where event_type = 'click')         as last_click_at,
    count(*) filter (where event_type = 'send')               as send_count,
    count(*) filter (where event_type = 'open')               as open_count,
    count(*) filter (where event_type = 'click')              as click_count,
    bool_or(event_type = 'bounce')                            as is_bounced,
    bool_or(event_type = 'unsubscribe')                       as is_unsubscribed,
    max(_loaded_at)                                           as _max_loaded_at,
    current_timestamp                                         as _processed_at
from scoped
group by campaign_user_key, campaign_id, user_id
