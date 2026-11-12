-- Grain: one row per monitored table x calendar day.
--
--   freshness: the worst staleness seen that day, i.e. the longest gap between
--              consecutive successful loads (or between the last success and
--              end of day). Must stay under freshness_target_minutes.
--   uptime:    successful runs / attempted runs, vs uptime_target_pct.
--   runtime:   slowest successful run, vs max_runtime_minutes.
--
-- A day with zero successful runs still gets a row (from the date spine) and
-- fails freshness, instead of silently disappearing from the report.
with runs as (
    select * from {{ ref('int_pipeline_runs') }}
),

tiers as (
    select * from {{ ref('table_criticality_tiers') }}
),

spine as (
    select cast(unnest(generate_series(
        (select min(cast(scheduled_at as date)) from runs),
        (select max(cast(scheduled_at as date)) from runs),
        interval 1 day
    )) as date) as sla_date
),

table_days as (
    select tiers.*, spine.sla_date
    from tiers
    cross join spine
),

successes as (
    select
        table_name,
        finished_at,
        date_diff(
            'second',
            lag(finished_at) over (partition by table_name order by finished_at),
            finished_at
        ) / 60.0 as minutes_since_previous_success
    from runs
    where is_success
),

gaps_by_day as (
    select
        table_name,
        cast(finished_at as date)            as sla_date,
        max(minutes_since_previous_success)  as max_gap_minutes
    from successes
    group by 1, 2
),

end_of_day_staleness as (
    select
        table_days.table_name,
        table_days.sla_date,
        date_diff('second', max(successes.finished_at), cast(table_days.sla_date + 1 as timestamp)) / 60.0
            as minutes_stale_at_day_end
    from table_days
    inner join successes
        on  successes.table_name = table_days.table_name
        and successes.finished_at < cast(table_days.sla_date + 1 as timestamp)
    group by 1, 2
),

run_stats as (
    select
        table_name,
        cast(scheduled_at as date)                               as sla_date,
        count(*)                                                 as attempted_runs,
        count(*) filter (where is_success)                       as successful_runs,
        max(duration_minutes) filter (where is_success)          as max_runtime_minutes_observed
    from runs
    group by 1, 2
),

scored as (
    select
        table_days.sla_date,
        table_days.table_name,
        table_days.tier,
        table_days.owner_team,
        table_days.freshness_target_minutes,
        table_days.uptime_target_pct,
        table_days.max_runtime_minutes,
        coalesce(run_stats.attempted_runs, 0)                                as attempted_runs,
        coalesce(run_stats.successful_runs, 0)                               as successful_runs,
        round(greatest(
            coalesce(gaps_by_day.max_gap_minutes, 0),
            coalesce(end_of_day_staleness.minutes_stale_at_day_end, 1440)
        ), 1)                                                                as max_staleness_minutes,
        round(100.0 * run_stats.successful_runs / nullif(run_stats.attempted_runs, 0), 2)
                                                                             as uptime_pct,
        round(run_stats.max_runtime_minutes_observed, 1)                     as max_runtime_minutes_observed
    from table_days
    left join run_stats            using (table_name, sla_date)
    left join gaps_by_day          using (table_name, sla_date)
    left join end_of_day_staleness using (table_name, sla_date)
)

select
    *,
    max_staleness_minutes <= freshness_target_minutes                     as met_freshness_sla,
    coalesce(uptime_pct, 0) >= uptime_target_pct                          as met_uptime_sla,
    coalesce(max_runtime_minutes_observed, 0) <= max_runtime_minutes      as met_runtime_sla,
    max_staleness_minutes <= freshness_target_minutes
        and coalesce(uptime_pct, 0) >= uptime_target_pct
        and coalesce(max_runtime_minutes_observed, 0) <= max_runtime_minutes as met_all_slas
from scored
