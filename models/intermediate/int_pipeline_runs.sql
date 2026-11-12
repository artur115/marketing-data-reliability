-- Grain: one row per pipeline run, joined to its SLA tier.
--
-- THE unit conversion for the reliability layer happens here, once:
-- the orchestrator reports seconds, every SLA target is in minutes.
-- tests/assert_run_duration_units_consistent.sql re-derives the duration from
-- the start/finish timestamps and fails if these two disagree.
with runs as (
    select * from {{ ref('stg_pipeline_runs') }}
),

tiers as (
    select * from {{ ref('table_criticality_tiers') }}
)

select
    runs.run_id,
    runs.table_name,
    tiers.tier,
    tiers.owner_team,
    runs.scheduled_at,
    runs.started_at,
    runs.finished_at,
    runs.run_status,
    runs.run_status = 'success'                          as is_success,
    runs.duration_seconds,
    runs.duration_seconds / 60.0                         as duration_minutes,
    tiers.max_runtime_minutes,
    runs.duration_seconds / 60.0 > tiers.max_runtime_minutes as is_runtime_breach,
    runs.rows_written
from runs
inner join tiers using (table_name)
