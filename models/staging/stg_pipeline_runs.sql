-- Orchestrator run log. Note the unit: duration_seconds is SECONDS.
-- Converting to minutes happens exactly once, in int_pipeline_runs.
with source as (
    select * from {{ source('raw', 'pipeline_runs') }}
)

select
    trim(run_id)                                as run_id,
    trim(table_name)                            as table_name,
    cast(scheduled_at as timestamp)             as scheduled_at,
    cast(started_at as timestamp)               as started_at,
    cast(finished_at as timestamp)              as finished_at,
    lower(trim(status))                         as run_status,
    cast(duration_seconds as integer)           as duration_seconds,
    cast(rows_written as bigint)                as rows_written
from source
