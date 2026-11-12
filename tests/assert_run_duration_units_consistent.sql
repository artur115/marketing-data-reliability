-- Catches seconds-vs-minutes mistakes in the reliability layer.
--
-- The orchestrator reports duration_seconds; SLA targets are in minutes;
-- int_pipeline_runs converts once. This test ignores that conversion and
-- re-derives each run's duration from its start/finish timestamps. If someone
-- drops the "/ 60" (every run looks 60x slower -> false runtime breaches) or
-- divides twice (60x faster -> real breaches silently pass), the two numbers
-- disagree by a factor of 60 and this test fails.
select
    run_id,
    table_name,
    duration_minutes,
    date_diff('second', started_at, finished_at) / 60.0 as minutes_from_timestamps
from {{ ref('int_pipeline_runs') }}
where abs(duration_minutes - date_diff('second', started_at, finished_at) / 60.0) > 1
