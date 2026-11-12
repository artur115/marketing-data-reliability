-- Every stage of the email funnel is a subset of the one before it:
-- recipients >= delivered >= unique opens >= unique clicks.
-- A violation means a join fanned out, dedupe broke, or a stage is counting
-- events instead of users.
select *
from {{ ref('fct_campaign_performance') }}
where delivered > recipients
   or unique_opens > delivered
   or unique_clicks > unique_opens
