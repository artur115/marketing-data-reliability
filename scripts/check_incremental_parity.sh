#!/usr/bin/env bash
# Proves the incremental model handles late data: build, append a late batch,
# build incrementally, then compare against a full rebuild of the same model.
#
#   ./scripts/check_incremental_parity.sh        # default lookback (24h) -> 0 rows differ
#   ./scripts/check_incremental_parity.sh 0      # lookback disabled -> slow-loader rows are missed
set -euo pipefail
cd "$(dirname "$0")/.."
LOOKBACK="${1:-24}"
PY="${PYTHON:-python}"

"$PY" scripts/generate_data.py > /dev/null
dbt build --full-refresh -q > /dev/null
"$PY" scripts/generate_data.py --late-batch
dbt build --vars "{late_data_lookback_hours: $LOOKBACK}" -q
"$PY" -c "import duckdb; duckdb.connect('data/warehouse.duckdb').execute('create or replace table main.parity_snapshot as select * exclude (_processed_at) from intermediate.int_campaign_user_engagement')"
dbt run -s int_campaign_user_engagement --full-refresh -q > /dev/null
"$PY" - "$LOOKBACK" <<'PYEOF'
import sys, duckdb
c = duckdb.connect("data/warehouse.duckdb")
diff = c.execute("""
    select count(*) from (
        select * exclude (_processed_at) from intermediate.int_campaign_user_engagement
        except all
        select * from main.parity_snapshot
    )""").fetchall()[0][0]
c.execute("drop table main.parity_snapshot")
print(f"lookback={sys.argv[1]}h: {diff} rows differ between incremental and full refresh")
PYEOF
