"""Generate synthetic marketing lifecycle data for the dbt project.

Everything here is fake and deterministic (fixed random seed). The data is
deliberately messy in the ways real event pipelines are messy:

* duplicate rows (an ingestion job re-delivers the same event)
* late-arriving rows (an event from Tuesday lands in the warehouse on Friday)
* inconsistent casing in categorical fields
* order rows that are updated after the fact (refunds)

Usage:
    python scripts/generate_data.py               # full reset: write all raw files
    python scripts/generate_data.py --late-batch  # append a batch of late + duplicate
                                                  # email events, then re-run `dbt build`
                                                  # to watch the incremental model pick
                                                  # them up

Only the Python standard library is used.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

SEED = 42
START = datetime(2026, 1, 1)
END = datetime(2026, 3, 31, 23, 59, 59)
N_USERS = 5000

SIGNUP_CHANNELS = [("organic", 0.40), ("paid_search", 0.25), ("referral", 0.15), ("social", 0.20)]
COUNTRIES = [("US", 0.45), ("GB", 0.10), ("CA", 0.08), ("DE", 0.07), ("IN", 0.15), ("BR", 0.08), ("AU", 0.07)]

# id, name, type, launched_at (None = triggered by registration)
CAMPAIGNS = [
    ("c_001", "Welcome series - day 1", "onboarding", None),
    ("c_002", "January product digest", "newsletter", datetime(2026, 1, 15, 14)),
    ("c_003", "Post your first project", "activation", datetime(2026, 1, 22, 15)),
    ("c_004", "Winter promo - 20% off fees", "promo", datetime(2026, 2, 3, 16)),
    ("c_005", "February product digest", "newsletter", datetime(2026, 2, 12, 14)),
    ("c_006", "We miss you", "reactivation", datetime(2026, 2, 19, 15)),
    ("c_007", "Spring hiring guide", "content", datetime(2026, 3, 3, 14)),
    ("c_008", "March product digest", "newsletter", datetime(2026, 3, 12, 14)),
    ("c_009", "Finish setting up your account", "activation", datetime(2026, 3, 18, 15)),
    ("c_010", "Quarter-end promo", "promo", datetime(2026, 3, 26, 16)),
]

# Tables tracked by the reliability layer. Must match seeds/table_criticality_tiers.csv.
# name -> (refresh cadence in minutes, typical runtime in minutes)
PIPELINE_TABLES = {
    "fct_campaign_performance": (60, 12),
    "fct_funnel_daily": (60, 9),
    "dim_users": (60, 6),
    "int_campaign_user_engagement": (60, 15),
    "stg_email_events": (30, 4),
    "stg_web_events": (30, 7),
    "stg_orders": (30, 3),
    "stg_campaigns": (1440, 1),
}


def weighted(rng: random.Random, options: list[tuple[str, float]]) -> str:
    return rng.choices([o for o, _ in options], weights=[w for _, w in options])[0]


def ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def load_delay(rng: random.Random, late_share: float = 0.03) -> timedelta:
    """Normal ingestion lag is minutes. A small share of rows land days later."""
    if rng.random() < late_share:
        return timedelta(days=rng.uniform(2, 6))
    return timedelta(minutes=rng.uniform(3, 90))


def write_csv(name: str, header: list[str], rows: list[list], mode: str = "w") -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{name}.csv"
    with path.open(mode, newline="") as f:
        w = csv.writer(f)
        if mode == "w":
            w.writerow(header)
        w.writerows(rows)
    print(f"{'appended' if mode == 'a' else 'wrote'} {len(rows):>6} rows -> {path.relative_to(RAW_DIR.parent.parent)}")


def generate(rng: random.Random) -> None:
    span = (END - START).total_seconds()

    # ---------------------------------------------------------------- users
    users = []
    user_rows = []
    for i in range(1, N_USERS + 1):
        uid = f"u_{i:05d}"
        registered_at = START + timedelta(seconds=rng.uniform(0, span * 0.97))
        channel = weighted(rng, SIGNUP_CHANNELS)
        country = weighted(rng, COUNTRIES)
        users.append({"user_id": uid, "registered_at": registered_at, "channel": channel})
        loaded = registered_at + timedelta(minutes=rng.uniform(1, 20))
        user_rows.append([uid, ts(registered_at), channel, country, ts(loaded)])
        # 2%: the user profile is re-exported later with a corrected country
        if rng.random() < 0.02:
            user_rows.append([uid, ts(registered_at), channel, weighted(rng, COUNTRIES), ts(loaded + timedelta(days=1))])
    write_csv("users", ["user_id", "registered_at", "signup_channel", "country", "_loaded_at"], user_rows)

    # ------------------------------------------------------- user journeys
    # Decide each user's first key action ("create_project") and first order up
    # front, so web events, emails and orders tell a consistent story.
    for u in users:
        u["first_action_at"] = None
        u["orders"] = []
        p_action = {"referral": 0.70, "organic": 0.60, "paid_search": 0.55, "social": 0.45}[u["channel"]]
        if rng.random() < p_action:
            u["first_action_at"] = u["registered_at"] + timedelta(hours=rng.expovariate(1 / 60))
            if rng.random() < 0.38:
                t = u["first_action_at"] + timedelta(hours=rng.expovariate(1 / 150))
                while t < END and len(u["orders"]) < 6:
                    u["orders"].append(t)
                    t += timedelta(days=rng.expovariate(1 / 20))
        u["first_action_at"] = u["first_action_at"] if u["first_action_at"] and u["first_action_at"] < END else None
        u["orders"] = [o for o in u["orders"] if o < END]

    # ----------------------------------------------------------- campaigns
    write_csv(
        "campaigns",
        ["campaign_id", "campaign_name", "campaign_type", "channel", "launched_at", "_loaded_at"],
        [
            [cid, name, ctype, "email", ts(launched) if launched else "", ts(launched or START)]
            for cid, name, ctype, launched in CAMPAIGNS
        ],
    )

    # -------------------------------------------------------- email events
    email_rows = []
    n = 0

    def emit(campaign_id: str, user_id: str, event_type: str, at: datetime) -> None:
        nonlocal n
        if at > END:
            return
        n += 1
        raw_type = event_type.upper() if rng.random() < 0.01 else event_type  # casing drift
        loaded = at + load_delay(rng)
        email_rows.append([f"ee_{n:07d}", campaign_id, user_id, raw_type, ts(at), ts(loaded)])
        if rng.random() < 0.02:  # re-delivered duplicate, same event_id
            email_rows.append([f"ee_{n:07d}", campaign_id, user_id, raw_type, ts(at), ts(loaded + timedelta(hours=rng.uniform(1, 12)))])

    for cid, _name, ctype, launched in CAMPAIGNS:
        if launched is None:
            audience = [(u, u["registered_at"] + timedelta(hours=1)) for u in users]
        else:
            eligible = [u for u in users if u["registered_at"] < launched]
            if ctype == "activation":
                eligible = [u for u in eligible if not u["first_action_at"] or u["first_action_at"] > launched]
            if ctype == "reactivation":
                eligible = [u for u in eligible if not u["orders"]]
            audience = [(u, launched + timedelta(minutes=rng.uniform(0, 45))) for u in eligible if rng.random() < 0.6]

        for u, sent_at in audience:
            emit(cid, u["user_id"], "send", sent_at)
            if rng.random() < 0.02:
                emit(cid, u["user_id"], "bounce", sent_at + timedelta(seconds=30))
                continue
            converted = bool(u["orders"])
            if rng.random() < (0.48 if converted else 0.30):
                opened_at = sent_at + timedelta(hours=rng.expovariate(1 / 8))
                emit(cid, u["user_id"], "open", opened_at)
                if rng.random() < 0.15:  # second open, same user
                    emit(cid, u["user_id"], "open", opened_at + timedelta(hours=rng.uniform(1, 48)))
                if rng.random() < (0.35 if converted else 0.18):
                    emit(cid, u["user_id"], "click", opened_at + timedelta(minutes=rng.uniform(0.2, 10)))
            if rng.random() < 0.008:
                emit(cid, u["user_id"], "unsubscribe", sent_at + timedelta(hours=rng.uniform(1, 72)))

    write_csv("email_events", ["event_id", "campaign_id", "user_id", "event_type", "event_ts", "_loaded_at"], email_rows)

    # ---------------------------------------------------------- web events
    web_rows = []
    m = 0

    def web(user_id: str | None, anon: str, session: str, name: str, at: datetime, utm: str = "") -> None:
        nonlocal m
        if at > END:
            return
        m += 1
        loaded = at + load_delay(rng, late_share=0.02)
        row = [f"we_{m:07d}", anon, user_id or "", session, name, ts(at), utm, ts(loaded)]
        web_rows.append(row)
        if rng.random() < 0.015:
            web_rows.append(row[:-1] + [ts(loaded + timedelta(minutes=rng.uniform(5, 300)))])

    for idx, u in enumerate(users):
        anon = f"a_{idx:06d}"
        s = f"s_{idx:06d}_0"
        web(None, anon, s, "page_view", u["registered_at"] - timedelta(minutes=rng.uniform(1, 20)))
        web(u["user_id"], anon, s, "signup_complete", u["registered_at"])
        moments = [u["first_action_at"]] if u["first_action_at"] else []
        moments += [u["registered_at"] + timedelta(days=rng.uniform(0, 30)) for _ in range(rng.randint(0, 5))]
        for k, at in enumerate(sorted(moments), start=1):
            sess = f"s_{idx:06d}_{k}"
            web(u["user_id"], anon, sess, "page_view", at - timedelta(minutes=2))
            if rng.random() < 0.5:
                web(u["user_id"], anon, sess, "search", at - timedelta(minutes=1))
            if at == u["first_action_at"]:
                web(u["user_id"], anon, sess, "create_project", at)
        for j, o in enumerate(u["orders"]):
            web(u["user_id"], anon, f"s_{idx:06d}_o{j}", "checkout", o)
    # anonymous traffic that never registers
    for k in range(15000):
        at = START + timedelta(seconds=rng.uniform(0, span))
        web(None, f"a_x{k:06d}", f"s_x{k:06d}", "page_view", at)

    write_csv(
        "web_events",
        ["event_id", "anonymous_id", "user_id", "session_id", "event_name", "event_ts", "utm_campaign", "_loaded_at"],
        web_rows,
    )

    # -------------------------------------------------------------- orders
    order_rows = []
    o_n = 0
    for u in users:
        for at in u["orders"]:
            o_n += 1
            oid = f"o_{o_n:06d}"
            amount = round(rng.lognormvariate(5.3, 0.8), 2)
            status = "completed" if rng.random() > 0.03 else "cancelled"
            order_rows.append([oid, u["user_id"], ts(at), f"{amount:.2f}", status, ts(at), ts(at + load_delay(rng))])
            if status == "completed" and rng.random() < 0.08:  # refunded later: new version of the row
                upd = at + timedelta(days=rng.uniform(1, 10))
                if upd < END:
                    order_rows.append([oid, u["user_id"], ts(at), f"{amount:.2f}", "refunded", ts(upd), ts(upd + load_delay(rng))])
    write_csv("orders", ["order_id", "user_id", "ordered_at", "amount_usd", "status", "updated_at", "_loaded_at"], order_rows)

    # ------------------------------------------------------- pipeline runs
    generate_pipeline_runs(rng)


def generate_pipeline_runs(rng: random.Random) -> None:
    """Synthetic orchestrator log: one row per scheduled run of each table.

    The orchestrator reports run duration in SECONDS (`duration_seconds`).
    SLA targets in the seed are in MINUTES. Mixing them up is the bug the
    reliability tests are designed to catch.
    """
    window_start = datetime(2026, 3, 2)
    # Injected incidents: (table, start, end, kind)
    incidents = [
        ("fct_funnel_daily", datetime(2026, 3, 14, 2), datetime(2026, 3, 14, 9), "fail"),
        ("stg_web_events", datetime(2026, 3, 20, 0), datetime(2026, 3, 20, 23, 59), "slow"),
        ("int_campaign_user_engagement", datetime(2026, 3, 9, 6), datetime(2026, 3, 9, 8), "fail"),
        ("stg_orders", datetime(2026, 3, 27, 10), datetime(2026, 3, 27, 18), "fail"),
    ]
    rows = []
    r = 0
    for table, (cadence, typical) in PIPELINE_TABLES.items():
        t = window_start
        while t <= END:
            r += 1
            started = t + timedelta(seconds=rng.uniform(5, 180))
            minutes = max(0.3, rng.gauss(typical, typical * 0.2))
            failed = rng.random() < 0.003
            for it, i_start, i_end, kind in incidents:
                if it == table and i_start <= t <= i_end:
                    if kind == "fail":
                        failed = True
                    else:
                        minutes *= 6
            if failed:
                minutes = min(minutes, rng.uniform(0.5, 3))
            finished = started + timedelta(minutes=minutes)
            duration_seconds = int(round((finished - started).total_seconds()))
            finished = started + timedelta(seconds=duration_seconds)
            rows.append([
                f"run_{r:07d}", table, ts(t), ts(started), ts(finished),
                "failed" if failed else "success", duration_seconds,
                0 if failed else rng.randint(500, 250000),
            ])
            t += timedelta(minutes=cadence)
    write_csv(
        "pipeline_runs",
        ["run_id", "table_name", "scheduled_at", "started_at", "finished_at", "status", "duration_seconds", "rows_written"],
        rows,
    )


def append_late_batch(rng: random.Random) -> None:
    """Append late-arriving and duplicate email events to the existing file.

    * opens/clicks for sends from the last week of the period, loaded after the
      warehouse's current high-water mark (classic late data: old event time,
      new load time)
    * a few rows whose _loaded_at sits slightly *before* the warehouse's current
      high-water mark (a slow parallel loader) - only the lookback window catches these
    * exact re-deliveries of existing events (should be deduplicated, not double counted)
    """
    path = RAW_DIR / "email_events.csv"
    if not path.exists():
        raise SystemExit("Run without --late-batch first to create the base files.")
    with path.open() as f:
        existing = list(csv.DictReader(f))

    watermark = max(datetime.fromisoformat(r["_loaded_at"]) for r in existing)
    recent_sends = [
        r for r in existing
        if r["event_type"].lower() == "send" and datetime.fromisoformat(r["event_ts"]) >= END - timedelta(days=7)
    ]
    rng.shuffle(recent_sends)
    max_id = max(int(r["event_id"].split("_")[1]) for r in existing)

    rows = []
    for i, s in enumerate(recent_sends[:400]):
        max_id += 1
        sent = datetime.fromisoformat(s["event_ts"])
        opened = min(sent + timedelta(hours=rng.uniform(1, 30)), END - timedelta(minutes=5))
        if i % 10 == 0:
            loaded = watermark - timedelta(hours=2)  # slow loader: behind the high-water mark
        else:
            loaded = watermark + timedelta(hours=rng.uniform(1, 6))  # late, but loaded now
        rows.append([f"ee_{max_id:07d}", s["campaign_id"], s["user_id"], "open", ts(opened), ts(loaded)])
        if i % 3 == 0:
            max_id += 1
            rows.append([f"ee_{max_id:07d}", s["campaign_id"], s["user_id"], "click", ts(opened + timedelta(minutes=2)), ts(loaded)])
    for d in rng.sample(existing, 200):
        rows.append([d["event_id"], d["campaign_id"], d["user_id"], d["event_type"], d["event_ts"], ts(watermark + timedelta(hours=7))])

    write_csv("email_events", [], rows, mode="a")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--late-batch", action="store_true", help="append late/duplicate email events")
    args = parser.parse_args()
    if args.late_batch:
        append_late_batch(random.Random(SEED + 1))
    else:
        generate(random.Random(SEED))


if __name__ == "__main__":
    main()
