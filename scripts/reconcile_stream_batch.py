"""
Offline vs real-time reconciliation for 10-minute concurrency.

Compares, window by window, the Spark streaming output of one run against the dbt
batch tables, and attributes every difference to a cause using the replay's
fault-injection ground truth.

Batch side (warehouse.duckdb, built by dbt):
  ads_concurrent   ads_platform_concurrency.concurrent_users, forward-filled
                   (the model omits steps with no start/stop events)
  batch_sessions   (user, stream) pairs online, recomputed from dwd_watch_session
  batch_users      distinct users online, recomputed from dwd_watch_session

Stream side (data/stream/spark_out/<run_id>/):
  sessions_online, users_online, events_raw

Difference categories per window (sessions_online vs ads_concurrent):
  match                    equal
  window_not_emitted       stream never wrote the window (watermark never passed it)
  late_beyond_watermark    stream short by <= the late events injected into the window
  duplicate_not_removed    stream higher than batch (dedup failed)
  unexplained              anything else
Also reported: late events dropped per query, and the users vs sessions definition gap.

Run from the live-dw root, for example:
  python scripts/reconcile_stream_batch.py --run-id clean2_d10_16
  python scripts/reconcile_stream_batch.py --run-id faulty_d10_16 --min-match-rate 0.95

Exit code 1 if the window match rate is below --min-match-rate, so the check can gate a pipeline.
"""
import argparse
import json
import sys
from pathlib import Path

import duckdb


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--min-match-rate", type=float, default=1.0)
    return p.parse_args()


def main():
    args = parse_args()
    replay = json.load(open(f"results/stream/replay_summary_{args.run_id}.json"))
    lo, hi = replay["steps"]
    out_dir = Path("data/stream/spark_out") / args.run_id
    truth = replay["truth_file"]

    con = duckdb.connect()
    con.execute(f"attach '{args.db}' as w (read_only)")

    con.execute(f"create table spine as select range as step from range({lo}, {hi})")

    # Batch: dbt ADS model, forward-filled, plus a direct recomputation from DWD.
    con.execute("""
        create table ads as
        select s.step, a.concurrent_users as ads_concurrent, a.step = s.step as ads_row_present
        from spine s asof left join w.main.ads_platform_concurrency a on a.step <= s.step
    """)
    con.execute(f"""
        create table batch as
        with beats as (
            select user_id, unnest(range(start_step, stop_step)) as step
            from w.main.dwd_watch_session
            where start_step < {hi} and stop_step > {lo}
        )
        select step, count(*) as batch_sessions, count(distinct user_id) as batch_users
        from beats where step >= {lo} and step < {hi}
        group by step
    """)

    def stream_table(metric):
        files = list((out_dir / metric).glob("*.parquet"))
        if not files:
            return f"select null::bigint as step, null::bigint as {metric} where false"
        return f"select step, {metric} from read_parquet('{out_dir / metric}/*.parquet')"

    con.execute(f"create table s_sessions as {stream_table('sessions_online')}")
    con.execute(f"create table s_users as {stream_table('users_online')}")
    con.execute(f"create table s_raw as {stream_table('events_raw')}")

    # Ground truth: late events per window, excluding those that also got an on-time
    # duplicate (that copy keeps the event alive even if the late original is dropped).
    con.execute(f"""
        create table inj as select * from read_csv('{truth}', header=true,
            columns={{'event_id':'VARCHAR','step':'BIGINT','kind':'VARCHAR',
                      'delay_steps':'BIGINT','emit_step':'BIGINT'}})
    """)
    con.execute("""
        create table late_w as
        select l.step, count(*) as late_events,
               count(*) filter (where d.event_id is null) as late_no_dup
        from inj l left join (select distinct event_id from inj where kind = 'duplicate') d
          using (event_id)
        where l.kind = 'late'
        group by l.step
    """)
    con.execute("""
        create table dup_w as
        select step, count(*) as dup_events from inj where kind = 'duplicate' group by step
    """)

    con.execute("""
        create table windows as
        select sp.step,
               a.ads_concurrent, a.ads_row_present,
               b.batch_sessions, b.batch_users,
               ss.sessions_online, su.users_online, sr.events_raw,
               coalesce(l.late_events, 0) as late_events,
               coalesce(l.late_no_dup, 0) as late_no_dup,
               coalesce(d.dup_events, 0) as dup_events,
               ss.sessions_online - a.ads_concurrent as diff,
               case
                   when ss.sessions_online is null then 'window_not_emitted'
                   when ss.sessions_online = a.ads_concurrent then 'match'
                   when ss.sessions_online > a.ads_concurrent then 'duplicate_not_removed'
                   when a.ads_concurrent - ss.sessions_online <= coalesce(l.late_no_dup, 0)
                        then 'late_beyond_watermark'
                   else 'unexplained'
               end as category
        from spine sp
        left join ads a using (step)
        left join batch b using (step)
        left join s_sessions ss using (step)
        left join s_users su using (step)
        left join s_raw sr using (step)
        left join late_w l using (step)
        left join dup_w d using (step)
        order by sp.step
    """)

    q = lambda sql: con.execute(sql).fetchall()
    n = q("select count(*) from windows")[0][0]
    matched = q("select count(*) from windows where category = 'match'")[0][0]

    categories = {c: {"windows": w, "events": e} for c, w, e in q("""
        select category, count(*), coalesce(sum(abs(diff)), 0)
        from windows group by category order by category""")}

    diff_dist = dict(q("""
        select case when diff is null then 'not_emitted'
                    when diff = 0 then '0'
                    when abs(diff) <= 5 then '1-5'
                    when abs(diff) <= 20 then '6-20'
                    else '>20' end as bucket, count(*)
        from windows group by bucket order by bucket"""))

    totals = q("""
        select sum(ads_concurrent), sum(batch_sessions), sum(batch_users),
               sum(sessions_online), sum(users_online), sum(events_raw),
               sum(abs(diff)),
               count(*) filter (where batch_sessions = ads_concurrent),
               count(*) filter (where not ads_row_present)
        from windows""")[0]
    (ads_sum, bs_sum, bu_sum, so_sum, uo_sum, raw_sum, absdiff, ads_eq_dwd, ffilled) = totals

    dup_n = q("select count(*) from inj where kind = 'duplicate'")[0][0]
    late_by_delay = q("""
        select delay_steps, count(*) from inj where kind = 'late' group by 1 order by 1""")

    summary = {
        "run_id": args.run_id,
        "steps": [lo, hi],
        "windows": n,
        "windows_matched": matched,
        "window_match_rate": round(matched / n, 6) if n else None,
        "event_abs_diff": absdiff,
        "event_abs_diff_rate": round(absdiff / ads_sum, 8) if ads_sum and absdiff is not None else None,
        "categories": categories,
        "diff_distribution_windows": diff_dist,
        "batch_check": {
            "ads_vs_dwd_recompute_equal_windows": ads_eq_dwd,
            "ads_steps_forward_filled": ffilled,
            "ads_concurrent_sum": ads_sum,
            "dwd_sessions_sum": bs_sum,
        },
        "stream_totals": {"sessions_online": so_sum, "users_online": uo_sum, "events_raw": raw_sum},
        # Duplicates are always sent on time, so events_raw = batch + duplicates - late
        # events dropped by that query. The dedup queries drop late events independently,
        # so the two late-drop counts below can differ.
        "duplicates_injected": dup_n,
        "late_events_dropped_by_query": {
            "sessions_online": (bs_sum - so_sum) if so_sum is not None else None,
            "events_raw": (bs_sum + dup_n - raw_sum) if raw_sum is not None else None,
        },
        "late": {
            "injected_by_delay_steps": {str(d): c for d, c in late_by_delay},
            "injected_without_on_time_duplicate":
                q("select coalesce(sum(late_no_dup), 0) from windows")[0][0],
        },
        "definition_gap_users_vs_sessions": {
            "batch_sessions_sum": bs_sum,
            "batch_users_sum": bu_sum,
            "users_lower_by": (bs_sum - bu_sum) if bs_sum and bu_sum else None,
            "stream_users_equal_batch_users_windows":
                q("select count(*) from windows where users_online = batch_users")[0][0],
        },
    }

    res = Path("results/stream")
    con.execute(f"copy windows to '{res}/reconcile_{args.run_id}_windows.csv' (header)")
    with open(res / f"reconcile_{args.run_id}_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(json.dumps(summary, indent=2, default=str))

    if n and matched / n < args.min_match_rate:
        print(f"FAIL: window match rate {matched / n:.4%} < {args.min_match_rate:.2%}")
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
