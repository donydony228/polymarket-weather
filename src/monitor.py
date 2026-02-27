"""
monitor.py — Supabase 資料健康監測

檢查項目：
  1. 最後一次收集是否超過 2.5 小時前（代表連續 2 次以上未執行）
  2. 最近執行是否有城市失敗
  3. 各城市最新預報快照是否過舊
  4. 各城市市場資料是否齊全（9 個選項）

發現問題時：退出碼 1，並將報告印到 stdout（供 GitHub Actions 建立 Issue）
一切正常時：退出碼 0
"""

import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "")

STALE_THRESHOLD_HOURS = 2.5   # 超過此時間未更新視為異常


def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def main():
    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    now_utc = datetime.now(timezone.utc)
    issues  = []

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ── 1. collection_log：最後執行時間 + 近期失敗 ────────────────────
            cur.execute("""
                SELECT run_time, cities_ok, cities_failed, duration_sec, errors
                FROM collection_log
                ORDER BY run_time DESC
                LIMIT 5
            """)
            recent_runs = [dict(r) for r in cur.fetchall()]

            if not recent_runs:
                issues.append("❌ `collection_log` 沒有任何執行記錄")
            else:
                last_run     = recent_runs[0]
                last_run_age = (now_utc - last_run["run_time"]).total_seconds() / 3600

                if last_run_age > STALE_THRESHOLD_HOURS:
                    issues.append(
                        f"❌ 最後一次收集距今 **{last_run_age:.1f} 小時**"
                        f"（超過 {STALE_THRESHOLD_HOURS} 小時閾值）\n"
                        f"   最後執行：`{last_run['run_time'].strftime('%Y-%m-%d %H:%M UTC')}`"
                    )

                for r in recent_runs:
                    if r["cities_failed"] > 0:
                        issues.append(
                            f"⚠️ `{r['run_time'].strftime('%Y-%m-%d %H:%M UTC')}` "
                            f"收集失敗 **{r['cities_failed']}** 個城市"
                        )

            # ── 2. 各城市最新預報快照是否過舊 ────────────────────────────────
            cur.execute("""
                SELECT location_key, MAX(snapshot_time) AS last_snap
                FROM forecast_hourly_snapshots
                WHERE target_date >= CURRENT_DATE - INTERVAL '2 days'
                GROUP BY location_key
            """)
            forecast_snaps = {r["location_key"]: r["last_snap"] for r in cur.fetchall()}

            cur.execute("SELECT location_key, name FROM cities ORDER BY name")
            cities = {r["location_key"]: r["name"] for r in cur.fetchall()}

            missing_forecast = []
            stale_forecast   = []
            for key, name in cities.items():
                if key not in forecast_snaps:
                    missing_forecast.append(name)
                else:
                    age_h = (now_utc - forecast_snaps[key]).total_seconds() / 3600
                    if age_h > STALE_THRESHOLD_HOURS:
                        stale_forecast.append(f"{name} ({age_h:.1f}h 前)")

            if missing_forecast:
                issues.append(f"❌ 無預報資料：{', '.join(missing_forecast)}")
            if stale_forecast:
                issues.append(f"⚠️ 預報資料過舊（>{STALE_THRESHOLD_HOURS}h）：{', '.join(stale_forecast)}")

            # ── 3. 各城市市場選項數是否齊全 ──────────────────────────────────
            cur.execute("""
                WITH latest AS (
                    SELECT location_key, MAX(snapshot_time) AS snap_t
                    FROM market_snapshots
                    WHERE market_date >= CURRENT_DATE - INTERVAL '2 days'
                    GROUP BY location_key
                )
                SELECT ms.location_key, COUNT(ms.option_label) AS opts
                FROM market_snapshots ms
                JOIN latest l ON ms.location_key = l.location_key
                              AND ms.snapshot_time = l.snap_t
                GROUP BY ms.location_key
            """)
            market_opts = {r["location_key"]: int(r["opts"]) for r in cur.fetchall()}

            missing_market = []
            bad_opts       = []
            for key, name in cities.items():
                if key not in market_opts:
                    missing_market.append(name)
                elif market_opts[key] < 9:
                    bad_opts.append(f"{name} ({market_opts[key]} opts)")

            if missing_market:
                issues.append(f"⚠️ 無市場資料：{', '.join(missing_market)}")
            if bad_opts:
                issues.append(f"⚠️ 市場選項不足 9 個：{', '.join(bad_opts)}")

    finally:
        conn.close()

    # ── 輸出結果 ──────────────────────────────────────────────────────────────
    if issues:
        print(f"## ⚠️ Polymarket 資料收集異常報告")
        print(f"\n**偵測時間：** `{now_utc.strftime('%Y-%m-%d %H:%M UTC')}`\n")
        print("### 發現的問題\n")
        for issue in issues:
            print(f"- {issue}")
        print(f"\n---\n*由 GitHub Actions [monitor.py](../blob/main/src/monitor.py) 自動偵測*")
        sys.exit(1)
    else:
        print(f"✅ 所有檢查通過（{now_utc.strftime('%Y-%m-%d %H:%M UTC')}）")
        sys.exit(0)


if __name__ == "__main__":
    main()
