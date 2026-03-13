#!/usr/bin/env python3
"""
forecast_accuracy.py — WU 預報精確度分析

分析 Weather Underground 預報最高溫在不同 lead time 下的誤差。

輸出：
  1. 各 lead time bucket 的 MAE / RMSE / Bias（預報 − 實際，+ 代表 WU 高估）
  2. 各城市匯總（所有 lead time 合計）
  3. WU 逐時預報 vs 逐時實測的誤差（--hourly）

用法：
  python analysis/forecast_accuracy.py
  python analysis/forecast_accuracy.py --city KATL
  python analysis/forecast_accuracy.py --csv
  python analysis/forecast_accuracy.py --hourly
"""

import argparse
import math
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Lead time bucket 定義（hours_before_close 左閉右開區間）
LEAD_BINS: list[tuple[float, float, str]] = [
    (0,   3,  " 0– 3h"),
    (3,   6,  " 3– 6h"),
    (6,  12,  " 6–12h"),
    (12, 18,  "12–18h"),
    (18, 24,  "18–24h"),
    (24, 36,  "24–36h"),
]


# ── 資料庫 ─────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        DATABASE_URL, sslmode="require",
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=5,
    )


def load_daily_data(since: str | None = None) -> pd.DataFrame:
    """
    forecast_snapshots.forecast_high_f  vs  weather_daily_summary.official_high_f

    每一列代表：在 snapshot_time（距結算 hours_before_close 小時前），
    WU 對 target_date 的最高溫預報是多少，以及當天的官方實際最高溫。
    error_f = forecast − actual（正 = WU 高估）
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    fs.location_key,
                    c.name                                  AS city_name,
                    c.celsius,
                    fs.target_date,
                    fs.snapshot_time,
                    fs.hours_before_close,
                    fs.forecast_high_f,
                    ds.official_high_f,
                    fs.forecast_high_f - ds.official_high_f AS error_f
                FROM forecast_snapshots fs
                JOIN weather_daily_summary ds
                  ON ds.location_key = fs.location_key
                 AND ds.obs_date     = fs.target_date
                JOIN cities c ON c.location_key = fs.location_key
                WHERE fs.forecast_high_f    IS NOT NULL
                  AND ds.official_high_f    IS NOT NULL
                  AND fs.hours_before_close IS NOT NULL
                  AND (%s IS NULL OR fs.target_date >= %s::date)
                ORDER BY fs.location_key, fs.target_date, fs.hours_before_close DESC
            """, (since, since))
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return pd.DataFrame(rows)


def load_hourly_data(since: str | None = None) -> pd.DataFrame:
    """
    forecast_hourly_snapshots.temp_f  vs  weather_actuals_hourly.temp_f

    每一列代表：在 snapshot_time 時，WU 對 target_date 的第 forecast_hour 小時的
    溫度預報 vs 那個小時的實際觀測溫度。
    hours_before_close 從同時刻的 forecast_snapshots 借來（LEFT JOIN）。
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    fh.location_key,
                    c.name              AS city_name,
                    c.celsius,
                    fh.target_date,
                    fh.snapshot_time,
                    fh.forecast_hour,
                    fh.temp_f           AS forecast_f,
                    wa.temp_f           AS actual_f,
                    fh.temp_f - wa.temp_f AS error_f,
                    fs.hours_before_close
                FROM forecast_hourly_snapshots fh
                JOIN weather_actuals_hourly wa
                  ON  wa.location_key = fh.location_key
                  AND wa.obs_date     = fh.target_date
                  AND wa.obs_hour     = fh.forecast_hour
                JOIN cities c ON c.location_key = fh.location_key
                LEFT JOIN forecast_snapshots fs
                  ON  fs.location_key  = fh.location_key
                  AND fs.target_date   = fh.target_date
                  AND fs.snapshot_time = fh.snapshot_time
                WHERE fh.temp_f IS NOT NULL
                  AND wa.temp_f IS NOT NULL
                  AND (%s IS NULL OR fh.target_date >= %s::date)
                ORDER BY fh.location_key, fh.target_date, fh.snapshot_time, fh.forecast_hour
            """, (since, since))
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return pd.DataFrame(rows)


# ── 統計工具 ──────────────────────────────────────────────────────────────────

def metrics(errors: pd.Series) -> dict:
    n = len(errors)
    if n == 0:
        return {"n": 0, "mae": None, "rmse": None, "bias": None}
    return {
        "n":    n,
        "mae":  errors.abs().mean(),
        "rmse": math.sqrt((errors ** 2).mean()),
        "bias": errors.mean(),   # + = WU 高估，− = WU 低估
    }


def assign_lead_bin(val: float) -> str | None:
    for lo, hi, label in LEAD_BINS:
        if lo <= val < hi:
            return label
    return None


def filter_city(df: pd.DataFrame, city_filter: str | None) -> pd.DataFrame:
    if not city_filter:
        return df
    mask = (
        df["location_key"].str.contains(city_filter, case=False) |
        df["city_name"].str.contains(city_filter, case=False)
    )
    return df[mask]


# ── 輸出 ──────────────────────────────────────────────────────────────────────

def print_daily_summary(df: pd.DataFrame, city_filter: str | None = None):
    df = filter_city(df, city_filter)
    if df.empty:
        print("\n⚠️  無符合的 daily high 資料。")
        return

    df = df.copy()
    df["lead_bin"] = df["hours_before_close"].apply(assign_lead_bin)
    df = df.dropna(subset=["lead_bin"])

    n_cities = df["location_key"].nunique()
    n_dates  = df["target_date"].nunique()

    print(f"\n{'═' * 66}")
    print(f"  WU 預報最高溫精確度  vs  官方實際最高溫（°F）")
    print(f"  {n_dates} 天  ×  {n_cities} 城市  ·  共 {len(df)} 個快照")
    print(f"  誤差 = 預報 − 實際　　正值 = WU 高估　負值 = WU 低估")
    print(f"{'═' * 66}")

    print(f"\n  {'Lead Time':>9}  {'N':>4}  {'MAE °F':>7}  {'RMSE °F':>8}  {'Bias °F':>8}  {'方向'}")
    print(f"  {'─' * 9}  {'─'*4}  {'─'*7}  {'─'*8}  {'─'*8}  {'─'*10}")

    for _, __, label in LEAD_BINS:
        sub = df[df["lead_bin"] == label]["error_f"]
        if len(sub) == 0:
            continue
        m = metrics(sub)
        if m["bias"] > 0.5:
            direction = "↑ WU 高估"
        elif m["bias"] < -0.5:
            direction = "↓ WU 低估"
        else:
            direction = "≈ 接近準確"
        print(f"  {label}  {m['n']:>4}  {m['mae']:>7.2f}  {m['rmse']:>8.2f}  {m['bias']:>+8.2f}  {direction}")

    # 各城市匯總
    print(f"\n  {'─' * 66}")
    print(f"  {'城市':30s}  {'N':>4}  {'MAE':>6}  {'RMSE':>6}  {'Bias':>7}")
    print(f"  {'─'*30}  {'─'*4}  {'─'*6}  {'─'*6}  {'─'*7}")
    for city_name, grp in df.groupby("city_name"):
        m = metrics(grp["error_f"])
        print(f"  {city_name:30s}  {m['n']:>4}  {m['mae']:>6.2f}  {m['rmse']:>6.2f}  {m['bias']:>+7.2f}")

    # 全體匯總
    m_all = metrics(df["error_f"])
    print(f"  {'─'*30}  {'─'*4}  {'─'*6}  {'─'*6}  {'─'*7}")
    print(f"  {'全體':30s}  {m_all['n']:>4}  {m_all['mae']:>6.2f}  {m_all['rmse']:>6.2f}  {m_all['bias']:>+7.2f}")


def print_hourly_summary(df: pd.DataFrame, city_filter: str | None = None):
    df = filter_city(df, city_filter)
    df = df.dropna(subset=["hours_before_close"]).copy()
    df["lead_bin"] = df["hours_before_close"].apply(assign_lead_bin)
    df = df.dropna(subset=["lead_bin"])

    if df.empty:
        print("\n（無可比對的逐時資料）")
        return

    print(f"\n{'═' * 58}")
    print(f"  WU 逐時預報溫度精確度  vs  逐時實際觀測（°F）")
    print(f"  共 {len(df)} 個逐時比對點")
    print(f"{'═' * 58}")

    print(f"\n  {'Lead Time':>9}  {'N':>5}  {'MAE °F':>7}  {'RMSE °F':>8}  {'Bias °F':>8}")
    print(f"  {'─'*9}  {'─'*5}  {'─'*7}  {'─'*8}  {'─'*8}")
    for _, __, label in LEAD_BINS:
        sub = df[df["lead_bin"] == label]["error_f"]
        if len(sub) == 0:
            continue
        m = metrics(sub)
        print(f"  {label}  {m['n']:>5}  {m['mae']:>7.2f}  {m['rmse']:>8.2f}  {m['bias']:>+8.2f}")

    # 各城市匯總
    print(f"\n  {'─' * 58}")
    print(f"  {'城市':30s}  {'N':>5}  {'MAE':>6}  {'Bias':>7}")
    print(f"  {'─'*30}  {'─'*5}  {'─'*6}  {'─'*7}")
    for city_name, grp in df.groupby("city_name"):
        m = metrics(grp["error_f"])
        print(f"  {city_name:30s}  {m['n']:>5}  {m['mae']:>6.2f}  {m['bias']:>+7.2f}")


def save_csv(df_daily: pd.DataFrame, df_hourly: pd.DataFrame):
    out_dir = _PROJECT_ROOT / "analysis" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    if not df_daily.empty:
        path = out_dir / f"forecast_accuracy_daily_{ts}.csv"
        df_daily.to_csv(path, index=False)
        print(f"\n✅ 已儲存：{path.relative_to(_PROJECT_ROOT)}")

    if not df_hourly.empty:
        path = out_dir / f"forecast_accuracy_hourly_{ts}.csv"
        df_hourly.to_csv(path, index=False)
        print(f"✅ 已儲存：{path.relative_to(_PROJECT_ROOT)}")


# ── 主程式 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WU 預報精確度分析")
    parser.add_argument("--city",    "-c", help="過濾城市（模糊匹配 location_key 或城市名）")
    parser.add_argument("--csv",     action="store_true", help="同時輸出 CSV 至 analysis/output/")
    parser.add_argument("--hourly",  action="store_true", help="顯示逐時預報 vs 逐時實測誤差")
    parser.add_argument("--since",   help="只分析此日期之後的資料（YYYY-MM-DD），預設 2026-03-01（修正前資料不可靠）")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    since = args.since or "2026-03-01"  # 修正前資料不可靠
    print(f"載入資料中（since={since}）...")
    df_daily = load_daily_data(since=since)
    print(f"  daily snapshots:  {len(df_daily)} 筆")

    df_hourly = pd.DataFrame()
    if args.hourly or args.csv:
        df_hourly = load_hourly_data(since=since)
        print(f"  hourly snapshots: {len(df_hourly)} 筆")

    print_daily_summary(df_daily, city_filter=args.city)

    if args.hourly:
        print_hourly_summary(df_hourly, city_filter=args.city)

    if args.csv:
        save_csv(df_daily, df_hourly)


if __name__ == "__main__":
    main()
