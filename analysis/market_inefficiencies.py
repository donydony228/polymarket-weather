#!/usr/bin/env python3
"""
market_inefficiencies.py — 挖掘市場定價低效點

找尋 alpha 機會的三個角度：
  1. 預報突變 → 市場反應延遲
  2. Lead time 縮短時，市場價格的收斂情況
  3. 預報精度 × 市場流動性的不匹配

用法：
  python analysis/market_inefficiencies.py                    # 全景分析
  python analysis/market_inefficiencies.py --city atlanta      # 單城市
  python analysis/market_inefficiencies.py --market-date 2026-03-10  # 單日
"""

import argparse
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


# ── 資料庫連線 ─────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        DATABASE_URL, sslmode="require",
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=5,
    )


# ── 角度 1: 預報突變 + 市場反應延遲 ────────────────────────────────────────────

def load_forecast_changes(city_filter: str | None = None) -> pd.DataFrame:
    """找出預報變化最大的時刻"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH forecast_changes AS (
                    SELECT
                        location_key,
                        target_date,
                        snapshot_time,
                        hours_before_close,
                        n_forecast_hours,
                        forecast_high_f,
                        LAG(forecast_high_f) OVER (
                            PARTITION BY location_key, target_date
                            ORDER BY hours_before_close DESC
                        ) as prev_forecast,
                        ABS(forecast_high_f - LAG(forecast_high_f) OVER (
                            PARTITION BY location_key, target_date
                            ORDER BY hours_before_close DESC
                        )) as forecast_change
                    FROM forecast_snapshots
                    WHERE n_forecast_hours >= 12
                      AND target_date >= '2026-03-01'
                )
                SELECT *
                FROM forecast_changes
                WHERE forecast_change > 2
                  AND prev_forecast IS NOT NULL
                ORDER BY target_date DESC, forecast_change DESC
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    df = pd.DataFrame(rows)
    if city_filter and not df.empty:
        df = df[df['location_key'].str.contains(city_filter, case=False)]
    return df


def analyze_forecast_changes(df: pd.DataFrame):
    """分析預報變化對應的市場反應"""
    if df.empty:
        print("\n⚠️  無預報變化數據")
        return

    print(f"\n{'═' * 90}")
    print(f"  1️⃣  預報突變分析（WU forecast_high_f 的變化）")
    print(f"  共 {len(df)} 個時刻發生 > 2°F 的預報變化")
    print(f"{'═' * 90}")

    print(f"\n  {'日期':12s}  {'城市':30s}  {'Lead H':>6s}  {'舊→新 (°F)':>12s}  {'變化':>6s}")
    print(f"  {'─'*12}  {'─'*30}  {'─'*6}  {'─'*12}  {'─'*6}")

    for _, row in df.head(30).iterrows():
        old = row['prev_forecast'] if row['prev_forecast'] else '?'
        new = row['forecast_high_f']
        change = f"{new - old:+.1f}" if isinstance(old, float) else "?"
        print(f"  {row['target_date']}  {row['location_key']:30s}  {row['hours_before_close']:>6.1f}  "
              f"{old:>5}→{new:<5.1f}  {change:>6s}")

    # 統計預報變化的分佈
    print(f"\n  {'預報變化分佈':─^50s}")
    change_stats = df['forecast_change'].describe()
    print(f"  平均變化：{change_stats['mean']:.2f}°F")
    print(f"  最大變化：{change_stats['max']:.2f}°F")
    print(f"  中位數：  {change_stats['50%']:.2f}°F")
    print(f"  最小變化：{change_stats['min']:.2f}°F")


# ── 角度 2: Lead Time 縮短時的市場收斂性 ────────────────────────────────────────

def load_market_convergence(city_filter: str | None = None,
                            market_date: str | None = None) -> pd.DataFrame:
    """追踪市場選項在 lead time 縮短時的價格變化"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where_clauses = ["ms.market_date >= '2026-03-01'"]
            params = []

            if market_date:
                where_clauses.append("ms.market_date = %s")
                params.append(market_date)

            where_sql = " AND ".join(where_clauses)

            cur.execute(f"""
                SELECT
                    ms.location_key,
                    ms.market_date,
                    ms.option_label,
                    ms.hours_before_close,
                    ms.yes_prob,
                    ms.best_bid,
                    ms.best_ask,
                    ms.spread,
                    fs.forecast_high_f,
                    fs.n_forecast_hours,
                    ROW_NUMBER() OVER (
                        PARTITION BY ms.location_key, ms.market_date, ms.option_label
                        ORDER BY ms.hours_before_close DESC
                    ) as snapshot_rank
                FROM market_snapshots ms
                LEFT JOIN forecast_snapshots fs
                  ON fs.location_key = ms.location_key
                  AND fs.target_date = ms.market_date
                  AND fs.snapshot_time = ms.snapshot_time
                WHERE {where_sql}
                ORDER BY ms.market_date DESC, ms.hours_before_close DESC
            """, params)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    df = pd.DataFrame(rows)
    if city_filter and not df.empty:
        df = df[df['location_key'].str.contains(city_filter, case=False)]
    return df


def analyze_market_convergence(df: pd.DataFrame):
    """檢查市場在接近結算時的價格收斂"""
    if df.empty:
        print("\n⚠️  無市場快照數據")
        return

    print(f"\n{'═' * 90}")
    print(f"  2️⃣  市場價格收斂分析（Lead Time 縮短時的價格變化）")
    print(f"{'═' * 90}")

    # 找出波動度異常高的選項（price stickiness）
    print(f"\n  【異常：價格僵化的選項】\n")
    print(f"  {'市場':35s}  {'選項':20s}  {'快照數':>4s}  {'Prob 波動':>8s}  {'Spread':>7s}")
    print(f"  {'─'*35}  {'─'*20}  {'─'*4}  {'─'*8}  {'─'*7}")

    anomalies = []
    for (location, market_date, option), group in df.groupby(['location_key', 'market_date', 'option_label']):
        group = group.sort_values('hours_before_close', ascending=False)

        # 只看 lead time >= 3h 的快照（排除結算前噪音）
        early_snapshots = group[group['hours_before_close'] >= 3]
        if len(early_snapshots) < 3:
            continue

        prob_std = early_snapshots['yes_prob'].std()
        avg_spread = early_snapshots['spread'].mean()

        # 如果價格波動度很低（< 0.5%），但 bid-ask spread 很寬，是個異常
        if prob_std < 0.01 and avg_spread and avg_spread > 0.02:
            anomalies.append({
                'location_key': location,
                'market_date': market_date,
                'option_label': option,
                'n_snapshots': len(early_snapshots),
                'prob_std': prob_std,
                'avg_spread': avg_spread
            })

    if anomalies:
        anomalies_df = pd.DataFrame(anomalies)
        anomalies_df = anomalies_df.sort_values('avg_spread', ascending=False)
        for _, row in anomalies_df.head(20).iterrows():
            print(f"  {row['location_key']:35s}  {row['option_label']:20s}  "
                  f"{row['n_snapshots']:>4}  {row['prob_std']:>7.3%}  {row['avg_spread']:>6.2%}")
    else:
        print("  （無異常）")

    # 每個城市的市場覆蓋統計
    print(f"\n  【市場覆蓋統計】\n")
    print(f"  {'城市':30s}  {'市場日期':12s}  {'選項數':>4s}  {'快照數':>6s}  {'平均 Spread':>10s}")
    print(f"  {'─'*30}  {'─'*12}  {'─'*4}  {'─'*6}  {'─'*10}")

    for (location, market_date), group in df.groupby(['location_key', 'market_date']):
        n_options = group['option_label'].nunique()
        n_snapshots = len(group)
        avg_spread = group['spread'].mean()

        print(f"  {location:30s}  {market_date}  {n_options:>4}  {n_snapshots:>6}  {avg_spread:>9.2%}")


# ── 角度 3: 預報精度 × 市場流動性不匹配 ────────────────────────────────────────

def load_forecast_accuracy_by_city() -> pd.DataFrame:
    """各城市的預報精度（MAE 和 Bias）"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.location_key,
                    c.name as city_name,
                    COUNT(*) as n_snapshots,
                    AVG(ABS(fs.forecast_high_f - ds.official_high_f)) as mae,
                    AVG(fs.forecast_high_f - ds.official_high_f) as bias,
                    STDDEV(fs.forecast_high_f - ds.official_high_f) as rmse_equiv
                FROM forecast_snapshots fs
                JOIN weather_daily_summary ds
                  ON ds.location_key = fs.location_key
                  AND ds.obs_date = fs.target_date
                JOIN cities c ON c.location_key = fs.location_key
                WHERE fs.forecast_high_f IS NOT NULL
                  AND ds.official_high_f IS NOT NULL
                  AND fs.n_forecast_hours >= 12
                  AND fs.target_date >= '2026-03-01'
                GROUP BY c.location_key, c.name
                ORDER BY mae ASC
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return pd.DataFrame(rows)


def load_market_liquidity_by_city(market_date: str | None = None) -> pd.DataFrame:
    """各城市的市場流動性"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where_clause = "WHERE ms.market_date >= '2026-03-01'"
            params = []
            if market_date:
                where_clause += " AND ms.market_date = %s"
                params.append(market_date)

            cur.execute(f"""
                SELECT
                    ms.location_key,
                    c.name as city_name,
                    ms.market_date,
                    COUNT(*) as n_snapshots,
                    COUNT(DISTINCT ms.option_label) as n_options,
                    AVG(ms.spread) as avg_spread,
                    AVG(ms.volume_usdc) as avg_volume,
                    AVG(ms.liquidity_usdc) as avg_liquidity,
                    SUM(ms.liquidity_usdc) as total_liquidity
                FROM market_snapshots ms
                JOIN cities c ON c.location_key = ms.location_key
                {where_clause}
                GROUP BY ms.location_key, c.name, ms.market_date
                ORDER BY ms.location_key, ms.market_date DESC
            """, params)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return pd.DataFrame(rows)


def analyze_efficiency_mismatch():
    """預報精度好的城市，流動性是否相應更好？"""
    accuracy_df = load_forecast_accuracy_by_city()
    liquidity_df = load_market_liquidity_by_city()

    if accuracy_df.empty or liquidity_df.empty:
        print("\n⚠️  數據不足")
        return

    # 合併資料（只看最新的流動性數據）
    liquidity_latest = liquidity_df.sort_values('market_date').groupby('location_key').tail(1)

    merged = accuracy_df.merge(
        liquidity_latest[['location_key', 'avg_spread', 'avg_volume', 'total_liquidity']],
        on='location_key'
    )

    print(f"\n{'═' * 100}")
    print(f"  3️⃣  預報精度 × 市場流動性對比")
    print(f"{'═' * 100}")

    print(f"\n  {'城市':30s}  {'精度 MAE':>8s}  {'Bias':>7s}  {'Avg Spread':>10s}  {'Liquidity':>10s}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*7}  {'─'*10}  {'─'*10}")

    for _, row in merged.iterrows():
        spread_str = f"{row['avg_spread']:.2%}" if row['avg_spread'] else "N/A"
        liquidity_str = f"${row['total_liquidity']:,.0f}" if row['total_liquidity'] else "N/A"

        print(f"  {row['city_name']:30s}  {row['mae']:>7.2f}°F  {row['bias']:>+6.2f}°F  {spread_str:>10s}  {liquidity_str:>10s}")

    # 發現不匹配的情況
    print(f"\n  【發現】")
    best_accuracy = merged.loc[merged['mae'].idxmin()]
    worst_accuracy = merged.loc[merged['mae'].idxmax()]

    print(f"  預報最準確：{best_accuracy['city_name']} (MAE {best_accuracy['mae']:.2f}°F)")
    print(f"  預報最差：  {worst_accuracy['city_name']} (MAE {worst_accuracy['mae']:.2f}°F)")

    # 檢查是否有「預報差卻流動性好」的情況（可能是低估）
    high_spread_low_accuracy = merged[
        (merged['avg_spread'] > merged['avg_spread'].median()) &
        (merged['mae'] > merged['mae'].median())
    ]

    if not high_spread_low_accuracy.empty:
        print(f"\n  ⚠️  預報較差但 Spread 較寬的城市（可能被低估的流動性溢價）：")
        for _, row in high_spread_low_accuracy.iterrows():
            print(f"    - {row['city_name']}: MAE {row['mae']:.2f}°F, Spread {row['avg_spread']:.2%}")


# ── 主程式 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="挖掘市場定價低效點",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  python analysis/market_inefficiencies.py
  python analysis/market_inefficiencies.py --city atlanta
  python analysis/market_inefficiencies.py --market-date 2026-03-10
        """
    )
    parser.add_argument("--city", "-c", help="過濾城市（模糊匹配）")
    parser.add_argument("--market-date", "-d", help="查看特定日期（YYYY-MM-DD）")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print(f"\n{'═' * 90}")
    print(f"  🔍 市場定價低效分析（Polymarket 最高溫市場）")
    print(f"  時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 90}")

    # 角度 1
    print("\n⏳ 載入預報變化數據...")
    forecast_changes_df = load_forecast_changes(args.city)
    analyze_forecast_changes(forecast_changes_df)

    # 角度 2
    print("\n⏳ 載入市場快照數據...")
    market_conv_df = load_market_convergence(args.city, args.market_date)
    analyze_market_convergence(market_conv_df)

    # 角度 3
    print("\n⏳ 載入預報精度與流動性數據...")
    analyze_efficiency_mismatch()

    print(f"\n{'═' * 90}\n")


if __name__ == "__main__":
    main()
