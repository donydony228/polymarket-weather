#!/usr/bin/env python3
"""
hypothesis_d_daily_trend.py — 假說 D 深化：Spread 異常的持續性分析

核心問題：Chicago、London、Atlanta 的 Spread 異常是持續的還是短期波動？

如果異常持續，說明市場有系統性盲點（強買入信號）
如果異常快速消失，說明市場在自我修正（只是暫時套利機會）

用法：
  python analysis/hypothesis_d_daily_trend.py
"""

import os
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np
from scipy import stats
import json

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")

CITY_COORDS = {
    "Seattle, WA": (47.4502, -122.3088),
    "Chicago, IL": (41.9742, -87.9073),
    "Incheon, KR": (37.4613, 126.4407),
    "Toronto, ON": (43.6767, -79.6306),
    "Sao Paulo, BR": (-23.5505, -46.6333),
    "Miami, FL": (25.7617, -80.1918),
    "Atlanta, GA": (33.7490, -84.3880),
    "Paris, FR": (48.8566, 2.3522),
    "Ankara, TR": (39.9334, 32.8597),
    "Dallas, TX": (32.7767, -96.7970),
    "Buenos Aires, AR": (-34.6037, -58.3816),
    "Wellington, NZ": (-41.2865, 174.7762),
    "New York City, NY": (40.7128, -74.0060),
    "London, UK": (51.5074, -0.1278),
}

def get_conn():
    return psycopg2.connect(
        DATABASE_URL, sslmode="require",
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=5,
    )


def load_daily_spread_data() -> pd.DataFrame:
    """
    按日期和城市統計高溫選項的 Spread
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    ms.market_date,
                    c.name as city_name,
                    c.location_key,
                    AVG(ms.spread) as daily_avg_spread,
                    STDDEV(ms.spread) as daily_std_spread,
                    MIN(ms.spread) as min_spread,
                    MAX(ms.spread) as max_spread,
                    COUNT(*) as n_snapshots,
                    AVG(ms.yes_prob) as avg_yes_prob,
                    SUM(ms.volume_usdc) as total_volume,
                    AVG(ms.liquidity_usdc) as avg_liquidity
                FROM market_snapshots ms
                JOIN cities c ON c.location_key = ms.location_key
                WHERE ms.option_label LIKE '%or higher%'
                  AND ms.market_date >= '2026-03-01'
                  AND c.name IN ('Chicago, IL', 'London, UK', 'Atlanta, GA', 'Miami, FL')
                GROUP BY ms.market_date, c.name, c.location_key
                ORDER BY ms.market_date DESC, c.name
            """)

            data = [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()

    return pd.DataFrame(data)


def load_risk_data() -> dict:
    """
    載入每城市的風險評分（用於對比）
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.name as city_name,
                    STDDEV(fs.forecast_high_f) as forecast_std,
                    STDDEV(ds.official_high_f) as actual_high_std
                FROM forecast_snapshots fs
                JOIN weather_daily_summary ds
                  ON ds.location_key = fs.location_key
                  AND ds.obs_date = fs.target_date
                JOIN cities c ON c.location_key = fs.location_key
                WHERE fs.forecast_high_f IS NOT NULL
                  AND ds.official_high_f IS NOT NULL
                  AND fs.n_forecast_hours >= 12
                  AND fs.target_date >= '2026-03-01'
                GROUP BY c.name
            """)

            data = {dict(r)['city_name']: (dict(r)['forecast_std'] + dict(r)['actual_high_std']) / 2
                    for r in cur.fetchall()}

    finally:
        conn.close()

    return data


def analyze_daily_trends(df: pd.DataFrame, risk_scores: dict):
    """
    分析逐日 Spread 變化趨勢
    """
    print(f"\n{'═' * 130}")
    print(f"  假說 D 深化：Spread 異常的持續性分析")
    print(f"{'═' * 130}\n")

    # 重點城市排序
    priority_cities = ['Chicago, IL', 'London, UK', 'Atlanta, GA', 'Miami, FL']

    for city in priority_cities:
        city_data = df[df['city_name'] == city].sort_values('market_date')

        if city_data.empty:
            continue

        risk_score = risk_scores.get(city, 0)

        print(f"\n  {'─'*130}")
        print(f"  【{city}】— Risk Score: {risk_score:.2f}°F")
        print(f"  {'─'*130}\n")

        print(f"  {'日期':>10s}  {'Spread':>10s}  {'日內波動':>12s}  {'成交量($M)':>12s}  {'Yes%':>8s}  {'快照數':>8s}  {'趨勢'}")
        print(f"  {'─'*10}  {'─'*10}  {'─'*12}  {'─'*12}  {'─'*8}  {'─'*8}  {'─'*20}")

        spreads = []
        for _, row in city_data.iterrows():
            spread_pct = row['daily_avg_spread'] * 100
            spreads.append(spread_pct)

            spread_range = f"{row['min_spread']*100:.2f}%-{row['max_spread']*100:.2f}%"
            volume_m = row['total_volume'] / 1e6 if row['total_volume'] else 0

            # 計算趨勢符號
            if len(spreads) >= 2:
                if spreads[-1] > spreads[-2]:
                    trend = "📈 上升"
                elif spreads[-1] < spreads[-2]:
                    trend = "📉 下降"
                else:
                    trend = "➡️  持平"
            else:
                trend = "🔵 首日"

            print(f"  {row['market_date']}  {spread_pct:>9.2f}%  {spread_range:>12s}  ${volume_m:>10.1f}M  "
                  f"{row['avg_yes_prob']*100:>7.1f}%  {row['n_snapshots']:>7.0f}  {trend}")

        # 計算統計指標
        print(f"\n  【統計指標】")
        print(f"  平均 Spread：{np.mean(spreads):.2f}%")
        print(f"  Spread 範圍：{np.min(spreads):.2f}% - {np.max(spreads):.2f}%")
        print(f"  標準差：{np.std(spreads):.2f}%")

        # 趨勢判斷
        if len(spreads) >= 2:
            # 簡單線性迴歸計算趨勢
            x = np.arange(len(spreads))
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, spreads)

            trend_direction = "📈 上升" if slope > 0 else "📉 下降"
            print(f"  趨勢：{trend_direction}（斜率 {slope:+.4f}%/day, p={p_value:.3f}）")

            if abs(slope) < 0.1:
                stability = "✅ 穩定"
            elif abs(slope) < 0.3:
                stability = "🟡 輕度變化"
            else:
                stability = "🔴 劇烈變化"

            print(f"  穩定性：{stability}")

            # 機會評估
            print(f"\n  【機會評估】")
            if abs(slope) < 0.1:
                print(f"  ✅ Spread 相對穩定，異常持續存在")
                print(f"     → 這是一個持久的市場盲點，有交易價值")
            else:
                print(f"  ⚠️  Spread 在變化，需要持續監控")
                if slope > 0:
                    print(f"     → Spread 在擴張，市場在自我修正")
                else:
                    print(f"     → Spread 在縮小，市場偏見在加強")

    print(f"\n{'═' * 130}\n")

    # 跨城市對比
    print(f"\n  {'─'*130}")
    print(f"  【跨城市對比：哪個城市的異常最值得關注？】")
    print(f"  {'─'*130}\n")

    summary_data = []

    for city in priority_cities:
        city_data = df[df['city_name'] == city].sort_values('market_date')

        if city_data.empty:
            continue

        risk_score = risk_scores.get(city, 0)
        avg_spread = city_data['daily_avg_spread'].mean()
        std_spread = city_data['daily_avg_spread'].std()

        # 簡單迴歸趨勢
        x = np.arange(len(city_data))
        y = city_data['daily_avg_spread'].values * 100
        if len(x) >= 2:
            slope, _, _, p_value, _ = stats.linregress(x, y)
        else:
            slope = 0
            p_value = 1

        summary_data.append({
            'city': city,
            'risk': risk_score,
            'avg_spread': avg_spread,
            'std_spread': std_spread,
            'trend_slope': slope,
            'stability': abs(slope) < 0.1,
        })

    summary_df = pd.DataFrame(summary_data).sort_values('avg_spread')

    print(f"  {'城市':30s}  {'Risk':>8s}  {'平均Spread':>12s}  {'波動度':>10s}  {'趨勢':>10s}  {'評價'}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*12}  {'─'*10}  {'─'*10}  {'─'*20}")

    for _, row in summary_df.iterrows():
        stability = "✅ 穩定" if row['stability'] else "⚠️ 變化"
        trend = f"{row['trend_slope']:+.3f}%/d"

        if row['city'] == 'Chicago, IL':
            rating = "🎯 最強信號"
        elif row['city'] == 'London, UK':
            rating = "🎯 次強信號"
        else:
            rating = "📊 參考"

        print(f"  {row['city']:30s}  {row['risk']:>7.2f}°F  {row['avg_spread']*100:>11.2f}%  "
              f"{row['std_spread']*100:>9.2f}%  {trend:>10s}  {rating}")

    print(f"\n{'═' * 130}\n")

    return summary_df


def main():
    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print(f"\n{'═' * 130}")
    print(f"  假說 D 深化：逐日 Spread 變化分析")
    print(f"  目標：判斷 Spread 異常是持續的（買入信號）還是短期的（虛驚一場）")
    print(f"{'═' * 130}")

    print("\n⏳ 載入數據...")
    df_daily = load_daily_spread_data()
    risk_scores = load_risk_data()

    if df_daily.empty:
        print("❌ 無數據")
        sys.exit(1)

    summary = analyze_daily_trends(df_daily, risk_scores)

    # 最終建議
    print(f"\n{'═' * 130}")
    print(f"  💡 最終建議")
    print(f"{'═' * 130}\n")

    chicago = summary[summary['city'] == 'Chicago, IL'].iloc[0] if any(summary['city'] == 'Chicago, IL') else None
    london = summary[summary['city'] == 'London, UK'].iloc[0] if any(summary['city'] == 'London, UK') else None

    if chicago is not None and chicago['stability']:
        print(f"  ✅ Chicago 的低估異常穩定，Spread 保持在 {chicago['avg_spread']*100:.2f}% 左右")
        print(f"     → 強烈買入信號：購買 Chicago 高溫選項，等待 Spread 擴張")
        print(f"     → 預期利潤：Spread 從 {chicago['avg_spread']*100:.2f}% 回升到 1.49%")
        print(f"     → 利潤空間：{(0.0149 - chicago['avg_spread'])*100:.2f} basis points")

    if london is not None and london['stability']:
        print(f"\n  ✅ London 的低估異常穩定，Spread 保持在 {london['avg_spread']*100:.2f}% 左右")
        print(f"     → 強烈買入信號：購買 London 高溫選項，等待 Spread 擴張")
        print(f"     → 預期利潤：Spread 從 {london['avg_spread']*100:.2f}% 回升到 0.99%")
        print(f"     → 利潤空間：{(0.0099 - london['avg_spread'])*100:.2f} basis points")

    print(f"\n  💰 下一步：監控這兩個城市，一旦 Spread 開始擴張或市場結算，馬上驗證盈虧")

    print(f"\n{'═' * 130}\n")


if __name__ == "__main__":
    main()
