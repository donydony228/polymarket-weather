#!/usr/bin/env python3
"""
hypothesis_d_spread_pricing.py — 假說 D：Spread 定價無效

核心問題：某些城市的 Spread 是否相對於其內在風險被高估或低估？

方法論：
1. 計算「風險因子」= 預報波動性 + 歷史溫度波動性 + 樣本不確定性
2. 用迴歸建立「風險 vs Spread 的正常關係」
3. 計算殘差 = 實際 Spread - 預期 Spread
4. 正殘差 = Spread 高估（機會：做空高溫選項）
5. 負殘差 = Spread 低估（機會：做多高溫選項）

用法：
  python analysis/hypothesis_d_spread_pricing.py
"""

import os
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

import json
import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np
from scipy import stats

try:
    import statsmodels.api as sm
except ImportError:
    print("❌ statsmodels 未安裝")
    sys.exit(1)

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_conn():
    return psycopg2.connect(
        DATABASE_URL, sslmode="require",
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=5,
    )


def load_spread_and_risk_data() -> pd.DataFrame:
    """
    載入每城市的：
      1. 實際 Spread（高溫選項）
      2. 預報波動性（forecast_std）
      3. 實際溫度波動性（官方高溫的 std）
      4. 樣本量
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.location_key,
                    c.name as city_name,
                    -- 預報波動性
                    STDDEV(fs.forecast_high_f) as forecast_std,
                    COUNT(DISTINCT fs.snapshot_time) as n_forecast_samples,
                    -- 實際溫度波動性
                    STDDEV(ds.official_high_f) as actual_high_std,
                    COUNT(DISTINCT ds.obs_date) as n_days,
                    -- 市場 Spread（高溫選項）
                    AVG(CASE
                        WHEN ms.option_label LIKE '%or higher%'
                        THEN ms.spread
                        ELSE NULL
                    END) as avg_spread_high,
                    -- 市場 Spread（低溫選項）
                    AVG(CASE
                        WHEN ms.option_label LIKE '%or below%'
                        THEN ms.spread
                        ELSE NULL
                    END) as avg_spread_low,
                    -- 流動性指標
                    AVG(CASE
                        WHEN ms.option_label LIKE '%or higher%'
                        THEN ms.liquidity_usdc
                        ELSE NULL
                    END) as avg_liquidity_high,
                    -- 成交量
                    SUM(CASE
                        WHEN ms.option_label LIKE '%or higher%'
                        THEN ms.volume_usdc
                        ELSE NULL
                    END) as total_volume_high
                FROM forecast_snapshots fs
                JOIN weather_daily_summary ds
                  ON ds.location_key = fs.location_key
                  AND ds.obs_date = fs.target_date
                JOIN cities c ON c.location_key = fs.location_key
                LEFT JOIN market_snapshots ms
                  ON ms.location_key = fs.location_key
                  AND ms.market_date = fs.target_date
                WHERE fs.forecast_high_f IS NOT NULL
                  AND ds.official_high_f IS NOT NULL
                  AND fs.n_forecast_hours >= 12
                  AND fs.target_date >= '2026-03-01'
                GROUP BY c.location_key, c.name
                ORDER BY c.name
            """)

            data = [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()

    return pd.DataFrame(data)


def analyze_spread_pricing(df: pd.DataFrame):
    """
    分析 Spread 與風險的關係，找出異常城市
    """
    print(f"\n{'═' * 120}")
    print(f"  假說 D：Spread 定價無效分析")
    print(f"{'═' * 120}\n")

    # 篩選有效數據
    df_valid = df[
        df['avg_spread_high'].notna() &
        df['forecast_std'].notna() &
        df['actual_high_std'].notna()
    ].copy()

    print(f"  有效城市：{len(df_valid)} 個\n")

    # 計算風險指標
    # 風險 = (預報波動性 + 實際波動性) / 2
    df_valid['risk_score'] = (
        df_valid['forecast_std'] + df_valid['actual_high_std']
    ) / 2

    # 加入「樣本不確定性懲罰」（樣本少風險更高）
    # uncertainty_penalty = 1 / sqrt(min(n_days, n_forecast_samples))
    df_valid['sample_uncertainty'] = 1.0 / np.sqrt(
        df_valid[['n_days', 'n_forecast_samples']].min(axis=1)
    )

    df_valid['adjusted_risk_score'] = (
        df_valid['risk_score'] + df_valid['sample_uncertainty']
    )

    print(f"  【城市風險評估】\n")
    print(f"  {'城市':30s}  {'預報波動':>10s}  {'實際波動':>10s}  {'風險評分':>10s}  {'實際Spread':>10s}")
    print(f"  {'─'*30}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}")

    for _, row in df_valid.iterrows():
        print(f"  {row['city_name']:30s}  {row['forecast_std']:>9.2f}°F  "
              f"{row['actual_high_std']:>9.2f}°F  {row['adjusted_risk_score']:>9.3f}  "
              f"{row['avg_spread_high']:>9.2%}")

    # 建立迴歸模型：Spread = f(risk_score)
    print(f"\n  {'─'*120}")
    print(f"  【Spread vs Risk 迴歸分析】")
    print(f"  {'─'*120}\n")

    X = df_valid[['adjusted_risk_score']].values
    y = df_valid['avg_spread_high'].values

    X_scaled = (X - X.mean()) / X.std()
    X_sm = sm.add_constant(X_scaled)

    model = sm.OLS(y, X_sm).fit()

    print(f"  迴歸方程：Spread = {model.params[0]:.4f} + {model.params[1]:.4f} × (Risk Score 標準化)")
    print(f"  R² = {model.rsquared:.4f}  （Risk 解釋 {model.rsquared*100:.1f}% 的 Spread 變異）")
    print(f"  P-value = {model.pvalues[1]:.4f}")

    if model.pvalues[1] < 0.05:
        print(f"  ✅ 統計顯著：Risk 與 Spread 有顯著關係\n")
    else:
        print(f"  ⚠️  不顯著：Risk 與 Spread 的關係不明確\n")

    # 計算殘差（異常）
    df_valid['predicted_spread'] = model.predict(X_sm)
    df_valid['residual_spread'] = df_valid['avg_spread_high'] - df_valid['predicted_spread']
    df_valid['residual_pct'] = (
        df_valid['residual_spread'] / df_valid['predicted_spread']
    ) * 100

    # 按殘差排序
    df_sorted = df_valid.sort_values('residual_spread', ascending=False)

    print(f"  {'─'*120}")
    print(f"  【Spread 異常排序】— 正值 = 被高估，負值 = 被低估")
    print(f"  {'─'*120}\n")

    print(f"  {'城市':30s}  {'實際Spread':>10s}  {'預期Spread':>10s}  {'差異':>10s}  {'%差異':>8s}  {'機會'}")
    print(f"  {'─'*30}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*8}  {'─'*15}")

    for _, row in df_sorted.iterrows():
        residual_pct = row['residual_pct']

        # 判斷機會
        if residual_pct > 5:
            opportunity = "📈 做空（Spread高估）"
        elif residual_pct < -5:
            opportunity = "📉 做多（Spread低估）"
        else:
            opportunity = "✅ 合理"

        print(f"  {row['city_name']:30s}  {row['avg_spread_high']:>9.2%}  "
              f"{row['predicted_spread']:>9.2%}  {row['residual_spread']:>+9.2%}  "
              f"{residual_pct:>+7.1f}%  {opportunity}")

    # 機會總結
    print(f"\n  {'─'*120}")
    print(f"  【交易機會識別】")
    print(f"  {'─'*120}\n")

    opportunities_high = df_sorted[df_sorted['residual_pct'] > 5][['city_name', 'residual_pct', 'residual_spread', 'avg_spread_high']]
    opportunities_low = df_sorted[df_sorted['residual_pct'] < -5][['city_name', 'residual_pct', 'residual_spread', 'avg_spread_high']]

    if len(opportunities_high) > 0:
        print(f"  📈 Spread 被高估的城市（做空高溫選項）：")
        for idx, (_, row) in enumerate(opportunities_high.iterrows(), 1):
            print(f"     {idx}. {row['city_name']:30s} — Spread 高估 {row['residual_pct']:+.1f}%  "
                  f"（實際 {row['avg_spread_high']:.2%}）")
    else:
        print(f"  📈 沒有明顯被高估的 Spread\n")

    if len(opportunities_low) > 0:
        print(f"\n  📉 Spread 被低估的城市（做多高溫選項）：")
        for idx, (_, row) in enumerate(opportunities_low.iterrows(), 1):
            print(f"     {idx}. {row['city_name']:30s} — Spread 低估 {row['residual_pct']:+.1f}%  "
                  f"（實際 {row['avg_spread_high']:.2%}）")
    else:
        print(f"  📉 沒有明顯被低估的 Spread\n")

    # 交易建議
    print(f"\n  {'─'*120}")
    print(f"  【交易建議】")
    print(f"  {'─'*120}\n")

    if len(opportunities_high) > 0:
        print(f"  💰 Spread 高估機會：")
        for _, row in opportunities_high.iterrows():
            premium = row['residual_spread']
            spread = row['avg_spread_high']
            print(f"\n     {row['city_name']}:")
            print(f"       - 實際 Spread：{spread:.2%}")
            print(f"       - 高估溢價：{premium:.2%}")
            print(f"       - 策略：賣出高溫選項（買入賠率不利的一方），等待 Spread 收斂")
            print(f"       - 預期利潤：~{premium*100:.0f} bps（如果 Spread 收斂到合理水準）")

    if len(opportunities_low) > 0:
        print(f"  💰 Spread 低估機會：")
        for _, row in opportunities_low.iterrows():
            discount = -row['residual_spread']
            spread = row['avg_spread_high']
            print(f"\n     {row['city_name']}:")
            print(f"       - 實際 Spread：{spread:.2%}")
            print(f"       - 低估折扣：{discount:.2%}")
            print(f"       - 策略：買入高溫選項（賣出賠率不利的一方），等待 Spread 擴張")
            print(f"       - 預期利潤：~{discount*100:.0f} bps（如果 Spread 擴張到合理水準）")

    print(f"\n{'═' * 120}\n")

    return df_valid, model


def main():
    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print(f"\n{'═' * 120}")
    print(f"  假說 D：Spread 定價無效 — 尋找流動性溢價異常")
    print(f"{'═' * 120}")

    print("\n⏳ 載入數據...")
    df = load_spread_and_risk_data()

    if df.empty:
        print("❌ 無數據")
        sys.exit(1)

    df_result, model = analyze_spread_pricing(df)


if __name__ == "__main__":
    main()
