#!/usr/bin/env python3
"""
hypothesis_c_deep_dive.py — 假說 C 深化分析：Bias × 市場補償

主要檢驗：是否存在「WU Bias 越大 → 市場高溫選項定價越高」的線性關係？

如果存在，說明市場精細地反應了 WU 的低估傾向。
如果不存在，說明市場的補償是「一刀切」的。

用法：
  python analysis/hypothesis_c_deep_dive.py
  python analysis/hypothesis_c_deep_dive.py --plot
"""

import argparse
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


def load_city_level_analysis() -> pd.DataFrame:
    """
    按城市維度計算：
      - WU Bias（平均）
      - 低估/高估城市分類
      - 市場對該城市高溫選項的平均定價
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 第 1 部分：各城市的 WU Bias
            cur.execute("""
                SELECT
                    c.location_key,
                    c.name as city_name,
                    AVG(fs.forecast_high_f - ds.official_high_f) as avg_bias,
                    STDDEV(fs.forecast_high_f - ds.official_high_f) as bias_std,
                    COUNT(*) as n_forecast_samples
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
                ORDER BY avg_bias
            """)

            bias_data = [dict(r) for r in cur.fetchall()]

            # 第 2 部分：各城市的市場定價（高溫選項）
            cur.execute("""
                SELECT
                    ms.location_key,
                    c.name as city_name,
                    -- 識別高溫選項：包含 "or higher" 或 "or below"
                    CASE
                        WHEN ms.option_label LIKE '%or higher%' THEN 'high_temp'
                        WHEN ms.option_label LIKE '%or below%' THEN 'low_temp'
                        ELSE 'other'
                    END as option_type,
                    AVG(ms.yes_prob) as avg_market_prob,
                    COUNT(*) as n_market_samples
                FROM market_snapshots ms
                JOIN cities c ON c.location_key = ms.location_key
                WHERE ms.market_date >= '2026-03-01'
                  AND ms.hours_before_close >= 3
                GROUP BY ms.location_key, c.name, option_type
            """)

            market_data = [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()

    # 合併資料
    df_bias = pd.DataFrame(bias_data)
    df_market = pd.DataFrame(market_data)

    # 對於每個城市，計算高溫選項和低溫選項的平均概率
    df_high = df_market[df_market['option_type'] == 'high_temp'].copy()
    df_high = df_high.groupby('location_key')[['avg_market_prob']].mean().reset_index()
    df_high.columns = ['location_key', 'high_temp_prob']

    df_low = df_market[df_market['option_type'] == 'low_temp'].copy()
    df_low = df_low.groupby('location_key')[['avg_market_prob']].mean().reset_index()
    df_low.columns = ['location_key', 'low_temp_prob']

    # 合併 bias 和市場資料
    result = df_bias.copy()
    result = result.merge(df_high, on='location_key', how='left')
    result = result.merge(df_low, on='location_key', how='left')

    return result


def analyze_bias_market_relationship(df: pd.DataFrame):
    """
    分析 Bias 與市場高溫定價的關係
    """
    print(f"\n{'═' * 100}")
    print(f"  深化分析：WU Bias vs 市場高溫選項定價")
    print(f"{'═' * 100}\n")

    # 篩選有高溫選項數據的城市
    df_valid = df[df['high_temp_prob'].notna()].copy()

    print(f"  有效城市：{len(df_valid)} 個\n")

    # 表格展示
    print(f"  {'城市':30s}  {'WU Bias':>8s}  {'市場高溫概率':>10s}  {'分類':10s}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*10}  {'─'*10}")

    for _, row in df_valid.iterrows():
        if row['avg_bias'] < -1.5:
            category = "低估"
        elif row['avg_bias'] > 0.5:
            category = "高估"
        else:
            category = "中性"

        print(f"  {row['city_name']:30s}  {row['avg_bias']:>+7.2f}°F  {row['high_temp_prob']:>9.1%}  {category:>10s}")

    # 相關性分析
    print(f"\n  {'─'*100}")
    print(f"  【相關性檢驗】")
    print(f"  {'─'*100}\n")

    # Pearson 相關係數
    correlation, p_value = stats.pearsonr(df_valid['avg_bias'], df_valid['high_temp_prob'])

    print(f"  Pearson 相關係數：{correlation:+.4f}")
    print(f"  P-Value：{p_value:.4f}")

    if p_value < 0.05:
        print(f"  ✅ 顯著相關（p < 0.05）")
    elif p_value < 0.10:
        print(f"  🟡 接近顯著（0.05 < p < 0.10）")
    else:
        print(f"  ❌ 不顯著（p >= 0.10）")

    # 簡單線性迴歸
    print(f"\n  【簡單線性迴歸】")
    print(f"  因變數：市場高溫選項概率")
    print(f"  自變數：WU Bias\n")

    slope, intercept, r_value, p_reg, std_err = stats.linregress(df_valid['avg_bias'], df_valid['high_temp_prob'])

    print(f"  迴歸方程：高溫概率 = {intercept:.4f} + {slope:.4f} × Bias")
    print(f"  R²（解釋方差）：{r_value**2:.4f}  （{r_value**2*100:.1f}% 的市場定價變異由 Bias 解釋）")
    print(f"  Slope 標準誤：{std_err:.4f}")
    print(f"  P-Value：{p_reg:.4f}\n")

    # 解釋斜率
    if slope > 0:
        print(f"  ✅ 正相關：WU 每低估 1°F，市場高溫概率上升 {slope*100:.2f}%")
        print(f"     → 市場知道 WU 會低估，並做出補償")
    elif slope < 0:
        print(f"  ❌ 負相關：市場的反應方向錯誤（不太可能）")
    else:
        print(f"  ⚠️  無相關：Bias 與市場定價無關係")

    # 詳細解釋
    print(f"\n  【解釋】")
    if abs(slope) < 0.1:
        sensitivity = "市場反應不敏感"
    elif abs(slope) < 0.3:
        sensitivity = "市場反應適度"
    else:
        sensitivity = "市場反應敏銳"

    print(f"  • {sensitivity}：slope = {slope:.4f}")
    print(f"  • R² = {r_value**2:.4f} 意味著 Bias 解釋了 {r_value**2*100:.1f}% 的市場定價變異")

    if r_value**2 > 0.6:
        print(f"    → 高度依賴 Bias（市場主要基於 WU 低估來定價）")
    elif r_value**2 > 0.3:
        print(f"    → 中度依賴 Bias（還有其他因素影響定價）")
    else:
        print(f"    → 低度依賴 Bias（市場定價主要由其他因素決定）")

    # 異常城市（殘差分析）
    print(f"\n  {'─'*100}")
    print(f"  【異常城市】— 市場定價與 Bias 不符的城市")
    print(f"  {'─'*100}\n")

    df_valid['predicted_prob'] = intercept + slope * df_valid['avg_bias']
    df_valid['residual'] = df_valid['high_temp_prob'] - df_valid['predicted_prob']

    # 按殘差排序
    df_outliers = df_valid.sort_values('residual', ascending=False)

    print(f"  {'城市':30s}  {'實際概率':>10s}  {'預期概率':>10s}  {'差異':>10s}  {'解釋'}")
    print(f"  {'─'*30}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*20}")

    for _, row in df_outliers.head(7).iterrows():
        diff = row['high_temp_prob'] - row['predicted_prob']
        if diff > 0.05:
            explain = "市場過度定價高溫"
        elif diff < -0.05:
            explain = "市場低估高溫概率"
        else:
            explain = "符合預期"

        print(f"  {row['city_name']:30s}  {row['high_temp_prob']:>9.1%}  {row['predicted_prob']:>9.1%}  "
              f"{diff:>+9.1%}  {explain:>20s}")

    return df_valid


def main():
    parser = argparse.ArgumentParser(
        description="假說 C 深化分析：Bias × 市場補償",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  python analysis/hypothesis_c_deep_dive.py
        """
    )
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print(f"\n{'═' * 100}")
    print(f"  假說 C 深化分析：是否存在「Bias × 市場補償」的線性關係？")
    print(f"{'═' * 100}")

    print("\n⏳ 載入城市級別數據...")
    df = load_city_level_analysis()

    if df.empty:
        print("❌ 無數據")
        sys.exit(1)

    df_result = analyze_bias_market_relationship(df)

    # 結論
    print(f"\n{'═' * 100}")
    print(f"  📊 結論")
    print(f"{'═' * 100}\n")

    correlation, p_value = stats.pearsonr(
        df_result['avg_bias'],
        df_result['high_temp_prob']
    )

    if p_value < 0.05 or (p_value < 0.10 and abs(correlation) > 0.4):
        print(f"  ✅ 發現顯著的 Bias-市場補償關係")
        print(f"     → 市場確實根據 WU 的低估傾向來調整定價")
        print(f"     → 假說 C 的邏輯成立，但補償方式需進一步研究")
    else:
        print(f"  ⚠️  Bias 與市場定價的關係不夠強")
        print(f"     → 可能原因：")
        print(f"        1. 市場的補償是「城市統一標準」而非「因城施異」")
        print(f"        2. 市場定價主要基於其他因素（地理、氣候、流動性）")
        print(f"        3. 樣本量不足（n=14 個城市）")

    print(f"\n{'═' * 100}\n")


if __name__ == "__main__":
    main()
