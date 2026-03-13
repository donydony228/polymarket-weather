#!/usr/bin/env python3
"""
hypothesis_c_testing.py — 假說 C：WU 系統偏差定價檢驗

假說：Polymarket 是否充分定價 WU 的系統性低估/高估傾向？

例如：NYC WU Bias -3.24°F（系統性低估）
      Market 有沒有相應調整「高溫選項」的價格來補償？

方法：
  1. 計算每城的 WU bias 和預報 MAE
  2. 按 bias 分類：低估城市 vs 高估城市 vs 中性城市
  3. 比較各類城市的「高溫選項」市場價格
  4. 統計檢驗：低估城市的高溫選項是否被低估定價？

用法：
  python analysis/hypothesis_c_testing.py
  python analysis/hypothesis_c_testing.py --city nyc
  python analysis/hypothesis_c_testing.py --detailed
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from scipy import stats
import numpy as np

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


# ── 第 1 步：計算每城的 Bias 和 MAE ────────────────────────────────────────────

def load_forecast_bias_by_city() -> pd.DataFrame:
    """計算每城市的平均 bias、MAE、標準差"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    c.location_key,
                    c.name as city_name,
                    COUNT(*) as n_samples,
                    AVG(fs.forecast_high_f - ds.official_high_f) as avg_bias,
                    STDDEV(fs.forecast_high_f - ds.official_high_f) as bias_stddev,
                    AVG(ABS(fs.forecast_high_f - ds.official_high_f)) as mae,
                    MIN(fs.forecast_high_f - ds.official_high_f) as min_error,
                    MAX(fs.forecast_high_f - ds.official_high_f) as max_error
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
                ORDER BY avg_bias DESC
            """)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    return pd.DataFrame(rows)


# ── 第 2 步：按 Bias 分類城市 ───────────────────────────────────────────────────

def categorize_cities(df: pd.DataFrame) -> tuple:
    """
    分類城市：
      - 低估（bias < -1.5°F）
      - 中性（-1.5 ≤ bias ≤ 0.5°F）
      - 高估（bias > 0.5°F）
    """
    underestimate = df[df['avg_bias'] < -1.5].copy()
    neutral = df[(df['avg_bias'] >= -1.5) & (df['avg_bias'] <= 0.5)].copy()
    overestimate = df[df['avg_bias'] > 0.5].copy()

    return underestimate, neutral, overestimate


# ── 第 3 步：提取市場價格數據（按溫度範圍） ─────────────────────────────────────

def load_market_prices_by_category(
    underestimate_cities: list,
    neutral_cities: list,
    overestimate_cities: list
) -> pd.DataFrame:
    """
    提取各城市的市場價格，分類為「高溫選項」vs「低溫選項」

    高溫選項例：「74°F or higher」、「90°F or higher」
    低溫選項例：「45°F or below」、「32°F or below」
    """

    if not underestimate_cities and not neutral_cities and not overestimate_cities:
        return pd.DataFrame()

    conn = get_conn()
    all_rows = []

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 分別查詢各類城市
            for cities, category in [
                (underestimate_cities, 'underestimate'),
                (neutral_cities, 'neutral'),
                (overestimate_cities, 'overestimate')
            ]:
                if not cities:
                    continue

                placeholders = ','.join(['%s'] * len(cities))

                # 使用 CASE 語句在 SQL 中設置 city_category，而不是作為參數
                query = f"""
                    SELECT
                        ms.location_key,
                        c.name as city_name,
                        ms.market_date,
                        ms.option_label,
                        AVG(ms.yes_prob) as avg_yes_prob,
                        COUNT(*) as n_snapshots,
                        '{category}' as city_category
                    FROM market_snapshots ms
                    JOIN cities c ON c.location_key = ms.location_key
                    WHERE ms.location_key IN ({placeholders})
                      AND ms.market_date >= '2026-03-01'
                      AND ms.hours_before_close >= 3
                    GROUP BY ms.location_key, c.name, ms.market_date, ms.option_label
                """
                cur.execute(query, cities)
                all_rows.extend([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()

    return pd.DataFrame(all_rows)


def classify_option(option_label: str) -> str | None:
    """
    將選項分類為「高溫」或「低溫」

    高溫：含有 "higher", "or higher"
    低溫：含有 "below", "or below"

    注意：不同城市用華氏度或攝氏度，所以只檢查"higher"/"below"關鍵字
    """
    label = option_label.lower()

    # 高溫選項：「X or higher」（不論單位）
    if "or higher" in label or "higher" in label and "or" in label:
        return "high_temp"

    # 低溫選項：「X or below」（不論單位）
    if "or below" in label or "below" in label and "or" in label:
        return "low_temp"

    return None


# ── 第 4 步：統計檢驗（T-test） ──────────────────────────────────────────────

def run_ttest(market_df: pd.DataFrame) -> dict:
    """
    H0: 低估城市的高溫選項概率 = 中性城市的高溫選項概率
    H1: 低估城市的高溫選項概率 < 中性城市的高溫選項概率（被低估定價）

    返回：t-statistic, p-value, 是否顯著
    """
    if market_df.empty:
        return None

    market_df = market_df.copy()
    market_df['option_type'] = market_df['option_label'].apply(classify_option)

    # 只看高溫選項
    high_temp = market_df[market_df['option_type'] == 'high_temp'].copy()

    if high_temp.empty:
        print("\n    ℹ️ 無有效的高溫選項數據")
        return None

    underestimate_probs = high_temp[high_temp['city_category'] == 'underestimate']['avg_yes_prob'].dropna()
    neutral_probs = high_temp[high_temp['city_category'] == 'neutral']['avg_yes_prob'].dropna()

    print(f"\n    ℹ️ 樣本量：低估城市 {len(underestimate_probs)}, 中性城市 {len(neutral_probs)}")

    # 如果樣本太少（< 3），無法進行 t-test
    if len(underestimate_probs) < 3 or len(neutral_probs) < 3:
        return None

    # 獨立樣本 t-test
    t_stat, p_value = stats.ttest_ind(underestimate_probs, neutral_probs)

    return {
        'underestimate_mean': underestimate_probs.mean(),
        'neutral_mean': neutral_probs.mean(),
        'underestimate_std': underestimate_probs.std(),
        'neutral_std': neutral_probs.std(),
        't_statistic': t_stat,
        'p_value': p_value,
        'n_underestimate': len(underestimate_probs),
        'n_neutral': len(neutral_probs),
        'significant': p_value < 0.05,
    }


# ── 輸出函數 ────────────────────────────────────────────────────────────────────

def print_bias_summary(df_bias: pd.DataFrame):
    """打印 bias 分類摘要"""
    print(f"\n{'═' * 90}")
    print(f"  1️⃣  WU 預報 Bias 分析（各城市系統性低估/高估傾向）")
    print(f"{'═' * 90}\n")

    print(f"  {'城市':30s}  {'Bias':>8s}  {'Stddev':>7s}  {'MAE':>6s}  {'N':>4s}  {'分類'}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*7}  {'─'*6}  {'─'*4}  {'─'*15}")

    for _, row in df_bias.iterrows():
        bias = row['avg_bias']
        if bias < -1.5:
            category = "🔴 低估"
        elif bias > 0.5:
            category = "🟢 高估"
        else:
            category = "⚪ 中性"

        print(f"  {row['city_name']:30s}  {bias:>+7.2f}°F  {row['bias_stddev']:>6.2f}  "
              f"{row['mae']:>5.2f}°F  {row['n_samples']:>4}  {category}")


def print_ttest_results(results: dict):
    """打印 T-test 結果"""
    if results is None:
        print("\n⚠️  樣本不足，無法進行統計檢驗")
        return

    print(f"\n{'═' * 90}")
    print(f"  2️⃣  假說檢驗：低估城市的高溫選項是否被定價不足？")
    print(f"{'═' * 90}\n")

    print(f"  H0 (null hypothesis)：低估城市 = 中性城市的高溫選項定價")
    print(f"  H1 (alternative)：低估城市 < 中性城市的高溫選項定價（被低估）\n")

    print(f"  低估城市高溫選項平均概率：{results['underestimate_mean']:.2%}  (n={results['n_underestimate']})")
    print(f"  中性城市高溫選項平均概率：{results['neutral_mean']:.2%}  (n={results['n_neutral']})")
    print(f"  差異：{results['underestimate_mean'] - results['neutral_mean']:+.2%}\n")

    print(f"  T-Statistic：{results['t_statistic']:+.4f}")
    print(f"  P-Value：{results['p_value']:.4f}")

    if results['significant']:
        print(f"\n  ✅ 結果顯著（p < 0.05）")
        print(f"     → 低估城市的高溫選項確實被定價不足")
        print(f"     → 可能的 alpha 策略：在低估城市做多高溫選項")
    else:
        print(f"\n  ❌ 結果不顯著（p ≥ 0.05）")
        print(f"     → 市場可能已充分反映 WU 的低估傾向")
        print(f"     → 或者樣本量太小，需要更多數據")


def print_detailed_city_analysis(df_market: pd.DataFrame, df_bias: pd.DataFrame):
    """詳細分析各城市的高溫選項定價"""
    if df_market.empty:
        return

    print(f"\n{'═' * 90}")
    print(f"  3️⃣  各城市詳細分析：高溫選項定價 vs Bias")
    print(f"{'═' * 90}\n")

    # 合併 bias 信息
    df_market = df_market.copy()
    df_market['option_type'] = df_market['option_label'].apply(classify_option)

    # 只看高溫選項
    high_temp = df_market[df_market['option_type'] == 'high_temp'].copy()

    if high_temp.empty:
        print("  （無有效的高溫選項數據）")
        return

    # 按城市匯總
    summary = high_temp.groupby(['city_name', 'city_category']).agg({
        'avg_yes_prob': 'mean',
        'n_snapshots': 'sum'
    }).reset_index()

    # 加入 bias 信息
    summary = summary.merge(
        df_bias[['city_name', 'avg_bias', 'mae']],
        on='city_name',
        how='left'
    )

    summary = summary.sort_values('avg_bias')

    print(f"  {'城市':30s}  {'Bias':>8s}  {'高溫概率':>8s}  {'分類':12s}  {'樣本數':>4s}")
    print(f"  {'─'*30}  {'─'*8}  {'─'*8}  {'─'*12}  {'─'*4}")

    for _, row in summary.iterrows():
        print(f"  {row['city_name']:30s}  {row['avg_bias']:>+7.2f}°F  {row['avg_yes_prob']:>7.1%}  "
              f"{row['city_category']:12s}  {row['n_snapshots']:>4.0f}")


# ── 主程式 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="假說 C：WU 系統偏差定價檢驗",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例：
  python analysis/hypothesis_c_testing.py
  python analysis/hypothesis_c_testing.py --detailed
        """
    )
    parser.add_argument("--detailed", action="store_true", help="顯示詳細城市分析")
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print(f"\n{'═' * 90}")
    print(f"  假說 C 檢驗：WU 系統偏差是否被 Polymarket 定價？")
    print(f"  時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 90}")

    # 第 1 步：計算 bias
    print("\n⏳ 計算每城市的 WU Bias...")
    df_bias = load_forecast_bias_by_city()

    if df_bias.empty:
        print("❌ 無預報數據")
        sys.exit(1)

    print_bias_summary(df_bias)

    # 第 2 步：分類城市
    underestimate, neutral, overestimate = categorize_cities(df_bias)

    underestimate_cities = underestimate['location_key'].tolist()
    neutral_cities = neutral['location_key'].tolist()
    overestimate_cities = overestimate['location_key'].tolist()

    print(f"\n  分類結果：")
    print(f"    🔴 低估城市（bias < -1.5°F）：{', '.join(underestimate['city_name'])}  ({len(underestimate)} 個)")
    print(f"    ⚪ 中性城市：{', '.join(neutral['city_name'])}  ({len(neutral)} 個)")
    print(f"    🟢 高估城市（bias > 0.5°F）：{', '.join(overestimate['city_name'])}  ({len(overestimate)} 個)")


    # 第 3 步：提取市場價格
    print("\n⏳ 提取市場價格數據...")
    df_market = load_market_prices_by_category(
        underestimate_cities,
        neutral_cities,
        overestimate_cities
    )

    if df_market.empty:
        print("❌ 無市場價格數據")
        sys.exit(1)

    # 第 4 步：統計檢驗
    print("\n⏳ 執行 T-test...")
    results = run_ttest(df_market)
    print_ttest_results(results)

    # 第 5 步：詳細分析（可選）
    if args.detailed:
        print_detailed_city_analysis(df_market, df_bias)

    # 最後的建議
    print(f"\n{'═' * 90}")
    print(f"  📊 結論與後續行動")
    print(f"{'═' * 90}\n")

    if results and results['significant']:
        print(f"  ✅ 發現定價不效率：")
        print(f"     低估城市（NYC 等）的高溫選項被定價偏低")
        print(f"     建議：做多這些城市的高溫選項\n")
        print(f"  📈 進入 Phase 3C 完整回測")
        print(f"     - 等 market_resolutions 更新（或手動補入）")
        print(f"     - 計算做多高溫選項的 sharpe ratio")
    else:
        print(f"  ⚠️  市場可能已充分定價：")
        print(f"     低估城市的高溫選項定價與中性城市無顯著差異")
        print(f"     下一步：檢驗假說 A 或 B")

    print(f"\n{'═' * 90}\n")


if __name__ == "__main__":
    main()
