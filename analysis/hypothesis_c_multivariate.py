#!/usr/bin/env python3
"""
hypothesis_c_multivariate.py — 假說 C 多變量回歸分析

在簡單線性回歸（Bias → 高溫概率）的基礎上，加入控制變數：
  1. 城市特徵：緯度、預報波動性
  2. 市場特徵：流動性、Spread
  3. 時間特徵：平均 lead time

用法：
  python analysis/hypothesis_c_multivariate.py
"""

import argparse
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
    from statsmodels.stats.outliers_influence import variance_inflation_factor
except ImportError:
    print("❌ statsmodels 未安裝，請先執行:")
    print("   ./venv/bin/pip install statsmodels")
    sys.exit(1)

from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# 城市坐標（用於緯度）
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

# 氣候分類（基於緯度和地理位置）
def classify_climate(city_name: str, latitude: float) -> str:
    """分類氣候帶"""
    if abs(latitude) < 23.5:
        return "tropical"
    elif 23.5 <= abs(latitude) < 35:
        return "subtropical"
    elif 35 <= abs(latitude) < 55:
        return "temperate"
    else:
        return "arctic"


def get_conn():
    return psycopg2.connect(
        DATABASE_URL, sslmode="require",
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=5,
    )


def load_city_metadata() -> pd.DataFrame:
    """加載城市基本資訊"""
    cities_path = _PROJECT_ROOT / "config" / "cities.json"
    with open(cities_path) as f:
        cities = json.load(f)

    data = []
    for city in cities:
        name = city["name"]
        lat, lon = CITY_COORDS.get(name, (0, 0))
        climate = classify_climate(name, lat)
        data.append({
            "city_name": name,
            "location_key": city["location_key"],
            "latitude": lat,
            "longitude": lon,
            "climate": climate,
        })

    return pd.DataFrame(data)


def load_city_level_analysis() -> pd.DataFrame:
    """
    載入城市級別分析數據（與 hypothesis_c_deep_dive.py 相同）
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 第 1 部分：各城市的 WU Bias 和波動性
            cur.execute("""
                SELECT
                    c.location_key,
                    c.name as city_name,
                    AVG(fs.forecast_high_f - ds.official_high_f) as avg_bias,
                    STDDEV(fs.forecast_high_f - ds.official_high_f) as bias_std,
                    STDDEV(fs.forecast_high_f) as forecast_std,
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
                    CASE
                        WHEN ms.option_label LIKE '%or higher%' THEN 'high_temp'
                        WHEN ms.option_label LIKE '%or below%' THEN 'low_temp'
                        ELSE 'other'
                    END as option_type,
                    AVG(ms.yes_prob) as avg_market_prob,
                    AVG(ms.best_bid) as avg_bid,
                    AVG(ms.best_ask) as avg_ask,
                    AVG(ms.spread) as avg_spread,
                    SUM(ms.volume_usdc) as total_volume,
                    COUNT(*) as n_market_samples,
                    AVG(ms.hours_before_close) as avg_hours_before_close
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

    # 對於每個城市，計算高溫選項的平均概率、spread、流動性
    df_high = df_market[df_market['option_type'] == 'high_temp'].copy()
    df_high_agg = df_high.groupby('location_key').agg({
        'avg_market_prob': 'mean',
        'avg_spread': 'mean',
        'total_volume': 'sum',
        'avg_hours_before_close': 'mean',
        'n_market_samples': 'sum',
    }).reset_index()

    df_high_agg.columns = [
        'location_key', 'high_temp_prob', 'avg_spread',
        'total_volume', 'avg_lead_time', 'n_market_samples'
    ]

    # 合併 bias 和市場資料
    result = df_bias.copy()
    result = result.merge(df_high_agg, on='location_key', how='left')

    return result


def analyze_multivariate(df: pd.DataFrame, df_meta: pd.DataFrame):
    """
    多變量回歸分析
    """
    print(f"\n{'═' * 120}")
    print(f"  多變量線性回歸分析：市場高溫選項定價 vs 多個自變數")
    print(f"{'═' * 120}\n")

    # 合併城市元數據
    df_analysis = df.merge(df_meta[['location_key', 'latitude', 'climate']],
                           on='location_key', how='left')

    # 篩選有高溫選項數據的城市
    df_valid = df_analysis[df_analysis['high_temp_prob'].notna()].copy()

    # 創建虛擬變數（climate）
    df_valid = pd.get_dummies(df_valid, columns=['climate'], prefix='climate', drop_first=True)

    print(f"  有效城市：{len(df_valid)} 個\n")

    # 準備回歸變數
    X_vars = [
        'avg_bias',           # WU 預報偏差
        'forecast_std',       # WU 預報波動性
        'latitude',           # 城市緯度
        'avg_spread',         # 平均 bid-ask spread
        'avg_lead_time',      # 平均 lead time（距結算）
        'total_volume',       # 交易量
    ]

    # 加入氣候虛擬變數
    climate_cols = [col for col in df_valid.columns if col.startswith('climate_')]
    X_vars.extend(climate_cols)

    # 去除缺失值
    df_model = df_valid[X_vars + ['high_temp_prob']].dropna()

    if len(df_model) < 5:
        print(f"  ❌ 樣本太少（n={len(df_model)}），無法進行回歸")
        return

    print(f"  【資料摘要】")
    print(f"  {'變數':30s}  {'平均值':>12s}  {'標準差':>12s}  {'最小值':>12s}  {'最大值':>12s}")
    print(f"  {'─'*30}  {'─'*12}  {'─'*12}  {'─'*12}  {'─'*12}")

    for var in ['high_temp_prob'] + X_vars:
        if var in df_model.columns:
            print(f"  {var:30s}  {df_model[var].mean():>11.4f}  {df_model[var].std():>11.4f}  "
                  f"{df_model[var].min():>11.4f}  {df_model[var].max():>11.4f}")

    # 準備 X 和 Y
    X = df_model[X_vars]
    y = df_model['high_temp_prob']

    # 標準化 X（使係數更容易解釋）
    X_scaled = (X - X.mean()) / X.std()
    X_scaled = sm.add_constant(X_scaled)

    # 多變量線性回歸
    print(f"\n  {'─'*120}")
    print(f"  【多變量線性回歸結果】")
    print(f"  {'─'*120}\n")

    model = sm.OLS(y, X_scaled).fit()
    print(model.summary())

    # 計算 VIF（多重共線性）— 只針對數值變數
    print(f"\n  {'─'*120}")
    print(f"  【多重共線性檢查 (VIF)】")
    print(f"  {'─'*120}\n")

    try:
        # 只用非虛擬變數計算 VIF
        numeric_vars = ['avg_bias', 'forecast_std', 'latitude', 'avg_spread', 'avg_lead_time', 'total_volume']
        X_numeric = df_model[numeric_vars]

        vif_data = pd.DataFrame()
        vif_data["Variable"] = numeric_vars
        vif_data["VIF"] = [variance_inflation_factor(X_numeric.values, i) for i in range(X_numeric.shape[1])]
        print(vif_data.to_string(index=False))
        print(f"\n  （VIF > 5 表示可能有共線性問題）\n")
    except Exception as e:
        print(f"  ⚠️  VIF 計算失敗：{e}\n")

    # 與簡單回歸比較
    print(f"  {'─'*120}")
    print(f"  【模型比較】")
    print(f"  {'─'*120}\n")

    # 簡單回歸（只有 Bias）
    X_simple = df_model[['avg_bias']].values
    X_simple = sm.add_constant(X_simple)
    model_simple = sm.OLS(y, X_simple).fit()

    print(f"  簡單回歸（只含 Bias）")
    print(f"    R² = {model_simple.rsquared:.4f}")
    print(f"    Adjusted R² = {model_simple.rsquared_adj:.4f}")
    print(f"    AIC = {model_simple.aic:.2f}")

    print(f"\n  多變量回歸（Bias + 其他變數）")
    print(f"    R² = {model.rsquared:.4f}  ← 提升 {(model.rsquared - model_simple.rsquared)*100:+.2f}%")
    print(f"    Adjusted R² = {model.rsquared_adj:.4f}  ← 提升 {(model.rsquared_adj - model_simple.rsquared_adj)*100:+.2f}%")
    print(f"    AIC = {model.aic:.2f}")

    # F-test（整體模型顯著性）
    f_stat = model.fvalue
    f_pval = model.f_pvalue
    print(f"\n  F-test（整體模型顯著性）")
    print(f"    F-statistic = {f_stat:.4f}")
    print(f"    P-value = {f_pval:.6f}")

    if f_pval < 0.05:
        print(f"    ✅ 整體模型顯著（p < 0.05）")
    else:
        print(f"    ❌ 整體模型不顯著（p >= 0.05）")

    # 殘差分析
    print(f"\n  {'─'*120}")
    print(f"  【殘差診斷】")
    print(f"  {'─'*120}\n")

    residuals = model.resid
    print(f"  平均殘差：{residuals.mean():.6f}  （應接近 0）")
    print(f"  殘差標準差：{residuals.std():.6f}")
    print(f"  Durbin-Watson：{sm.stats.durbin_watson(residuals):.4f}  （2 表示無自相關）")

    # Jarque-Bera 正態性檢驗
    jb_stat = sm.stats.jarque_bera(residuals)
    print(f"  Jarque-Bera：統計量 {jb_stat[0]:.4f}, p-value {jb_stat[1]:.6f}")

    return model, model_simple, df_model


def main():
    parser = argparse.ArgumentParser(
        description="假說 C 多變量回歸分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    args = parser.parse_args()

    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print(f"\n{'═' * 120}")
    print(f"  假說 C 多變量回歸分析：加入控制變數後的市場定價模型")
    print(f"{'═' * 120}")

    print("\n⏳ 載入城市元資料...")
    df_meta = load_city_metadata()

    print("⏳ 載入城市級別分析數據...")
    df = load_city_level_analysis()

    if df.empty:
        print("❌ 無數據")
        sys.exit(1)

    model, model_simple, df_model = analyze_multivariate(df, df_meta)

    # 結論
    print(f"\n{'═' * 120}")
    print(f"  📊 結論")
    print(f"{'═' * 120}\n")

    r2_improvement = (model.rsquared - model_simple.rsquared) * 100

    if r2_improvement > 10:
        print(f"  ✅ 加入控制變數大幅提升模型（R² 提升 {r2_improvement:.1f}%）")
        print(f"     → 市場定價確實依賴多個因素，不只是 WU 低估")
        print(f"     → 推薦進一步檢查各係數的顯著性")
    elif r2_improvement > 0:
        print(f"  🟡 加入控制變數有小幅改善（R² 提升 {r2_improvement:.1f}%）")
        print(f"     → Bias 仍是主要驅動因素，但其他變數也有貢獻")
    else:
        print(f"  ❌ 加入控制變數沒有改善，甚至惡化（R² {r2_improvement:+.1f}%）")
        print(f"     → 可能存在共線性或過擬合問題")

    print(f"\n{'═' * 120}\n")


if __name__ == "__main__":
    main()
