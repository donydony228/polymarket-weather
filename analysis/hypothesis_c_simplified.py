#!/usr/bin/env python3
"""
hypothesis_c_simplified.py — 簡化模型：只用關鍵變數

移除高共線性變數，只保留：
  1. avg_spread（流動性）
  2. climate（氣候帶）

目標：驗證這兩個變數是否足以解釋 R² > 0.80
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

def classify_climate(city_name: str, latitude: float) -> str:
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

def load_data() -> pd.DataFrame:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # WU Bias
            cur.execute("""
                SELECT
                    c.location_key,
                    c.name as city_name,
                    AVG(fs.forecast_high_f - ds.official_high_f) as avg_bias,
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
            """)
            bias_data = [dict(r) for r in cur.fetchall()]

            # 市場定價 + Spread
            cur.execute("""
                SELECT
                    ms.location_key,
                    c.name as city_name,
                    CASE
                        WHEN ms.option_label LIKE '%or higher%' THEN 'high_temp'
                        ELSE NULL
                    END as option_type,
                    AVG(ms.yes_prob) as avg_market_prob,
                    AVG(ms.spread) as avg_spread
                FROM market_snapshots ms
                JOIN cities c ON c.location_key = ms.location_key
                WHERE ms.market_date >= '2026-03-01'
                  AND ms.hours_before_close >= 3
                  AND ms.option_label LIKE '%or higher%'
                GROUP BY ms.location_key, c.name, option_type
            """)
            market_data = [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()

    df_bias = pd.DataFrame(bias_data)
    df_market = pd.DataFrame(market_data)

    result = df_bias.merge(
        df_market[['location_key', 'avg_market_prob', 'avg_spread']],
        on='location_key', how='left'
    )

    return result

def main():
    print(f"\n{'═' * 100}")
    print(f"  簡化模型：只用 Spread + 氣候帶預測高溫概率")
    print(f"{'═' * 100}\n")

    if not DATABASE_URL:
        print("❌ DATABASE_URL 未設定")
        sys.exit(1)

    print("⏳ 載入數據...")
    df = load_data()

    # 補充城市元數據
    df['latitude'] = df['city_name'].map(lambda x: CITY_COORDS.get(x, (0, 0))[0])
    df['climate'] = df.apply(lambda r: classify_climate(r['city_name'], r['latitude']), axis=1)

    # 篩選有效數據
    df_model = df[df['avg_market_prob'].notna() & df['avg_spread'].notna()].copy()
    df_model = pd.get_dummies(df_model, columns=['climate'], prefix='climate', drop_first=True)

    print(f"\n  有效城市：{len(df_model)} 個\n")

    # 簡化模型：只用 Spread + Climate
    X_vars = ['avg_spread']
    climate_cols = [col for col in df_model.columns if col.startswith('climate_')]
    X_vars.extend(climate_cols)

    X = df_model[X_vars]
    y = df_model['avg_market_prob']

    # 標準化
    X_scaled = (X - X.mean()) / X.std()
    X_scaled = sm.add_constant(X_scaled)

    print(f"{'─'*100}")
    print(f"【簡化模型：avg_spread + climate】")
    print(f"{'─'*100}\n")

    model = sm.OLS(y, X_scaled).fit()
    print(model.summary())

    # 與完整模型比較
    print(f"\n{'─'*100}")
    print(f"【模型性能比較】")
    print(f"{'─'*100}\n")

    print(f"  完整模型（9 變數）")
    print(f"    R² = 0.8995, Adj R² = 0.7387, AIC = -22.47")

    print(f"\n  簡化模型（Spread + Climate）")
    print(f"    R² = {model.rsquared:.4f}, Adj R² = {model.rsquared_adj:.4f}, AIC = {model.aic:.2f}")

    if model.rsquared > 0.80:
        print(f"\n  ✅ 簡化模型足夠強大（R² > 0.80）")
        print(f"     → Spread 和氣候變數足以解釋 80% 以上的市場定價")
        print(f"     → 其他變數（Bias, Lead time 等）是冗餘的")
    else:
        print(f"\n  ⚠️  簡化模型仍有改善空間（R² {model.rsquared:.2%}）")

    print(f"\n{'═' * 100}\n")

if __name__ == "__main__":
    main()
