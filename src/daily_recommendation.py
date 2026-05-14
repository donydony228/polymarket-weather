#!/usr/bin/env python3
"""
Daily Polymarket weather market betting recommendation.

Trains GBR quantile models on historical forecast data, predicts today's
risk_score per city, applies strategy logic, and sends a Slack message.

Required env vars (or .env):
    DATABASE_URL        Supabase transaction pooler URL
    SLACK_WEBHOOK_URL   Slack incoming webhook URL
"""

import os
import sys
import warnings
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
LEAD_HOURS_TARGET  = 20
LEAD_HOURS_WINDOW  = (10, 30)
MIN_FORECAST_HOURS = 6
PEAK_WINDOW        = (11, 19)
PLATEAU_TOL_F      = 1.0
RANDOM_SEED        = 42
RISK_REF           = 5.9    # historical median risk_score
MAX_MULT           = 2.5    # strategy C scaling cap
FIXED_BET          = 5.0
QUANTILES          = [0.10, 0.50, 0.90]
N_CV_SPLITS        = 5

# ── City metadata ─────────────────────────────────────────────────────────────
CITY_NAMES = {
    "us/ny/new-york-city/KLGA": "NYC",
    "us/il/chicago/KORD":       "Chicago",
    "gb/london/EGLC":           "London",
    "us/fl/miami/KMIA":         "Miami",
    "us/ga/atlanta/KATL":       "Atlanta",
    "us/tx/dallas/KDAL":        "Dallas",
    "us/wa/seatac/KSEA":        "Seattle",
    "ca/mississauga/CYYZ":      "Toronto",
    "fr/paris/LFPG":            "Paris",
    "tr/çubuk/LTAC":            "Ankara",
    "kr/incheon/RKSI":          "Incheon",
    "nz/wellington/NZWN":       "Wellington",
    "br/guarulhos/SBGR":        "São Paulo",
    "ar/ezeiza/SAEZ":           "Buenos Aires",
}
CITY_LAT = {
    "us/ny/new-york-city/KLGA": 40.8, "us/il/chicago/KORD": 42.0, "gb/london/EGLC": 51.5,
    "us/fl/miami/KMIA": 25.8, "us/ga/atlanta/KATL": 33.7, "us/tx/dallas/KDAL": 32.8,
    "us/wa/seatac/KSEA": 47.4, "ca/mississauga/CYYZ": 43.7, "fr/paris/LFPG": 49.0,
    "tr/çubuk/LTAC": 40.0, "kr/incheon/RKSI": 37.5, "nz/wellington/NZWN": -41.3,
    "br/guarulhos/SBGR": -23.4, "ar/ezeiza/SAEZ": -34.8,
}
CITY_TZ = {
    "us/ny/new-york-city/KLGA": -5, "us/il/chicago/KORD": -6, "gb/london/EGLC": 0,
    "us/fl/miami/KMIA": -5, "us/ga/atlanta/KATL": -5, "us/tx/dallas/KDAL": -6,
    "us/wa/seatac/KSEA": -8, "ca/mississauga/CYYZ": -5, "fr/paris/LFPG": 1,
    "tr/çubuk/LTAC": 3, "kr/incheon/RKSI": 9, "nz/wellington/NZWN": 13,
    "br/guarulhos/SBGR": -3, "ar/ezeiza/SAEZ": -3,
}
STRATEGY_MAP = {
    "us/ny/new-york-city/KLGA":  "C",       # risk_score 分層兩次一致
    "us/fl/miami/KMIA":          "C",       # risk_score 分層兩次一致
    "us/wa/seatac/KSEA":         "A",       # 分層不穩定
    "gb/london/EGLC":            "A",       # 分層不穩定
    "us/ga/atlanta/KATL":        "A",       # 觀察中
    "ar/ezeiza/SAEZ":            "A",       # 觀察中
    "fr/paris/LFPG":             "A",       # 觀察中
    "tr/çubuk/LTAC":             "A",       # 觀察中
    "us/tx/dallas/KDAL":         "A",       # 觀察中
    "nz/wellington/NZWN":        "A",       # 觀察中
    "br/guarulhos/SBGR":         "A",       # 觀察中
    "ca/mississauga/CYYZ":       "PAUSE",   # 勝率 29%
    "us/il/chicago/KORD":        "PAUSE",   # 勝率 38%
    "kr/incheon/RKSI":           "REMOVE",  # 系統性虧損
}

FEATURES_NUM = [
    "fc_peak", "fc_peak_hour", "fc_min", "fc_range", "n_at_peak",
    "plateau_w", "plateau_w2", "afternoon_mean", "morning_min",
    "rise_4h", "fall_4h", "curv_peak", "feels_gap_peak",
    "wind_peak", "wind_day_mean", "late_peak",
    "prev_fc_error", "prev_humidity", "prev_dewpoint", "prev_pressure",
    "pressure_delta", "fc_minus_prev_actual",
    "doy_sin", "doy_cos", "lead_hours", "lat",
]
FEATURES_CAT = ["zone"]
ALL_FEATS = FEATURES_NUM + FEATURES_CAT


# ── Helpers ───────────────────────────────────────────────────────────────────
def climate_zone(lat):
    a = abs(lat)
    if a < 23.5: return "tropical"
    if a < 35:   return "subtropical"
    if a < 50:   return "temperate"
    return "cool"


def get_conn():
    return psycopg2.connect(
        os.environ["DATABASE_URL"], sslmode="require",
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )


def city_local_date(lk):
    """Return the current local date for a given city."""
    tz_offset = CITY_TZ.get(lk, 0)
    local_now = datetime.now(timezone.utc) + timedelta(hours=tz_offset)
    return local_now.date()


def curve_features(g):
    """Compress one hourly forecast snapshot into a feature row."""
    s  = g.set_index("forecast_hour")["temp_f"].reindex(range(24))
    fl = g.set_index("forecast_hour")["feels_like_f"].reindex(range(24))
    wd = g.set_index("forecast_hour")["wind_mph"].reindex(range(24))
    PW0, PW1 = PEAK_WINDOW
    day = s.loc[PW0:PW1].dropna()
    if day.empty:
        day = s.dropna()
    if day.empty:
        return None
    peak = float(day.max())
    ph   = int(day.idxmax())
    vis  = s.dropna()
    return pd.Series({
        "fc_peak":        peak,
        "fc_peak_hour":   ph,
        "fc_min":         float(vis.min()),
        "fc_range":       peak - float(vis.min()),
        "n_at_peak":      int((day >= peak - 0.01).sum()),
        "plateau_w":      int((s >= peak - PLATEAU_TOL_F).sum()),
        "plateau_w2":     int((s >= peak - 2 * PLATEAU_TOL_F).sum()),
        "afternoon_mean": float(s.loc[12:17].mean()),
        "morning_min":    float(s.loc[4:8].min()),
        "rise_4h":        peak - float(s.get(ph - 4, np.nan)),
        "fall_4h":        peak - float(s.get(ph + 4, np.nan)),
        "curv_peak":      float(s.get(ph - 1, np.nan)) - 2 * peak + float(s.get(ph + 1, np.nan)),
        "feels_gap_peak": float(fl.get(ph, np.nan)) - peak,
        "wind_peak":      float(wd.get(ph, np.nan)),
        "wind_day_mean":  float(wd.loc[10:18].mean()),
        "late_peak":      int(ph >= 15),
    })


def add_context_features(df):
    """Add lat, zone, doy_sin/cos from location_key and target_date."""
    df["lat"]  = df["location_key"].map(CITY_LAT)
    df["zone"] = df["lat"].map(climate_zone)
    dt = pd.to_datetime(df["target_date"])
    doy = dt.dt.dayofyear
    df["doy_sin"] = np.sin(2 * np.pi * doy / 365.25)
    df["doy_cos"] = np.cos(2 * np.pi * doy / 365.25)
    return df


def _pre():
    return ColumnTransformer(
        [("cat", OneHotEncoder(handle_unknown="ignore"), FEATURES_CAT)],
        remainder="passthrough",
    )


def make_gbr(alpha):
    return Pipeline([
        ("pre", _pre()),
        ("gbr", GradientBoostingRegressor(
            loss="quantile", alpha=alpha, n_estimators=300,
            max_depth=2, learning_rate=0.04, subsample=0.8,
            min_samples_leaf=20, random_state=RANDOM_SEED,
        )),
    ])


# ── Data loading ──────────────────────────────────────────────────────────────
def load_historical(conn):
    """Load all historical city-days with known actual high, for model training."""
    lo, hi = LEAD_HOURS_WINDOW
    base = pd.read_sql("""
        WITH picked AS (
          SELECT DISTINCT ON (fs.location_key, fs.target_date)
                 fs.location_key, fs.target_date, fs.snapshot_time,
                 fs.hours_before_close
          FROM forecast_snapshots fs
          WHERE fs.n_forecast_hours >= %(minh)s
            AND fs.hours_before_close BETWEEN %(lo)s AND %(hi)s
          ORDER BY fs.location_key, fs.target_date,
                   abs(fs.hours_before_close - %(tgt)s)
        )
        SELECT p.*, ds.official_high_f,
               ds.avg_humidity_pct, ds.avg_dew_point_f, ds.avg_pressure_inhg
        FROM picked p
        JOIN cities c USING (location_key)
        LEFT JOIN weather_daily_summary ds
               ON ds.location_key = p.location_key AND ds.obs_date = p.target_date
        WHERE ds.official_high_f IS NOT NULL
        ORDER BY p.location_key, p.target_date
    """, conn, params={"minh": MIN_FORECAST_HOURS, "lo": lo, "hi": hi,
                       "tgt": LEAD_HOURS_TARGET})

    if base.empty:
        return pd.DataFrame()

    cur = conn.cursor()
    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _hist_pick
        (location_key text, target_date date, snapshot_time timestamptz)
    """)
    cur.execute("DELETE FROM _hist_pick")
    execute_values(
        cur, "INSERT INTO _hist_pick VALUES %s",
        list(base[["location_key", "target_date", "snapshot_time"]]
             .drop_duplicates().itertuples(index=False, name=None)),
    )
    conn.commit()

    curves = pd.read_sql("""
        SELECT h.location_key, h.target_date, h.forecast_hour,
               h.temp_f, h.feels_like_f, h.wind_mph
        FROM forecast_hourly_snapshots h
        JOIN _hist_pick p ON p.location_key  = h.location_key
                         AND p.target_date   = h.target_date
                         AND p.snapshot_time = h.snapshot_time
        ORDER BY 1, 2, 3
    """, conn)

    feat = (curves.groupby(["location_key", "target_date"])
            .apply(curve_features).reset_index().dropna())
    df = base.merge(feat, on=["location_key", "target_date"], how="inner")

    # Yesterday features via shift within each city
    df = df.sort_values(["location_key", "target_date"]).reset_index(drop=True)
    grp = df.groupby("location_key")
    df["prev_actual_high"]     = grp["official_high_f"].shift(1)
    df["prev_fc_peak"]         = grp["fc_peak"].shift(1)
    df["prev_fc_error"]        = df["prev_actual_high"] - df["prev_fc_peak"]
    df["prev_humidity"]        = grp["avg_humidity_pct"].shift(1)
    df["prev_dewpoint"]        = grp["avg_dew_point_f"].shift(1)
    df["prev_pressure"]        = grp["avg_pressure_inhg"].shift(1)
    df["pressure_delta"]       = (grp["avg_pressure_inhg"].shift(1)
                                  - grp["avg_pressure_inhg"].shift(2))
    df["fc_minus_prev_actual"] = df["fc_peak"] - df["prev_actual_high"]
    df["lead_hours"]           = df["hours_before_close"]
    df["actual_high"]          = df["official_high_f"]

    df = add_context_features(df)
    return df.dropna(subset=["actual_high", "fc_peak"]).reset_index(drop=True)


def load_today_snapshots(conn, target_dates):
    """
    Load the most recent forecast snapshot for each city's current market date.
    target_dates: dict {location_key: date}
    """
    pairs = [(lk, str(d)) for lk, d in target_dates.items()]
    if not pairs:
        return pd.DataFrame(), pd.DataFrame()

    cur = conn.cursor()
    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _today_target
        (location_key text, target_date date)
    """)
    cur.execute("DELETE FROM _today_target")
    execute_values(cur, "INSERT INTO _today_target VALUES %s", pairs)
    conn.commit()

    base = pd.read_sql("""
        WITH latest AS (
          SELECT DISTINCT ON (fs.location_key)
                 fs.location_key, fs.target_date, fs.snapshot_time,
                 fs.hours_before_close, fs.n_forecast_hours
          FROM forecast_snapshots fs
          JOIN _today_target t ON t.location_key = fs.location_key
                              AND t.target_date  = fs.target_date
          WHERE fs.n_forecast_hours >= %(minh)s
          ORDER BY fs.location_key, fs.snapshot_time DESC
        )
        SELECT l.*
        FROM latest l
        ORDER BY l.location_key
    """, conn, params={"minh": MIN_FORECAST_HOURS})

    if base.empty:
        return pd.DataFrame(), pd.DataFrame()

    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _today_pick
        (location_key text, target_date date, snapshot_time timestamptz)
    """)
    cur.execute("DELETE FROM _today_pick")
    execute_values(
        cur, "INSERT INTO _today_pick VALUES %s",
        list(base[["location_key", "target_date", "snapshot_time"]]
             .drop_duplicates().itertuples(index=False, name=None)),
    )
    conn.commit()

    curves = pd.read_sql("""
        SELECT h.location_key, h.target_date, h.forecast_hour,
               h.temp_f, h.feels_like_f, h.wind_mph
        FROM forecast_hourly_snapshots h
        JOIN _today_pick p ON p.location_key  = h.location_key
                          AND p.target_date   = h.target_date
                          AND p.snapshot_time = h.snapshot_time
        ORDER BY 1, 2, 3
    """, conn)

    return base, curves


def build_today_features(df_hist, today_base, today_curves):
    """
    Assemble feature rows for today's prediction.
    Uses the most recent historical row per city as 'yesterday' data.
    """
    if today_curves.empty:
        return pd.DataFrame()

    feat = (today_curves.groupby(["location_key", "target_date"])
            .apply(curve_features).reset_index().dropna())
    df = today_base.merge(feat, on=["location_key", "target_date"], how="inner")

    # Use latest historical row per city as "yesterday"
    last_hist = (df_hist.sort_values("target_date")
                 .groupby("location_key").last().reset_index())

    # avg_pressure_inhg = most recent day's pressure → used as prev_pressure for today
    # prev_pressure (shift-1 in hist) = two days ago → used only for pressure_delta
    prev = last_hist[["location_key", "fc_peak", "actual_high",
                       "avg_humidity_pct", "avg_dew_point_f", "avg_pressure_inhg",
                       "prev_pressure"]].rename(columns={
        "fc_peak":            "prev_fc_peak",
        "actual_high":        "prev_actual_high",
        "avg_humidity_pct":   "prev_humidity",
        "avg_dew_point_f":    "prev_dewpoint",
        "avg_pressure_inhg":  "prev_pressure",
        "prev_pressure":      "_prev_p_d2",
    })
    df = df.merge(prev, on="location_key", how="left")

    df["prev_fc_error"]        = df["prev_actual_high"] - df["prev_fc_peak"]
    df["pressure_delta"]       = df["prev_pressure"] - df["_prev_p_d2"]
    df["fc_minus_prev_actual"] = df["fc_peak"] - df["prev_actual_high"]
    df["lead_hours"]           = df["hours_before_close"]

    df = add_context_features(df)
    return df


# ── Model training & prediction ───────────────────────────────────────────────
def train_and_predict(df_hist, df_today):
    """
    Train quantile GBR on all historical data, predict P10/P90 for today.
    Returns df_today with risk_score, pred_q10, pred_q50, pred_q90 columns.
    """
    dfm = df_hist.copy()
    for c in FEATURES_NUM:
        dfm[c] = dfm[c].fillna(dfm[c].median())
    X_train = dfm[ALL_FEATS]
    y_train = dfm["actual_high"].values

    df_pred = df_today.copy()
    medians = dfm[FEATURES_NUM].median()
    for c in FEATURES_NUM:
        df_pred[c] = df_pred[c].fillna(medians[c])
    X_pred = df_pred[ALL_FEATS]

    preds = {}
    for q in QUANTILES:
        m = make_gbr(q).fit(X_train, y_train)
        preds[q] = m.predict(X_pred)

    q10, q50, q90 = preds[0.10], preds[0.50], preds[0.90]
    q10 = np.minimum(q10, q50)
    q90 = np.maximum(q90, q50)

    df_pred["pred_q10"]   = q10
    df_pred["pred_q50"]   = q50
    df_pred["pred_q90"]   = q90
    df_pred["risk_score"] = q90 - q10
    return df_pred


# ── Strategy logic ────────────────────────────────────────────────────────────
def compute_bet(lk, risk_score):
    strategy = STRATEGY_MAP.get(lk, "A")
    if strategy == "PAUSE" or strategy == "REMOVE":
        return strategy, 0.0, 1.0
    if strategy == "C":
        mult = min(RISK_REF / max(risk_score, 1.0), MAX_MULT)
        return "C", round(FIXED_BET * mult, 2), round(mult, 2)
    return "A", FIXED_BET, 1.0


# ── Slack notification ────────────────────────────────────────────────────────
def send_slack(results, today_label):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("[Slack] SLACK_WEBHOOK_URL not set — printing to stdout only.")
        return

    c_lines, a_lines, pause_lines = [], [], []
    for r in results:
        lk, name, rs, fc_peak, bet, mult, strategy = (
            r["lk"], r["name"], r["risk_score"], r["fc_peak"],
            r["bet"], r["mult"], r["strategy"],
        )
        risk_flag = " :rotating_light:" if rs >= 9.0 else ""
        if strategy == "C":
            c_lines.append(
                f"• *{name}*: risk={rs:.1f}°F → 下注 {bet:.1f} (×{mult:.2f}){risk_flag}"
                f"  _(WU峰值 {fc_peak:.0f}°F +1°F 基準)_"
            )
        elif strategy == "A":
            c_lines_ref = a_lines
            c_lines_ref.append(
                f"• *{name}*: risk={rs:.1f}°F → 下注 {bet:.1f}{risk_flag}"
                f"  _(WU峰值 {fc_peak:.0f}°F)_"
            )
        else:
            pause_lines.append(f"• {name}")

    sections = []
    if c_lines:
        sections.append(":white_check_mark: *策略 C（NYC + Miami）*\n" + "\n".join(c_lines))
    if a_lines:
        sections.append(":large_blue_circle: *策略 A（固定 5）*\n" + "\n".join(a_lines))
    if pause_lines:
        sections.append(":double_vertical_bar: *暫停 / 移除*\n" + "\n".join(pause_lines))

    text = (
        f":bar_chart: *Polymarket 下注建議* — {today_label}\n\n"
        + "\n\n".join(sections)
        + f"\n\n_risk_ref={RISK_REF}°F, MAX_MULT=×{MAX_MULT}, 基準下注={FIXED_BET}_"
        + "\n_risk ≥ 9°F :rotating_light: = 降注 50% 或跳過_"
    )

    resp = requests.post(webhook, json={"text": text}, timeout=10)
    if resp.status_code != 200:
        print(f"[Slack] Error {resp.status_code}: {resp.text}")
    else:
        print("[Slack] Message sent.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Connecting to database...")
    conn = get_conn()

    print("Loading historical data for model training...")
    df_hist = load_historical(conn)
    if df_hist.empty:
        print("ERROR: No historical data found.")
        sys.exit(1)
    print(f"  {len(df_hist)} historical city-days loaded "
          f"({df_hist['target_date'].nunique()} dates, "
          f"{df_hist['location_key'].nunique()} cities)")

    # Determine each city's current local market date
    target_dates = {lk: city_local_date(lk) for lk in CITY_NAMES}
    today_label = str(next(iter(target_dates.values())))  # use NYC date as header
    print(f"\nFetching today's forecasts (target dates vary by city)...")

    today_base, today_curves = load_today_snapshots(conn, target_dates)
    conn.close()

    if today_base.empty:
        print("No today forecasts found. Check that the collector has run today.")
        sys.exit(1)
    print(f"  Found snapshots for: "
          f"{', '.join(today_base['location_key'].map(CITY_NAMES).tolist())}")

    print("\nBuilding feature matrix for today...")
    df_today = build_today_features(df_hist, today_base, today_curves)
    if df_today.empty:
        print("No today features could be built (curves empty?).")
        sys.exit(1)

    print("Training models and predicting risk scores...")
    df_pred = train_and_predict(df_hist, df_today)

    # Print and collect results
    print(f"\n{'='*60}")
    print(f"  Polymarket 下注建議 — {today_label}")
    print(f"{'='*60}")

    results = []
    for _, row in df_pred.sort_values("risk_score").iterrows():
        lk   = row["location_key"]
        name = CITY_NAMES.get(lk, lk)
        rs   = row["risk_score"]
        fc   = row["fc_peak"]
        strategy, bet, mult = compute_bet(lk, rs)

        flag = " ⚠️" if rs >= 9.0 else ""
        print(f"  {name:<14} risk={rs:4.1f}°F  fc_peak={fc:5.1f}°F  "
              f"strategy={strategy}  bet={bet:4.1f}  ×{mult:.2f}{flag}")

        results.append({
            "lk": lk, "name": name, "risk_score": rs, "fc_peak": fc,
            "strategy": strategy, "bet": bet, "mult": mult,
        })
    print(f"{'='*60}\n")

    # Sort results: C first, then A, then paused
    order = {"C": 0, "A": 1, "PAUSE": 2, "REMOVE": 3}
    results.sort(key=lambda r: (order.get(r["strategy"], 4), r["risk_score"]))

    send_slack(results, today_label)
    print("Done.")


if __name__ == "__main__":
    main()