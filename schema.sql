-- ============================================================
-- Polymarket 氣象市場歷史資料庫 Schema
-- 引擎: PostgreSQL / Supabase
-- 更新: 2026-02-27
--
-- 在 Supabase SQL Editor 執行即可建立所有資料表。
-- ============================================================


-- ============================================================
-- 0. 城市參考表
-- ============================================================

CREATE TABLE IF NOT EXISTS cities (
    location_key     TEXT PRIMARY KEY,   -- "us/ga/atlanta/KATL"
    name             TEXT NOT NULL,      -- "Atlanta, GA"
    series_slug      TEXT,               -- "atlanta-daily-weather"
    event_slug_city  TEXT,               -- "atlanta"
    wu_station       TEXT,               -- "KATL"
    timezone_offset  REAL NOT NULL,      -- UTC offset，例如 -5、13
    celsius          BOOLEAN NOT NULL DEFAULT FALSE
);


-- ============================================================
-- 1. 逐時實際氣象觀測
--    來源：WU /history/daily/{location_key}
--    頻率：每日收盤後補齊前一天全天 24 筆
-- ============================================================

CREATE TABLE IF NOT EXISTS weather_actuals_hourly (
    id               BIGSERIAL PRIMARY KEY,
    location_key     TEXT        NOT NULL REFERENCES cities(location_key),
    obs_date         DATE        NOT NULL,   -- 城市本地日期
    obs_hour         SMALLINT    NOT NULL,   -- 0–23
    temp_f           REAL,
    feels_like_f     REAL,
    dew_point_f      REAL,
    humidity_pct     REAL,
    precip_in        REAL,
    cloud_cover_pct  REAL,
    wind_mph         REAL,
    wind_dir         TEXT,                   -- "NW", "S", ...
    pressure_inhg    REAL,
    scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, obs_date, obs_hour)
);

CREATE INDEX IF NOT EXISTS idx_actuals_city_date
    ON weather_actuals_hourly(location_key, obs_date);


-- ============================================================
-- 2. 每日官方最高 / 最低（Polymarket 結算依據）
--    is_final = FALSE 代表可能還會被 WU 修正
-- ============================================================

CREATE TABLE IF NOT EXISTS weather_daily_summary (
    id                BIGSERIAL PRIMARY KEY,
    location_key      TEXT        NOT NULL REFERENCES cities(location_key),
    obs_date          DATE        NOT NULL,
    official_high_f   REAL,
    official_low_f    REAL,
    total_precip_in   REAL,
    avg_humidity_pct  REAL,
    avg_dew_point_f   REAL,
    avg_wind_mph      REAL,
    avg_pressure_inhg REAL,
    is_final          BOOLEAN     NOT NULL DEFAULT FALSE,
    scraped_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, obs_date)
);


-- ============================================================
-- 3. WU 最高溫預報快照
--    每小時記錄：WU 此時預測 target_date 的最高 / 最低溫
--    收集窗口：target_date 結束前 36 小時起
-- ============================================================

CREATE TABLE IF NOT EXISTS forecast_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    location_key        TEXT        NOT NULL REFERENCES cities(location_key),
    target_date         DATE        NOT NULL,
    snapshot_time       TIMESTAMPTZ NOT NULL,
    hours_before_close  REAL,               -- 距 target_date 23:59 還有幾小時（正數）
    forecast_high_f     REAL,
    forecast_low_f      REAL,
    n_forecast_hours    SMALLINT,             -- 此快照包含幾個小時的預報（用於判斷代表性）
    forecast_precip_in  REAL,
    forecast_precip_pct REAL,
    scraped_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, target_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_forecast_city_target
    ON forecast_snapshots(location_key, target_date);


-- ============================================================
-- 4. WU 逐時預報曲線快照
--    同一 snapshot_time 下，target_date 每個小時的預報值
--    用途：分析預報曲線形狀、找系統性偏差
-- ============================================================

CREATE TABLE IF NOT EXISTS forecast_hourly_snapshots (
    id               BIGSERIAL PRIMARY KEY,
    location_key     TEXT        NOT NULL REFERENCES cities(location_key),
    target_date      DATE        NOT NULL,
    snapshot_time    TIMESTAMPTZ NOT NULL,
    forecast_hour    SMALLINT    NOT NULL,   -- 0–23（城市本地）
    temp_f           REAL,
    feels_like_f     REAL,
    precip_pct       REAL,
    wind_mph         REAL,
    scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, target_date, snapshot_time, forecast_hour)
);

CREATE INDEX IF NOT EXISTS idx_forecast_hourly_city_target
    ON forecast_hourly_snapshots(location_key, target_date, snapshot_time);


-- ============================================================
-- 5. 市場選項元資料（含 CLOB token ID，Phase 4 下單必需）
--    每個城市每天的 9 個選項只需抓一次
-- ============================================================

CREATE TABLE IF NOT EXISTS market_options (
    id               BIGSERIAL PRIMARY KEY,
    location_key     TEXT        NOT NULL REFERENCES cities(location_key),
    market_date      DATE        NOT NULL,
    option_label     TEXT        NOT NULL,   -- "62-63°F"
    option_rank      SMALLINT    NOT NULL,   -- 1 = 最低溫區間
    token_id_yes     TEXT,                   -- CLOB Yes token（下單用）
    token_id_no      TEXT,
    gamma_market_id  TEXT,
    scraped_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, market_date, option_label)
);


-- ============================================================
-- 6. 市場賠率快照（核心資料表）
--    每小時 × 14 城市 × 9 選項 ≈ 1,260 筆 / 天（收集窗口內）
-- ============================================================

CREATE TABLE IF NOT EXISTS market_snapshots (
    id                 BIGSERIAL PRIMARY KEY,
    location_key       TEXT        NOT NULL REFERENCES cities(location_key),
    market_date        DATE        NOT NULL,
    snapshot_time      TIMESTAMPTZ NOT NULL,
    hours_before_close REAL,
    option_label       TEXT        NOT NULL,
    yes_prob           REAL,                 -- 0–1，CLOB midpoint 優先
    no_prob            REAL,                 -- = 1 - yes_prob
    best_bid           REAL,
    best_ask           REAL,
    spread             REAL,
    volume_usdc        REAL,
    liquidity_usdc     REAL,
    accepting_orders   BOOLEAN,
    scraped_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, market_date, snapshot_time, option_label)
);

CREATE INDEX IF NOT EXISTS idx_mkt_snap_city_date
    ON market_snapshots(location_key, market_date);

CREATE INDEX IF NOT EXISTS idx_mkt_snap_time
    ON market_snapshots(snapshot_time);


-- ============================================================
-- 7. 市場結算結果
-- ============================================================

CREATE TABLE IF NOT EXISTS market_resolutions (
    id                 BIGSERIAL PRIMARY KEY,
    location_key       TEXT        NOT NULL REFERENCES cities(location_key),
    market_date        DATE        NOT NULL,
    resolved_option    TEXT,
    wu_official_high_f REAL,
    volume_total_usdc  REAL,
    resolved_at        TIMESTAMPTZ,
    scraped_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(location_key, market_date)
);


-- ============================================================
-- 8. 收集器執行記錄
--    偵測資料缺口、監控爬蟲健康
-- ============================================================

CREATE TABLE IF NOT EXISTS collection_log (
    id               BIGSERIAL PRIMARY KEY,
    run_time         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_type         TEXT        NOT NULL,  -- "hourly" | "daily_close" | "backfill"
    cities_attempted SMALLINT    NOT NULL DEFAULT 0,
    cities_ok        SMALLINT    NOT NULL DEFAULT 0,
    cities_failed    SMALLINT    NOT NULL DEFAULT 0,
    errors           JSONB,                 -- [{"city": "...", "error": "..."}]
    duration_sec     REAL
);
