"""
collector.py — Polymarket 氣象市場每小時資料收集器

每次執行做三件事：
  1. 若城市在「收集窗口」內（距當日結束 ≤ 36 小時）：
       - 快照 WU 逐時預報（預報最高溫 + 逐時曲線）
       - 快照 Polymarket 市場賠率
  2. 補抓「昨日」WU 實測資料（若 DB 尚未有完整紀錄）
  3. 寫入本次執行記錄到 collection_log

用法:
    python collector.py              # 正常執行
    python collector.py --dry-run    # 只印計畫，不寫入 DB

排程 (crontab):
    0 * * * * cd /path/to/project && ./venv/bin/python collector.py >> logs/collector.log 2>&1
"""

import asyncio
import json
import os
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo

_PROJECT_ROOT = Path(__file__).parent.parent  # src/ -> project root

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from playwright.async_api import async_playwright

from weather_scraper import (
    MAX_CONCURRENT_CITIES,
    extract_temp_f,
    fetch_page,
    parse_mat_table,
    parse_time_to_hour,
)


# ── 設定 ──────────────────────────────────────────────────────────────────────

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL", "")

GAMMA_API    = "https://gamma-api.polymarket.com"
CLOB_API     = "https://clob.polymarket.com"
API_HEADERS  = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

COLLECTION_WINDOW_HOURS = 36   # 距城市當日結束幾小時前開始收集

CITY_TZINFO: dict[str, ZoneInfo] = {
    "nz/wellington/NZWN":        ZoneInfo("Pacific/Auckland"),
    "kr/incheon/RKSI":           ZoneInfo("Asia/Seoul"),
    "tr/çubuk/LTAC":             ZoneInfo("Europe/Istanbul"),
    "fr/paris/LFPG":             ZoneInfo("Europe/Paris"),
    "gb/london/EGLC":            ZoneInfo("Europe/London"),
    "ar/ezeiza/SAEZ":            ZoneInfo("America/Argentina/Buenos_Aires"),
    "br/guarulhos/SBGR":         ZoneInfo("America/Sao_Paulo"),
    "us/ny/new-york-city/KLGA":  ZoneInfo("America/New_York"),
    "ca/mississauga/CYYZ":       ZoneInfo("America/Toronto"),
    "us/fl/miami/KMIA":          ZoneInfo("America/New_York"),
    "us/ga/atlanta/KATL":        ZoneInfo("America/New_York"),
    "us/tx/dallas/KDAL":         ZoneInfo("America/Chicago"),
    "us/il/chicago/KORD":        ZoneInfo("America/Chicago"),
    "us/wa/seatac/KSEA":         ZoneInfo("America/Los_Angeles"),
}


# ── DB 連線 ───────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


# ── HTTP 工具 ─────────────────────────────────────────────────────────────────

def http_get(url: str) -> dict | list:
    req = urllib.request.Request(url, headers=API_HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


# ── 城市時區工具 ──────────────────────────────────────────────────────────────

def city_tz(location_key: str) -> ZoneInfo:
    """回傳城市的 IANA 時區（自動處理 DST）。"""
    return CITY_TZINFO.get(location_key, ZoneInfo("UTC"))


def city_local_date(location_key: str) -> date:
    return datetime.now(city_tz(location_key)).date()


def hours_before_eod(location_key: str, target_date: date) -> float:
    """距離 target_date 23:59:59（城市本地時間）還有幾小時。負值代表已過。"""
    tz  = city_tz(location_key)
    eod = datetime(target_date.year, target_date.month, target_date.day,
                   23, 59, 59, tzinfo=tz)
    return (eod - datetime.now(tz)).total_seconds() / 3600


# ── 強化版 WU 解析（提取全欄位）──────────────────────────────────────────────

def _first_float(text: str) -> float | None:
    m = re.search(r"(-?\d+(?:\.\d+)?)", text or "")
    return float(m.group(1)) if m else None


def _wind_mph(text: str) -> float | None:
    m = re.search(r"(\d+(?:\.\d+)?)\s*mph", text or "", re.I)
    return float(m.group(1)) if m else _first_float(text)


def _wind_dir(text: str) -> str | None:
    m = re.search(r"[NSEW]{1,3}", text or "")
    return m.group(0) if m else None


def _col(entry: dict, *names) -> str:
    """從多個可能的欄位名稱中，回傳第一個有值的。"""
    for name in names:
        v = entry.get(name, "").strip()
        if v:
            return v
    return ""


def parse_history_full(html: str) -> list[dict]:
    """解析 WU 歷史頁全欄位（Time / Temp / Feels Like / Dew Point / Humidity / Wind / Pressure / Precip / Cloud）"""
    headers, rows = parse_mat_table(html)
    results = []
    for row in rows:
        if len(row) < len(headers):
            continue
        e    = dict(zip(headers, row))
        hour = parse_time_to_hour(_col(e, "Time"))
        temp = extract_temp_f(_col(e, "Temperature", "Temp."))
        if hour is None or temp is None:
            continue
        results.append({
            "hour":            hour,
            "time":            _col(e, "Time"),
            "temp_f":          temp,
            "source":          "actual",
            "feels_like_f":    extract_temp_f(_col(e, "Feels Like", "FeelsLike")),
            "dew_point_f":     extract_temp_f(_col(e, "Dew Point",  "DewPoint")),
            "humidity_pct":    _first_float(_col(e, "Humidity")),
            "precip_in":       _first_float(_col(e, "Amount",       "Precip. Amount")),
            "cloud_cover_pct": _first_float(_col(e, "Cloud Cover",  "CloudCover")),
            "wind_mph":        _wind_mph(_col(e, "Wind")),
            "wind_dir":        _wind_dir(_col(e, "Wind")),
            "pressure_inhg":   _first_float(_col(e, "Pressure")),
        })
    return results


def parse_forecast_full(html: str, current_local_hour: int | None = None) -> list[dict]:
    """解析 WU 預報頁全欄位。

    current_local_hour: 城市目前的本地小時（0–23）。
        傳入後會在偵測到跨午夜（小時數折回）時截斷，
        確保只回傳 target_date 當天剩餘的小時，不含隔天。
        未傳入則回傳全部列（向下相容）。
    """
    headers, rows = parse_mat_table(html)
    results  = []
    prev_hour = None  # 上一列的小時數，用於偵測跨日

    for row in rows:
        if len(row) < len(headers):
            continue
        e    = dict(zip(headers, row))
        hour = parse_time_to_hour(_col(e, "Time"))
        temp = extract_temp_f(_col(e, "Temp.", "Temperature"))
        if hour is None or temp is None:
            continue

        # ── 跨日偵測：WU 頁面按時間順序排列，小時數只有跨午夜才會折回 ──
        if current_local_hour is not None and prev_hour is not None:
            if hour < prev_hour:
                # 小時數折回 = 進入隔天，截斷並停止
                break

        prev_hour = hour
        results.append({
            "hour":            hour,
            "time":            _col(e, "Time"),
            "temp_f":          temp,
            "source":          "forecast",
            "feels_like_f":    extract_temp_f(_col(e, "Feels Like", "FeelsLike")),
            "dew_point_f":     extract_temp_f(_col(e, "Dew Point",  "DewPoint")),
            "humidity_pct":    _first_float(_col(e, "Humidity")),
            "precip_in":       _first_float(_col(e, "Amount",       "Precip. Amount")),
            "precip_pct":      _first_float(_col(e, "Precip.",      "Precip. Chance")),
            "cloud_cover_pct": _first_float(_col(e, "Cloud Cover",  "CloudCover")),
            "wind_mph":        _wind_mph(_col(e, "Wind")),
            "pressure_inhg":   _first_float(_col(e, "Pressure")),
        })
    return results


# ── DB 寫入（upsert）────────────────────────────────────────────────────────

def upsert_actuals(conn, location_key: str, obs_date: date, rows: list[dict]):
    with conn.cursor() as cur:
        for r in rows:
            cur.execute("""
                INSERT INTO weather_actuals_hourly
                    (location_key, obs_date, obs_hour,
                     temp_f, feels_like_f, dew_point_f, humidity_pct,
                     precip_in, cloud_cover_pct, wind_mph, wind_dir, pressure_inhg)
                VALUES (%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s)
                ON CONFLICT (location_key, obs_date, obs_hour) DO UPDATE SET
                    temp_f          = EXCLUDED.temp_f,
                    feels_like_f    = EXCLUDED.feels_like_f,
                    dew_point_f     = EXCLUDED.dew_point_f,
                    humidity_pct    = EXCLUDED.humidity_pct,
                    precip_in       = EXCLUDED.precip_in,
                    cloud_cover_pct = EXCLUDED.cloud_cover_pct,
                    wind_mph        = EXCLUDED.wind_mph,
                    wind_dir        = EXCLUDED.wind_dir,
                    pressure_inhg   = EXCLUDED.pressure_inhg,
                    scraped_at      = NOW()
            """, (
                location_key, obs_date, r["hour"],
                r.get("temp_f"), r.get("feels_like_f"), r.get("dew_point_f"),
                r.get("humidity_pct"), r.get("precip_in"), r.get("cloud_cover_pct"),
                r.get("wind_mph"), r.get("wind_dir"), r.get("pressure_inhg"),
            ))
    conn.commit()


def upsert_daily_summary(conn, location_key: str, obs_date: date, actuals: list[dict]):
    """從 actuals 推算當日統計，is_final=FALSE 表示可能仍需修正。"""
    temps = [r["temp_f"] for r in actuals if r.get("temp_f") is not None]
    if not temps:
        return

    def avg(lst):
        valid = [x for x in lst if x is not None]
        return sum(valid) / len(valid) if valid else None

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO weather_daily_summary
                (location_key, obs_date,
                 official_high_f, official_low_f, total_precip_in,
                 avg_humidity_pct, avg_dew_point_f, avg_wind_mph, avg_pressure_inhg,
                 is_final)
            VALUES (%s,%s, %s,%s,%s, %s,%s,%s,%s, FALSE)
            ON CONFLICT (location_key, obs_date) DO UPDATE SET
                official_high_f   = EXCLUDED.official_high_f,
                official_low_f    = EXCLUDED.official_low_f,
                total_precip_in   = EXCLUDED.total_precip_in,
                avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
                avg_dew_point_f   = EXCLUDED.avg_dew_point_f,
                avg_wind_mph      = EXCLUDED.avg_wind_mph,
                avg_pressure_inhg = EXCLUDED.avg_pressure_inhg,
                scraped_at        = NOW()
            WHERE weather_daily_summary.is_final = FALSE
        """, (
            location_key, obs_date,
            max(temps), min(temps),
            sum(r.get("precip_in") or 0 for r in actuals) or None,
            avg([r.get("humidity_pct")    for r in actuals]),
            avg([r.get("dew_point_f")     for r in actuals]),
            avg([r.get("wind_mph")        for r in actuals]),
            avg([r.get("pressure_inhg")   for r in actuals]),
        ))
    conn.commit()


def upsert_forecast(conn, location_key: str, target_date: date,
                    snapshot_time: datetime, hours_before: float,
                    forecast_pts: list[dict]):
    temps = [r["temp_f"] for r in forecast_pts if r.get("temp_f") is not None]
    if not temps:
        return

    def avg(lst):
        valid = [x for x in lst if x is not None]
        return sum(valid) / len(valid) if valid else None

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO forecast_snapshots
                (location_key, target_date, snapshot_time, hours_before_close,
                 forecast_high_f, forecast_low_f, n_forecast_hours,
                 forecast_precip_pct)
            VALUES (%s,%s,%s,%s, %s,%s,%s, %s)
            ON CONFLICT (location_key, target_date, snapshot_time) DO NOTHING
        """, (
            location_key, target_date, snapshot_time, hours_before,
            max(temps), min(temps), len(temps),
            avg([r.get("precip_pct") for r in forecast_pts]),
        ))

        for r in forecast_pts:
            cur.execute("""
                INSERT INTO forecast_hourly_snapshots
                    (location_key, target_date, snapshot_time, forecast_hour,
                     temp_f, feels_like_f, precip_pct, wind_mph)
                VALUES (%s,%s,%s,%s, %s,%s,%s,%s)
                ON CONFLICT (location_key, target_date, snapshot_time, forecast_hour)
                DO NOTHING
            """, (
                location_key, target_date, snapshot_time, r["hour"],
                r.get("temp_f"), r.get("feels_like_f"),
                r.get("precip_pct"), r.get("wind_mph"),
            ))
    conn.commit()


def upsert_market_options(conn, location_key: str, market_date: date,
                          markets: list[dict]):
    with conn.cursor() as cur:
        for rank, m in enumerate(markets, 1):
            label = m.get("groupItemTitle", "")
            ids   = m.get("clobTokenIds", "[]")
            if isinstance(ids, str):
                ids = json.loads(ids)
            cur.execute("""
                INSERT INTO market_options
                    (location_key, market_date, option_label, option_rank,
                     token_id_yes, token_id_no, gamma_market_id)
                VALUES (%s,%s,%s,%s, %s,%s,%s)
                ON CONFLICT (location_key, market_date, option_label) DO NOTHING
            """, (
                location_key, market_date, label, rank,
                ids[0] if len(ids) > 0 else None,
                ids[1] if len(ids) > 1 else None,
                m.get("id"),
            ))
    conn.commit()


def upsert_market_snapshot(conn, location_key: str, market_date: date,
                            snapshot_time: datetime, hours_before: float,
                            markets: list[dict]):
    with conn.cursor() as cur:
        for m in markets:
            yes_p = m.get("yes_prob")
            if yes_p is None:
                op = m.get("outcomePrices", "[]")
                if isinstance(op, str):
                    op = json.loads(op)
                yes_p = float(op[0]) if op else None

            cur.execute("""
                INSERT INTO market_snapshots
                    (location_key, market_date, snapshot_time, hours_before_close,
                     option_label, yes_prob, no_prob, spread,
                     volume_usdc, liquidity_usdc, accepting_orders)
                VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s)
                ON CONFLICT (location_key, market_date, snapshot_time, option_label)
                DO NOTHING
            """, (
                location_key, market_date, snapshot_time, hours_before,
                m.get("groupItemTitle", ""),
                yes_p,
                (1 - yes_p) if yes_p is not None else None,
                m.get("spread"),
                m.get("volume"),
                m.get("liquidity"),
                bool(m.get("acceptingOrders")),
            ))
    conn.commit()


def upsert_cities_table(conn, cities: list[dict], poly_map: dict[str, dict]):
    """在每次執行開始時，確保 cities 表有所有城市記錄（upsert）。"""
    with conn.cursor() as cur:
        for c in cities:
            key      = c["location_key"]
            poly     = poly_map.get(key, {})
            station  = poly.get("wu_station") or key.split("/")[-1]
            tz_info  = CITY_TZINFO.get(key)
            tz_off   = datetime.now(tz_info).utcoffset().total_seconds() / 3600 if tz_info else 0
            cur.execute("""
                INSERT INTO cities
                    (location_key, name, series_slug, event_slug_city,
                     wu_station, timezone_offset, celsius)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (location_key) DO UPDATE SET
                    name            = EXCLUDED.name,
                    series_slug     = EXCLUDED.series_slug,
                    event_slug_city = EXCLUDED.event_slug_city,
                    wu_station      = EXCLUDED.wu_station,
                    timezone_offset = EXCLUDED.timezone_offset,
                    celsius         = EXCLUDED.celsius
            """, (
                key,
                c["name"],
                poly.get("series_slug"),
                poly.get("event_slug_city"),
                station,
                tz_off,
                c.get("celsius", False),
            ))
    conn.commit()


def try_upsert_resolution(conn, location_key: str, market_date: date,
                           event: dict, markets: list[dict]) -> bool:
    """若市場已結算，寫入結算選項與官方最高溫。回傳是否成功寫入。"""
    if not event.get("closed"):
        return False
    resolved = next(
        (m.get("groupItemTitle") for m in markets
         if m.get("winner") is True or str(m.get("winner", "")).lower() == "true"),
        None,
    )
    if resolved is None:
        return False

    # 從 weather_daily_summary 查官方最高溫（結算依據）
    official_high = None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT official_high_f FROM weather_daily_summary "
            "WHERE location_key=%s AND obs_date=%s",
            (location_key, market_date),
        )
        row = cur.fetchone()
        if row:
            official_high = row[0]

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO market_resolutions
                (location_key, market_date, resolved_option,
                 wu_official_high_f, volume_total_usdc)
            VALUES (%s,%s,%s, %s,%s)
            ON CONFLICT (location_key, market_date) DO UPDATE SET
                wu_official_high_f = COALESCE(
                    market_resolutions.wu_official_high_f, EXCLUDED.wu_official_high_f)
        """, (location_key, market_date, resolved,
              official_high, event.get("volume")))
    conn.commit()
    return True


# ── Polymarket 取資料 ──────────────────────────────────────────────────────────

def fetch_poly_markets(event_slug_city: str,
                       target_date: date) -> tuple[dict, list[dict]] | None:
    month = target_date.strftime("%B").lower()
    slug  = (f"highest-temperature-in-{event_slug_city}"
             f"-on-{month}-{target_date.day}-{target_date.year}")
    try:
        events = http_get(f"{GAMMA_API}/events?slug={slug}&limit=1")
        if not events:
            return None
        event   = events[0]
        markets = event.get("markets", [])
        if not markets:
            return None

        def get_clob(m):
            ids = m.get("clobTokenIds", "[]")
            if isinstance(ids, str):
                ids = json.loads(ids)
            if not ids:
                return None, None
            try:
                mid_r    = http_get(f"{CLOB_API}/midpoint?token_id={ids[0]}")
                spread_r = http_get(f"{CLOB_API}/spread?token_id={ids[0]}")
                return (float(mid_r.get("mid")    or 0),
                        float(spread_r.get("spread") or 0))
            except Exception:
                return None, None

        with ThreadPoolExecutor(max_workers=min(10, len(markets))) as ex:
            futs = {ex.submit(get_clob, m): i for i, m in enumerate(markets)}
            clob = [None] * len(markets)
            for fut in as_completed(futs):
                clob[futs[fut]] = fut.result()

        for m, (mid, spread) in zip(markets, clob):
            m["yes_prob"] = mid
            m["spread"]   = spread

        return event, markets

    except Exception as e:
        print(f"    [Poly ERR] {event_slug_city} {target_date}: {e}")
        return None


# ── 每城市邏輯 ───────────────────────────────────────────────────────────────

async def collect_city(
    browser,
    conn,
    city:       dict,
    poly_city:  dict | None,
    snapshot_t: datetime,
    dry_run:    bool,
) -> list[str]:
    key              = city["location_key"]
    target_date      = city_local_date(key)
    hours_left       = hours_before_eod(key, target_date)
    local_now        = datetime.now(city_tz(key))
    current_local_hr = local_now.hour
    logs             = []

    # ── 窗口內：預報 + 市場賠率 ──────────────────────────────────────────────
    if 0 <= hours_left <= COLLECTION_WINDOW_HOURS:
        # WU 逐時預報
        try:
            forecast_url = f"https://www.wunderground.com/hourly/{key}"
            html         = await fetch_page(browser, forecast_url)
            pts          = parse_forecast_full(html, current_local_hour=current_local_hr)
            if dry_run:
                logs.append(f"[DRY] forecast {len(pts)} pts")
            else:
                upsert_forecast(conn, key, target_date, snapshot_t, hours_left, pts)
                logs.append(f"forecast {len(pts)} pts")
        except Exception as e:
            if conn:
                conn.rollback()
            logs.append(f"forecast ERR: {e}")

        # Polymarket 市場賠率
        if poly_city:
            try:
                result = fetch_poly_markets(poly_city["event_slug_city"], target_date)
                if result:
                    event, markets = result
                    if dry_run:
                        logs.append(f"[DRY] market {len(markets)} opts")
                    else:
                        upsert_market_options(conn, key, target_date, markets)
                        upsert_market_snapshot(conn, key, target_date,
                                               snapshot_t, hours_left, markets)
                        resolved = try_upsert_resolution(conn, key, target_date,
                                                         event, markets)
                        logs.append(
                            f"market {len(markets)} opts"
                            + (" [resolved]" if resolved else "")
                        )
                else:
                    logs.append("market: no data")
            except Exception as e:
                if conn:
                    conn.rollback()
                logs.append(f"market ERR: {e}")
    else:
        logs.append(f"outside window ({hours_left:+.1f}h)")

    # ── 補抓昨日 actuals（若 DB 尚無完整紀錄）──────────────────────────────
    yesterday = target_date - timedelta(days=1)
    try:
        need_actuals = True
        if not dry_run:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM weather_actuals_hourly "
                    "WHERE location_key=%s AND obs_date=%s",
                    (key, yesterday),
                )
                need_actuals = cur.fetchone()[0] < 20  # < 20 筆視為不完整

        if need_actuals:
            date_str    = yesterday.strftime("%Y-%m-%d")
            history_url = f"https://www.wunderground.com/history/daily/{key}/date/{date_str}"
            html        = await fetch_page(browser, history_url)
            actuals     = parse_history_full(html)
            if dry_run:
                logs.append(f"[DRY] actuals-yesterday {len(actuals)} pts")
            else:
                upsert_actuals(conn, key, yesterday, actuals)
                upsert_daily_summary(conn, key, yesterday, actuals)
                logs.append(f"actuals-yesterday {len(actuals)} pts")
    except Exception as e:
        if conn:
            conn.rollback()
        logs.append(f"actuals ERR: {e}")

    return logs


# ── 主執行 ───────────────────────────────────────────────────────────────────

async def main(dry_run: bool = False):
    run_start = datetime.now(timezone.utc)
    print(f"\n{'='*60}")
    print(f"  collector.py  {run_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if dry_run:
        print("  ** DRY RUN — 不寫入資料庫 **")
    print(f"{'='*60}")

    cities = json.loads((_PROJECT_ROOT / "config" / "cities.json").read_text(encoding="utf-8"))
    poly_cities: dict[str, dict] = {}
    poly_path = _PROJECT_ROOT / "config" / "polymarket_cities.json"
    if poly_path.exists():
        for c in json.loads(poly_path.read_text(encoding="utf-8")):
            poly_cities[c["location_key"]] = c

    snapshot_t = run_start
    conn       = None if dry_run else get_conn()

    # 確保 cities 表有所有城市（FK constraint 的前提）
    if conn:
        upsert_cities_table(conn, cities, poly_cities)

    semaphore  = asyncio.Semaphore(MAX_CONCURRENT_CITIES)

    city_ok, city_fail = 0, 0
    all_errors: list[dict] = []

    async def do_one(browser, city):
        nonlocal city_ok, city_fail
        async with semaphore:
            key = city["location_key"]
            try:
                logs = await collect_city(
                    browser, conn, city,
                    poly_cities.get(key), snapshot_t, dry_run,
                )
                print(f"  {city['name']:<22} {' | '.join(logs)}")
                city_ok += 1
            except Exception as e:
                msg = f"{city['name']}: {e}"
                print(f"  [ERR] {msg}")
                all_errors.append({"city": city["name"], "error": str(e)})
                city_fail += 1

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        await asyncio.gather(*[do_one(browser, c) for c in cities])
        await browser.close()

    duration = (datetime.now(timezone.utc) - run_start).total_seconds()
    print(f"\n  完成：成功 {city_ok}，失敗 {city_fail}，耗時 {duration:.0f}s\n")

    if conn and not dry_run:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO collection_log
                        (run_time, run_type, cities_attempted,
                         cities_ok, cities_failed, errors, duration_sec)
                    VALUES (%s, 'hourly', %s, %s, %s, %s, %s)
                """, (
                    run_start, len(cities), city_ok, city_fail,
                    json.dumps(all_errors) if all_errors else None,
                    duration,
                ))
            conn.commit()
        except Exception as e:
            print(f"  [WARN] collection_log 寫入失敗: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    asyncio.run(main(dry_run=dry))
