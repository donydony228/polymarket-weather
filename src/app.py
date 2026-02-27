"""
Weather Dashboard — Streamlit UI
資料來源: Weather Underground (cities.json) + Polymarket
"""

import asyncio
import json
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent  # src/ -> project root

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from playwright.async_api import async_playwright

from weather_scraper import (
    MAX_CONCURRENT_CITIES,
    f_to_c,
    merge_data,
    scrape_city,
    stats,
)


# ── 時區排序（UTC offset，越大 = 越早）────────────────────────────────────────

TIMEZONE_UTC_OFFSET: dict[str, float] = {
    "nz/wellington/NZWN":        13,
    "kr/incheon/RKSI":            9,
    "tr/çubuk/LTAC":              3,
    "fr/paris/LFPG":              1,
    "gb/london/EGLC":             0,
    "ar/ezeiza/SAEZ":            -3,
    "br/guarulhos/SBGR":         -3,
    "us/ny/new-york-city/KLGA":  -5,
    "ca/mississauga/CYYZ":       -5,
    "us/fl/miami/KMIA":          -5,
    "us/ga/atlanta/KATL":        -5,
    "us/tx/dallas/KDAL":         -6,
    "us/il/chicago/KORD":        -6,
    "us/wa/seatac/KSEA":         -8,
}

# ── 城市座標（用於地圖）──────────────────────────────────────────────────────

CITY_COORDS: dict[str, tuple[float, float]] = {
    "us/wa/seatac/KSEA":        (47.45,  -122.31),
    "us/il/chicago/KORD":       (41.97,   -87.91),
    "kr/incheon/RKSI":          (37.46,   126.44),
    "ca/mississauga/CYYZ":      (43.68,   -79.62),
    "br/guarulhos/SBGR":        (-23.44,  -46.47),
    "us/fl/miami/KMIA":         (25.80,   -80.29),
    "us/ga/atlanta/KATL":       (33.64,   -84.43),
    "fr/paris/LFPG":            (49.01,     2.55),
    "tr/çubuk/LTAC":            (40.13,    32.99),
    "us/tx/dallas/KDAL":        (32.85,   -96.85),
    "ar/ezeiza/SAEZ":           (-34.82,  -58.54),
    "nz/wellington/NZWN":       (-41.33,  174.81),
    "us/ny/new-york-city/KLGA": (40.78,   -73.87),
    "gb/london/EGLC":           (51.51,     0.06),
}

# ── Polymarket API ────────────────────────────────────────────────────────────

_GAMMA_API   = "https://gamma-api.polymarket.com"
_CLOB_API    = "https://clob.polymarket.com"
_API_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


# ── WU 爬取（含快取）──────────────────────────────────────────────────────────

async def _async_fetch_all(cities: list[dict]) -> list[list[dict] | None]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CITIES)
    results: list[list[dict] | None] = [None] * len(cities)

    async def one(browser, idx: int, city: dict):
        async with semaphore:
            try:
                history, forecast = await scrape_city(browser, city["location_key"])
                results[idx] = merge_data(history, forecast)
            except Exception as e:
                results[idx] = None
                print(f"[ERROR] {city['name']}: {e}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        await asyncio.gather(*[one(browser, i, c) for i, c in enumerate(cities)])
        await browser.close()

    return results


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_all(cities_raw: str) -> list[list[dict] | None]:
    """cities_raw 作為 cache key，cities.json 變更即自動失效"""
    return asyncio.run(_async_fetch_all(json.loads(cities_raw)))


async def _async_fetch_one(location_key: str) -> list[dict] | None:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            history, forecast = await scrape_city(browser, location_key)
            return merge_data(history, forecast)
        except Exception as e:
            print(f"[ERROR] {location_key}: {e}")
            return None
        finally:
            await browser.close()


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_one_city(location_key: str, cache_ver: int) -> list[dict] | None:
    """單城市爬取；cache_ver 遞增即可強制失效。"""
    return asyncio.run(_async_fetch_one(location_key))


# ── Polymarket 資料（含快取）──────────────────────────────────────────────────

def _http_get(url: str) -> dict | list:
    req = urllib.request.Request(url, headers=_API_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_poly_city(event_slug_city: str, today_str: str) -> dict | None:
    """
    取得今日某城市的 Polymarket 市場資料。
    Cache key: (event_slug_city, today_str) — 跨日自動失效。
    """
    try:
        date  = datetime.strptime(today_str, "%Y-%m-%d")
        month = date.strftime("%B").lower()
        day   = date.day
        year  = date.year
        event_slug = (
            f"highest-temperature-in-{event_slug_city}"
            f"-on-{month}-{day}-{year}"
        )

        events = _http_get(f"{_GAMMA_API}/events?slug={event_slug}&limit=1")
        if not events:
            return None
        event   = events[0]
        markets = event.get("markets", [])
        if not markets:
            return None

        def get_mid(m):
            ids = m.get("clobTokenIds", "[]")
            if isinstance(ids, str):
                ids = json.loads(ids)
            if not ids:
                return None
            try:
                r = _http_get(f"{_CLOB_API}/midpoint?token_id={ids[0]}")
                return float(r.get("mid", 0))
            except Exception:
                return None

        mids = [None] * len(markets)
        with ThreadPoolExecutor(max_workers=min(10, len(markets))) as ex:
            futs = {ex.submit(get_mid, m): i for i, m in enumerate(markets)}
            for fut in as_completed(futs):
                mids[futs[fut]] = fut.result()

        enriched = []
        for m, mid in zip(markets, mids):
            op = m.get("outcomePrices", "[]")
            if isinstance(op, str):
                op = json.loads(op)
            gamma_yes = float(op[0]) if op else None
            enriched.append({**m, "yes_prob": mid if mid is not None else gamma_yes})

        return {
            "event_slug": event_slug,
            "closed":     event.get("closed", False),
            "volume":     event.get("volume", 0),
            "markets":    enriched,
            "fetched_at": datetime.now().strftime("%H:%M"),
        }
    except Exception:
        return None


# ── 溫度區間解析 ──────────────────────────────────────────────────────────────

def _parse_bucket(title: str) -> tuple[float | None, float | None, str]:
    """
    解析 Polymarket 選項標題的溫度區間。
    回傳 (low, high, unit)，None 代表無界。
    """
    t = title.strip()
    m = re.match(r'(-?\d+(?:\.\d+)?)°?([CF])\s+or\s+below', t, re.I)
    if m:
        return None, float(m.group(1)), m.group(2).upper()

    m = re.match(r'(-?\d+(?:\.\d+)?)°?([CF])\s+or\s+(?:higher|above)', t, re.I)
    if m:
        return float(m.group(1)), None, m.group(2).upper()

    m = re.match(r'(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)°?([CF])', t, re.I)
    if m:
        return float(m.group(1)), float(m.group(2)), m.group(3).upper()

    m = re.match(r'(-?\d+(?:\.\d+)?)°?([CF])', t, re.I)
    if m:
        v = float(m.group(1))
        return v, v, m.group(2).upper()

    return None, None, "?"


def _wu_in_bucket(wu_max_f: float, title: str) -> bool:
    """判斷 WU 最高溫是否落在此 Polymarket 溫度區間內。"""
    low, high, unit = _parse_bucket(title)
    if low is None and high is None:
        return False
    wu   = f_to_c(wu_max_f) if unit == "C" else wu_max_f
    wu_r = round(wu)
    if low is None:
        return wu_r <= high
    if high is None:
        return wu_r >= low
    return low <= wu_r <= high


# ── 工具 ─────────────────────────────────────────────────────────────────────

def city_local_date(location_key: str) -> str:
    """依城市 UTC offset 推算該城市的當地日期。"""
    from datetime import timezone, timedelta
    offset = TIMEZONE_UTC_OFFSET.get(location_key, 0)
    tz = timezone(timedelta(hours=offset))
    return datetime.now(tz).strftime("%Y-%m-%d")


def disp_temp(val_f: float | None, celsius: bool) -> str:
    if val_f is None:
        return "—"
    v    = f_to_c(val_f) if celsius else val_f
    unit = "°C" if celsius else "°F"
    return f"{v:.1f} {unit}"


def sort_by_timezone(cities: list[dict]) -> list[tuple[int, dict]]:
    """回傳 (原始索引, city) 清單，依時區由早到晚（UTC offset 由大到小）排序"""
    def tz_key(item):
        _, city = item
        offset = TIMEZONE_UTC_OFFSET.get(city["location_key"], 0)
        return (-offset, city["name"])
    return sorted(enumerate(cities), key=tz_key)


# ── 總覽頁 ───────────────────────────────────────────────────────────────────

def make_overview_map(cities: list[dict], all_results: list) -> go.Figure:
    lats, lons, names, hover_texts, max_temps_c = [], [], [], [], []

    for city, merged in zip(cities, all_results):
        key = city["location_key"]
        lat, lon = CITY_COORDS.get(key, (None, None))
        if lat is None or merged is None:
            continue

        all_f   = [d["temp_f"] for d in merged]
        max_f   = max(all_f) if all_f else None
        min_f   = min(all_f) if all_f else None
        celsius = city.get("celsius", False)

        lats.append(lat)
        lons.append(lon)
        names.append(city["name"])
        max_temps_c.append(f_to_c(max_f) if max_f is not None else 0)
        hover_texts.append(
            f"<b>{city['name']}</b><br>"
            f"最高：{disp_temp(max_f, celsius)}<br>"
            f"最低：{disp_temp(min_f, celsius)}"
        )

    fig = go.Figure(go.Scattergeo(
        lat          = lats,
        lon          = lons,
        text         = names,
        hovertext    = hover_texts,
        hoverinfo    = "text",
        mode         = "markers+text",
        textposition = "top center",
        textfont     = dict(size=11, color="#333333"),
        marker = dict(
            size       = 16,
            color      = max_temps_c,
            colorscale = "RdYlBu_r",
            cmin       = -10,
            cmax       = 40,
            colorbar   = dict(
                title      = "最高溫 (°C)",
                thickness  = 14,
                len        = 0.6,
                ticksuffix = "°C",
            ),
            line    = dict(color="white", width=1.5),
            opacity = 0.9,
        ),
    ))

    fig.update_layout(
        height        = 460,
        margin        = dict(l=0, r=0, t=0, b=0),
        paper_bgcolor = "white",
        geo = dict(
            projection_type = "natural earth",
            showland        = True,
            landcolor       = "#E8E8E8",
            showocean       = True,
            oceancolor      = "#C8DCF0",
            showcoastlines  = True,
            coastlinecolor  = "#888888",
            showframe       = False,
            bgcolor         = "white",
            lakecolor       = "#C8DCF0",
            showcountries   = True,
            countrycolor    = "#BBBBBB",
        ),
    )
    return fig


def render_overview(
    cities:       list[dict],
    sorted_pairs: list[tuple[int, dict]],
    all_results:  list,
    poly_cities:  dict[str, dict],
):
    st.subheader("當日各城市氣溫總覽")
    st.caption(
        f"擷取時間：{datetime.now().strftime('%Y/%m/%d %H:%M')}"
        f"　·　共 {len(cities)} 個城市"
    )

    # 並行預取所有城市的 Polymarket 資料（填滿快取，之後格子渲染直接命中）
    if poly_cities:
        def _prefetch_one(pair):
            _, city = pair
            info = poly_cities.get(city["location_key"])
            if info:
                try:
                    fetch_poly_city(
                        info["event_slug_city"],
                        city_local_date(city["location_key"]),
                    )
                except Exception:
                    pass

        with st.spinner("載入市場對比資料..."):
            with ThreadPoolExecutor(max_workers=8) as ex:
                list(ex.map(_prefetch_one, sorted_pairs))

    # ── 預報 vs 市場對比卡片 ──────────────────────────────────────────────────
    st.markdown("#### 天氣預報 vs 市場預測")
    st.caption("依時區由早到晚排列　·　顯示 WU 預報最高溫是否與市場共識一致")

    COLS = 2
    rows = [sorted_pairs[i:i+COLS] for i in range(0, len(sorted_pairs), COLS)]

    for row in rows:
        cols = st.columns(COLS)
        for col, (orig_idx, city) in zip(cols, row):
            merged    = all_results[orig_idx] if orig_idx < len(all_results) else None
            celsius   = city.get("celsius", False)
            poly_info = poly_cities.get(city["location_key"])
            tz_offset = TIMEZONE_UTC_OFFSET.get(city["location_key"], "?")
            tz_label  = f"UTC{tz_offset:+g}" if isinstance(tz_offset, (int, float)) else ""

            with col:
                label = f"{city['name']}  `{tz_label}`"

                if merged is None:
                    st.metric(label=label, value="—", delta="無資料",
                              delta_color="off")
                    continue

                all_f     = [d["temp_f"] for d in merged]
                all_max_f = max(all_f) if all_f else None
                wu_str    = disp_temp(all_max_f, celsius)

                poly_data = None
                if poly_info and all_max_f is not None:
                    poly_data = fetch_poly_city(
                        poly_info["event_slug_city"],
                        city_local_date(city["location_key"]),
                    )

                if poly_data and poly_data.get("markets"):
                    markets    = poly_data["markets"]
                    market_top = max(markets, key=lambda m: m.get("yes_prob") or 0)
                    top_title  = market_top.get("groupItemTitle", "—")
                    top_pct    = f'{(market_top.get("yes_prob") or 0)*100:.0f}%'
                    is_aligned = _wu_in_bucket(all_max_f, top_title)
                    delta_str  = f"{'一致' if is_aligned else '不符'} | WU: {wu_str}"
                    st.metric(
                        label       = label,
                        value       = f"{top_title} ({top_pct})",
                        delta       = delta_str,
                        delta_color = "normal" if is_aligned else "inverse",
                        help        = f"市場共識：{top_title} {top_pct}　·　WU 今日最高：{wu_str}",
                    )
                else:
                    # 無市場資料，回退到顯示氣溫高低
                    min_f = min(all_f) if all_f else None
                    st.metric(
                        label       = label,
                        value       = f"▲ {wu_str}",
                        delta       = f"▼ {disp_temp(min_f, celsius)}",
                        delta_color = "off",
                        help        = "當日最高 / 最低（實際 + 預報綜合）",
                    )

    st.divider()

    # ── 世界地圖（置底）──────────────────────────────────────────────────────
    st.markdown("#### 世界各城市氣溫地圖")
    st.plotly_chart(make_overview_map(cities, all_results), width="stretch")


# ── 賠率橫向分布圖（Yes + No 堆疊）──────────────────────────────────────────

def make_odds_chart(markets: list[dict], wu_max_f: float | None) -> go.Figure:
    labels     = []
    yes_probs  = []
    no_probs   = []
    yes_colors = []

    for m in markets:
        title = m.get("groupItemTitle", "")
        yes_p = round((m.get("yes_prob") or 0) * 100, 1)
        no_p  = round(100 - yes_p, 1)
        in_wu = wu_max_f is not None and _wu_in_bucket(wu_max_f, title)

        labels.append(title)
        yes_probs.append(yes_p)
        no_probs.append(no_p)
        yes_colors.append("#1E88E5" if in_wu else "#90CAF9")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name             = "Yes",
        x                = yes_probs,
        y                = labels,
        orientation      = "h",
        marker_color     = yes_colors,
        text             = [f"{p:.0f}%" for p in yes_probs],
        textposition     = "inside",
        insidetextanchor = "middle",
        textfont         = dict(color="#0D47A1", size=11),
        hovertemplate    = "<b>%{y}</b><br>Yes: %{x:.1f}%<extra></extra>",
    ))

    fig.add_trace(go.Bar(
        name             = "No",
        x                = no_probs,
        y                = labels,
        orientation      = "h",
        marker_color     = "#FFE082",
        text             = [f"{p:.0f}%" for p in no_probs],
        textposition     = "inside",
        insidetextanchor = "middle",
        textfont         = dict(color="#E65100", size=11),
        hovertemplate    = "<b>%{y}</b><br>No: %{x:.1f}%<extra></extra>",
    ))

    fig.update_layout(
        barmode       = "stack",
        height        = max(240, len(markets) * 40),
        margin        = dict(l=0, r=20, t=8, b=0),
        plot_bgcolor  = "#FAFAFA",
        paper_bgcolor = "white",
        xaxis = dict(
            range          = [0, 100],
            showgrid       = False,
            showticklabels = False,
            zeroline       = False,
        ),
        yaxis = dict(
            autorange = "reversed",
            showgrid  = False,
            tickfont  = dict(size=12),
        ),
        legend = dict(
            orientation = "h",
            yanchor     = "bottom",
            y           = 1.02,
            xanchor     = "right",
            x           = 1,
        ),
    )
    return fig


# ── 單一城市頁面 ──────────────────────────────────────────────────────────────

def render_city(
    city:      dict,
    merged:    list[dict] | None,
    poly_info: dict | None,
):
    if merged is None:
        st.error(
            f"無法取得資料，請確認 location_key"
            f" `{city['location_key']}` 是否正確。"
        )
        st.divider()
        _refresh_button(city["location_key"])
        return

    celsius = city.get("celsius", False)
    unit    = "°C" if celsius else "°F"

    def disp(val_f):
        return disp_temp(val_f, celsius)

    actual_max_f,   actual_min_f   = stats(merged, "actual")
    forecast_max_f, forecast_min_f = stats(merged, "forecast")
    all_f     = [d["temp_f"] for d in merged]
    all_max_f = max(all_f) if all_f else None

    # ── 市場預測對比 ───────────────────────────────────────────────────────────
    if poly_info:
        st.subheader("市場預測對比")
        today_str = city_local_date(city["location_key"])

        with st.spinner("載入市場資料..."):
            poly_data = fetch_poly_city(poly_info["event_slug_city"], today_str)

        if poly_data is None:
            st.caption("Polymarket 今日市場資料暫時無法取得")
        else:
            closed  = poly_data["closed"]
            markets = poly_data["markets"]
            vol     = float(poly_data.get("volume") or 0)
            vol_str = f"${vol/1000:.1f}K" if vol >= 1000 else f"${vol:.0f}"
            status  = "已結算" if closed else "交易中"

            st.caption(
                f"來源：Polymarket　·　快取 5 分鐘　·　更新 {poly_data['fetched_at']}"
                f"　·　{status}　·　今日成交量 {vol_str}"
            )

            wu_bucket = next(
                (m for m in markets
                 if all_max_f is not None
                 and _wu_in_bucket(all_max_f, m.get("groupItemTitle", ""))),
                None,
            )
            market_top = max(
                markets, key=lambda m: m.get("yes_prob") or 0
            ) if markets else None

            col_chart, col_metrics = st.columns([3, 1])

            with col_chart:
                st.caption(
                    "深藍 Yes / 淺紅 No：WU 預報最高溫所在區間　·　灰 Yes / 淺紅 No：其他選項"
                    if all_max_f is not None
                    else "各溫度區間的 Yes / No 市場機率"
                )
                st.plotly_chart(
                    make_odds_chart(markets, all_max_f),
                    width="stretch",
                )

            with col_metrics:
                st.metric(
                    "WU 今日最高",
                    disp(all_max_f),
                    help="Weather Underground 今日最高溫（實際 + 預報綜合）",
                )
                if market_top:
                    top_title = market_top.get("groupItemTitle", "—")
                    top_pct   = f'{(market_top.get("yes_prob") or 0)*100:.1f}%'
                    st.metric("市場共識", top_title, delta=top_pct,
                              delta_color="off")
                if wu_bucket:
                    wu_pct     = f'{(wu_bucket.get("yes_prob") or 0)*100:.1f}%'
                    wu_title   = wu_bucket.get("groupItemTitle", "—")
                    is_aligned = (
                        market_top is not None
                        and wu_bucket.get("id") == market_top.get("id")
                    )
                    align_note = "與市場共識一致" if is_aligned else "與市場共識不同"
                    st.metric(
                        "WU 區間機率",
                        wu_pct,
                        delta=align_note,
                        delta_color="normal" if is_aligned else "inverse",
                        help=f"市場對「{wu_title}」的機率",
                    )

        st.divider()

    # ── 逐時氣溫折線圖 ────────────────────────────────────────────────────────
    actual_cnt   = sum(1 for d in merged if d["source"] == "actual")
    forecast_cnt = sum(1 for d in merged if d["source"] == "forecast")
    st.subheader("逐時氣溫")
    st.caption(
        f"實際紀錄 {actual_cnt} 筆（藍色實線）"
        f"　·　預報 {forecast_cnt} 筆（橘色虛線）"
        f"　·　紅色三角：最高點　·　藍色三角：最低點"
    )
    st.plotly_chart(make_chart(city, merged), width="stretch")

    st.divider()

    # ── 溫度統計 ──────────────────────────────────────────────────────────────
    st.subheader("溫度統計")
    col_a1, col_a2, col_sep, col_f1, col_f2, col_sep2, col_o1, col_o2 = st.columns(
        [1, 1, 0.15, 1, 1, 0.15, 1, 1]
    )
    with col_a1:
        st.metric("實際最高", disp(actual_max_f),   help="今日已觀測到的最高氣溫")
    with col_a2:
        st.metric("實際最低", disp(actual_min_f),   help="今日已觀測到的最低氣溫")
    with col_sep:
        st.markdown(
            "<div style='border-left:1px solid #ddd;height:80px;margin:auto'></div>",
            unsafe_allow_html=True,
        )
    with col_f1:
        st.metric("預報最高", disp(forecast_max_f), help="今日預報的最高氣溫")
    with col_f2:
        st.metric("預報最低", disp(forecast_min_f), help="今日預報的最低氣溫")
    with col_sep2:
        st.markdown(
            "<div style='border-left:1px solid #ddd;height:80px;margin:auto'></div>",
            unsafe_allow_html=True,
        )
    with col_o1:
        st.metric("當日最高（綜合）", disp(all_max_f),
                  help="實際 + 預報合併後的最高氣溫")
    with col_o2:
        st.metric("當日最低（綜合）", disp(min(all_f) if all_f else None),
                  help="實際 + 預報合併後的最低氣溫")

    # ── 原始資料表 ────────────────────────────────────────────────────────────
    with st.expander("查看原始資料表"):
        rows = []
        for d in merged:
            t = f_to_c(d["temp_f"]) if celsius else d["temp_f"]
            rows.append({
                "時間": d["time"],
                f"溫度 ({unit})": round(t, 1),
                "來源": "實際紀錄" if d["source"] == "actual" else "預報",
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    st.divider()
    _refresh_button(city["location_key"])


# ── 折線圖（含最高 / 最低標記）───────────────────────────────────────────────

def make_chart(city: dict, merged: list[dict]) -> go.Figure:
    celsius = city.get("celsius", False)
    unit    = "°C" if celsius else "°F"

    def to_display(pts):
        return [
            round(f_to_c(d["temp_f"]), 1) if celsius else d["temp_f"]
            for d in pts
        ]

    def to_times(pts):
        return [d["time"] for d in pts]

    actual_pts   = [d for d in merged if d["source"] == "actual"]
    forecast_pts = [d for d in merged if d["source"] == "forecast"]

    fig = go.Figure()

    if actual_pts:
        fig.add_trace(go.Scatter(
            x    = to_times(actual_pts),
            y    = to_display(actual_pts),
            name = "實際紀錄",
            mode = "lines+markers",
            line   = dict(color="#1565C0", width=2.5, dash="solid"),
            marker = dict(size=7, symbol="circle", color="#1565C0"),
            hovertemplate = "%{x}<br><b>%{y}" + unit + "</b><extra>實際紀錄</extra>",
        ))

    if forecast_pts:
        connect = actual_pts[-1:] + forecast_pts if actual_pts else forecast_pts
        fig.add_trace(go.Scatter(
            x    = to_times(connect),
            y    = to_display(connect),
            name = "預報",
            mode = "lines+markers",
            line   = dict(color="#E65100", width=2.5, dash="dash"),
            marker = dict(size=7, symbol="diamond", color="#E65100"),
            hovertemplate = "%{x}<br><b>%{y}" + unit + "</b><extra>預報</extra>",
        ))

    if actual_pts and forecast_pts:
        all_y   = to_display(merged)
        y_range = (min(all_y) - 2, max(all_y) + 2)
        bx      = actual_pts[-1]["time"]
        fig.add_shape(
            type="line", x0=bx, x1=bx, y0=y_range[0], y1=y_range[1],
            line=dict(color="#9E9E9E", width=1.5, dash="dot"),
        )
        fig.add_annotation(
            x=bx, y=y_range[1], text="　現在　", showarrow=False,
            font=dict(size=11, color="#757575"),
            bgcolor="white", bordercolor="#9E9E9E", borderwidth=1,
            yanchor="top",
        )

    # ── 最高 / 最低標記 ────────────────────────────────────────────────────────
    if merged:
        all_disp  = to_display(merged)
        all_times = to_times(merged)
        max_val   = max(all_disp)
        min_val   = min(all_disp)
        max_idx   = all_disp.index(max_val)
        min_idx   = all_disp.index(min_val)

        # 最高點
        fig.add_trace(go.Scatter(
            x          = [all_times[max_idx]],
            y          = [max_val],
            mode       = "markers",
            marker     = dict(size=14, color="#D32F2F", symbol="triangle-up"),
            showlegend = False,
            hovertemplate = f"最高點: %{{y}}{unit}<extra></extra>",
        ))
        fig.add_annotation(
            x           = all_times[max_idx],
            y           = max_val,
            text        = f"▲ {max_val}{unit}",
            showarrow   = True,
            arrowhead   = 2,
            arrowcolor  = "#D32F2F",
            font        = dict(size=11, color="#D32F2F"),
            bgcolor     = "rgba(255,255,255,0.85)",
            bordercolor = "#D32F2F",
            borderwidth = 1,
            ay          = -35,
        )

        # 最低點（若與最高點不同）
        if min_idx != max_idx:
            fig.add_trace(go.Scatter(
                x          = [all_times[min_idx]],
                y          = [min_val],
                mode       = "markers",
                marker     = dict(size=14, color="#1565C0", symbol="triangle-down"),
                showlegend = False,
                hovertemplate = f"最低點: %{{y}}{unit}<extra></extra>",
            ))
            fig.add_annotation(
                x           = all_times[min_idx],
                y           = min_val,
                text        = f"▼ {min_val}{unit}",
                showarrow   = True,
                arrowhead   = 2,
                arrowcolor  = "#1565C0",
                font        = dict(size=11, color="#1565C0"),
                bgcolor     = "rgba(255,255,255,0.85)",
                bordercolor = "#1565C0",
                borderwidth = 1,
                ay          = 35,
            )

    fig.update_layout(
        height        = 400,
        margin        = dict(l=0, r=10, t=50, b=0),
        plot_bgcolor  = "#FAFAFA",
        paper_bgcolor = "white",
        xaxis = dict(
            title      = "時間",
            showgrid   = True,
            gridcolor  = "#EEEEEE",
            tickangle  = -30,
            showline   = True,
            linecolor  = "#BDBDBD",
        ),
        yaxis = dict(
            title     = f"溫度 ({unit})",
            showgrid  = True,
            gridcolor = "#EEEEEE",
            showline  = True,
            linecolor = "#BDBDBD",
        ),
        legend = dict(
            orientation = "h",
            yanchor     = "bottom",
            y           = 1.02,
            xanchor     = "right",
            x           = 1,
            bgcolor     = "rgba(255,255,255,0.8)",
        ),
        hovermode = "x unified",
    )
    return fig


# ── 重刷按鈕（單城市）────────────────────────────────────────────────────────

def _refresh_button(location_key: str):
    col, _ = st.columns([1, 3])
    with col:
        if st.button(
            "重新爬取此城市",
            key=f"refresh_{location_key}",
            width="stretch",
        ):
            vers = st.session_state.setdefault("city_ver", {})
            vers[location_key] = vers.get(location_key, 0) + 1
            st.rerun()


# ── 主程式 ───────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title = "Weather Dashboard",
        layout     = "wide",
    )

    st.title("Weather Dashboard")
    st.caption(
        f"資料來源：[Weather Underground](https://www.wunderground.com)"
        f" + [Polymarket](https://polymarket.com)"
        f"　·　更新日期：{datetime.now().strftime('%Y/%m/%d %H:%M')}"
    )

    cities_path = _PROJECT_ROOT / "config" / "cities.json"
    if not cities_path.exists():
        st.error("找不到 `config/cities.json`，請確認檔案存在。")
        st.stop()

    cities_raw = cities_path.read_text(encoding="utf-8")
    cities     = json.loads(cities_raw)

    # 載入 Polymarket 城市對應表
    poly_cities: dict[str, dict] = {}
    poly_path = _PROJECT_ROOT / "config" / "polymarket_cities.json"
    if poly_path.exists():
        for c in json.loads(poly_path.read_text(encoding="utf-8")):
            poly_cities[c["location_key"]] = c

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("設定")
        st.info(
            f"共載入 **{len(cities)}** 個城市\n\n"
            f"WU 資料快取 **30 分鐘**\n\n"
            f"市場資料快取 **5 分鐘**"
        )
        if st.button("強制重新爬取所有城市", width="stretch", type="primary"):
            fetch_all.clear()
            st.rerun()
        st.divider()
        st.markdown("**城市列表（依時區）**")
        for _, city in sort_by_timezone(cities):
            unit   = "°C" if city.get("celsius") else "°F"
            offset = TIMEZONE_UTC_OFFSET.get(city["location_key"], "?")
            tz_str = f"UTC{offset:+g}" if isinstance(offset, (int, float)) else ""
            st.markdown(f"- {city['name']}　`{unit}`　`{tz_str}`")
        st.divider()
        st.caption("修改 `config/cities.json` 即可增減城市或切換溫度單位")

    # ── WU 爬取 ───────────────────────────────────────────────────────────────
    with st.spinner(
        f"正在爬取 {len(cities)} 個城市的氣溫資料，"
        f"首次載入約需 1～2 分鐘..."
    ):
        all_results = fetch_all(cities_raw)

    while len(all_results) < len(cities):
        all_results.append(None)

    sorted_pairs = sort_by_timezone(cities)

    # ── 分頁 ──────────────────────────────────────────────────────────────────
    tab_labels = ["總覽"] + [city["name"] for _, city in sorted_pairs]
    tabs       = st.tabs(tab_labels)

    with tabs[0]:
        render_overview(cities, sorted_pairs, all_results, poly_cities)

    for tab, (orig_idx, city) in zip(tabs[1:], sorted_pairs):
        with tab:
            vers     = st.session_state.get("city_ver", {})
            city_ver = vers.get(city["location_key"], 0)
            if city_ver > 0:
                with st.spinner(f"重新爬取 {city['name']} 中..."):
                    merged = fetch_one_city(city["location_key"], city_ver)
            else:
                merged = all_results[orig_idx] if orig_idx < len(all_results) else None
            poly_info = poly_cities.get(city["location_key"])
            render_city(city, merged, poly_info)


if __name__ == "__main__":
    main()
