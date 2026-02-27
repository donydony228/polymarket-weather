"""
Weather Dashboard — Streamlit UI
資料來源: Supabase（由 GitHub Actions 每小時自動收集）
"""

import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent

load_dotenv(_PROJECT_ROOT / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    try:
        DATABASE_URL = st.secrets["DATABASE_URL"]
    except Exception:
        pass


# ── 常數 ──────────────────────────────────────────────────────────────────────

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


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def f_to_c(f: float) -> float:
    return (f - 32) * 5 / 9


def _hour_label(h: int) -> str:
    if h == 0:  return "12:00 AM"
    if h < 12:  return f"{h}:00 AM"
    if h == 12: return "12:00 PM"
    return f"{h - 12}:00 PM"


def disp_temp(val_f: float | None, celsius: bool) -> str:
    if val_f is None:
        return "—"
    v    = f_to_c(val_f) if celsius else val_f
    unit = "°C" if celsius else "°F"
    return f"{v:.1f} {unit}"


def sort_by_timezone(cities: list[dict]) -> list[tuple[int, dict]]:
    def tz_key(item):
        _, city = item
        offset = TIMEZONE_UTC_OFFSET.get(city["location_key"], 0)
        return (-offset, city["name"])
    return sorted(enumerate(cities), key=tz_key)


def _parse_bucket(title: str) -> tuple[float | None, float | None, str]:
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
    low, high, unit = _parse_bucket(title)
    if low is None and high is None:
        return False
    wu   = f_to_c(wu_max_f) if unit == "C" else wu_max_f
    wu_r = round(wu)
    if low is None:  return wu_r <= high
    if high is None: return wu_r >= low
    return low <= wu_r <= high


# ── DB 查詢（5 分鐘快取）─────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_all_data() -> dict:
    """一次查詢所有需要的資料，5 分鐘自動失效。"""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # 1. 城市清單
            cur.execute("""
                SELECT location_key, name, celsius, timezone_offset, event_slug_city
                FROM cities
                ORDER BY timezone_offset DESC, name
            """)
            cities = [dict(r) for r in cur.fetchall()]

            # 2. 最新預報（每城市最新一次 snapshot 的逐時曲線）
            cur.execute("""
                WITH latest AS (
                    SELECT location_key, MAX(snapshot_time) AS snap_t
                    FROM forecast_hourly_snapshots
                    WHERE target_date >= CURRENT_DATE - INTERVAL '2 days'
                    GROUP BY location_key
                )
                SELECT f.location_key, f.target_date, f.snapshot_time,
                       f.forecast_hour, f.temp_f
                FROM forecast_hourly_snapshots f
                JOIN latest l ON f.location_key = l.location_key
                              AND f.snapshot_time = l.snap_t
                ORDER BY f.location_key, f.forecast_hour
            """)
            forecast_rows = [dict(r) for r in cur.fetchall()]

            # 3. 最新市場快照（每城市）
            cur.execute("""
                WITH latest AS (
                    SELECT location_key, MAX(snapshot_time) AS snap_t
                    FROM market_snapshots
                    WHERE market_date >= CURRENT_DATE - INTERVAL '2 days'
                    GROUP BY location_key
                )
                SELECT ms.location_key, ms.market_date, ms.snapshot_time,
                       ms.option_label, ms.yes_prob, ms.no_prob,
                       ms.spread, ms.volume_usdc
                FROM market_snapshots ms
                JOIN latest l ON ms.location_key = l.location_key
                              AND ms.snapshot_time = l.snap_t
                ORDER BY ms.location_key, COALESCE(ms.yes_prob, 0) DESC
            """)
            market_rows = [dict(r) for r in cur.fetchall()]

            # 4. 最後一次收集執行記錄
            cur.execute("""
                SELECT run_time, cities_ok, cities_failed, duration_sec
                FROM collection_log
                ORDER BY run_time DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            last_run = dict(row) if row else {}

    finally:
        conn.close()

    # 整理為以 location_key 為 key 的 dict
    forecasts: dict[str, dict] = {}
    for r in forecast_rows:
        key = r["location_key"]
        if key not in forecasts:
            forecasts[key] = {
                "snapshot_time": r["snapshot_time"],
                "target_date":   r["target_date"],
                "merged": [],
            }
        if r["temp_f"] is not None:
            forecasts[key]["merged"].append({
                "hour":   r["forecast_hour"],
                "time":   _hour_label(r["forecast_hour"]),
                "temp_f": r["temp_f"],
                "source": "forecast",
            })

    markets: dict[str, dict] = {}
    for r in market_rows:
        key = r["location_key"]
        if key not in markets:
            markets[key] = {
                "snapshot_time": r["snapshot_time"],
                "market_date":   r["market_date"],
                "markets": [],
            }
        markets[key]["markets"].append({
            "groupItemTitle": r["option_label"],
            "yes_prob":       r["yes_prob"],
            "no_prob":        r["no_prob"],
            "spread":         r["spread"],
            "volume_usdc":    r["volume_usdc"],
        })

    return {
        "cities":    cities,
        "forecasts": forecasts,
        "markets":   markets,
        "last_run":  last_run,
    }


# ── 圖表 ──────────────────────────────────────────────────────────────────────

def make_chart(city: dict, merged: list[dict]) -> go.Figure:
    celsius = city.get("celsius", False)
    unit    = "°C" if celsius else "°F"

    def to_display(pts):
        return [round(f_to_c(d["temp_f"]), 1) if celsius else d["temp_f"] for d in pts]

    def to_times(pts):
        return [d["time"] for d in pts]

    actual_pts   = [d for d in merged if d["source"] == "actual"]
    forecast_pts = [d for d in merged if d["source"] == "forecast"]

    fig = go.Figure()

    if actual_pts:
        fig.add_trace(go.Scatter(
            x=to_times(actual_pts), y=to_display(actual_pts),
            name="實際紀錄", mode="lines+markers",
            line=dict(color="#1565C0", width=2.5, dash="solid"),
            marker=dict(size=7, symbol="circle", color="#1565C0"),
            hovertemplate="%{x}<br><b>%{y}" + unit + "</b><extra>實際紀錄</extra>",
        ))

    if forecast_pts:
        connect = actual_pts[-1:] + forecast_pts if actual_pts else forecast_pts
        fig.add_trace(go.Scatter(
            x=to_times(connect), y=to_display(connect),
            name="預報", mode="lines+markers",
            line=dict(color="#E65100", width=2.5, dash="dash"),
            marker=dict(size=7, symbol="diamond", color="#E65100"),
            hovertemplate="%{x}<br><b>%{y}" + unit + "</b><extra>預報</extra>",
        ))

    if actual_pts and forecast_pts:
        all_y  = to_display(merged)
        y_rng  = (min(all_y) - 2, max(all_y) + 2)
        bx     = actual_pts[-1]["time"]
        fig.add_shape(type="line", x0=bx, x1=bx, y0=y_rng[0], y1=y_rng[1],
                      line=dict(color="#9E9E9E", width=1.5, dash="dot"))
        fig.add_annotation(x=bx, y=y_rng[1], text="　現在　", showarrow=False,
                           font=dict(size=11, color="#757575"),
                           bgcolor="white", bordercolor="#9E9E9E", borderwidth=1, yanchor="top")

    if merged:
        all_disp  = to_display(merged)
        all_times = to_times(merged)
        max_val   = max(all_disp)
        min_val   = min(all_disp)
        max_idx   = all_disp.index(max_val)
        min_idx   = all_disp.index(min_val)

        fig.add_trace(go.Scatter(x=[all_times[max_idx]], y=[max_val], mode="markers",
                                 marker=dict(size=14, color="#D32F2F", symbol="triangle-up"),
                                 showlegend=False,
                                 hovertemplate=f"最高點: %{{y}}{unit}<extra></extra>"))
        fig.add_annotation(x=all_times[max_idx], y=max_val,
                           text=f"▲ {max_val}{unit}", showarrow=True, arrowhead=2,
                           arrowcolor="#D32F2F", font=dict(size=11, color="#D32F2F"),
                           bgcolor="rgba(255,255,255,0.85)", bordercolor="#D32F2F",
                           borderwidth=1, ay=-35)

        if min_idx != max_idx:
            fig.add_trace(go.Scatter(x=[all_times[min_idx]], y=[min_val], mode="markers",
                                     marker=dict(size=14, color="#1565C0", symbol="triangle-down"),
                                     showlegend=False,
                                     hovertemplate=f"最低點: %{{y}}{unit}<extra></extra>"))
            fig.add_annotation(x=all_times[min_idx], y=min_val,
                               text=f"▼ {min_val}{unit}", showarrow=True, arrowhead=2,
                               arrowcolor="#1565C0", font=dict(size=11, color="#1565C0"),
                               bgcolor="rgba(255,255,255,0.85)", bordercolor="#1565C0",
                               borderwidth=1, ay=35)

    fig.update_layout(
        height=400, margin=dict(l=0, r=10, t=50, b=0),
        plot_bgcolor="#FAFAFA", paper_bgcolor="white",
        xaxis=dict(title="時間", showgrid=True, gridcolor="#EEEEEE",
                   tickangle=-30, showline=True, linecolor="#BDBDBD"),
        yaxis=dict(title=f"溫度 ({unit})", showgrid=True, gridcolor="#EEEEEE",
                   showline=True, linecolor="#BDBDBD"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, bgcolor="rgba(255,255,255,0.8)"),
        hovermode="x unified",
    )
    return fig


def make_odds_chart(markets: list[dict], wu_max_f: float | None) -> go.Figure:
    labels, yes_probs, no_probs, yes_colors = [], [], [], []
    for m in markets:
        title = m.get("groupItemTitle", "")
        yes_p = round((m.get("yes_prob") or 0) * 100, 1)
        in_wu = wu_max_f is not None and _wu_in_bucket(wu_max_f, title)
        labels.append(title)
        yes_probs.append(yes_p)
        no_probs.append(round(100 - yes_p, 1))
        yes_colors.append("#1E88E5" if in_wu else "#90CAF9")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Yes", x=yes_probs, y=labels, orientation="h",
        marker_color=yes_colors,
        text=[f"{p:.0f}%" for p in yes_probs],
        textposition="inside", insidetextanchor="middle",
        textfont=dict(color="#0D47A1", size=11),
        hovertemplate="<b>%{y}</b><br>Yes: %{x:.1f}%<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="No", x=no_probs, y=labels, orientation="h",
        marker_color="#FFE082",
        text=[f"{p:.0f}%" for p in no_probs],
        textposition="inside", insidetextanchor="middle",
        textfont=dict(color="#E65100", size=11),
        hovertemplate="<b>%{y}</b><br>No: %{x:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        barmode="stack", height=max(240, len(markets) * 40),
        margin=dict(l=0, r=20, t=8, b=0),
        plot_bgcolor="#FAFAFA", paper_bgcolor="white",
        xaxis=dict(range=[0, 100], showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(autorange="reversed", showgrid=False, tickfont=dict(size=12)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def make_overview_map(cities: list[dict], forecasts: dict) -> go.Figure:
    lats, lons, names, hover_texts, max_temps_c = [], [], [], [], []
    for city in cities:
        key       = city["location_key"]
        lat, lon  = CITY_COORDS.get(key, (None, None))
        if lat is None:
            continue
        merged = forecasts.get(key, {}).get("merged", [])
        if not merged:
            continue
        all_f   = [d["temp_f"] for d in merged]
        max_f   = max(all_f)
        min_f   = min(all_f)
        celsius = city.get("celsius", False)
        lats.append(lat)
        lons.append(lon)
        names.append(city["name"])
        max_temps_c.append(f_to_c(max_f))
        hover_texts.append(
            f"<b>{city['name']}</b><br>"
            f"預報最高：{disp_temp(max_f, celsius)}<br>"
            f"預報最低：{disp_temp(min_f, celsius)}"
        )

    fig = go.Figure(go.Scattergeo(
        lat=lats, lon=lons, text=names, hovertext=hover_texts, hoverinfo="text",
        mode="markers+text", textposition="top center",
        textfont=dict(size=11, color="#333333"),
        marker=dict(
            size=16, color=max_temps_c, colorscale="RdYlBu_r", cmin=-10, cmax=40,
            colorbar=dict(title="最高溫 (°C)", thickness=14, len=0.6, ticksuffix="°C"),
            line=dict(color="white", width=1.5), opacity=0.9,
        ),
    ))
    fig.update_layout(
        height=460, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="white",
        geo=dict(
            projection_type="natural earth",
            showland=True, landcolor="#E8E8E8",
            showocean=True, oceancolor="#C8DCF0",
            showcoastlines=True, coastlinecolor="#888888",
            showframe=False, bgcolor="white",
            lakecolor="#C8DCF0", showcountries=True, countrycolor="#BBBBBB",
        ),
    )
    return fig


# ── 頁面渲染 ──────────────────────────────────────────────────────────────────

def render_overview(cities: list[dict], sorted_pairs: list, forecasts: dict, markets: dict):
    st.subheader("當日各城市氣溫總覽")
    st.caption("天氣預報 vs 市場預測　·　依時區由早到晚排列")

    COLS = 2
    rows = [sorted_pairs[i:i+COLS] for i in range(0, len(sorted_pairs), COLS)]

    for row in rows:
        cols = st.columns(COLS)
        for col, (_, city) in zip(cols, row):
            key      = city["location_key"]
            celsius  = city.get("celsius", False)
            offset   = TIMEZONE_UTC_OFFSET.get(key, "?")
            tz_label = f"UTC{offset:+g}" if isinstance(offset, (int, float)) else ""

            merged   = forecasts.get(key, {}).get("merged", [])
            mkt_list = markets.get(key, {}).get("markets", [])

            all_f     = [d["temp_f"] for d in merged]
            all_max_f = max(all_f) if all_f else None

            with col:
                label = f"{city['name']}  `{tz_label}`"

                if not merged:
                    st.metric(label=label, value="—", delta="無預報資料", delta_color="off")
                    continue

                if mkt_list:
                    market_top = mkt_list[0]  # 已按 yes_prob DESC 排序
                    top_title  = market_top.get("groupItemTitle", "—")
                    top_pct    = f'{(market_top.get("yes_prob") or 0)*100:.0f}%'
                    is_aligned = all_max_f is not None and _wu_in_bucket(all_max_f, top_title)
                    st.metric(
                        label=label,
                        value=f"{top_title} ({top_pct})",
                        delta=f"{'一致' if is_aligned else '不符'} | 預報: {disp_temp(all_max_f, celsius)}",
                        delta_color="normal" if is_aligned else "inverse",
                        help=f"市場共識：{top_title} {top_pct}　·　WU 預報最高：{disp_temp(all_max_f, celsius)}",
                    )
                else:
                    min_f = min(all_f) if all_f else None
                    st.metric(
                        label=label,
                        value=f"▲ {disp_temp(all_max_f, celsius)}",
                        delta=f"▼ {disp_temp(min_f, celsius)}",
                        delta_color="off",
                        help="預報最高 / 最低溫",
                    )

    st.divider()
    st.markdown("#### 世界各城市氣溫地圖")
    st.plotly_chart(make_overview_map(cities, forecasts), width="stretch")


def render_city(city: dict, fcast: dict, mkt: dict):
    celsius  = city.get("celsius", False)
    unit     = "°C" if celsius else "°F"
    merged   = fcast.get("merged", [])
    mkt_list = mkt.get("markets", [])

    if not merged:
        st.warning("此城市目前無預報資料（尚未進入 36 小時收集窗口）。")
        return

    all_f     = [d["temp_f"] for d in merged]
    all_max_f = max(all_f)
    all_min_f = min(all_f)

    snap_t   = fcast.get("snapshot_time")
    snap_str = snap_t.strftime("%Y/%m/%d %H:%M UTC") if snap_t else "未知"

    # ── 市場預測對比 ───────────────────────────────────────────────────────────
    if mkt_list:
        st.subheader("市場預測對比")

        mkt_snap_t   = mkt.get("snapshot_time")
        mkt_snap_str = mkt_snap_t.strftime("%Y/%m/%d %H:%M UTC") if mkt_snap_t else "未知"
        total_vol    = sum(m.get("volume_usdc") or 0 for m in mkt_list)
        vol_str      = f"${total_vol/1000:.1f}K" if total_vol >= 1000 else f"${total_vol:.0f}"
        st.caption(f"資料時間：{mkt_snap_str}　·　總成交量 {vol_str}")

        market_top = mkt_list[0]
        wu_bucket  = next(
            (m for m in mkt_list
             if _wu_in_bucket(all_max_f, m.get("groupItemTitle", ""))),
            None,
        )

        col_chart, col_metrics = st.columns([3, 1])
        with col_chart:
            st.caption("深藍 Yes：WU 預報最高溫所在區間　·　淺藍 Yes：其他選項")
            st.plotly_chart(make_odds_chart(mkt_list, all_max_f), width="stretch")
        with col_metrics:
            st.metric("預報最高", disp_temp(all_max_f, celsius),
                      help="WU 最新預報中的最高溫")
            if market_top:
                top_title = market_top.get("groupItemTitle", "—")
                top_pct   = f'{(market_top.get("yes_prob") or 0)*100:.1f}%'
                st.metric("市場共識", top_title, delta=top_pct, delta_color="off")
            if wu_bucket:
                wu_pct     = f'{(wu_bucket.get("yes_prob") or 0)*100:.1f}%'
                is_aligned = (
                    market_top is not None and
                    wu_bucket.get("groupItemTitle") == market_top.get("groupItemTitle")
                )
                st.metric(
                    "預報區間機率", wu_pct,
                    delta="與市場共識一致" if is_aligned else "與市場共識不同",
                    delta_color="normal" if is_aligned else "inverse",
                )
        st.divider()

    # ── 逐時預報折線圖 ────────────────────────────────────────────────────────
    st.subheader("逐時預報氣溫")
    st.caption(f"WU 預報 {len(merged)} 筆　·　資料時間：{snap_str}")
    st.plotly_chart(make_chart(city, merged), width="stretch")
    st.divider()

    # ── 溫度統計 ──────────────────────────────────────────────────────────────
    st.subheader("預報溫度統計")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("預報最高", disp_temp(all_max_f, celsius))
    with col2:
        st.metric("預報最低", disp_temp(all_min_f, celsius))

    with st.expander("查看原始資料表"):
        rows = []
        for d in merged:
            t = f_to_c(d["temp_f"]) if celsius else d["temp_f"]
            rows.append({"時間": d["time"], f"溫度 ({unit})": round(t, 1), "來源": "預報"})
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")


# ── 主程式 ───────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Weather Dashboard", layout="wide")
    st.title("Weather Dashboard")

    if not DATABASE_URL:
        st.error("DATABASE_URL 未設定。請在 `.env`（本機）或 Streamlit Secrets（雲端）中設定。")
        st.stop()

    with st.spinner("載入資料中..."):
        try:
            data = load_all_data()
        except Exception as e:
            st.error(f"無法連接資料庫：{e}")
            st.stop()

    cities    = data["cities"]
    forecasts = data["forecasts"]
    markets   = data["markets"]
    last_run  = data["last_run"]

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("設定")

        if last_run:
            run_t   = last_run.get("run_time")
            run_str = run_t.strftime("%Y/%m/%d %H:%M UTC") if run_t else "未知"
            ok      = last_run.get("cities_ok", 0)
            err     = last_run.get("cities_failed", 0)
            dur     = last_run.get("duration_sec", 0)
            st.info(
                f"最後收集：**{run_str}**\n\n"
                f"成功 **{ok}** 城市 / 失敗 **{err}** 城市\n\n"
                f"耗時 **{dur:.0f}s**"
            )
        else:
            st.info(f"共 **{len(cities)}** 個城市")

        if st.button("重新整理資料", type="primary", width="stretch"):
            load_all_data.clear()
            st.rerun()

        st.divider()
        st.markdown("**城市列表（依時區）**")
        for _, city in sort_by_timezone(cities):
            unit   = "°C" if city.get("celsius") else "°F"
            offset = TIMEZONE_UTC_OFFSET.get(city["location_key"], "?")
            tz_str = f"UTC{offset:+g}" if isinstance(offset, (int, float)) else ""
            st.markdown(f"- {city['name']}　`{unit}`　`{tz_str}`")

    # ── 分頁 ──────────────────────────────────────────────────────────────────
    sorted_pairs = sort_by_timezone(cities)
    tab_labels   = ["總覽"] + [city["name"] for _, city in sorted_pairs]
    tabs         = st.tabs(tab_labels)

    with tabs[0]:
        st.caption(
            f"資料來源：[Weather Underground](https://www.wunderground.com)"
            f" + [Polymarket](https://polymarket.com)"
            f"　·　每 5 分鐘自動刷新　·　收集器每小時執行"
        )
        render_overview(cities, sorted_pairs, forecasts, markets)

    for tab, (_, city) in zip(tabs[1:], sorted_pairs):
        with tab:
            key   = city["location_key"]
            fcast = forecasts.get(key, {})
            mkt   = markets.get(key, {})
            render_city(city, fcast, mkt)


if __name__ == "__main__":
    main()
