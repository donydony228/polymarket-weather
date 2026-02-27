"""
Weather Underground 當日 24 小時氣溫整合爬蟲（批次版）

用法:
    # 跑 cities.json 裡所有城市
    python weather_scraper.py

    # 指定其他設定檔
    python weather_scraper.py my_cities.json

    # 單一城市（向下相容）
    python weather_scraper.py us/il/schiller-park/KORD

cities.json 格式:
    [
      { "name": "Schiller Park, IL", "location_key": "us/il/schiller-park/KORD", "celsius": false },
      { "name": "London, UK",        "location_key": "gb/london/EGLC",           "celsius": true  }
    ]
"""

import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Browser
from bs4 import BeautifulSoup


BASE_HISTORY  = "https://www.wunderground.com/history/daily"
BASE_FORECAST = "https://www.wunderground.com/hourly"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 最多同時開幾個城市（每個城市佔 2 個 browser context）
MAX_CONCURRENT_CITIES = 3


# ── 溫度換算 ──────────────────────────────────────────────────────────────────

def f_to_c(f: float) -> float:
    return (f - 32) * 5 / 9


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def build_urls(location_key: str) -> tuple[str, str]:
    key = location_key.strip("/")
    return f"{BASE_HISTORY}/{key}", f"{BASE_FORECAST}/{key}"


def clean(text: str) -> str:
    text = re.sub(r"(\d)\°([a-zA-Z%])", r"\1 \2", text)
    text = re.sub(r"(\d)\s*(mph)([A-Z]+)", r"\1 \2 \3", text)
    return text.strip()


def extract_temp_f(text: str) -> float | None:
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[°]?\s*F", text, re.IGNORECASE)
    return float(m.group(1)) if m else None


def parse_time_to_hour(time_str: str) -> int | None:
    for fmt in ("%I:%M %p", "%I:%M%p"):
        try:
            return datetime.strptime(time_str.strip().upper(), fmt).hour
        except ValueError:
            pass
    return None


# ── 頁面爬取 ──────────────────────────────────────────────────────────────────

async def fetch_page(browser: Browser, url: str) -> str:
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1280, "height": 900},
        locale="en-US",
    )
    page = await context.new_page()
    try:
        # 用 domcontentloaded 取代 networkidle：
        # WU 的 SPA 背景請求（analytics/ads）會讓 networkidle 永遠不觸發，
        # 改用 domcontentloaded 後讓 wait_for_selector 等 Angular 表格渲染即可。
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_selector("table.mat-mdc-table", timeout=45000)
        await asyncio.sleep(3)
        return await page.content()
    finally:
        await context.close()


async def scrape_city(browser: Browser, location_key: str) -> tuple[list[dict], list[dict]]:
    history_url, forecast_url = build_urls(location_key)
    history_html, forecast_html = await asyncio.gather(
        fetch_page(browser, history_url),
        fetch_page(browser, forecast_url),
    )
    return parse_history(history_html), parse_forecast(forecast_html)


# ── 解析函式 ──────────────────────────────────────────────────────────────────

def parse_mat_table(html: str) -> tuple[list[str], list[list[str]]]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_="mat-mdc-table")
    if not table:
        return [], []
    rows = table.find_all("tr")
    if not rows:
        return [], []
    headers   = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    data_rows = [
        [clean(c.get_text(strip=True)) for c in row.find_all(["th", "td"])]
        for row in rows[1:]
    ]
    return headers, data_rows


def parse_history(html: str) -> list[dict]:
    headers, rows = parse_mat_table(html)
    results = []
    for row in rows:
        if len(row) < len(headers):
            continue
        entry = dict(zip(headers, row))
        hour  = parse_time_to_hour(entry.get("Time", ""))
        temp  = extract_temp_f(entry.get("Temperature", ""))
        if hour is None or temp is None:
            continue
        results.append({"hour": hour, "time": entry["Time"], "temp_f": temp, "source": "actual"})
    return results


def parse_forecast(html: str) -> list[dict]:
    headers, rows = parse_mat_table(html)
    results = []
    for row in rows:
        if len(row) < len(headers):
            continue
        entry = dict(zip(headers, row))
        hour  = parse_time_to_hour(entry.get("Time", ""))
        temp  = extract_temp_f(entry.get("Temp.", ""))
        if hour is None or temp is None:
            continue
        results.append({"hour": hour, "time": entry["Time"], "temp_f": temp, "source": "forecast"})
    return results


# ── 整合邏輯 ──────────────────────────────────────────────────────────────────

def merge_data(history: list[dict], forecast: list[dict]) -> list[dict]:
    by_hour: dict[int, dict] = {}
    for item in forecast:
        by_hour[item["hour"]] = item
    for item in history:
        by_hour[item["hour"]] = item
    return [by_hour[h] for h in sorted(by_hour.keys())]


def stats(merged: list[dict], source: str) -> tuple[float | None, float | None]:
    temps = [d["temp_f"] for d in merged if d["source"] == source]
    return (max(temps), min(temps)) if temps else (None, None)


# ── 顯示 ──────────────────────────────────────────────────────────────────────

def fmt_temp(temp_f: float, use_celsius: bool) -> str:
    if use_celsius:
        return f"{f_to_c(temp_f):.1f} °C"
    return f"{temp_f:.1f} °F"


def print_report(city: dict, merged: list[dict]):
    name     = city["name"]
    celsius  = city.get("celsius", False)
    unit_lbl = "°C" if celsius else "°F"

    actual_max_f,   actual_min_f   = stats(merged, "actual")
    forecast_max_f, forecast_min_f = stats(merged, "forecast")
    all_temps_f = [d["temp_f"] for d in merged]

    actual_count   = sum(1 for d in merged if d["source"] == "actual")
    forecast_count = sum(1 for d in merged if d["source"] == "forecast")

    print("\n" + "=" * 57)
    print(f"  {name}")
    print(f"  日期 : {datetime.now().strftime('%Y/%m/%d')}")
    print(f"  單位 : {unit_lbl}{'  (原始 °F 透過公式換算)' if celsius else ''}")
    print("=" * 57)
    print(f"  {'時間':<12} {'溫度 (' + unit_lbl + ')':<14} {'來源'}")
    print("-" * 57)

    for d in merged:
        label = "實際紀錄" if d["source"] == "actual" else "預報"
        print(f"  {d['time']:<12} {fmt_temp(d['temp_f'], celsius):<14} {label}")

    print("=" * 57)
    print(f"  實際紀錄: {actual_count} 筆　預報: {forecast_count} 筆")

    if actual_max_f is not None:
        print(f"\n  實際最高: {fmt_temp(actual_max_f, celsius)}")
        print(f"  實際最低: {fmt_temp(actual_min_f, celsius)}")
    else:
        print(f"\n  實際紀錄: 尚無資料")

    if forecast_max_f is not None:
        print(f"\n  預報最高: {fmt_temp(forecast_max_f, celsius)}")
        print(f"  預報最低: {fmt_temp(forecast_min_f, celsius)}")
    else:
        print(f"\n  預報: 尚無資料")

    if all_temps_f:
        print(f"\n  當日綜合最高: {fmt_temp(max(all_temps_f), celsius)}")
        print(f"  當日綜合最低: {fmt_temp(min(all_temps_f), celsius)}")
    print()


# ── 批次執行 ──────────────────────────────────────────────────────────────────

async def run_all(cities: list[dict]):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CITIES)
    results: list[tuple[dict, list[dict]] | tuple[dict, None]] = [None] * len(cities)

    async def scrape_one(browser: Browser, idx: int, city: dict):
        async with semaphore:
            print(f"  [{idx+1}/{len(cities)}] 爬取 {city['name']} ...")
            try:
                history, forecast = await scrape_city(browser, city["location_key"])
                results[idx] = (city, merge_data(history, forecast))
            except Exception as e:
                print(f"  [{idx+1}/{len(cities)}] ✗ {city['name']} 失敗: {e}")
                results[idx] = (city, None)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        await asyncio.gather(*[
            scrape_one(browser, i, city) for i, city in enumerate(cities)
        ])
        await browser.close()

    # 依原順序顯示
    print(f"\n擷取時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    for city, merged in results:
        if merged is None:
            print(f"\n[錯誤] {city['name']} 未取得資料")
        else:
            print_report(city, merged)


# ── 主程式 ────────────────────────────────────────────────────────────────────

def load_cities(arg: str | None) -> list[dict] | None:
    """
    回傳 city list，或 None 代表是單一 location_key（向下相容模式）
    """
    if arg is None:
        path = Path("cities.json")
    else:
        path = Path(arg)
        if not path.exists():
            # 不是檔案路徑，視為 location_key
            return None

    with open(path, encoding="utf-8") as f:
        return json.load(f)


async def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    cities = load_cities(arg)

    if cities is None:
        # 向下相容：單一 location_key
        location_key = arg
        cities = [{"name": location_key.split("/")[-1], "location_key": location_key, "celsius": False}]

    print(f"共 {len(cities)} 個城市，最多 {MAX_CONCURRENT_CITIES} 個同時爬取")
    await run_all(cities)


if __name__ == "__main__":
    asyncio.run(main())
