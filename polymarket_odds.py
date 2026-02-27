"""
Polymarket 即時賠率查詢 CLI

用法:
    python polymarket_odds.py <event-slug>
    python polymarket_odds.py https://polymarket.com/event/<event-slug>
    python polymarket_odds.py https://polymarket.com/event/<event-slug>/<market-slug>

範例:
    python polymarket_odds.py highest-temperature-in-atlanta-on-february-26-2026
"""

import json
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
HEADERS   = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def _get(url: str) -> dict | list:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def parse_slug(arg: str) -> str:
    """從 URL 或直接輸入的 slug 提取 event slug。"""
    if "polymarket.com" in arg:
        # 支援: /event/、/zh/event/、/ja/event/ 等語系前綴
        if "/event/" in arg:
            parts = arg.split("/event/", 1)[1].split("/")
            return parts[0]
    return arg.strip("/")


# ── Gamma API ────────────────────────────────────────────────────────────────

def fetch_event(event_slug: str) -> dict:
    """取得事件資料，包含所有內嵌市場（選項）。"""
    url = f"{GAMMA_API}/events?slug={event_slug}&limit=1"
    data = _get(url)
    if not data:
        raise ValueError(f"找不到事件：{event_slug!r}")
    return data[0]


# ── CLOB API ─────────────────────────────────────────────────────────────────

def fetch_clob(token_id: str) -> dict:
    """並行查詢用：取得單一 token 的即時 midpoint 與 spread。"""
    result = {"mid": None, "spread": None}
    try:
        r = _get(f"{CLOB_API}/midpoint?token_id={token_id}")
        result["mid"] = float(r.get("mid", 0))
    except Exception:
        pass
    try:
        r = _get(f"{CLOB_API}/spread?token_id={token_id}")
        result["spread"] = float(r.get("spread", 0))
    except Exception:
        pass
    return result


def fetch_all_clob(markets: list[dict]) -> list[dict]:
    """
    對所有市場並行查詢 CLOB，回傳與 markets 同索引的即時資料清單。
    """
    token_ids = []
    for m in markets:
        ids = m.get("clobTokenIds", "[]")
        if isinstance(ids, str):
            ids = json.loads(ids)
        token_ids.append(ids[0] if ids else None)  # Yes token

    results = [None] * len(markets)

    def task(idx, token_id):
        if token_id:
            return idx, fetch_clob(token_id)
        return idx, {"mid": None, "spread": None}

    with ThreadPoolExecutor(max_workers=min(10, len(markets))) as ex:
        futures = {ex.submit(task, i, tid): i for i, tid in enumerate(token_ids)}
        for fut in as_completed(futures):
            idx, data = fut.result()
            results[idx] = data

    return results


# ── 顯示 ──────────────────────────────────────────────────────────────────────

def fmt_pct(v) -> str:
    if v is None:
        return "  —  "
    return f"{float(v)*100:5.1f}%"


def fmt_price(v) -> str:
    if v is None:
        return "   —  "
    return f"${float(v):.3f}"


def fmt_vol(v) -> str:
    if v is None:
        return "—"
    v = float(v)
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def print_table(event: dict, markets: list[dict], clob_data: list[dict]):
    title    = event.get("title", event.get("slug", ""))
    end_date = event.get("endDate", "")[:10]
    total_vol = event.get("volume", 0)

    print()
    print(f"  {title}")
    print(f"  結算日：{end_date}　·　總交易量：{fmt_vol(total_vol)}")
    print(f"  擷取時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 欄寬
    col_option = max(16, max(len(m.get("groupItemTitle", m.get("slug",""))[:30]) for m in markets) + 2)

    header = (
        f"  {'選項':<{col_option}}"
        f"{'Yes%':>8}"
        f"{'No%':>8}"
        f"{'中間價':>8}"
        f"{'價差':>7}"
        f"  {'成交量':>10}"
        f"  {'狀態'}"
    )
    sep = "  " + "─" * (len(header) - 2)

    print(header)
    print(sep)

    for m, clob in zip(markets, clob_data):
        label = m.get("groupItemTitle") or m.get("slug", "")
        label = label[:col_option]

        # Gamma 快取價格（outcomePrices）
        op = m.get("outcomePrices", "[]")
        if isinstance(op, str):
            op = json.loads(op)
        yes_gamma = float(op[0]) if op else None
        no_gamma  = float(op[1]) if len(op) > 1 else None

        # CLOB 即時 midpoint
        mid    = clob.get("mid") if clob else None
        spread = clob.get("spread") if clob else None

        # 若 CLOB 有值就用，否則 fallback 到 Gamma
        yes_pct = mid if mid is not None else yes_gamma
        no_pct  = (1 - yes_pct) if yes_pct is not None else no_gamma

        vol    = m.get("volume", 0)
        active = "✓ 開放" if m.get("acceptingOrders") else "已停止"

        spread_str = f"{float(spread):.3f}" if spread is not None else "  —  "

        print(
            f"  {label:<{col_option}}"
            f"{fmt_pct(yes_pct):>8}"
            f"{fmt_pct(no_pct):>8}"
            f"{fmt_price(yes_pct):>8}"
            f"  {spread_str:>5}"
            f"  {fmt_vol(vol):>10}"
            f"  {active}"
        )

    print(sep)
    print()


# ── 主程式 ────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    slug = parse_slug(sys.argv[1])
    print(f"\n  正在查詢事件：{slug}")

    # 1. 取得事件 + 所有市場
    try:
        event = fetch_event(slug)
    except ValueError as e:
        print(f"  錯誤：{e}")
        sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f"  HTTP 錯誤 {e.code}：無法取得事件資料")
        sys.exit(1)

    markets = event.get("markets", [])
    if not markets:
        print("  此事件沒有市場資料。")
        sys.exit(1)

    print(f"  找到 {len(markets)} 個選項，正在查詢即時價格...")

    # 2. 並行查詢 CLOB 即時資料
    clob_data = fetch_all_clob(markets)

    # 3. 印出表格
    print_table(event, markets, clob_data)


if __name__ == "__main__":
    main()
