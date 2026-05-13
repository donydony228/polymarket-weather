"""
backtest_with_wu_proxy.py — 用 WU 實測最高溫當作 Polymarket 結算代理

目的:
  由於 market_resolutions 尚無資料，改用 weather_daily_summary.official_high_f
  推算每個 (city, date) 的勝出選項，回測「在 T-Nh 買入各選項」的損益。

輸出:
  1. 市場校準曲線（yes_prob bin vs 實際勝率）
  2. 各城市 Brier score（市場定價有效性）
  3. 策略 PnL：
     - S1 買入市場共識（yes_prob 最高）
     - S2 買入長尾（yes_prob < 10%）
     - S3 假說 D 驗證：Chicago/London 低 Spread 選項

用法:
    python analysis/backtest_with_wu_proxy.py
    python analysis/backtest_with_wu_proxy.py --lead 6    # 指定進場 lead time
    python analysis/backtest_with_wu_proxy.py --settle floor  # 結算規則
"""

import argparse
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


# ── 設定 ──────────────────────────────────────────────────────────────────────

DEFAULT_LEAD_HOURS  = 12
LEAD_TOLERANCE      = 1.5        # ±1.5h 內的 snapshot 都算
SETTLE_RULE_DEFAULT = "nearest"  # nearest | floor


# ── 單位與選項解析 ──────────────────────────────────────────────────────────

def f_to_c(f: float) -> float:
    return (f - 32) * 5 / 9


_RANGE_RE = re.compile(r"(-?\d+)\s*-\s*(-?\d+)\s*°?\s*[CF]?", re.IGNORECASE)
_BOUND_RE = re.compile(r"(-?\d+)\s*°?\s*[CF]?\s*(or\s+below|or\s+higher|or\s+above)",
                       re.IGNORECASE)
_SINGLE_RE = re.compile(r"(-?\d+)\s*°?\s*[CF]?\s*$", re.IGNORECASE)


def parse_option_label(label: str) -> tuple[int | None, int | None, str]:
    """
    解析選項 label，回傳 (lo, hi, kind)
    kind: 'exact' | 'range' | 'below' | 'above'
    例:
      '16°C'           -> (16, 16, 'exact')
      '50-51°F'        -> (50, 51, 'range')
      '13°C or below'  -> (None, 13, 'below')
      '19°C or higher' -> (19, None, 'above')
    """
    label = label.strip()
    m = _BOUND_RE.search(label)
    if m:
        temp = int(m.group(1))
        tail = m.group(2).lower()
        if "below" in tail:
            return None, temp, "below"
        return temp, None, "above"

    m = _RANGE_RE.search(label)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return lo, hi, "range"

    m = _SINGLE_RE.search(label)
    if m:
        t = int(m.group(1))
        return t, t, "exact"

    return None, None, "exact"


def option_covers(lo: int | None, hi: int | None, kind: str,
                   target: float, rule: str) -> bool:
    """
    判斷一個選項是否 cover 特定溫度值。

    - exact 單點（°C 市場）：floor/nearest 判斷
    - range 雙整數（°F 市場，常 2 度一桶）：actual ∈ [lo, hi+1) 視為命中
    - below/above 邊界：對應比較
    """
    if kind == "below":
        return target < hi + 1                # "13 or below" ≈ target ≤ 13.x
    if kind == "above":
        return target >= lo
    if kind == "range":
        return lo <= target < hi + 1          # e.g., 50-51°F 包含 [50, 52)
    if kind == "exact":
        if rule == "floor":
            return lo <= target < lo + 1
        return abs(target - lo) < 0.5         # nearest（四捨五入）
    return False


def determine_winner(options: list[dict], actual_high: float,
                      rule: str = "nearest") -> str | None:
    """根據 WU 實測最高溫（已轉成該市場單位），判斷勝出選項 label。"""
    if not options:
        return None

    parsed = [(o["option_label"], *parse_option_label(o["option_label"]))
              for o in options]

    # 優先匹配 exact/range（內部 buckets），再 fallback 到 bookends
    for label, lo, hi, kind in parsed:
        if kind in ("exact", "range") and option_covers(lo, hi, kind, actual_high, rule):
            return label

    for label, lo, hi, kind in parsed:
        if kind == "below" and actual_high < hi + 1:
            return label
        if kind == "above" and actual_high >= lo:
            return label

    # Fallback：找最接近的 exact/range
    candidates = [(label, lo, hi) for label, lo, hi, kind in parsed
                  if kind in ("exact", "range")]
    if candidates:
        def dist(x):
            label, lo, hi = x
            mid = (lo + hi) / 2
            return abs(mid - actual_high)
        return min(candidates, key=dist)[0]
    return None


# ── DB 工具 ───────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")


def load_dataset(conn, lead_hours: float, tolerance: float):
    """
    載入 (city, date) 配對 + 實測 high + 該 lead time 的所有選項 yes_prob。
    回傳 list of dicts，每筆代表一個市場（一個 city-date），含 options 子列表。
    """
    sql = """
    WITH target_snapshots AS (
      SELECT DISTINCT ON (location_key, market_date)
             location_key, market_date, snapshot_time, hours_before_close
      FROM market_snapshots
      WHERE hours_before_close BETWEEN %s AND %s
      ORDER BY location_key, market_date,
               ABS(hours_before_close - %s) ASC
    )
    SELECT
      ts.location_key, ts.market_date, ts.snapshot_time, ts.hours_before_close,
      c.celsius, wds.official_high_f,
      ms.option_label, ms.yes_prob, ms.spread,
      ms.volume_usdc, ms.liquidity_usdc
    FROM target_snapshots ts
    JOIN cities c ON c.location_key = ts.location_key
    JOIN weather_daily_summary wds
      ON wds.location_key = ts.location_key AND wds.obs_date = ts.market_date
    JOIN market_snapshots ms
      ON ms.location_key = ts.location_key
     AND ms.market_date  = ts.market_date
     AND ms.snapshot_time = ts.snapshot_time
    WHERE wds.official_high_f IS NOT NULL
      AND ms.yes_prob IS NOT NULL
    ORDER BY ts.location_key, ts.market_date, ms.option_label
    """
    lo, hi = lead_hours - tolerance, lead_hours + tolerance
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (lo, hi, lead_hours))
        rows = cur.fetchall()

    markets: dict[tuple, dict] = {}
    for r in rows:
        key = (r["location_key"], r["market_date"])
        if key not in markets:
            celsius = r["celsius"]
            high_f = float(r["official_high_f"])
            high_native = f_to_c(high_f) if celsius else high_f
            markets[key] = {
                "location_key":    r["location_key"],
                "market_date":     r["market_date"],
                "snapshot_time":   r["snapshot_time"],
                "hours_before":    float(r["hours_before_close"]),
                "celsius":         celsius,
                "actual_high_f":   high_f,
                "actual_high_nat": high_native,
                "options":         [],
            }
        markets[key]["options"].append({
            "option_label":   r["option_label"],
            "yes_prob":       float(r["yes_prob"]),
            "spread":         float(r["spread"]) if r["spread"] is not None else None,
            "volume_usdc":    float(r["volume_usdc"]) if r["volume_usdc"] else None,
            "liquidity_usdc": float(r["liquidity_usdc"]) if r["liquidity_usdc"] else None,
        })
    return list(markets.values())


# ── 分析 ──────────────────────────────────────────────────────────────────

def annotate_winner(markets: list[dict], rule: str):
    """為每個市場加上 winner_label（根據 actual_high + 規則）"""
    for m in markets:
        m["winner_label"] = determine_winner(m["options"], m["actual_high_nat"], rule)
        for o in m["options"]:
            o["is_winner"] = (o["option_label"] == m["winner_label"])


def calibration_curve(markets: list[dict], n_bins: int = 10):
    """
    將所有 (yes_prob) bin 後，計算每 bin 的平均 yes_prob vs 實際勝率。
    理想市場：兩者應在對角線上。
    """
    bins = defaultdict(lambda: {"n": 0, "sum_p": 0.0, "wins": 0})
    for m in markets:
        for o in m["options"]:
            p = o["yes_prob"]
            if p is None or not (0 <= p <= 1):
                continue
            b = min(int(p * n_bins), n_bins - 1)
            bins[b]["n"]     += 1
            bins[b]["sum_p"] += p
            bins[b]["wins"]  += int(o["is_winner"])

    print(f"\n{'─'*70}")
    print("📊 市場校準曲線 (yes_prob bin → 實際勝率)")
    print(f"{'─'*70}")
    print(f"{'區間':<12} {'樣本數':>6} {'平均概率':>10} {'實際勝率':>10} {'偏差':>8}")
    for b in sorted(bins.keys()):
        d       = bins[b]
        avg_p   = d["sum_p"] / d["n"] if d["n"] else 0
        win_rt  = d["wins"]  / d["n"] if d["n"] else 0
        bias    = win_rt - avg_p
        lo, hi  = b / n_bins, (b + 1) / n_bins
        print(f"[{lo:.2f}, {hi:.2f}) {d['n']:>6} {avg_p:>10.3f} "
              f"{win_rt:>10.3f} {bias:>+8.3f}")


def per_city_brier(markets: list[dict]):
    """每城市 Brier score = mean((yes_prob - is_winner)^2)"""
    per_city = defaultdict(lambda: {"n": 0, "se": 0.0, "wins": 0, "markets": 0})
    for m in markets:
        per_city[m["location_key"]]["markets"] += 1
        for o in m["options"]:
            per_city[m["location_key"]]["n"] += 1
            per_city[m["location_key"]]["se"] += (o["yes_prob"] - int(o["is_winner"])) ** 2
            per_city[m["location_key"]]["wins"] += int(o["is_winner"])

    print(f"\n{'─'*70}")
    print("📈 各城市市場有效性 (Brier Score，越低越精準)")
    print(f"{'─'*70}")
    print(f"{'城市':<28} {'市場數':>6} {'Brier':>8} {'勝出/總選項':>14}")
    ranked = sorted(per_city.items(), key=lambda x: x[1]["se"] / max(x[1]["n"], 1))
    for loc, d in ranked:
        brier = d["se"] / d["n"] if d["n"] else 0
        print(f"{loc:<28} {d['markets']:>6} {brier:>8.4f} "
              f"{d['wins']:>6}/{d['n']:<6}")


def strategy_pnl(markets: list[dict]):
    """
    策略回測 — 對每個市場下 $1 注，計算累計 PnL
    - S1: 買入 yes_prob 最高的選項（市場共識）
    - S2: 買入 yes_prob < 0.10 的長尾（平均分配）
    - S3: 買入實際勝出選項（oracle baseline，最高可能收益）
    - S4: 買入 yes_prob 2nd high（懷疑市場共識）
    """
    def bet_pnl(price: float, won: bool) -> float:
        """賭 $1 於 yes：勝得 1/price - 1，輸 -1"""
        if price <= 0 or price >= 1:
            return 0.0
        return (1 / price - 1) if won else -1.0

    stats = {k: {"n": 0, "pnl": 0.0, "wins": 0, "cost": 0.0}
             for k in ("S1_consensus", "S2_longshot", "S3_oracle", "S4_second")}

    for m in markets:
        if not m["winner_label"]:
            continue
        opts = sorted(m["options"], key=lambda o: -o["yes_prob"])

        # S1: 最高 yes_prob
        top = opts[0]
        s = stats["S1_consensus"]
        s["n"]    += 1
        s["pnl"]  += bet_pnl(top["yes_prob"], top["is_winner"])
        s["wins"] += int(top["is_winner"])
        s["cost"] += top["yes_prob"]

        # S2: 所有 yes_prob < 0.10 平均下注
        longshots = [o for o in m["options"] if 0 < o["yes_prob"] < 0.10]
        if longshots:
            per_bet = 1.0 / len(longshots)
            s = stats["S2_longshot"]
            for o in longshots:
                s["n"]   += 1
                s["pnl"] += per_bet * bet_pnl(o["yes_prob"], o["is_winner"])
                s["cost"] += per_bet * o["yes_prob"]
                s["wins"] += int(o["is_winner"])

        # S3: oracle — 買勝出選項
        win_opt = next((o for o in m["options"] if o["is_winner"]), None)
        if win_opt and win_opt["yes_prob"] > 0:
            s = stats["S3_oracle"]
            s["n"]    += 1
            s["pnl"]  += bet_pnl(win_opt["yes_prob"], True)
            s["wins"] += 1
            s["cost"] += win_opt["yes_prob"]

        # S4: 第二高
        if len(opts) >= 2:
            second = opts[1]
            s = stats["S4_second"]
            s["n"]    += 1
            s["pnl"]  += bet_pnl(second["yes_prob"], second["is_winner"])
            s["wins"] += int(second["is_winner"])
            s["cost"] += second["yes_prob"]

    print(f"\n{'─'*70}")
    print("💰 策略回測 (每市場下注 $1 等比)")
    print(f"{'─'*70}")
    print(f"{'策略':<16} {'下注數':>6} {'勝率':>8} {'累計 PnL':>12} "
          f"{'平均 ROI':>10} {'平均成本':>10}")
    for name, d in stats.items():
        if d["n"] == 0:
            continue
        winrate = d["wins"] / d["n"]
        avg_cost = d["cost"] / d["n"]
        roi     = d["pnl"]  / d["cost"] if d["cost"] > 0 else 0
        print(f"{name:<16} {d['n']:>6} {winrate:>8.2%} "
              f"{d['pnl']:>+12.2f} {roi:>+10.2%} {avg_cost:>10.4f}")


def hypothesis_d_check(markets: list[dict]):
    """假說 D 驗證：Chicago / London 各選項 PnL"""
    print(f"\n{'─'*70}")
    print("🎯 假說 D 驗證 — Chicago & London 各選項收益")
    print(f"{'─'*70}")
    target = {
        "us/il/chicago/KORD": "Chicago",
        "gb/london/EGLC":     "London",
    }
    for loc, name in target.items():
        city_markets = [m for m in markets if m["location_key"] == loc]
        if not city_markets:
            continue
        per_opt = defaultdict(lambda: {"n": 0, "pnl": 0.0, "wins": 0,
                                        "avg_p": 0.0, "avg_spread": 0.0})
        for m in city_markets:
            for o in m["options"]:
                if o["yes_prob"] <= 0 or o["yes_prob"] >= 1:
                    continue
                d = per_opt[o["option_label"]]
                d["n"]       += 1
                d["pnl"]     += (1 / o["yes_prob"] - 1) if o["is_winner"] else -1
                d["wins"]    += int(o["is_winner"])
                d["avg_p"]   += o["yes_prob"]
                d["avg_spread"] += (o["spread"] or 0)

        print(f"\n  【{name}】 {len(city_markets)} 個市場")
        print(f"  {'選項':<20} {'N':>4} {'avg_p':>7} {'win_rt':>7} "
              f"{'spread':>7} {'PnL':>8}")
        def sort_key(lbl):
            lo, hi, kind = parse_option_label(lbl)
            return ((lo if lo is not None else (hi or 0)), kind)
        for label in sorted(per_opt.keys(), key=sort_key):
            d = per_opt[label]
            if d["n"] == 0:
                continue
            avg_p = d["avg_p"] / d["n"]
            win_rt = d["wins"] / d["n"]
            avg_sp = d["avg_spread"] / d["n"]
            print(f"  {label:<20} {d['n']:>4} {avg_p:>7.3f} "
                  f"{win_rt:>7.2%} {avg_sp:>7.4f} {d['pnl']:>+8.2f}")


# ── 主執行 ────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lead",   type=float, default=DEFAULT_LEAD_HOURS,
                    help="進場 lead time（距結算幾小時）")
    ap.add_argument("--tol",    type=float, default=LEAD_TOLERANCE,
                    help="lead time 容忍範圍")
    ap.add_argument("--settle", choices=["nearest", "floor"],
                    default=SETTLE_RULE_DEFAULT,
                    help="結算規則：nearest=四捨五入，floor=無條件捨去")
    args = ap.parse_args()

    print(f"\n{'='*70}")
    print(f"  WU Proxy 回測  |  lead=T-{args.lead:.1f}h ±{args.tol}h  |  "
          f"settle={args.settle}")
    print(f"{'='*70}")

    conn = get_conn()
    markets = load_dataset(conn, args.lead, args.tol)
    conn.close()

    if not markets:
        print("❌ 無資料")
        return

    annotate_winner(markets, args.settle)

    resolved = sum(1 for m in markets if m["winner_label"])
    print(f"\n載入 {len(markets)} 個市場，其中 {resolved} 個可判定勝出選項 "
          f"({resolved/len(markets):.1%})")

    calibration_curve(markets)
    per_city_brier(markets)
    strategy_pnl(markets)
    hypothesis_d_check(markets)
    print()


if __name__ == "__main__":
    main()
