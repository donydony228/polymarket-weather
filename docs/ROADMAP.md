# Polymarket 氣象市場 Alpha 研究與自動化交易 — 路線圖

> **終極目標**：在 Polymarket 每日最高氣溫預測市場中找到可持續的定價偏差（alpha），建立自動化策略並實盤交易。

---

## 現況盤點

已完成：
- [x] Weather Underground 爬蟲（`weather_scraper.py`）：取得 14 個城市逐時實測 + 預報
- [x] Streamlit 氣象儀表板（`app.py`）：視覺化溫度曲線
- [x] Polymarket 市場查詢（`polymarket_odds.py`）：即時賠率 CLI
- [x] 城市對應表（`polymarket_cities.json`）：14 城市 WU ↔ Polymarket 完整對照

技術確認：
- Polymarket 14 個城市的解析來源與 WU 爬蟲用同一套氣象站資料
- 每個城市每日有 9 個溫度區間選項，均為 neg-risk 市場結構

---

## 階段一：整合儀表板 ✅ 已完成

**目標**：在同一個畫面上看到「WU 氣象預報預測的最高溫」與「Polymarket 市場認為最可能的溫度區間」，讓人工判斷成為可能。

### 完成項目

- [x] 在 `app.py` 每個城市分頁新增「市場預測對比」區塊
  - 從 `polymarket_cities.json` 找到對應 event_slug_city
  - 動態組出城市本地日期的 event_slug（已修正跨時區日期 bug）
  - 呼叫 Gamma API + CLOB API（並行）取得所有選項即時賠率
  - 以 Yes/No 堆疊橫向長條圖顯示各溫度區間的機率分布
- [x] 首頁總覽：並排顯示 WU 預報與市場共識一致性（依時區排列）
- [x] 各城市分頁：市場預測對比 → 逐時氣溫圖（含最高最低標記）→ 溫度統計
- [x] Polymarket 賠率快取（TTL 5 分鐘）
- [x] 各城市分頁獨立重刷按鈕（不影響其他城市）
- [x] 城市本地日期修正（Wellington/Seoul 已進入次日時，市場資料正確對應次日）

---

## 階段二：歷史資料庫

**目標**：累積足夠的歷史數據，才能做有意義的回測。資料庫設計見 `schema.sql`。

### 待辦

- [x] 設計資料庫 schema（`schema.sql`）
  - `weather_actuals_hourly`：逐時實測全欄位
  - `weather_daily_summary`：每日官方最高最低（Polymarket 結算依據）
  - `forecast_snapshots`：每小時快照 WU 對目標日的預報最高溫
  - `forecast_hourly_snapshots`：每小時快照 WU 對目標日的逐時預報曲線
  - `market_options`：各城市各日期的選項清單 + CLOB token ID
  - `market_snapshots`：每小時快照各選項的 Yes/No 機率、價差、成交量
  - `market_resolutions`：最終結算選項與官方最高溫
  - `collection_log`：收集器執行記錄（便於偵測資料缺口）
- [ ] 建立每小時排程收集器（`collector.py`）
  - 每小時對所有城市快照市場賠率（目標日 −36h 起至結算）
  - 每小時快照 WU 逐時預報（目標日 −36h 起至結算）
  - 每日結算後寫入官方最高溫與結算選項
  - 收集失敗寫入 collection_log，不中斷其他城市
- [ ] 補齊歷史資料
  - Polymarket Gamma API `events` 清單含過去事件，可批次補抓歷史賠率
  - WU 歷史頁面（`/history/daily/{location_key}/{YYYYMMDD}`）可補抓實測

---

## 階段三：Edge 分析與回測

**目標**：用數據驗證哪些情況下市場存在系統性偏差。

### 待辦

- [ ] 校準分析（Calibration）
  - 當市場給某選項 X% 機率時，它實際發生的頻率是多少？
  - 繪製校準曲線，找出市場是否系統性高估或低估某溫度段
- [ ] WU 預報精確度分析
  - WU 預報在距離結算 T 小時前的準確率如何？
  - 誤差分布是否對稱，還是有城市/季節性偏差？
- [ ] Alpha 假說測試
  - 假說 1：市場在開盤初期定價不精確，隨時間收斂
  - 假說 2：WU 預報系統性偏向某方向（例如在特定條件下高估）
  - 假說 3：結算前 2 小時，當日實測數據已明朗，市場反應有滯後
- [ ] 回測框架（`backtest.py`）
  - 定義進出場規則（例如：WU 最高溫預報與市場最高機率選項不符時買入）
  - 計算歷史勝率、期望值、最大回撤
  - 考慮 Polymarket 手續費（CLOB：maker 0% / taker 依費率）
- [ ] Kelly Criterion 倉位計算
  - 根據估算的 edge 和賠率，計算最優下注比例

---

## 階段四：CLOB API 串接（實盤準備）

**目標**：具備實際在 Polymarket 下單的技術能力。

### 待辦

- [ ] 設定 Polymarket 帳號與 API Key
  - 需要連接 MetaMask 或 WalletConnect
  - CLOB API 下單需要 L1 授權（Polygon 鏈）或 L2 proxy wallet
  - 參考：`py-clob-client` SDK
- [ ] 實作下單模組（`trader.py`）
  - `create_order(token_id, side, price, size)` — 掛單
  - `cancel_order(order_id)` — 撤單
  - `get_positions()` — 查看持倉
  - `get_balance()` — 查看 USDC 餘額
- [ ] 風險控制層
  - 單筆最大下注額限制
  - 每日最大損失停損
  - 倉位不重複（同一市場不累積超過上限）
- [ ] 乾跑測試（Paper Trading）
  - 策略發出訊號但不實際下單，記錄到資料庫觀察一段時間

---

## 階段五：自動化執行

**目標**：全自動化，從資料收集到下單無需人工介入。

### 待辦

- [ ] 建立執行引擎（`engine.py`）
  - 讀取最新 WU 預報 + 當前市場賠率
  - 套用策略邏輯，產生交易訊號
  - 透過 `trader.py` 執行下單
  - 記錄所有操作到資料庫
- [ ] 排程（每小時 or 每 N 分鐘執行一次）
  - macOS: `launchd` 或 `cron`
  - 或包成 Docker container 長駐執行
- [ ] 監控與通知
  - 結算後自動計算損益並推播（Telegram Bot / LINE Notify）
  - 異常（API 錯誤、餘額不足、爬蟲失敗）即時告警
- [ ] 儀表板加入 PnL 追蹤頁
  - 累計損益曲線
  - 各城市勝率統計
  - 倉位總覽

---

## 技術風險與注意事項

| 風險 | 說明 |
|------|------|
| **結算爭議** | Polymarket 結算以 WU「最終確認版」為準，當日資料有時會在數小時後被修正 |
| **市場流動性** | 部分城市某些溫度區間成交量極低，大單可能移動價格 |
| **地理限制** | Polymarket 對部分國家有訪問限制（VPN 使用需注意條款） |
| **Gas 費用** | Polygon 鏈交易需要少量 MATIC，需定期補充 |
| **API 穩定性** | Gamma API 目前無官方 SLA，需加入重試機制 |
| **WU 預報延遲** | WU 預報頁面是 JS 渲染，Playwright 爬取有時延遲，需監控爬蟲健康狀況 |

---

## 目前優先順序

```
階段一（儀表板整合）  ✅ 完成
  ↓
階段二（資料收集）    ← 現在做（越早越好，歷史數據需要時間累積）
  ↓
階段三（回測）        ← 資料足夠後
  ↓
階段四（API 串接）    ← 策略驗證後
  ↓
階段五（全自動化）    ← 最後
```

---

*最後更新：2026-02-27*
