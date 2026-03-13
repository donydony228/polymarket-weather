# TODO — 階段三：策略驗證與執行

> 目標：驗證並執行發現的交易策略，等待市場結算結果驗證 alpha。
>
> **進度**：2026-03-13
> - ✅ Phase 2 完成：14 城市 × 13 日 × 38K+ 市場快照已收集
> - ✅ Phase 3A 完成：假說 C（WU 低估補償）驗證 + 假說 D（Spread 定價）發現
> - 📍 Phase 3B 進行中：策略驗證與待命執行

---

## 📍 當前進行中：策略 D 執行與驗證

**所有分析腳本已完成，詳見 [docs/strategy.md](strategy.md)**

### 🎯 當前優先級 1：等待市場結算數據驗證

- [ ] **監控 market_resolutions 更新**
  ```bash
  # 定期檢查 Polymarket API 是否返回結算數據
  python src/collector.py
  # 查看 market_snapshots 表的 market_date 最新時間點
  ```
  目標：等待 2026-03-14+ 的市場結算結果

- [ ] **一旦有結算數據，計算實際 PnL**
  ```bash
  # Chicago 高溫選項
  買入成本：Spread 0.67%
  結算價格：根據官方高溫結果
  利潤/虧損：(結算價 - 買入價) × 數量
  ```

- [ ] **驗證策略有效性**
  - ✓ 如果 Chicago 高溫出現 + Spread 擴張 = 雙倍利潤
  - ✓ 如果 Chicago 高溫出現 + Spread 保持 = 單倍利潤
  - ✓ 如果 Chicago 高溫未出現 + Spread 保持 = 虧損買入成本（0.67%）
  - ✓ 如果 Chicago 高溫未出現 + Spread 縮小 = 更大虧損

### 🎯 當前優先級 2：持續監控 Spread 變化

- [ ] **每日檢查 Chicago & London 的 Spread**
  ```bash
  # 看 Spread 是否開始擴張
  python << 'EOF'
  SELECT market_date, AVG(spread) as daily_spread
  FROM market_snapshots
  WHERE location_key = 'us/il/chicago/KORD'
    AND option_label LIKE '%or higher%'
  GROUP BY market_date
  ORDER BY market_date DESC
  EOF
  ```

- [ ] **如果 Spread 擴張到 0.9%+**
  → 可考慮部分平倉獲利
  → 記錄實時 PnL

- [ ] **如果 Spread 下跌到 0.5%-**
  → 評估是否投降止損
  → 分析市場偏見是否在加強

---

## 📚 已完成的分析

### 假說 C：WU 系統偏差定價（❌ 已破產）
- 執行腳本：hypothesis_c_testing.py, hypothesis_c_deep_dive.py, hypothesis_c_multivariate.py
- 結論：WU Bias 並非市場定價因素，真正驅動因素是 Spread 和氣候特性

### 假說 D：Spread 定價無效（✅ 已發現）
- 執行腳本：hypothesis_d_spread_pricing.py, hypothesis_d_daily_trend.py
- 結論：Chicago & London 的 Spread 存在持續的 50%+ 異常，強力買入信號

---

## 📚 保留作為參考的備選策略（長期研究）

詳見 [docs/strategy.md](strategy.md)：
- 策略 E：Lead time 定價延遲（需要細粒度數據）
- 策略 F：氣候定價偏見（需要更多樣本）

---

## 📊 工具與腳本準備

- [x] `analysis/forecast_accuracy.py` — WU 預報精度分析 ✅
- [x] `analysis/market_inefficiencies.py` — 市場低效分析 ✅
- [ ] `analysis/hypothesis_testing.py` — 統計假設檢驗（T-test, 迴歸）**待開發**
- [ ] `analysis/backtest_strategy.py` — 完整策略回測引擎 **待開發**
- [x] `docs/script.md` — 分析腳本使用指南 ✅
- [x] `docs/DATA_GUIDE.md` — 資料結構與回測指南 ✅

---

## 📈 進度檢查點

- **今日（3/13）**：完成 3A 的假說 C 驗證，決定是否繼續 3B
- **明日（3/14）**：完成 3B，選定回測策略
- **後天（3/15）**：監控 market_resolutions，準備 3C
- **本週末**：若 API 更新，啟動完整回測；否則開發替代爬蟲

---

## 注意事項

- **數據限制**：13 天太短做統計顯著性回測，只能驗證假說方向
- **市場結算延遲**：market_resolutions 為空（見 [BUGS.md](BUGS.md) BUG-6），可能需等待或手動補入
- **Bias 校正**：若發現 WU 系統低估，回測時必須用「實際值 + WU bias」而非純預報
- **Liquidity 成本**：回測時計入 bid-ask spread 和交易成本（目前市場平均 1-2%）
