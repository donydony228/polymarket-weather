# TODO — Polymarket 氣象市場研究

> **最後更新：2026-05-13**
>
> **目前進度**
> - ✅ Phase 1：Streamlit 儀表板
> - ✅ Phase 2：每小時資料收集 cron + Supabase DB（14 城市 × 73 天）
> - ✅ Phase 3：假說 C / D 分析（Spread 定價、WU 偏差）
> - ✅ Phase 4A：氣溫走勢 ML 分析（`temp_trajectory_ml.ipynb`）
> - ✅ Phase 4B：Risk Score 回測（`risk_score_backtest.ipynb`）
> - 📍 Phase 4C：驗證與優化（進行中）

---

## 📍 當前優先事項

### 🥇 優先級 1：拆城市單獨跑回測 ✅ 已完成（2026-05-14）

已對 14 個城市分別跑策略 A vs C，結論：

- **聚焦城市（策略 C 穩定盈利）**：NYC、Miami、Seattle、London、Atlanta、Buenos Aires
- **暫停城市（負期望，共識策略無效）**：Toronto、Chicago
- **移除城市（系統性虧損）**：Incheon（C 策略最終資金轉負 −17）
- **謹慎城市（C 比 A 差）**：Paris、São Paulo（市場效率較高）

詳見 [docs/insight.md](insight.md) 各城市完整結果表格

### 🥇 優先級 1（新）：聚焦城市的 risk_score 分層驗證 ✅ 已完成（2026-05-14）

對 NYC / Miami / Seattle / London 各自跑 Q1–Q4 分層分析，結論：

- **NYC**：兩次分析一致，Q4 明確負期望 → 策略 C 在 NYC 有紮實基本面
- **Miami**：兩次大致一致，Q1/Q2 正期望 → 策略 C 在 Miami 合理
- **Seattle**：兩次結果完全相反（一次 +0.40，一次 −0.35）→ risk_score 不穩定，改用固定策略 A
- **London**：Q4 高風險天反而勝率最高 → risk_score 無效，固定策略 A

**最終聚焦城市縮減為 NYC + Miami**，其餘固定策略 A 觀察

### 🥈 優先級 2：實際觀察幾天

把 notebook 改成「每天跑一次，印出今天的下注建議」：

- [ ] 新增一個 cell，輸出今天 14 城市的 `risk_score` 和建議下注金額
- [ ] 觀察 3–5 天的實際結算，感受 risk_score 在現實中的準確度
- [ ] 記錄哪些城市的預測明顯準、哪些不準

### 🥉 優先級 3：等資料累積再重跑（2026-08 以後）

- [ ] 現在只有 73 個不同日期，策略 C 的優勢在噪音邊緣
- [ ] 等資料累積到 5–6 個月（約 150 天）後重跑所有回測
- [ ] 屆時 walk-forward 驗證也比較有意義

---

## 📋 待辦清單（依難度排序）

### 簡單、立刻可做

- [x] **調整 `MAX_MULT` 看邊際效益** ✅ 已完成（2026-05-14）
  - 掃描 ×1.0/1.5/2.0/2.5/3.0：NYC+Miami 三指標全在 ×2.5 同時最高；全體 14 城市 ×2.0 P5 最佳但差距極小
  - **結論：維持 MAX_MULT = 2.5 不變**，×3.0 是邊界（P5 開始惡化）

- [x] **單城市回測** ✅ 已完成（見優先級 1 結論）

- [ ] **每日下注建議 cell**（見優先級 2）

### 中等難度、值得做
- [ ] **不同參數的彈性測試**

- [ ] **Walk-forward 驗證（解決 lookahead bias）**
  - 現在的 OOF 雖然誠實，但訓練資料和回測資料完全重疊（同一批 73 天）
  - 真正嚴謹的做法：用前 N 天訓練 → 預測第 N+1 天以後
  - 如果 walk-forward 結論跟現在一樣，策略的可信度大幅提升
  - 建議資料 ≥ 150 天後再做，否則訓練集太小

- [ ] **把 `risk_score` 接進每日 cron 輸出**
  - 每天 collector 跑完後，順便算出當天的 risk_score 並存入 DB 或 log
  - 這樣就能長期累積「模型預測 vs 實際結果」的對照記錄

### 較複雜、長期研究

- [ ] **策略 E：Lead time 定價延遲**
  - 找出 WU 預報大幅更新（> 5°F）後，市場反應是否有延遲
  - 需要細粒度的逐時快照分析

- [ ] **策略 F：氣候定價偏見**
  - 溫帶 vs 熱帶城市的賠率是否被系統性地錯估
  - 需要更多樣本才能驗證

---

## ✅ 已完成

### Phase 4C：各城市拆分分析（2026-05-14）

- [x] 14 個城市分別跑策略 A vs C，發現城市間差異極大
  - **移除**：Incheon（系統性虧損，策略 C 最終資金 −17）
  - **暫停**：Toronto、Chicago（勝率 29–38%，共識策略無優勢）
  - **聚焦**：NYC、Miami、Seattle、London（穩定盈利，Sharpe ≥ 1）
  - C > A：11/14 城市，平均 ROI 差 +3.6%

### Phase 4B：Risk Score 回測（2026-05-13）

- [x] `analysis/temp_trajectory_ml.ipynb` — 氣溫走勢 ML 分析
  - WU 系統性低估峰值 +1.49°F（62% 的天實際更高）
  - 平台型走勢突破率 75%（尖峰型僅 46%），Logistic 回歸顯著（p=0.002）
  - risk_score 範圍 1–26°F，有效區分高低風險天
- [x] `analysis/risk_score_backtest.ipynb` — 三策略回測比較
  - 策略 C（反比縮放）淨 ROI +6.2%，Sharpe 0.34，MC P(獲利) 93%
  - Q1 低風險天勝率 50.9%（正期望），Q3/Q4 高風險天負期望

### Phase 3：假說分析（2026-03-13）

- [x] `analysis/hypothesis_c_*.py` — 假說 C 破產：WU 偏差不是市場定價因素
- [x] `analysis/hypothesis_d_*.py` — 假說 D：Spread 定價異常（Chicago / London）
- [x] `analysis/backtest_notebook.ipynb` — 共識買入策略回測
- [x] `analysis/betting_strategy_analysis.ipynb` — 固定金額 vs 固定比例策略分析

### Phase 2：資料收集

- [x] `collector.py` — 每小時 cron，WU 預報 + Polymarket 賠率 → Supabase
- [x] `schema.sql` — 8 張資料表
- [x] 14 城市 × 73 天資料穩定收集中

---

## 注意事項

- `market_resolutions` 表至今仍為空，結算驗證改用 `weather_daily_summary.official_high_f` 代理
- 有效樣本遠小於 1011（73 天 × 城市間高度相關），所有結論都要保守看待
- risk_score 的 80% 區間實測覆蓋率 73%（目標 80%），尾端略低估風險