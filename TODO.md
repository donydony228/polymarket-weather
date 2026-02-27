# TODO — 階段二：歷史資料庫

> 目標：從現在起每小時自動收集天氣預報與市場賠率，累積回測所需的數據。

---

## 環境設定

- [ ] 安裝新依賴
  ```bash
  pip install psycopg2-binary python-dotenv
  ```
- [ ] 建立 Supabase 專案（[supabase.com](https://supabase.com)）
- [ ] 在 Supabase SQL Editor 執行 `schema.sql`，建立 8 張資料表
- [ ] 複製 `.env.example` 為 `.env`，填入 Supabase 連線字串
  - Dashboard > Project Settings > Database > Connection string (URI)
  - 使用 Transaction pooler（port 6543）

---

## 收集器測試

- [ ] 跑 dry-run，確認所有城市都能正確判斷收集窗口
  ```bash
  python collector.py --dry-run
  ```
- [ ] 正式執行一次，到 Supabase Table Editor 確認資料寫入
  ```bash
  python collector.py
  ```
- [ ] 確認 WU 解析欄位正確（feels_like、humidity、wind 等有值）
  - 若欄位名稱與實際 WU HTML 不符，調整 `collector.py` 裡的 `_col()` 對應

---

## 排程設定

- [ ] 建立 log 目錄
  ```bash
  mkdir -p /Users/desmond/Documents/作品集/Polymarket/logs
  ```
- [ ] 設定 cron，每小時整點執行
  ```bash
  crontab -e
  ```
  加入：
  ```
  0 * * * * cd /Users/desmond/Documents/作品集/Polymarket && ./venv/bin/python collector.py >> logs/collector.log 2>&1
  ```
- [ ] 確認 cron 第一次自動執行有成功（查看 `logs/collector.log`）

---

## 資料品質驗證（第一週）

- [ ] 每天查看 `collection_log` 確認無城市持續失敗
- [ ] 抽查 `forecast_snapshots`：同一城市同一 target_date 應有 36 筆，hours_before_close 依序遞減
- [ ] 抽查 `market_snapshots`：同一城市同一 market_date 應有 36 × 9 = 324 筆
- [ ] 確認 `weather_actuals_hourly` 每日有補前一天的 24 筆實測

---

## 注意事項

- Supabase 免費方案（500MB）估計可存約 **6 個月**資料；到 350MB 時評估是否升級 Pro（$25/月）
- `forecast_hourly_snapshots` 是最大的表（佔約 45% 空間），若空間不夠可優先刪除此表的舊資料
- Supabase 免費方案若連續 **7 天無請求**會自動暫停，cron 每天跑就不會觸發
