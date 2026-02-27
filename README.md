# 🌤️ Weather Dashboard

從 [Weather Underground](https://www.wunderground.com) 爬取全球各城市的當日氣溫，整合歷史實測與未來預報，以 Streamlit 介面呈現。

---

## 功能

- **雙來源整合**：過去時段使用實際觀測紀錄，未來時段使用逐時預報，自動銜接成完整的當日 24 小時氣溫曲線
- **多城市並發爬取**：所有城市同時爬取，顯著縮短等待時間
- **溫度單位切換**：可依城市設定顯示 °F 或 °C（°C 由 °F 透過公式換算）
- **互動折線圖**：實際紀錄（藍色實線）與預報（橘色虛線）一眼區分，附現在時刻分界線
- **快取機制**：首次爬取後快取 30 分鐘，重新整理不需重爬
- **彈性設定**：修改 `cities.json` 即可增減城市，無需動程式碼

---

## 專案結構

```
.
├── app.py              # Streamlit 介面
├── weather_scraper.py  # 爬蟲核心（可獨立使用 CLI）
├── cities.json         # 城市設定檔
├── run.sh              # 快速啟動腳本
└── venv/               # Python 虛擬環境
```

---

## 安裝

```bash
# 建立虛擬環境並安裝套件
python3 -m venv venv
source venv/bin/activate
pip install playwright beautifulsoup4 lxml streamlit plotly pandas

# 安裝 Chromium（Playwright 使用）
playwright install chromium
```

---

## 啟動

**Streamlit 介面**
```bash
./run.sh
# 或
./venv/bin/streamlit run app.py
```
開啟瀏覽器前往 http://localhost:8501

**CLI 模式（不需 Streamlit）**
```bash
# 跑 cities.json 裡所有城市
source venv/bin/activate
python weather_scraper.py

# 單一城市
python weather_scraper.py us/ga/atlanta/KATL
```

---

## cities.json 格式

```json
[
  {
    "name": "Atlanta, GA",
    "location_key": "us/ga/atlanta/KATL",
    "celsius": false
  },
  {
    "name": "London, UK",
    "location_key": "gb/london/EGLC",
    "celsius": true
  }
]
```

| 欄位 | 說明 |
|------|------|
| `name` | 顯示名稱（自由填寫） |
| `location_key` | Weather Underground URL 中 `/history/daily/` 後的路徑 |
| `celsius` | `true` 顯示攝氏，`false` 顯示華氏 |

`location_key` 可從城市頁面網址取得，例如：
- `https://www.wunderground.com/history/daily/us/ga/atlanta/KATL` → `us/ga/atlanta/KATL`

---

## 注意事項

- Weather Underground 不論哪個國家，未登入狀態下資料一律以 **°F** 回傳，攝氏換算由本程式自行計算：`°C = (°F − 32) × 5 ÷ 9`
- 頁面結構若有異動可能造成爬取失敗，失敗的城市在介面上會顯示錯誤訊息，不影響其他城市
