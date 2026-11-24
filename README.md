# 台股 V21.2 二十大指標戰情室（GitHub Pages + Actions 全自動盤後更新版）

本專案整合 **GitHub Actions 每日盤後自動排程** 與 **GitHub Pages HTTPS PWA 戰情室**，讓您完全免開電腦、免開 iPhone a-Shell，每天收盤後自動更新全市場 `2,140+` 檔評分與真實 K 線，打開 iPhone 桌面圖示即是最新的零誤差盤後數據。

## 檔案架構
```text
├── .github/workflows/daily_v21_update.yml  # 每日 15:45 & 18:10 (台灣時間) 自動執行排程
├── scripts/v21_ci.py                       # V21.2 零誤差雲端分析腳本（純 Python 內建零套件）
├── data/
│   ├── v21_latest.json                     # 戰情室即時同步主檔（大盤 + 20指標 + 2,140檔）
│   ├── kline_bundle.json                   # 大盤、TOP20 與權值股真實日K / 60分K整合包
│   ├── V21_最新每日報告.md                   # 15項戰情報告（可一鍵下載匯入 NotebookLM）
│   ├── V21_全市場評分_最新.csv               # 全市場評分總表
│   ├── v21_hist.json                       # 每日大盤分數與 TOP3 勝率回測歷史
│   └── macro_config.json                   # 每月宏觀設定（景氣燈號 / PMI / Fed利率）
├── index.html                              # 三竹風格手機桌面 PWA 戰情室主程式
├── manifest.json                           # PWA 主畫面全螢幕設定檔
├── sw.js                                   # Service Worker 離線快取引擎
├── apple-touch-icon.png                    # iPhone 桌面圖示 (180x180)
└── icon-512.png                            # Android / 高解析桌面圖示 (512x512)
```

## 快速部署 4 步驟
1. 在 GitHub 建立新 Repository（建議名稱 `tw-v21-warroom`，勾選 **Public** 以免費啟用 GitHub Pages）。
2. 將本壓縮包解壓後的所有檔案（含隱藏資料夾 `.github/`）上傳至該儲存庫的 `main` 分支。
3. 前往儲存庫 **Settings → Pages**：
   - Source 選擇 **Deploy from a branch**
   - Branch 選擇 **`main`**、資料夾選擇 **`/ (root)`** → 點 **Save**。
4. 前往 **Actions** 分頁啟用工作流程，點 **「台股 V21.2 每日盤後全自動更新」→「Run workflow」** 立即試跑一次。
5. 用 iPhone **Safari** 開啟 `https://您的帳號.github.io/tw-v21-warroom/`，點底部 **「分享 ⬆️」→「加入主畫面 ➕」** 即可完成！
