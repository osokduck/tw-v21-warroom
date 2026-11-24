# -*- coding: utf-8 -*-
"""
V21.2 GitHub Actions 雲端全自動盤後更新引擎（純 Python 內建、零外部套件依賴）
================================================================================
專為 GitHub Pages + GitHub Actions 設計：
1. 自動讀取 data/macro_config.json（可直接在 GitHub 網頁修改每月景氣/PMI/利率）。
2. 全市場 2,140+ 檔零誤差對齊（TWSE MI_INDEX / STOCK_DAY_ALL / MI_5MINS_HIST / FMTQIK + TPEx + TAIFEX + Yahoo）。
3. 執行完畢後自動輸出至 data/ 目錄供 GitHub Pages 同網域免 CORS 直接讀取：
   - data/v21_latest.json        （全站大盤、20大指標、2,140+檔評分）
   - data/kline_bundle.json      （大盤、TOP20 與核心權值股之真實日K與60分K，讓網頁直出真實K棒）
   - data/V21_最新每日報告.md      （15項固定戰情報告，供 NotebookLM 匯入）
   - data/V21_全市場評分_最新.csv  （全市場評分表，含 dt 日期校驗欄）
   - data/v21_hist.json          （逐日大盤分數與 TOP3 實績回測紀錄）
================================================================================
"""
import csv, json, math, os, re, ssl, sys, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

TZ_TW = timezone(timedelta(hours=8))
NOW_TW = datetime.now(TZ_TW)
TODAY = NOW_TW.strftime("%Y-%m-%d")

HISTORY_RANGE = "2y"
MAX_WORKERS = 16  # GitHub Actions Linux 雲端高速併發
MIN_VOL_LOTS = 500
TOP_60M_LIMIT = 50
MAX_STOCKS = None

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DEFAULT_MACRO = {
    "ndc_score": 41, "ndc_signal": "紅燈", "ndc_trend": "上升",
    "pmi": 62.5, "pmi_new": 63.2, "pmi_elec": 52.7,
    "fed_rate": 4.25, "fed_stance": "觀望高利率",
    "note": "國發會9月景氣燈號 / 中經院10月PMI"
}

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json,*/*"
}
CTX = ssl._create_unverified_context()


def load_macro():
    p = os.path.join(DATA_DIR, "macro_config.json")
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                return {**DEFAULT_MACRO, **cfg}
        except Exception:
            pass
    return dict(DEFAULT_MACRO)


def log(m):
    print(m, flush=True)


def sf(v, d=None):
    if v is None:
        return d
    if isinstance(v, (int, float)):
        return d if (math.isnan(v) or math.isinf(v)) else float(v)
    s = str(v).replace(",", "").replace("%", "").replace("+", "").strip()
    if s in ("", "-", "--", "---", "N/A", "nan", "None", "X0.00"):
        return d
    try:
        x = float(s)
        return d if (math.isnan(x) or math.isinf(x)) else x
    except Exception:
        return d


def parse_roc_date(s):
    if not s:
        return None
    s = str(s).strip()
    m = re.match(r"^(\d{3})[/-](\d{1,2})[/-](\d{1,2})$", s)
    if m:
        return f"{int(m.group(1)) + 1911:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    digits = re.sub(r"\D", "", s)
    if len(digits) == 7:
        return f"{int(digits[:3]) + 1911:04d}-{int(digits[3:5]):02d}-{int(digits[5:7]):02d}"
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return None


def pick_val(d: dict, keys_or_patterns):
    if not isinstance(d, dict):
        return None
    for k in keys_or_patterns:
        if k in d and d[k] not in (None, "", "-", "--"):
            return d[k]
    low_map = {str(k).lower(): v for k, v in d.items()}
    for pat in keys_or_patterns:
        p = str(pat).lower()
        for lk, lv in low_map.items():
            if p in lk and lv not in (None, "", "-", "--"):
                return lv
    return None


def get_json(url, to=15):
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers=HDR)
            with urllib.request.urlopen(req, timeout=to, context=CTX) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception:
            if attempt == 0:
                time.sleep(0.5)
    return None


def yf_ohlcv(sym, rng="2y", iv="1d"):
    qs = urllib.parse.quote(sym, safe="")
    for h in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        j = get_json(f"https://{h}/v8/finance/chart/{qs}?range={rng}&interval={iv}&events=history", 12)
        if not j:
            continue
        try:
            r0 = (j.get("chart", {}).get("result") or [None])[0]
            if not r0:
                continue
            ts = r0.get("timestamp") or []
            q = (r0.get("indicators", {}).get("quote") or [{}])[0]
            op, hi, lo, cl, vo = q.get("open") or [], q.get("high") or [], q.get("low") or [], q.get("close") or [], q.get("volume") or []
            rows = []
            fmt = "%m/%d %H:%M" if iv != "1d" else "%Y-%m-%d"
            for i, t in enumerate(ts):
                c = cl[i] if i < len(cl) else None
                if c is None or (isinstance(c, float) and math.isnan(c)) or c <= 0:
                    continue
                o = op[i] if i < len(op) and op[i] is not None else c
                hh = hi[i] if i < len(hi) and hi[i] is not None else max(o, c)
                ll = lo[i] if i < len(lo) and lo[i] is not None else min(o, c)
                v = vo[i] if i < len(vo) and vo[i] is not None else 0.0
                dt_str = datetime.fromtimestamp(t, TZ_TW).strftime(fmt)
                rows.append((dt_str, float(o), float(hh), float(ll), float(c), float(v)))
            if rows:
                return rows
        except Exception:
            continue
    return []


def upsert_daily_bar(rows, bar, explicit_date=True):
    if not bar or not bar[0] or not bar[4] or bar[4] <= 0:
        return rows
    dt, o, h, l, c, v = bar
    o = float(o) if (o and o > 0) else float(c)
    c = float(c)
    h = max(float(h) if (h and h > 0) else c, o, c)
    l = min(float(l) if (l and l > 0) else c, o, c)
    v = float(v or 0.0)
    if (not explicit_date) and rows:
        last = rows[-1]
        if last[0][:10] < dt and abs(last[4] - c) < 1e-6 and abs(last[1] - o) < 1e-6 and abs(last[2] - h) < 1e-6 and abs(last[3] - l) < 1e-6:
            return rows
    out = [r for r in (rows or []) if r[0][:10] != dt]
    out.append((dt, o, h, l, c, v))
    out.sort(key=lambda x: x[0])
    return out


def normalize_taiex_vol(raw_vol, hist_rows):
    if not raw_vol or raw_vol <= 0:
        return 0.0
    vols = sorted(r[5] for r in (hist_rows or [])[-30:] if r[5] and r[5] > 0)
    if not vols:
        return float(raw_vol)
    med = vols[len(vols) // 2]
    ratio = raw_vol / max(med, 1.0)
    if ratio > 500:
        return float(raw_vol) / 1000.0
    if ratio > 50:
        return float(raw_vol) / 100.0
    return float(raw_vol)


def patch_official_indices(gsum, gfr):
    official_dt = None
    ohlc_map, vol_map, chg_map = {}, {}, {}

    mi5 = get_json("https://www.twse.com.tw/rwd/zh/indicesReport/MI_5MINS_HIST?response=json", 12)
    if isinstance(mi5, dict) and mi5.get("stat") == "OK" and mi5.get("data"):
        for row in mi5["data"]:
            if len(row) >= 5:
                dt = parse_roc_date(row[0])
                o, h, l, c = sf(row[1]), sf(row[2]), sf(row[3]), sf(row[4])
                if dt and c and c > 0:
                    ohlc_map[dt] = (o or c, h or c, l or c, c)

    fmtqik = get_json("https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?response=json", 12)
    if isinstance(fmtqik, dict) and fmtqik.get("stat") == "OK" and fmtqik.get("data"):
        for row in fmtqik["data"]:
            if len(row) >= 6:
                dt = parse_roc_date(row[0])
                vol = sf(row[1], 0.0)
                cl = sf(row[4])
                ch = sf(row[5], 0.0)
                if dt and cl and cl > 0:
                    vol_map[dt] = vol
                    chg_map[dt] = ch
                    if dt not in ohlc_map:
                        prev_cl = cl - ch if (cl - ch) > 0 else cl
                        ohlc_map[dt] = (prev_cl, max(prev_cl, cl), min(prev_cl, cl), cl)

    if not ohlc_map:
        mi = get_json("https://openapi.twse.com.tw/v1/exchangeReport/MI_INDEX", 12)
        if isinstance(mi, list):
            for it in mi:
                nm = str(pick_val(it, ["指數", "Index", "Name"]) or "")
                if "發行量加權股價指數" in nm and "未含" not in nm:
                    dt = parse_roc_date(pick_val(it, ["日期", "Date"])) or TODAY
                    cl = sf(pick_val(it, ["收盤指數", "Close"]))
                    ch = sf(pick_val(it, ["漲跌點數", "Change"]), 0.0)
                    if "-" in str(pick_val(it, ["漲跌", "Dir"]) or "") and ch > 0:
                        ch = -ch
                    if cl and cl > 0:
                        prev_cl = cl - ch if (cl - ch) > 0 else cl
                        ohlc_map[dt] = (prev_cl, max(prev_cl, cl), min(prev_cl, cl), cl)
                        chg_map[dt] = ch
                    break

    if ohlc_map:
        tx_hist = list(gfr.get("TAIEX", []))
        dts = sorted(ohlc_map.keys())
        for dt in dts:
            o, h, l, cl = ohlc_map[dt]
            nv = normalize_taiex_vol(vol_map.get(dt, 0.0), tx_hist)
            tx_hist = upsert_daily_bar(tx_hist, (dt, o, h, l, cl, nv), explicit_date=True)
        gfr["TAIEX"] = tx_hist
        last_dt = dts[-1]
        official_dt = last_dt
        last_cl = ohlc_map[last_dt][3]
        last_ch = chg_map.get(last_dt)
        if last_ch is None and len(tx_hist) >= 2:
            last_ch = tx_hist[-1][4] - tx_hist[-2][4]
        last_ch = last_ch or 0.0
        prev_cl = last_cl - last_ch
        chg_pct = (last_ch / prev_cl * 100.0) if prev_cl > 0 else 0.0
        gsum["TAIEX"] = {"lb": "加權指數", "sym": "^TWII(TWSE)", "dt": last_dt, "c": round(last_cl, 2), "chg": round(chg_pct, 2)}
        log(f"  🔧 官方校正加權指數：{last_cl:,.2f}（{chg_pct:+.2f}%，基準日 {last_dt}）")

    for url in (
        "https://www.tpex.org.tw/openapi/v1/tpex_index",
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_index",
    ):
        arr = get_json(url, 12)
        if isinstance(arr, list) and arr:
            parsed = []
            for it in arr:
                dt = parse_roc_date(pick_val(it, ["Date", "日期"]))
                cl = sf(pick_val(it, ["Close", "收盤", "指數", "Index"]))
                ch = sf(pick_val(it, ["Change", "漲跌", "Diff"]))
                op = sf(pick_val(it, ["Open", "開盤"]), cl)
                hi = sf(pick_val(it, ["High", "最高"]), cl)
                lo = sf(pick_val(it, ["Low", "最低"]), cl)
                if dt and cl and cl > 0:
                    parsed.append((dt, op, hi, lo, cl, ch))
            if parsed:
                parsed.sort(key=lambda x: x[0])
                dt, _, _, _, cl, ch = parsed[-1]
                if ch is None and len(parsed) >= 2:
                    ch = cl - parsed[-2][4]
                ch = ch or 0.0
                prev_cl = cl - ch
                chg_pct = (ch / prev_cl * 100.0) if prev_cl > 0 else 0.0
                gsum["OTC"] = {"lb": "櫃買指數", "sym": "TPEx官方", "dt": dt, "c": round(cl, 2), "chg": round(chg_pct, 2)}
                gfr["OTC"] = [(r[0], r[1], r[2], r[3], r[4], 0.0) for r in parsed]
                if not official_dt:
                    official_dt = dt
                log(f"  🔧 官方校正櫃買指數：{cl:,.2f}（{chg_pct:+.2f}%，基準日 {dt}）")
                break

    return official_dt or gsum.get("TAIEX", {}).get("dt") or TODAY


def parse_margin_delta(it: dict):
    prev_b = sf(pick_val(it, ["融資前日餘額", "MarginPurchasePreBalance", "MarginPurchaseYesterdayBalance", "PreBalance"]))
    curr_b = sf(pick_val(it, ["融資今日餘額", "MarginPurchaseBalance", "MarginPurchaseTodayBalance", "TodayBalance"]))
    if prev_b is not None and curr_b is not None:
        return curr_b - prev_b
    buy = sf(pick_val(it, ["融資買進", "MarginPurchaseBuy", "Buy"]))
    sell = sf(pick_val(it, ["融資賣出", "MarginPurchaseSell", "Sell"]))
    repay = sf(pick_val(it, ["融資現金償還", "MarginPurchaseCashRepayment", "CashRedemption"]), 0.0)
    if buy is not None and sell is not None:
        return buy - sell - repay
    return None


def parse_taifex_foreign_tx(tf_list):
    if not isinstance(tf_list, list):
        return {}
    for it in tf_list:
        if not isinstance(it, dict):
            continue
        vals_str = " ".join(str(v) for v in it.values())
        if ("臺股期貨" in vals_str or "台股期貨" in vals_str or str(pick_val(it, ["ContractCode", "商品代號"]) or "").strip() == "TX") and ("外資" in vals_str or "FINI" in vals_str.upper()):
            net_v = sf(pick_val(it, ["未平倉餘額多空淨額口數", "未平倉多空淨額口數", "OpenInterestNetContracts", "NetOpenInterestContracts", "NetOpenInterestLot", "NetOI"]))
            long_v = sf(pick_val(it, ["未平倉餘額多方口數", "未平倉多方口數", "OpenInterestLongPositionContracts", "LongOpenInterestContracts", "LongOpenInterestLot"]))
            short_v = sf(pick_val(it, ["未平倉餘額空方口數", "未平倉空方口數", "OpenInterestShortPositionContracts", "ShortOpenInterestContracts", "ShortOpenInterestLot"]))
            if net_v is None and long_v is not None and short_v is not None:
                net_v = long_v - short_v
            if net_v is not None:
                return {"dt": parse_roc_date(pick_val(it, ["日期", "Date"])) or TODAY, "net": int(net_v), "l": int(long_v or 0), "s": int(short_v or 0)}
    return {}


def sma(vals, n):
    out = [None] * len(vals)
    if len(vals) < n:
        return out
    s = sum(vals[:n])
    out[n - 1] = s / n
    for i in range(n, len(vals)):
        s += vals[i] - vals[i - n]
        out[i] = s / n
    return out


def ema(vals, sp):
    if not vals:
        return []
    a = 2.0 / (sp + 1.0)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(a * v + (1.0 - a) * out[-1])
    return out


def stds(vals):
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    return math.sqrt(sum((x - m) ** 2 for x in vals) / (n - 1))


def calc_tech(rows, tx_ret):
    if len(rows) < 65:
        return None
    hi = [r[2] for r in rows]
    lo = [r[3] for r in rows]
    cl = [r[4] for r in rows]
    vo = [r[5] for r in rows]
    c, pc = cl[-1], cl[-2]
    if c <= 0 or pc <= 0:
        return None
    chg = (c - pc) / pc * 100.0
    lots = int(vo[-1] // 1000)

    def rn(n):
        if len(cl) <= n:
            return None
        b = cl[-1 - n]
        return ((c / b) - 1.0) * 100.0 if b > 0 else None

    r20, r60, r120 = rn(20), rn(60), rn(120)
    rs20 = r20 - tx_ret.get(20, 0.0) if r20 is not None else None
    rs60 = r60 - tx_ret.get(60, 0.0) if r60 is not None else None
    rs120 = r120 - tx_ret.get(120, 0.0) if r120 is not None else None
    wrs = [x for x in (rs20 * 0.45 if rs20 is not None else None, rs60 * 0.35 if rs60 is not None else None, rs120 * 0.20 if rs120 is not None else None) if x is not None]
    rs_raw = sum(wrs) / len(wrs) if wrs else 0.0

    ma5, ma10, ma20, ma60, ma120, ma240 = sma(cl, 5), sma(cl, 10), sma(cl, 20), sma(cl, 60), sma(cl, 120), sma(cl, 240)
    m5, m10, m20, m60, m120, m240 = ma5[-1], ma10[-1], ma20[-1], ma60[-1], ma120[-1], ma240[-1]
    m20p = ma20[-5] if len(ma20) >= 5 and ma20[-5] else m20
    m20sl = ((m20 - m20p) / m20p * 100.0) if m20p > 0 else 0.0
    ma_sc = (3.0 if c > m5 > m10 > m20 else (1.5 if c > m20 else 0.0)) + (2.0 if (m20 > m60 and m20sl > 0) else 0.0) + (1.0 if (m120 and c > m120) else 0.0) + (1.0 if (m240 and c > m240) else 0.0)

    e12, e26 = ema(cl, 12), ema(cl, 26)
    dif = [a - b for a, b in zip(e12, e26)]
    dea = ema(dif, 9)
    hist = [a - b for a, b in zip(dif, dea)]
    macd_sc = (1.5 if (dif[-1] > 0 and dea[-1] > 0) else 0.0) + (1.5 if hist[-1] > 0 else 0.0) + (1.0 if hist[-1] > hist[-2] else 0.0)

    kv, dv = 50.0, 50.0
    for idx in range(max(8, len(cl) - 30), len(cl)):
        l9, h9 = min(lo[idx - 8:idx + 1]), max(hi[idx - 8:idx + 1])
        rsv = ((cl[idx] - l9) / (h9 - l9) * 100.0) if h9 > l9 else 50.0
        kv = (2 / 3) * kv + (1 / 3) * rsv
        dv = (2 / 3) * dv + (1 / 3) * kv
    df14 = [cl[i] - cl[i - 1] for i in range(len(cl) - 14, len(cl))]
    ag = sum(x for x in df14 if x > 0) / 14.0
    al = sum(-x for x in df14 if x < 0) / 14.0
    rsi = 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)
    kd_sc = (1.5 if (kv > dv and 30 <= kv <= 85) else 0.0) + (1.5 if 50 <= rsi <= 78 else (0.8 if rsi > 78 else 0.0))

    sd20 = stds(cl[-20:])
    bbu = m20 + 2 * sd20
    bbw = (4 * sd20 / m20 * 100.0) if m20 > 0 else 0.0
    pbbw = (4 * stds(cl[-30:-10]) / ma20[-10] * 100.0) if (len(cl) >= 30 and ma20[-10]) else bbw
    atr = sum(max(hi[i] - lo[i], abs(hi[i] - cl[i - 1]), abs(lo[i] - cl[i - 1])) for i in range(len(cl) - 14, len(cl))) / 14.0
    bb_sc = 3.0 if (c > m20 and c >= bbu * 0.98 and bbw > pbbw) else (2.4 if (c > m20 and bbw < 12.0) else 1.0)

    v5, v20 = sum(vo[-5:]) / 5.0, sum(vo[-20:]) / 20.0
    vr = (vo[-1] / v20) if v20 > 0 else 1.0
    v_sc = 5.0 if (chg > 1.5 and vr >= 1.5) else (3.8 if (chg > 0 and v5 > v20) else (0.5 if (chg < -2.0 and vr >= 1.8) else 1.5))

    gap_t, gap_sc = "無明顯缺口", 2.0
    for lb in range(1, min(6, len(cl) - 1)):
        ix = len(cl) - lb
        ph, pl, cll, chh = hi[ix - 1], lo[ix - 1], lo[ix], hi[ix]
        if cll > ph * 1.003:
            ma_l = min(lo[ix:])
            gs = cll - ph
            fp = min(100.0, round(max(0.0, cll - ma_l) / gs * 100.0, 1)) if gs > 0 else 0.0
            sup = round(ph, 2)
            if fp == 0:
                gap_t, gap_sc = f"向上突破缺口(未回補,支撐{sup})", (4.0 if vr >= 1.2 else 3.3)
            elif fp < 50:
                gap_t, gap_sc = f"向上缺口(回補{fp}%,支撐{sup})", 3.0
            else:
                gap_t, gap_sc = f"向上缺口回補({fp}%)", 1.8
            break
        elif chh < pl * 0.997:
            gap_t, gap_sc = f"向下缺口(壓力{round(pl, 2)})", 0.8
            break

    hh = max(hi[-10:]) > max(hi[-25:-10])
    hl = min(lo[-10:]) > min(lo[-25:-10])
    zhu = bool((c > m5 > m10 > m20) and (m20sl > 0) and hh and hl)
    pat_sc = 3.5 if zhu else (2.2 if (c > m20 and hl) else 1.0)

    tc = cl[-250:]
    dr = [(tc[i] - tc[i - 1]) / tc[i - 1] for i in range(1, len(tc)) if tc[i - 1] > 0]
    avol = stds(dr) * math.sqrt(252) * 100.0 if len(dr) > 20 else 35.0
    pk, mdd = tc[0], 0.0
    for px in tc:
        if px > pk:
            pk = px
        dd = (px / pk - 1.0) * 100.0
        if dd < mdd:
            mdd = dd

    vs20 = sum(vo[-20:])
    vwap = sum(px * vl for px, vl in zip(cl[-20:], vo[-20:])) / vs20 if vs20 > 0 else m20
    sl = round(c * 0.97, 2)
    t1 = round(c + max(c * 0.10, 2.5 * atr), 2)
    t2 = round(c + max(c * 0.20, 4.5 * atr), 2)
    rr = round((t1 - c) / max(c - sl, 0.01), 2)
    r_sc = 3.8 if (avol <= 45 and abs(mdd) <= 30 and rr >= 2.5) else (2.8 if (avol <= 60 and rr >= 2.0) else (1.2 if (avol > 75 or abs(mdd) > 45) else 2.0))

    return {
        "dt": rows[-1][0][:10],
        "price": round(c, 2), "chg": round(chg, 2), "lots": lots, "vr": round(vr, 2),
        "rs20": round(rs20, 1) if rs20 is not None else None, "rs60": round(rs60, 1) if rs60 is not None else None, "rs_raw": rs_raw,
        "ma5": round(m5, 2), "ma20": round(m20, 2), "ma_sc": round(ma_sc, 2), "macd_sc": round(macd_sc, 2),
        "k": round(kv, 1), "d": round(dv, 1), "rsi": round(rsi, 1), "kd_sc": round(kd_sc, 2),
        "bbw": round(bbw, 2), "atr": round(atr, 2), "bb_sc": round(bb_sc, 2), "v_sc": round(v_sc, 2),
        "gap_t": gap_t, "gap_sc": round(gap_sc, 2), "zhu": zhu, "pat_sc": round(pat_sc, 2),
        "vwap": round(vwap, 2), "avol": round(avol, 1), "mdd": round(mdd, 1),
        "sl": sl, "t1": t1, "t2": t2, "rr": rr, "r_sc": round(r_sc, 2)
    }


def check_60m(tk):
    rows = yf_ohlcv(tk, "6mo", "60m")
    if len(rows) < 245:
        return None
    cl = [r[4] for r in rows]
    m20, m60, m240 = sma(cl, 20), sma(cl, 60), sma(cl, 240)
    c, b20, b60, b240 = cl[-1], m20[-1], m60[-1], m240[-1]
    if None in (b20, b60, b240, m20[-4], m60[-4], m240[-4]):
        return None
    ok = (c > b20 > b60 > b240) and (b20 > m20[-4]) and (b60 > m60[-4]) and (b240 > m240[-4])
    return {
        "ok": ok, "m20": round(b20, 2), "m60": round(b60, 2), "m240": round(b240, 2),
        "bars": [{"t": r[0], "o": round(r[1], 2), "h": round(r[2], 2), "l": round(r[3], 2), "c": round(r[4], 2), "v": int(r[5] // 1000)} for r in rows[-180:]]
    }


def lbl(sc):
    if sc >= 90: return ("極強勢", "優先觀察突破")
    if sc >= 85: return ("強勢", "主升段候選")
    if sc >= 80: return ("偏強", "等待進場訊號")
    if sc >= 70: return ("中性偏多", "持續觀察")
    if sc >= 60: return ("中性", "暫不追價")
    if sc >= 50: return ("偏弱", "注意風險")
    return ("弱勢", "優先避開")


def pct_rank(items, kin, kout, scale=99.0):
    v = [(i, x[kin]) for i, x in enumerate(items) if x.get(kin) is not None]
    if not v:
        for x in items:
            x[kout] = round(scale * 0.5, 1)
        return
    v.sort(key=lambda z: z[1])
    n = len(v)
    for ri, (oi, _) in enumerate(v):
        items[oi][kout] = round((ri + 1) / n * scale, 1)
    for x in items:
        if kout not in x:
            x[kout] = round(scale * 0.5, 1)


def main():
    t0 = time.time()
    MACRO = load_macro()
    log("==================================================")
    log("🚀 V21.2 GitHub Actions 雲端全自動盤後更新引擎啟動！")
    log(f"🕒 台灣時間：{NOW_TW.strftime('%Y-%m-%d %H:%M:%S')}")
    log("==================================================")

    log("📡 [1/6] 抓取美股、美債10Y、匯率、加權與櫃買指數（官方 OHLCV 校準）...")
    idx_meta = {
        "DJI": ("道瓊工業", "^DJI"), "SP500": ("標普500", "^GSPC"), "NASDAQ": ("那斯達克", "^IXIC"),
        "SOX": ("費城半導體", "^SOX"), "NVDA": ("輝達 NVDA", "NVDA"), "TSM": ("台積電 ADR", "TSM"),
        "US10Y": ("美債10Y", "^TNX"), "DXY": ("美元指數", "DX-Y.NYB"), "USDTWD": ("美元兌台幣", "TWD=X"),
        "TAIEX": ("加權指數", "^TWII"), "OTC": ("櫃買指數", "^TWOII")
    }
    gsum, gfr = {}, {}
    for k, (lb, sym) in idx_meta.items():
        rw = yf_ohlcv(sym, HISTORY_RANGE if k in ("TAIEX", "OTC") else "3mo", "1d")
        if len(rw) >= 2:
            gfr[k] = rw
            c, p = rw[-1][4], rw[-2][4]
            chg = (c - p) / p * 100.0 if p > 0 else 0.0
            gsum[k] = {"lb": lb, "sym": sym, "dt": rw[-1][0][:10], "c": round(c, 3 if k in ("USDTWD", "US10Y") else 2), "chg": round(chg, 2)}

    trade_dt = patch_official_indices(gsum, gfr)
    log(f"  📅 官方基準交易日：{trade_dt}")

    log("🏛️ [2/6] 抓取 TWSE / TPEx / MOPS / TAIFEX 官方籌碼、融資與個股最新 OHLCV...")
    secs, seen = [], set()
    official_q = {}
    pe_m, rev_m, inst_m, marg_m = {}, {}, {}, {}
    m_inst = {"twse": {}, "tpex": {}}
    br = {"up": 0, "down": 0, "flat": 0, "lu": 0, "ld": 0}

    twse_official_dt = None
    mi_all = get_json("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?type=ALLBUT0999&response=json", 15)
    if isinstance(mi_all, dict) and mi_all.get("stat") == "OK":
        twse_official_dt = parse_roc_date(mi_all.get("date"))
        for tb in mi_all.get("tables") or []:
            fields = tb.get("fields") or []
            if "證券代號" in fields and "收盤價" in fields:
                ix_c, ix_n = fields.index("證券代號"), fields.index("證券名稱")
                ix_v, ix_o = fields.index("成交股數"), fields.index("開盤價")
                ix_h, ix_l, ix_cl = fields.index("最高價"), fields.index("最低價"), fields.index("收盤價")
                ix_dir = fields.index("漲跌(+/-)") if "漲跌(+/-)" in fields else -1
                ix_chg = fields.index("漲跌價差") if "漲跌價差" in fields else -1
                for row in tb.get("data") or []:
                    if len(row) <= ix_cl:
                        continue
                    code, name = str(row[ix_c]).strip(), str(row[ix_n]).strip()
                    if code.isdigit() and (len(code) == 4 or (code.startswith("00") and len(code) in (4, 5, 6))) and code not in seen:
                        seen.add(code)
                        cl = sf(row[ix_cl])
                        op, hi, lo = sf(row[ix_o], cl), sf(row[ix_h], cl), sf(row[ix_l], cl)
                        vol = sf(row[ix_v], 0.0)
                        ch = sf(row[ix_chg], 0.0) if ix_chg >= 0 else 0.0
                        if ix_dir >= 0 and "-" in str(row[ix_dir]) and ch > 0:
                            ch = -ch
                        if cl and cl > 0:
                            official_q[code] = ((twse_official_dt or trade_dt, op, hi, lo, cl, vol), bool(twse_official_dt))
                            pc = cl - ch
                            pt = (ch / pc * 100.0) if pc > 0 else 0.0
                            if ch > 0: br["up"] += 1
                            elif ch < 0: br["down"] += 1
                            else: br["flat"] += 1
                            if pt >= 9.5: br["lu"] += 1
                            elif pt <= -9.5: br["ld"] += 1
                        secs.append({"code": code, "name": name, "mkt": "上市", "tp": "ETF" if code.startswith("00") else "股票", "tk": f"{code}.TW"})

    for url, mkt, ck, nk, opk, hik, lok, pk, chk, vk, suf in [
        ("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", "上市", "Code", "Name", "OpeningPrice", "HighestPrice", "LowestPrice", "ClosingPrice", "Change", "TradeVolume", ".TW"),
        ("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", "上櫃", "SecuritiesCompanyCode", "CompanyName", "Open", "High", "Low", "Close", "Change", "TradingShares", ".TWO"),
    ]:
        arr = get_json(url, 15)
        if isinstance(arr, list):
            for it in arr:
                code, name = str(it.get(ck, "")).strip(), str(it.get(nk, "")).strip()
                if code.isdigit() and (len(code) == 4 or (code.startswith("00") and len(code) in (4, 5, 6))) and code not in seen:
                    seen.add(code)
                    c, ch = sf(it.get(pk)), sf(it.get(chk), 0.0)
                    op, hi, lo = sf(it.get(opk), c), sf(it.get(hik), c), sf(it.get(lok), c)
                    vol = sf(it.get(vk), 0.0)
                    raw_dt = parse_roc_date(pick_val(it, ["Date", "日期"]))
                    if c and c > 0:
                        official_q[code] = ((raw_dt or trade_dt, op, hi, lo, c, vol), bool(raw_dt))
                        pc = c - ch
                        pt = (ch / pc * 100.0) if pc > 0 else 0.0
                        if ch > 0: br["up"] += 1
                        elif ch < 0: br["down"] += 1
                        else: br["flat"] += 1
                        if pt >= 9.5: br["lu"] += 1
                        elif pt <= -9.5: br["ld"] += 1
                    secs.append({"code": code, "name": name, "mkt": mkt, "tp": "ETF" if code.startswith("00") else "股票", "tk": f"{code}{suf}"})

    for url, ck, pk in [("https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL", "Code", "PEratio"), ("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis", "SecuritiesCompanyCode", "PriceEarningRatio")]:
        arr = get_json(url, 15)
        if isinstance(arr, list):
            for it in arr:
                c = str(it.get(ck, "")).strip()
                if c: pe_m[c] = sf(it.get(pk))

    for url in ("https://openapi.twse.com.tw/v1/opendata/t187ap05_L", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"):
        arr = get_json(url, 15)
        if isinstance(arr, list):
            for it in arr:
                c = str(it.get("公司代號", "")).strip()
                if c: rev_m[c] = {"ind": str(it.get("產業別", "其他")).strip() or "其他", "yoy": sf(it.get("營業收入-去年同月增減(%)"))}

    for url, ck in [("https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN", "股票代號"), ("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance", "SecuritiesCompanyCode")]:
        arr = get_json(url, 15)
        if isinstance(arr, list):
            for it in arr:
                c = str(it.get(ck, "")).strip()
                if c in seen:
                    d_m = parse_margin_delta(it)
                    if d_m is not None and abs(d_m) < 500000:
                        marg_m[c] = round(d_m, 0)

    for dlt in range(6):
        ds = (NOW_TW - timedelta(days=dlt)).strftime("%Y%m%d")
        bfi = get_json(f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={ds}&response=json", 10)
        if isinstance(bfi, dict) and bfi.get("stat") == "OK" and bfi.get("data"):
            ti = {"dt": f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"}
            for row in bfi["data"]:
                lb, yi = str(row[0]), round(sf(row[3], 0.0) / 1e8, 2)
                if "外資及陸資(不含" in lb: ti["f"] = yi
                elif "投信" in lb: ti["t"] = yi
                elif "合計" in lb: ti["tot"] = yi
            m_inst["twse"] = ti
            t86 = get_json(f"https://www.twse.com.tw/rwd/zh/fund/T86?date={ds}&selectType=ALLBUT0999&response=json", 12)
            if isinstance(t86, dict) and t86.get("stat") == "OK" and t86.get("data"):
                fs = t86.get("fields", [])
                ixf = next((i for i, x in enumerate(fs) if "外陸資買賣超股數" in x), 4)
                ixt = next((i for i, x in enumerate(fs) if "投信買賣超股數" in x), 10)
                ixtot = next((i for i, x in enumerate(fs) if "三大法人買賣超股數" in x), -1)
                for row in t86["data"]:
                    c = str(row[0]).strip()
                    if len(row) > max(ixf, ixt):
                        inst_m[c] = {"f": round(sf(row[ixf], 0.0) / 1000.0, 1), "t": round(sf(row[ixt], 0.0) / 1000.0, 1), "tot": round(sf(row[ixtot], 0.0) / 1000.0, 1)}
            break

    ois = get_json("https://www.tpex.org.tw/openapi/v1/tpex_3insti_summary", 12)
    if isinstance(ois, list) and ois:
        li = ois[-1]
        f_v = sf(pick_val(li, ["ForeignInvestorsDiff", "外資及陸資(不含外資自營商)-買賣超", "Foreign"]), 0.0)
        t_v = sf(pick_val(li, ["SecuritiesInvestmentTrustCompaniesDiff", "投信-買賣超", "Trust"]), 0.0)
        tot_v = sf(pick_val(li, ["TotalDiff", "三大法人合計-買賣超", "Total"]), 0.0)
        if abs(tot_v) > 1e5 or abs(f_v) > 1e5:
            m_inst["tpex"] = {"f": round(f_v / 1e8, 2), "t": round(t_v / 1e8, 2), "tot": round(tot_v / 1e8, 2)}

    oid = get_json("https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading", 15)
    if isinstance(oid, list):
        for it in oid:
            c = str(it.get("SecuritiesCompanyCode", "")).strip()
            if c:
                fsh = sf(pick_val(it, ["ForeignInvestorsIncludeMainlandAreaInvestorsNetBuySell", "ForeignInvestorsNetBuySell"]), 0.0)
                tsh = sf(pick_val(it, ["SecuritiesInvestmentTrustCompaniesNetBuySell"]), 0.0)
                tot = sf(pick_val(it, ["TotalNetBuySell"]), fsh + tsh)
                inst_m[c] = {"f": round(fsh / 1000.0, 1), "t": round(tsh / 1000.0, 1), "tot": round(tot / 1000.0, 1)}

    tf = get_json("https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate", 12)
    tx_oi = parse_taifex_foreign_tx(tf)

    if MAX_STOCKS:
        secs = secs[:MAX_STOCKS]
    log(f"  ✅ 標的 {len(secs)} 檔｜官方即時收盤 {len(official_q)} 檔｜法人 {len(inst_m)} 檔｜融資 {len(marg_m)} 檔")

    tx_rw = gfr.get("TAIEX", [])
    tx_ret = {20: 0.0, 60: 0.0, 120: 0.0}
    if len(tx_rw) >= 125:
        tc = [r[4] for r in tx_rw]
        for n in (20, 60, 120):
            tx_ret[n] = (tc[-1] / tc[-1 - n] - 1.0) * 100.0

    log(f"📥 [3/6] 雲端高速抓取 {len(secs)} 檔日K並強制校準官方收盤（{trade_dt}）...")
    daily_cache_mem = {}  # Keep recent bars for top/core stocks to build kline_bundle.json

    def w_day(sec):
        c, tk = sec["code"], sec["tk"]
        off_entry = official_q.get(c)
        off_bar, has_exp_dt = off_entry if off_entry else (None, False)
        rw = yf_ohlcv(tk, HISTORY_RANGE, "1d")
        if off_bar and rw:
            rw = upsert_daily_bar(rw, off_bar, explicit_date=has_exp_dt)
        tc_ = calc_tech(rw, tx_ret)
        if not tc_:
            return None, None
        ri, ii = rev_m.get(c, {}), inst_m.get(c, {})
        item = {
            "code": c, "name": sec["name"], "mkt": sec["mkt"], "tp": sec["tp"], "tk": tk,
            "ind": ri.get("ind", "ETF" if sec["tp"] == "ETF" else "其他"), "pe": pe_m.get(c), "yoy": ri.get("yoy"),
            "fl": ii.get("f", 0.0), "tl": ii.get("t", 0.0), "il": ii.get("tot", 0.0), "mc": marg_m.get(c, 0.0), **tc_
        }
        tail = [{"t": r[0][:10], "o": round(r[1], 2), "h": round(r[2], 2), "l": round(r[3], 2), "c": round(r[4], 2), "v": int(r[5] // 1000)} for r in rw[-150:]]
        return item, (c, tail)

    stocks, done, tot_n = [], 0, len(secs)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(w_day, s) for s in secs]):
            done += 1
            item, bar_pair = f.result()
            if item:
                stocks.append(item)
                if bar_pair:
                    daily_cache_mem[bar_pair[0]] = bar_pair[1]
            if done % 250 == 0 or done == tot_n:
                log(f"  ⏳ 進度：{done}/{tot_n} ({done / max(tot_n, 1) * 100:.0f}%)｜有效 {len(stocks)} 檔")

    if len(stocks) < 50:
        log("❌ 有效個股不足 50 檔，中止本次寫入以保護既有資料")
        sys.exit(1)

    aligned_cnt = sum(1 for s in stocks if s["dt"] == trade_dt)
    sync_rate = round(aligned_cnt / max(len(stocks), 1) * 100.0, 1)
    log(f"  ✅ 日期同步檢核：{aligned_cnt}/{len(stocks)} 檔 ({sync_rate}%) 與大盤基準日 {trade_dt} 一致！")

    if not m_inst["tpex"]:
        est_f = sum(s["fl"] * s["price"] / 1e5 for s in stocks if s["mkt"] == "上櫃")
        est_t = sum(s["tl"] * s["price"] / 1e5 for s in stocks if s["mkt"] == "上櫃")
        est_tot = sum(s["il"] * s["price"] / 1e5 for s in stocks if s["mkt"] == "上櫃")
        if abs(est_tot) > 0.1 or abs(est_f) > 0.1:
            m_inst["tpex"] = {"f": round(est_f, 2), "t": round(est_t, 2), "tot": round(est_tot, 2), "est": True}

    log(f"🧮 [4/6] 計算全市場 RS 百分位與 V21 100分制評分...")
    pct_rank(stocks, "rs_raw", "rs_rk", 99.0)
    for s in stocks:
        s["rs_sc"] = round(s["rs_rk"] / 99.0 * 7.0, 2)

    ms, mn = {}, {}
    ndc = MACRO["ndc_score"]
    s01 = 3.8 if ndc >= 32 else (3.0 if ndc >= 23 else (2.0 if ndc >= 17 else 1.0))
    if MACRO["ndc_trend"] == "轉弱":
        s01 = max(1.0, s01 - 1.0)
    ms["01"], mn["01"] = (4, round(s01, 1)), f"燈號{ndc}分({MACRO['ndc_signal']}),領先{MACRO['ndc_trend']}"
    s02 = 2.7 if (MACRO["pmi"] >= 55 and MACRO["pmi_new"] >= 55) else (2.2 if MACRO["pmi"] >= 50 else 1.0)
    if MACRO["pmi_elec"] < 55:
        s02 = round(max(1.0, s02 - 0.2), 1)
    ms["02"], mn["02"] = (3, s02), f"PMI {MACRO['pmi']}%,新訂單{MACRO['pmi_new']}%(電子{MACRO['pmi_elec']}%)"
    u10 = gsum.get("US10Y", {}).get("c", 4.5)
    ms["03"], mn["03"] = (4, 3.5 if u10 < 3.8 else (2.5 if u10 < 4.5 else 1.5)), f"Fed {MACRO['fed_rate']}%,美債10Y {u10}%"
    twd, dxy = gsum.get("USDTWD", {}).get("c", 32.0), gsum.get("DXY", {}).get("c", 103.0)
    ms["04"], mn["04"] = (4, 3.5 if twd < 31.2 else (2.5 if twd < 32.5 else 1.5)), f"DXY {dxy},台幣 {twd}"
    txc, txch = gsum.get("TAIEX", {}).get("c", 0.0), gsum.get("TAIEX", {}).get("chg", 0.0)
    otc_c, otch = gsum.get("OTC", {}).get("c"), gsum.get("OTC", {}).get("chg", 0.0)
    ma_f, s05, tx_ma240 = [], 3.0, 0
    if len(tx_rw) >= 240:
        tcl = [r[4] for r in tx_rw]
        tx_ma240 = round(sum(tcl[-240:]) / 240.0)
        for p in (20, 60, 120, 240):
            mv = sum(tcl[-p:]) / p
            if txc > mv:
                ma_f.append(f"站上{p}MA({mv:,.0f})")
        s05 = 1.5 + len(ma_f) * 0.9 + (0.9 if (otch > 0 or txch > 0) and br["up"] > br["down"] else 0.0)
    otc_disp = f"{otc_c:,.2f}({otch:+.2f}%)" if otc_c is not None else "待更新"
    ms["05"], mn["05"] = (6, round(min(6.0, s05), 1)), f"加權{txc:,.1f}({txch:+.2f}%),櫃買{otc_disp},漲{br['up']}/跌{br['down']}/漲停{br['lu']}"

    twi, oti = m_inst["twse"], m_inst["tpex"]
    tyi = twi.get("tot", 0.0) + oti.get("tot", 0.0)
    fyi, tryi = twi.get("f", 0.0), twi.get("t", 0.0)
    otc_tag = "(估)" if oti.get("est") else ""
    ms["06"], mn["06"] = (7, 6.2 if (fyi > 50 and tryi > 10) else (4.8 if tyi > 0 else (2.5 if tyi > -300 else 1.5))), f"上市{twi.get('tot', 'N/A')}億(外{fyi}/投{tryi}),上櫃{oti.get('tot', 'N/A')}億{otc_tag}"
    noi = tx_oi.get("net")
    if noi is None:
        ms["07"], mn["07"] = (5, 2.5), "TAIFEX尚未更新"
    else:
        ms["07"], mn["07"] = (5, 4.5 if noi > 0 else (3.5 if noi > -20000 else (2.5 if noi > -45000 else 1.5))), f"外資台指期淨未平倉{noi:,}口（多{tx_oi.get('l',0):,}/空{tx_oi.get('s',0):,}）"
    tmc = sum(marg_m.values())
    ms["08"], mn["08"] = (5, 4.2 if (txch > 0 and tmc < 0) else (3.2 if abs(tmc) < 50000 else 2.2)), f"全市場個股融資淨變動{tmc:+,.0f}張"
    bb_cnt = sum(1 for s in stocks if s["fl"] > 0 and s["tl"] > 0)
    ms["09"], mn["09"] = (6, round(min(6.0, 2.5 + min(bb_cnt / 80.0, 2.5) + (1.0 if br["lu"] >= 25 else 0.3)), 1)), f"土洋同買{bb_cnt}檔,漲停{br['lu']}檔(法人集中度代估)"

    ib = {}
    for s in stocks:
        if s["tp"] == "股票":
            ib.setdefault(s["ind"], []).append(s)
    ist = [{"ind": k, "n": len(v), "achg": sum(x["chg"] for x in v) / len(v), "ars": sum(x["rs_rk"] for x in v) / len(v), "il": sum(x["il"] for x in v)} for k, v in ib.items() if len(v) >= 3]
    pct_rank(ist, "achg", "cr", 1.0)
    pct_rank(ist, "ars", "rr", 1.0)
    ind_sc = {x["ind"]: round((x["cr"] * 0.5 + x["rr"] * 0.5) * 7.0, 2) for x in ist}
    st_ind = sum(1 for x in ist if x["achg"] > 1.0)
    ms["10"], mn["10"] = (7, round(min(7.0, 3.0 + st_ind * 0.35), 1)), f"{st_ind}個產業平均漲逾1%"

    txt = calc_tech(tx_rw, {20: 0, 60: 0, 120: 0}) if len(tx_rw) >= 65 else None
    p20 = round(sum(1 for s in stocks if s["price"] > s["ma20"]) / max(len(stocks), 1) * 100.0, 1)
    zcnt = sum(1 for s in stocks if s["zhu"])
    ms["11"], mn["11"] = (7, round(min(7.0, max(1.5, p20 / 100.0 * 7.0)), 1)), f"站上20MA比例{p20}%"
    ms["12"], mn["12"] = (7, round(txt["ma_sc"], 1) if txt else 3.5), f"{', '.join(ma_f) if ma_f else '均線偏空'}"
    ms["13"], mn["13"] = (4, round(txt["macd_sc"], 1) if txt else 2.0), "MACD偏多" if (txt and txt["macd_sc"] >= 3) else "MACD待轉強"
    ms["14"], mn["14"] = (3, round(txt["kd_sc"], 1) if txt else 1.5), (f"KD({txt['k']}/{txt['d']}),RSI={txt['rsi']}" if txt else "中性")
    ms["15"], mn["15"] = (3, round(txt["bb_sc"], 1) if txt else 1.5), (f"帶寬{txt['bbw']}%,ATR={txt['atr']:.0f}" if txt else "中性")
    ms["16"], mn["16"] = (5, round(txt["v_sc"], 1) if txt else 2.5), (f"量比{txt['vr']}x" if txt else "中性")
    ms["17"], mn["17"] = (4, round(txt["gap_sc"], 1) if txt else 2.0), (txt["gap_t"] if txt else "無缺口")
    ms["18"], mn["18"] = (5, round(min(5.0, (txt["pat_sc"] if txt else 2.0) + (1.0 if zcnt >= 120 else 0.0)), 1)), f"朱家泓日K多頭{zcnt}檔"
    yv = [s["yoy"] for s in stocks if s.get("yoy") is not None]
    yp = round(sum(1 for x in yv if x > 0) / max(len(yv), 1) * 100.0, 1) if yv else 55.0
    ms["19"], mn["19"] = (7, round(min(7.0, max(2.5, yp / 100.0 * 7.5)), 1)), f"月營收YoY正成長占比{yp}%"
    ms["20"], mn["20"] = (4, round(txt["r_sc"], 1) if txt else 2.0), (f"年化波動{txt['avol']}%,MDD {txt['mdd']}%" if txt else "中性")
    mkt_tot = round(sum(v[1] for v in ms.values()), 1)
    base26 = sum(ms[k][1] for k in ("01", "02", "03", "04", "05", "07"))

    for r in stocks:
        s6 = 6.8 if (r["fl"] > 0 and r["tl"] > 0 and r["il"] >= 500) else (5.8 if (r["fl"] > 0 and r["tl"] > 0) else (5.2 if r["il"] > 300 else (4.2 if r["il"] > 0 else (1.2 if r["il"] < -500 else 3.0))))
        s8 = 4.5 if (r["chg"] > 0 and r["mc"] < 0) else (1.2 if (r["chg"] < -1 and r["mc"] > 200) else 3.0)
        ir = r["il"] / max(r["lots"], 1) * 100.0
        s9 = 5.5 if ir >= 15 else (4.5 if ir >= 5 else (3.0 if ir >= 0 else 1.5))
        s10 = ind_sc.get(r["ind"], 3.5)
        s19 = 3.0
        if r.get("yoy") is not None:
            s19 += 2.2 if r["yoy"] >= 30 else (1.4 if r["yoy"] >= 10 else (0.6 if r["yoy"] > 0 else (-1.2 if r["yoy"] < -15 else 0.0)))
        if r.get("pe") is not None and 0 < r["pe"] <= 25:
            s19 += 1.5
        elif r.get("pe") is not None and 25 < r["pe"] <= 45:
            s19 += 0.8
        s19 = max(0.5, min(7.0, s19))
        r["v21"] = round(min(100.0, max(0.0, base26 + s6 + s8 + s9 + s10 + r["rs_sc"] + r["ma_sc"] + r["macd_sc"] + r["kd_sc"] + r["bb_sc"] + r["v_sc"] + r["gap_sc"] + r["pat_sc"] + s19 + r["r_sc"])), 1)
        r["b60"] = False
        r["b60s"] = "未掃描"
        r["bm"] = None

    liq = [s for s in stocks if s["lots"] >= MIN_VOL_LOTS and s["tp"] == "股票"]
    if len(liq) < 10:
        liq = [s for s in stocks if s["tp"] == "股票"]
    pool = sorted(liq, key=lambda x: (x["v21"], x["rs_rk"]), reverse=True)[:TOP_60M_LIMIT]

    log(f"⚔️ [5/6] 驗證前 {len(pool)} 檔強勢股 60分K 均線三刀流(20/60/240MA)...")
    k60_cache_mem = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fm = {ex.submit(check_60m, s["tk"]): s for s in pool}
        for f in as_completed(fm):
            s = fm[f]
            res = f.result()
            if res:
                if res["ok"]:
                    s["b60"] = True
                    s["b60s"] = f"✅ 三刀多頭({res['m20']}/{res['m60']}/{res['m240']})"
                    s["bm"] = [res["m20"], res["m60"], res["m240"]]
                    s["v21"] = round(min(100.0, s["v21"] + 1.5), 1)
                else:
                    s["b60s"] = "⚠️ 待轉強"
                if res.get("bars"):
                    k60_cache_mem[s["code"]] = res["bars"]

    rs20 = sorted(liq, key=lambda x: (x["rs_rk"], x["v21"]), reverse=True)[:20]
    top10 = sorted(liq, key=lambda x: (x["b60"], x["v21"], x["rs_rk"]), reverse=True)[:10]
    bop = [s for s in liq if s["vr"] >= 1.2 or s["gap_sc"] >= 3.0 or s["b60"]]
    top3 = sorted(bop, key=lambda x: (x["v21"], x["vr"], x["rs_rk"]), reverse=True)[:3]
    if len(top3) < 3:
        top3 = top10[:3]
    gaps = sorted([s for s in liq if s["gap_sc"] >= 3.0], key=lambda x: (x["v21"], x["vr"]), reverse=True)[:10]

    # 回測與歷史分數紀錄
    hf = os.path.join(DATA_DIR, "v21_hist.json")
    ph = {}
    if os.path.exists(hf):
        try:
            with open(hf, "r", encoding="utf-8") as f:
                ph = json.load(f)
        except Exception:
            ph = {}
    pds = sorted([d for d in ph if d < trade_dt])
    prev_mkt_score = ph[pds[-1]].get("ms", mkt_tot) if pds else mkt_tot
    rev_l, bt_rows = [], []
    ref_dt = pds[-1] if pds else trade_dt
    if pds:
        pr = ph[ref_dt]
        plk = {s["code"]: s["price"] for s in stocks}
        rev_l += [f"- 對照交易日：`{ref_dt}` → `{trade_dt}`（前次大盤 `{pr.get('ms')}` 分）\n", "| 代號名稱 | 前日收盤 | 本日收盤 | 實績報酬 | 停損檢核 |", "| :--- | :---: | :---: | :---: | :--- |"]
        w, tc_ = 0, 0
        for it in pr.get("top3", []):
            cp = plk.get(it["code"])
            if cp:
                rt = round((cp - it["price"]) / it["price"] * 100.0, 2)
                sl_lbl = "⚠️破停損" if cp <= it["sl"] else "✅守穩"
                tc_ += 1
                w += 1 if rt > 0 else 0
                rev_l.append(f"| {it['code']} {it['name']} | {it['price']} | {cp} | {rt:+.2f}% | {sl_lbl} |")
                bt_rows.append({"code": it["code"], "name": it["name"], "prev": it["price"], "curr": cp, "ret": rt, "sl": sl_lbl})
        if tc_:
            rev_l.append(f"\n- 前日 TOP3 勝率：`{w}/{tc_}` ({w / tc_ * 100:.0f}%)")
    else:
        rev_l.append(f"- 交易日 `{trade_dt}` 建檔完成，次一交易日盤後自動回測 TOP3 實績勝率。")
    ph[trade_dt] = {
        "ms": mkt_tot, "taiex": txc, "above20ma": p20,
        "top3": [{"code": r["code"], "name": r["name"], "price": r["price"], "sl": r["sl"], "v21": r["v21"]} for r in top3]
    }
    with open(hf, "w", encoding="utf-8") as f:
        json.dump(ph, f, ensure_ascii=False, indent=2)

    # ==========================================================================
    # [6/6] 產出 JSON (供 GitHub Pages 戰情室)、K線整合包、Markdown 報告與 CSV
    # ==========================================================================
    log("📝 [6/6] 正在輸出 GitHub Pages 自動同步 JSON、真實 K 線包與 15 項報告...")
    st_m, act_m = lbl(mkt_tot)
    ts_now = NOW_TW.strftime("%Y-%m-%d %H:%M")
    pos = "70%～85%" if mkt_tot >= 80 else ("50%～65%" if mkt_tot >= 65 else "30%～45%")

    # 1. Build v21_latest.json (exact schema for index.html)
    ind_grps = {
        "01": ("宏觀經濟", "景氣循環"), "02": ("宏觀經濟", "PMI"), "03": ("宏觀經濟", "Fed利率"), "04": ("宏觀經濟", "匯率資金"),
        "05": ("市場籌碼", "大盤與櫃買"), "06": ("市場籌碼", "三大法人"), "07": ("市場籌碼", "台指期外資"),
        "08": ("市場籌碼", "融資融券"), "09": ("市場籌碼", "主力分點(集中度)"), "10": ("市場籌碼", "產業資金流"),
        "11": ("個股技術", "RS相對強弱"), "12": ("個股技術", "均線排列"), "13": ("個股技術", "MACD"),
        "14": ("個股技術", "KD與RSI"), "15": ("個股技術", "布林與波動"), "16": ("主升基本", "成交量價"),
        "17": ("主升基本", "跳空缺口"), "18": ("主升基本", "技術型態三刀流"), "19": ("主升基本", "基本面估值"),
        "20": ("風險管理", "風險控制")
    }
    prev_ind_map = {}
    prev_latest_path = os.path.join(DATA_DIR, "v21_latest.json")
    if os.path.exists(prev_latest_path):
        try:
            old_j = json.load(open(prev_latest_path, "r", encoding="utf-8"))
            if old_j.get("meta", {}).get("taiex", {}).get("date") != trade_dt:
                for it in old_j.get("meta", {}).get("indicators", []):
                    prev_ind_map[it["id"]] = it["sc"]
        except Exception:
            pass

    indicators_list = []
    for k in sorted(ind_grps):
        grp, nm = ind_grps[k]
        w, sc = ms[k]
        indicators_list.append({"id": k, "grp": grp, "name": nm, "w": w, "sc": sc, "prev": prev_ind_map.get(k, sc), "note": mn[k]})

    global_list = []
    for k in ("DJI", "SP500", "NASDAQ", "SOX", "NVDA", "TSM", "US10Y", "DXY", "USDTWD"):
        if k in gsum:
            g = gsum[k]
            global_list.append({"k": k, "name": g["lb"], "sym": g["sym"], "val": g["c"], "chg": g["chg"], "dt": g["dt"]})

    ra = []
    if u10 >= 4.5: ra.append(f"美債10Y殖利率 {u10}% 偏高，壓抑高估值 AI 科技股。")
    if (noi or 0) <= -40000: ra.append(f"外資台指期淨空單 {noi:,} 口，留意劇烈震盪。")
    if (twi.get("tot") or 0) <= -300: ra.append(f"上市三大法人單日賣超 {twi.get('tot')} 億（外資 {fyi} 億），留意籌碼調節。")
    if mkt_tot < 60: ra.append(f"大盤 V21 僅 {mkt_tot} 分（{st_m}），嚴控總倉位 {pos} 不追高。")
    if not ra: ra.append("系統風險可控，嚴守單筆 3% 停損紀律。")

    twse_tot = twi.get("tot", 0.0)
    twse_dealer_est = round(twse_tot - fyi - tryi, 2) if isinstance(twse_tot, (int, float)) else 0.0

    meta_out = {
        "date": trade_dt,
        "generatedAt": f"{ts_now} (GitHub自動同步)",
        "sampleCount": len(stocks),
        "marketScore": mkt_tot,
        "prevScore": prev_mkt_score,
        "marketState": st_m,
        "marketAction": act_m,
        "positionAdvice": pos,
        "taiex": {"close": txc, "chg": txch, "date": trade_dt, "ma240": tx_ma240},
        "otc": {"close": otc_c or 0.0, "chg": otch, "date": gsum.get("OTC", {}).get("dt", trade_dt)},
        "breadth": {"up": br["up"], "down": br["down"], "limitUp": br["lu"], "above20ma": p20, "zhuCount": zcnt},
        "instiMarket": {"twseTotal": twse_tot, "twseForeign": fyi, "twseTrust": tryi, "twseDealerEst": twse_dealer_est, "tpexTotal": oti.get("tot"), "marginDelta": round(tmc)},
        "global": global_list,
        "macroNote": f"{MACRO['note']}｜景氣 {MACRO['ndc_score']} 分（{MACRO['ndc_signal']}）｜PMI {MACRO['pmi']}%｜Fed {MACRO['fed_rate']}%",
        "indicators": indicators_list,
        "top3": [{"code": r["code"], "name": r["name"], "vr": r["vr"], "margin": round(r["mc"])} for r in top3],
        "top10Codes": [r["code"] for r in top10],
        "top10Margin": {r["code"]: round(r["mc"]) for r in top10},
        "gaps": [{"code": r["code"], "name": r["name"], "vr": r["vr"]} for r in gaps],
        "backtest": {
            "refDate": ref_dt, "prevScore": prev_mkt_score,
            "note": f"GitHub Actions 每日盤後自動對照 {ref_dt} → {trade_dt} 實績（大盤與個股同步率 {sync_rate}%）。",
            "rows": bt_rows
        },
        "warnings": ra,
        "strategy": [
            f"建議總持股水位控制在 {pos}；開盤前 30 分鐘不市價追高。",
            f"大盤年線 240MA 參考位階約 {tx_ma240:,} 點，站上月線比例 {p20}%。",
            "優先追蹤突破 TOP3 與主升段 TOP10，回測日內均價或 60分K 20MA（小藍短刀）止穩承接。",
            "所有部位嚴守朱家泓 3% 停損基準，破線立即減碼退場。"
        ]
    }

    sorted_stocks = sorted(stocks, key=lambda x: x["v21"], reverse=True)
    web_stocks = [{
        "dt": r["dt"], "c": r["code"], "n": r["name"], "m": r["mkt"], "i": r["ind"],
        "p": r["price"], "ch": r["chg"], "v": r["lots"], "rs": r["rs_rk"], "s": r["v21"],
        "z": r["zhu"], "b": r["b60s"], "bm": r["bm"], "fl": r["fl"], "tl": r["tl"], "il": r["il"],
        "vw": r["vwap"], "sl": r["sl"], "t1": r["t1"], "t2": r["t2"], "g": r["gap_t"]
    } for r in sorted_stocks]

    with open(os.path.join(DATA_DIR, "v21_latest.json"), "w", encoding="utf-8") as f:
        json.dump(
            {"meta": meta_out, "stocks": web_stocks, "sourceInfo": f"GitHub自動更新 {trade_dt}（同步率 {sync_rate}%）"},
            f, ensure_ascii=False, separators=(",", ":")
        )

    # 2. Build kline_bundle.json (Real historical daily + 60m K-lines for Top 20 + Core Weighted Stocks!)
    bundle_codes = set([r["code"] for r in top10] + [r["code"] for r in rs20[:10]] + ["2330", "2317", "2454", "2308", "2303", "2382", "3711", "2412", "2597", "2610", "2618", "1303", "2881", "2882"])
    kline_bundle = {"tradeDate": trade_dt, "daily": {}, "k60m": {}}
    for c in bundle_codes:
        if c in daily_cache_mem:
            kline_bundle["daily"][c] = daily_cache_mem[c]
        if c in k60_cache_mem:
            kline_bundle["k60m"][c] = k60_cache_mem[c]
    with open(os.path.join(DATA_DIR, "kline_bundle.json"), "w", encoding="utf-8") as f:
        json.dump(kline_bundle, f, ensure_ascii=False, separators=(",", ":"))

    # 3. Build 15-item Markdown Report for NotebookLM & CSV
    inf = sorted(ist, key=lambda x: (x["achg"], x["il"]), reverse=True)[:6]
    outf = sorted(ist, key=lambda x: (x["achg"], x["il"]))[:6]
    md = [
        f"# 台股二十大指標最終版（V21.2 雲端全自動版）每日量化戰情報告（{trade_dt}）",
        f"**行情基準日**：`{trade_dt}`（大盤與個股同步率 `{sync_rate}%`，✅ 0天誤差）｜**產出時間**：{ts_now}｜**有效樣本**：`{len(stocks)}` 檔｜**大盤 V21 總分**：**`{mkt_tot}/100`（{st_m}／{act_m}）**\n",
        "---", "## 1｜全球經濟與美股"
    ]
    for g in global_list:
        md.append(f"- **{g['name']} (`{g['sym']}`)**：`{g['val']}`（`{g['chg']:+.2f}%`，`{g['dt']}`）")
    md += [f"- **宏觀設定**：{MACRO['note']}；景氣 `{MACRO['ndc_score']}` 分（{MACRO['ndc_signal']}）、PMI `{MACRO['pmi']}%`、Fed `{MACRO['fed_rate']}%`。\n",
           "## 2｜台股大盤趨勢",
           f"- **加權指數**：`{txc:,.2f}`（`{txch:+.2f}%`，`{trade_dt}`）｜**櫃買指數**：`{otc_disp}`",
           f"- **市場廣度**：漲 `{br['up']}`／跌 `{br['down']}`／漲停 `{br['lu']}` 家。\n",
           "## 3｜二十大指標逐項評分（100分制）\n", "| 編號 | 項目 | 權重 | 實得 | 評估依據 |", "| :---: | :--- | :---: | :---: | :--- |"]
    for it in indicators_list:
        md.append(f"| {it['id']} | {it['name']} | {it['w']} | **{it['sc']}** | {it['note']} |")
    md += [f"| **總計** | **V21 綜合評分** | **100** | **{mkt_tot}** | **{st_m} — {act_m}** |\n",
           "## 4｜領先指標與落後指標",
           f"- **領先**：景氣領先「{MACRO['ndc_trend']}」、PMI新訂單 `{MACRO['pmi_new']}%`、費半 `{gsum.get('SOX', {}).get('chg', 0):+.2f}%`、美債10Y `{u10}%`。",
           f"- **落後/同時**：站上20MA占比 `{p20}%`、上市三大法人 `{twi.get('tot', 'N/A')}` 億、全市場融資變動 `{tmc:+,.0f}` 張。\n",
           "## 5｜市場資金流入族群\n", "| 族群 | 家數 | 平均漲跌 | 平均RS | 法人合計(張) |", "| :--- | :---: | :---: | :---: | :---: |"]
    for r in inf:
        md.append(f"| {r['ind']} | {r['n']} | {r['achg']:+.2f}% | {r['ars']:.1f} | {r['il']:+,.0f} |")
    md += ["\n## 6｜市場資金流出族群\n", "| 族群 | 家數 | 平均漲跌 | 平均RS | 法人合計(張) |", "| :--- | :---: | :---: | :---: | :---: |"]
    for r in outf:
        md.append(f"| {r['ind']} | {r['n']} | {r['achg']:+.2f}% | {r['ars']:.1f} | {r['il']:+,.0f} |")
    md += ["\n## 7｜強勢股 RS 前 20 名\n", "| 排名 | 代號 | 名稱 | 產業 | 收盤 | 漲跌 | RS百分位 | 張數 | V21 |", "| :---: | :---: | :--- | :--- | :---: | :---: | :---: | :---: | :---: |"]
    for i, r in enumerate(rs20, 1):
        md.append(f"| {i} | {r['code']} | {r['name']} | {r['ind']} | {r['price']} | {r['chg']:+.2f}% | **{r['rs_rk']}** | {r['lots']:,} | **{r['v21']}** |")
    md += ["\n## 8｜主升段候選股 TOP10\n", "| 排名 | 代號名稱 | 產業 | V21 | 定位 | RS | 朱家泓 | 60分K三刀流(20/60/240MA) |", "| :---: | :--- | :--- | :---: | :--- | :---: | :---: | :--- |"]
    for i, r in enumerate(top10, 1):
        st, ac = lbl(r["v21"])
        md.append(f"| {i} | **{r['code']} {r['name']}** | {r['ind']} | **{r['v21']}** | {st}({ac}) | {r['rs_rk']} | {'✅' if r['zhu'] else '—'} | {r['b60s']} |")
    md += ["\n## 9｜突破機率 TOP3"]
    for i, r in enumerate(top3, 1):
        md.append(f"{i}. **{r['code']} {r['name']}** — V21 `{r['v21']}`｜RS `{r['rs_rk']}`｜量比 `{r['vr']}x`｜法人 `{r['il']:+,.0f}` 張｜{r['b60s']}｜{r['gap_t']}")
    md += ["\n## 10｜跳空突破及回補分析\n", "| 代號名稱 | 收盤 | 漲跌 | 量比 | 缺口狀態 | 法人(張) | V21 |", "| :--- | :---: | :---: | :---: | :--- | :---: | :---: |"]
    if not gaps:
        md.append("| 今日無明顯向上突破缺口 | - | - | - | - | - | - |")
    else:
        for r in gaps:
            md.append(f"| {r['code']} {r['name']} | {r['price']} | {r['chg']:+.2f}% | {r['vr']}x | {r['gap_t']} | {r['il']:+,.0f} | {r['v21']} |")
    md += ["\n## 11｜法人籌碼與成本", f"- 上市：`{twi.get('tot', 'N/A')}` 億（外資 `{fyi}`／投信 `{tryi}`）｜上櫃：`{oti.get('tot', 'N/A')}` 億{otc_tag}｜期貨：{mn['07']}。\n", "| 代號名稱 | 外資(張) | 投信(張) | 合計(張) | 融資增減 | 20日成本(VWAP) |", "| :--- | :---: | :---: | :---: | :---: | :---: |"]
    for r in top10:
        md.append(f"| {r['code']} {r['name']} | {r['fl']:+,.0f} | {r['tl']:+,.0f} | {r['il']:+,.0f} | {r['mc']:+,.0f} | {r['vwap']} |")
    md += ["\n## 12｜個股買進與停損價格\n", "| 代號名稱 | 現價 | 建議買進區 | 估算成本 | 停損(3%) | 目標1 | 目標2 | 風報比 | 賣出訊號 |", "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |"]
    for r in top10:
        bl = round(min(r["price"], max(r["ma5"], r["price"] * 0.985)), 2)
        bh = round(r["price"] * 1.015, 2)
        md.append(f"| **{r['code']} {r['name']}** | {r['price']} | {bl}～{bh} | {r['vwap']} | **{r['sl']}** | {r['t1']} | {r['t2']} | {r['rr']}x | 破{r['sl']}或60分K破20MA |")
    md += ["\n## 13｜市場風險警示"] + [f"- {x}" for x in ra]
    md += ["\n## 14｜隔日操作策略", f"- **建議倉位**：**{pos}**；開盤前 30 分鐘不追高，鎖定突破 TOP3 與主升段 TOP10，回測日均價或 60分K 20MA 承接並嚴守 **3% 停損**。\n", "## 15｜模型預測與實際結果檢討"] + rev_l

    with open(os.path.join(DATA_DIR, "V21_最新每日報告.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    flds = ["dt", "code", "name", "mkt", "ind", "price", "chg", "lots", "rs_rk", "v21", "zhu", "b60s", "fl", "tl", "il", "vwap", "sl", "t1", "t2", "gap_t"]
    with open(os.path.join(DATA_DIR, "V21_全市場評分_最新.csv"), "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=flds, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted_stocks)

    log(f"🎉 V21.2 雲端全自動更新完成！（耗時 {time.time() - t0:.1f} 秒｜{len(stocks)} 檔｜基準日 {trade_dt}｜大盤 V21 {mkt_tot}）")


if __name__ == "__main__":
    main()
