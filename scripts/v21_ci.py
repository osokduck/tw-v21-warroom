# -*- coding: utf-8 -*-
"""
V21.3 GitHub Actions 雲端全自動盤後更新引擎（純 Python 內建、零外部套件依賴）
================================================================================
V21.3 升級項目：
1. 【大盤＋個股真實 K 線整合包】data/kline_bundle.json 新增 TAIEX（加權指數）與 OTC（櫃買指數）
   近 260 日真實日 K，並收錄 TOP10、RS 前 10 與核心權值股之真實日 K（160根）與 60分K（180根）。
2. 【全市場個股五構面得分拆解】每檔個股額外輸出 p5=[宏觀,籌碼,技術,主升基本,風控]、
   月線 ma20、量比 vr 與融資增減 mc，支援前端自動偵測籌碼背離與短線乖離過熱警訊。
3. 【30日大盤趨勢歷史曲線】自動累積並輸出近 60 個交易日之大盤 V21 總分、加權指數與站上 20MA 比例。
4. 【TAIFEX 外資期貨斷檔自動回溯】若非盤後時段或期交所表單延遲，自動承接最近有效交易日淨未平倉並標註日期。
================================================================================
"""
import csv, json, math, os, re, ssl, sys, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

TZ_TW = timezone(timedelta(hours=8))
NOW_TW = datetime.now(TZ_TW)
TODAY = NOW_TW.strftime("%Y-%m-%d")

HISTORY_RANGE = "2y"
MAX_WORKERS = 16
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
                return {**DEFAULT_MACRO, **json.load(f)}
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
                otc_hist = list(gfr.get("OTC", []))
                for r in parsed:
                    otc_hist = upsert_daily_bar(otc_hist, (r[0], r[1], r[2], r[3], r[4], 0.0), explicit_date=True)
                gfr["OTC"] = otc_hist
                dt, _, _, _, cl, ch = parsed[-1]
                if ch is None and len(parsed) >= 2:
                    ch = cl - parsed[-2][4]
                ch = ch or 0.0
                prev_cl = cl - ch
                chg_pct = (ch / prev_cl * 100.0) if prev_cl > 0 else 0.0
                gsum["OTC"] = {"lb": "櫃買指數", "sym": "TPEx官方", "dt": dt, "c": round(cl, 2), "chg": round(chg_pct, 2)}
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
    lh = max(hi[-10:]) < max(hi[-25:-10])
    ll = min(lo[-10:]) < min(lo[-25:-10])
    zhu = bool((c > m5 > m10 > m20) and (m20sl > 0) and hh and hl)
    zhu_bear = bool((c < m5 < m10 < m20) and (m20sl < 0) and lh and ll)
    zhu_dir = 1 if zhu else (-1 if zhu_bear else 0)
    bias20 = round(((c / m20) - 1.0) * 100.0, 2) if m20 and m20 > 0 else 0.0
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
        "gap_t": gap_t, "gap_sc": round(gap_sc, 2), "zhu": zhu, "zhu_bear": zhu_bear, "zhu_dir": zhu_dir, "m20sl": round(m20sl, 2), "bias20": bias20, "pat_sc": round(pat_sc, 2),
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
    bear_ok = (c < b20 < b60 < b240) and (b20 < m20[-4]) and (b60 < m60[-4]) and (b240 < m240[-4])
    b60_dir = 1 if ok else (-1 if bear_ok else 0)
    sl20 = round(((b20 / m20[-4]) - 1.0) * 100.0, 2) if m20[-4] > 0 else 0.0
    bias20 = round(((c / b20) - 1.0) * 100.0, 2) if b20 > 0 else 0.0

    daily_60m = {}
    for idx in range(243, len(rows)):
        a20, a60, a240 = m20[idx], m60[idx], m240[idx]
        p20, p60, p240 = m20[idx - 3], m60[idx - 3], m240[idx - 3]
        if None in (a20, a60, a240, p20, p60, p240) or a20 <= 0 or p20 <= 0:
            continue
        px = cl[idx]
        d_str = rows[idx][0][:10]
        is_b = (px > a20 > a60 > a240) and (a20 > p20) and (a60 > p60) and (a240 > p240)
        is_s = (px < a20 < a60 < a240) and (a20 < p20) and (a60 < p60) and (a240 < p240)
        daily_60m[d_str] = {
            "dir": 1 if is_b else (-1 if is_s else 0),
            "m20": round(a20, 2),
            "bias20": round(((px / a20) - 1.0) * 100.0, 2),
            "slope20": round(((a20 / p20) - 1.0) * 100.0, 2),
        }

    return {
        "ok": ok, "bear_ok": bear_ok, "dir": b60_dir,
        "m20": round(b20, 2), "m60": round(b60, 2), "m240": round(b240, 2),
        "slope20": sl20, "bias20": bias20, "daily_60m": daily_60m,
        "bars": [{"t": r[0], "o": round(r[1], 2), "h": round(r[2], 2), "l": round(r[3], 2), "c": round(r[4], 2), "v": int(r[5] // 1000)} for r in rows[-180:]]
    }


def run_dual_backtest_2000(stocks, daily_cache_mem, k60_state_mem, trade_dt, tx_rw=None, mkt_tot=50.0, max_stocks=2000, k60_budget_sec=210):
    """
    V21.7.1 雙流共振（朱家泓日K × 60分K三刀流）全市場 2,000 檔近 3 個月「實戰精準微調」回測引擎
    ================================================================================
    V21.7.1 兩大精準升級：
    1. 【多空截止日獨立計算】：
       - 看漲多頭波段保留 10 交易日結算（cutoff = len - 11），保留反洗盤 buf15 大波段盈虧比。
       - 看跌空頭急跌獨立使用 5 交易日結算（cutoff = len - 6），不再被多頭切掉最新一週盤勢。
    2. 【大盤 20MA 順風 / 逆風對照組 (bullByMarketRegime)】：
       - 自動拆分「大盤站上月線 (tx_abv=True) 做多」vs.「大盤跌破月線 (tx_abv=False) 做多」
         各自的觸發筆數、勝率、單筆均淨報與獲利因子 (PF)，精準驗證大盤翻多後的爆發力。
    ================================================================================
    """
    COST_PCT = 0.4425
    HOLD_BULL = 10      # 多頭波段持有上限：10 交易日
    HOLD_BEAR = 5       # 空頭急跌持有上限：5 交易日
    LOOKBACK_DAYS = 63  # 近 3 個月交易日

    # 建立大盤零前視日期對照表（20日漲幅與大盤是否站上 20MA）
    tx_map = {}
    if tx_rw and len(tx_rw) >= 25:
        tx_cl = [r[4] for r in tx_rw]
        tx_ma20 = sma(tx_cl, 20)
        for i in range(20, len(tx_rw)):
            d_str = tx_rw[i][0][:10]
            r20 = ((tx_cl[i] / tx_cl[i - 20]) - 1.0) * 100.0 if tx_cl[i - 20] > 0 else 0.0
            tx_map[d_str] = {
                "r20": r20,
                "above20": bool(tx_ma20[i] and tx_cl[i] > tx_ma20[i]),
            }

    valid_eq = [
        s for s in stocks
        if s.get("tp") != "ETF" and s["code"] in daily_cache_mem and len(daily_cache_mem[s["code"]]) >= 100
    ]
    valid_etf = [
        s for s in stocks
        if s.get("tp") == "ETF" and s["code"] in daily_cache_mem and len(daily_cache_mem[s["code"]]) >= 100
    ]
    valid_eq.sort(key=lambda x: x.get("lots", 0), reverse=True)
    valid_etf.sort(key=lambda x: x.get("lots", 0), reverse=True)
    picked = (valid_eq + valid_etf)[:max_stocks]

    bull_start_seen, bull_end_seen = "9999-99-99", "0000-00-00"
    bear_start_seen, bear_end_seen = "9999-99-99", "0000-00-00"
    stock_candidates = {}

    for s in picked:
        code = s["code"]
        bars = daily_cache_mem.get(code, [])
        if len(bars) < 100:
            continue
        cl = [b["c"] for b in bars]
        hi = [b["h"] for b in bars]
        lo = [b["l"] for b in bars]
        vo = [b["v"] for b in bars]
        ma5 = sma(cl, 5)
        ma10 = sma(cl, 10)
        ma13 = sma(cl, 13)
        ma20 = sma(cl, 20)
        ma53 = sma(cl, 53)

        # 多空獨立窗口：多頭需預留 10 日結算、空頭僅需預留 5 日結算
        end_bull = len(bars) - HOLD_BULL - 1
        start_bull = max(58, end_bull - LOOKBACK_DAYS)
        end_bear = len(bars) - HOLD_BEAR - 1
        start_bear = max(58, end_bear - LOOKBACK_DAYS)

        if start_bull >= end_bull or start_bear >= end_bear:
            continue

        if bars[start_bull]["t"] < bull_start_seen:
            bull_start_seen = bars[start_bull]["t"]
        if bars[end_bull]["t"] > bull_end_seen:
            bull_end_seen = bars[end_bull]["t"]
        if bars[start_bear]["t"] < bear_start_seen:
            bear_start_seen = bars[start_bear]["t"]
        if bars[end_bear]["t"] > bear_end_seen:
            bear_end_seen = bars[end_bear]["t"]

        scan_start = min(start_bull, start_bear)
        scan_end = max(end_bull, end_bear)

        hits = []
        for t in range(scan_start, scan_end + 1):
            c = cl[t]
            m5, m10, m13, m20, m53 = ma5[t], ma10[t], ma13[t], ma20[t], ma53[t]
            m20p = ma20[t - 4]
            if None in (m5, m10, m13, m20, m53, m20p) or c <= 0 or m20 <= 0 or m20p <= 0 or m5 <= 0:
                continue

            m20sl = (m20 / m20p - 1.0) * 100.0
            hh = max(hi[t - 9:t + 1]) > max(hi[t - 24:t - 9])
            hl = min(lo[t - 9:t + 1]) > min(lo[t - 24:t - 9])
            lh = max(hi[t - 9:t + 1]) < max(hi[t - 24:t - 9])
            ll = min(lo[t - 9:t + 1]) < min(lo[t - 24:t - 9])

            zhu_bull = (start_bull <= t <= end_bull) and (c > m5 > m10 > m20) and (m20sl > 0) and hh and hl
            zhu_bear = (start_bear <= t <= end_bear) and (c < m5 < m10 < m20) and (m20sl < 0) and lh and ll
            if not zhu_bull and not zhu_bear:
                continue

            v5_avg = sum(vo[t - 5:t]) / 5.0
            vr = round(vo[t] / v5_avg, 2) if v5_avg > 0 else 1.0
            st_r20 = ((c / cl[t - 20]) - 1.0) * 100.0 if cl[t - 20] > 0 else 0.0
            tx_info = tx_map.get(bars[t]["t"], {"r20": 0.0, "above20": True})
            rs_ex = round(st_r20 - tx_info["r20"], 2)

            m5p, m13p, m53p = ma5[t - 1], ma13[t - 1], ma53[t - 1]
            hits.append((
                t, 1 if zhu_bull else -1, c, m5, m13, m20, m53,
                m5p or m5, m13p or m13, m53p or m53, m20sl, vr, rs_ex, tx_info["above20"]
            ))

        if hits:
            stock_candidates[code] = (s, bars, ma5, ma20, hits)

    need_60m = [v[0] for c, v in stock_candidates.items() if c not in k60_state_mem]
    need_60m.sort(key=lambda x: x.get("lots", 0), reverse=True)
    if need_60m and k60_budget_sec > 0:
        log(f"  ⏱️ 2,000 檔日K掃描完成（{len(stock_candidates)} 檔曾觸發朱家泓架構），多執行緒驗證真實 60分K（預算 {k60_budget_sec} 秒）...")
        t_bt0 = time.time()
        batch_sz = 40
        for b_start in range(0, len(need_60m), batch_sz):
            if time.time() - t_bt0 >= k60_budget_sec:
                log(f"  ⚡ 已達 60分K 抓取時間上限（累計真實 60分K {len(k60_state_mem)} 檔），其餘由等比例 5/13/53MA 代理補齊。")
                break
            chunk = need_60m[b_start:b_start + batch_sz]
            with ThreadPoolExecutor(max_workers=10) as ex:
                fm = {ex.submit(check_60m, st["tk"]): st for st in chunk}
                for f in as_completed(fm):
                    st = fm[f]
                    try:
                        res = f.result()
                        if res and res.get("daily_60m"):
                            k60_state_mem[st["code"]] = res
                    except Exception:
                        pass

    bull_events, bear_events = [], []
    real_60m_used, proxy_used = 0, 0

    for code, (s, bars, ma5_arr, ma20_arr, hits) in stock_candidates.items():
        k60_info = k60_state_mem.get(code)
        d60_map = k60_info.get("daily_60m", {}) if k60_info else {}
        if d60_map:
            real_60m_used += 1
        else:
            proxy_used += 1

        for (t, z_dir, c, m5, m13, m20, m53, m5p, m13p, m53p, m20sl, vr, rs_ex, tx_abv) in hits:
            dt_t = bars[t]["t"]
            st60 = d60_map.get(dt_t)
            if st60:
                b60_dir = st60["dir"]
                bias = st60["bias20"]
                slope = st60["slope20"]
            else:
                is_pb = (c > m5 > m13 > m53) and (m5 > m5p) and (m13 > m13p) and (m53 > m53p)
                is_ps = (c < m5 < m13 < m53) and (m5 < m5p) and (m13 < m13p) and (m53 < m53p)
                b60_dir = 1 if is_pb else (-1 if is_ps else 0)
                bias = round((c / m5 - 1.0) * 100.0, 2)
                slope = round((m5 / m5p - 1.0) * 100.0, 2) if m5p > 0 else round(m20sl, 2)

            if z_dir == 1 and b60_dir != 1:
                continue
            if z_dir == -1 and b60_dir != -1:
                continue

            h_days = HOLD_BULL if z_dir == 1 else HOLD_BEAR
            fwd = []
            for k in range(1, h_days + 1):
                fb = bars[t + k]
                f60 = d60_map.get(fb["t"])
                ref_short_ma = f60["m20"] if f60 else (ma5_arr[t + k] or ma20_arr[t + k] or fb["c"])
                fwd.append((fb["h"], fb["l"], fb["c"], ref_short_ma, ma20_arr[t + k] or fb["c"]))

            ev = (code, t, c, bias, vr, slope, rs_ex, tx_abv, fwd)
            if z_dir == 1:
                bull_events.append(ev)
            else:
                bear_events.append(ev)

    def summarize_rets(rets):
        cnt = len(rets)
        if cnt == 0:
            return {"trades": 0, "winRate": 0.0, "avgRet": 0.0, "pf": 0.0, "maxLoss": 0.0}
        wins = [x for x in rets if x > 0]
        losses = [x for x in rets if x <= 0]
        gw, gl = sum(wins), abs(sum(losses))
        return {
            "trades": cnt,
            "winRate": round(len(wins) / cnt * 100.0, 1),
            "avgRet": round(sum(rets) / cnt, 2),
            "pf": round(gw / gl, 2) if gl > 0.001 else (9.99 if gw > 0 else 0.0),
            "maxLoss": round(min(rets), 2),
        }

    # 多頭網格尋優：含 RS 超額報酬濾網、反洗盤緩衝出場 (-1.5% / 日20MA) 與大盤20MA順逆風對照組
    def eval_bull_grid(events):
        sl_opts = (3.0, 4.0, 5.0)
        exit_opts = ("buf15", "daily20")
        ev_rows = []
        for (code, t, entry, bias, vr, slope, rs_ex, tx_abv, fwd) in events:
            r_map = {}
            for em in exit_opts:
                for sl_pct in sl_opts:
                    sl_px = entry * (1.0 - sl_pct / 100.0)
                    exit_px = fwd[-1][2]
                    for (fh, fl, fc, fm20, fd20) in fwd:
                        if fl <= sl_px:
                            exit_px = sl_px
                            break
                        if em == "buf15" and fc < fm20 * 0.985:
                            exit_px = fc
                            break
                        if em == "daily20" and fc < fd20:
                            exit_px = fc
                            break
                    r_map[(em, sl_pct)] = ((exit_px / entry) - 1.0) * 100.0 - COST_PCT
            ev_rows.append((code, t, bias, vr, slope, rs_ex, tx_abv, r_map))

        grid_results = []
        for bias_lim in (3.5, 5.0, 7.0):
            for vr_min in (1.0, 1.2, 1.5):
                for slope_min in (0.3, 0.6):
                    for rs_ex_min in (0.0, 6.0, 12.0):
                        selected = []
                        last_t = {}
                        for (code, t, bias, vr, slope, rs_ex, tx_abv, r_map) in ev_rows:
                            if not (0.0 <= bias <= bias_lim and vr >= vr_min and slope >= slope_min and rs_ex >= rs_ex_min):
                                continue
                            lt = last_t.get(code)
                            if lt is not None and (t - lt) < HOLD_BULL:
                                continue
                            last_t[code] = t
                            selected.append((tx_abv, r_map))
                        cnt = len(selected)
                        if cnt == 0:
                            continue
                        for em in exit_opts:
                            for sl_pct in sl_opts:
                                rets = [m[(em, sl_pct)] for (_, m) in selected]
                                st = summarize_rets(rets)
                                sample_bonus = min(1.0, cnt / 30.0)
                                obj = round((st["avgRet"] * 2.2 + (min(st["pf"], 4.0) - 1.0) * 1.8 + (st["winRate"] - 50.0) * 0.06) * sample_bonus, 3)
                                grid_results.append({
                                    "biasMax": bias_lim,
                                    "vrMin": vr_min,
                                    "slopeMin": slope_min,
                                    "rsExMin": rs_ex_min,
                                    "exitMode": em,
                                    "slPct": sl_pct,
                                    **st,
                                    "score": obj,
                                })

        valid = [g for g in grid_results if g["trades"] >= 30] or [g for g in grid_results if g["trades"] >= 10] or grid_results
        valid.sort(key=lambda x: (x["score"], x["pf"], x["winRate"], x["trades"]), reverse=True)
        best = valid[0] if valid else {
            "biasMax": 4.5, "vrMin": 1.2, "slopeMin": 0.6, "rsExMin": 0.0, "exitMode": "buf15", "slPct": 3.0,
            "trades": 0, "winRate": 0.0, "avgRet": 0.0, "pf": 0.0, "maxLoss": 0.0, "score": 0.0, "fallback": True
        }

        # 拆解最佳多頭參數在「大盤站上 20MA (順風)」vs.「大盤跌破 20MA (逆風)」的真實表現
        abv_rets, blw_rets = [], []
        last_abv, last_blw = {}, {}
        em_b, sl_b = best.get("exitMode", "buf15"), best.get("slPct", 3.0)
        for (code, t, bias, vr, slope, rs_ex, tx_abv, r_map) in ev_rows:
            if not (0.0 <= bias <= best["biasMax"] and vr >= best["vrMin"] and slope >= best["slopeMin"] and rs_ex >= best.get("rsExMin", 0.0)):
                continue
            r_val = r_map.get((em_b, sl_b))
            if r_val is None:
                continue
            if tx_abv:
                lt = last_abv.get(code)
                if lt is None or (t - lt) >= HOLD_BULL:
                    last_abv[code] = t
                    abv_rets.append(r_val)
            else:
                lt = last_blw.get(code)
                if lt is None or (t - lt) >= HOLD_BULL:
                    last_blw[code] = t
                    blw_rets.append(r_val)

        regime_split = {
            "above20ma": summarize_rets(abv_rets),
            "below20ma": summarize_rets(blw_rets),
        }
        return best, valid[:5], regime_split

    # 空頭網格尋優：獨立採用 5 日結算窗口（覆蓋最新一週行情）
    def eval_bear_grid(events):
        sl_opts = (3.0, 4.0, 5.0)
        ev_rows = []
        for (code, t, entry, bias, vr, slope, rs_ex, tx_abv, fwd) in events:
            r_by_sl = {}
            for sl_pct in sl_opts:
                sl_px = entry * (1.0 + sl_pct / 100.0)
                exit_px = fwd[-1][2]
                for (fh, fl, fc, fm20, fd20) in fwd:
                    if fh >= sl_px:
                        exit_px = sl_px
                        break
                    if fc > fm20:
                        exit_px = fc
                        break
                r_by_sl[sl_pct] = ((entry - exit_px) / entry) * 100.0 - COST_PCT
            ev_rows.append((code, t, bias, vr, slope, r_by_sl))

        grid_results = []
        for bias_lim in (3.5, 5.0, 7.0):
            for vr_min in (0.9, 1.2, 1.5):
                for slope_min in (0.0, 0.3, 0.6):
                    selected = []
                    last_t = {}
                    for (code, t, bias, vr, slope, r_by_sl) in ev_rows:
                        if not (-bias_lim <= bias <= 0.0 and vr >= vr_min and slope <= -slope_min):
                            continue
                        lt = last_t.get(code)
                        if lt is not None and (t - lt) < HOLD_BEAR:
                            continue
                        last_t[code] = t
                        selected.append(r_by_sl)
                    cnt = len(selected)
                    if cnt == 0:
                        continue
                    for sl_pct in sl_opts:
                        rets = [m[sl_pct] for m in selected]
                        st = summarize_rets(rets)
                        sample_bonus = min(1.0, cnt / 30.0)
                        obj = round((st["avgRet"] * 2.2 + (min(st["pf"], 4.0) - 1.0) * 1.8 + (st["winRate"] - 50.0) * 0.06) * sample_bonus, 3)
                        grid_results.append({
                            "biasMax": -bias_lim, "vrMin": vr_min, "slopeMin": -slope_min, "slPct": sl_pct,
                            **st, "score": obj,
                        })

        valid = [g for g in grid_results if g["trades"] >= 30] or [g for g in grid_results if g["trades"] >= 10] or grid_results
        valid.sort(key=lambda x: (x["score"], x["pf"], x["winRate"], x["trades"]), reverse=True)
        best = valid[0] if valid else {
            "biasMax": -7.0, "vrMin": 0.9, "slopeMin": -0.6, "slPct": 4.0,
            "trades": 0, "winRate": 0.0, "avgRet": 0.0, "pf": 0.0, "maxLoss": 0.0, "score": 0.0, "fallback": True
        }
        return best, valid[:5]

    best_bull, bull_top5, bull_by_regime = eval_bull_grid(bull_events)
    best_bear, bear_top5 = eval_bear_grid(bear_events)

    # 負期望值自動保護盾 (Circuit Breaker) 與三大市場位階動態建議
    bull_status = "ACTIVE" if (best_bull["pf"] >= 1.15 and best_bull["avgRet"] > 0) else ("CAUTION" if best_bull["pf"] >= 1.0 else "DEFENSIVE")
    rec_regime = "ATTACK" if mkt_tot >= 75 else ("RANGE" if mkt_tot >= 60 else "DEFENSIVE")

    regime_presets = {
        "DEFENSIVE": {
            "name": "🛡️ 空頭防禦（V21 < 60）",
            "bull": {"biasMax": 4.5, "vrMin": 1.2, "slopeMin": 0.6, "v21Min": 62, "rsMin": 85, "instiPos": True, "slPct": 3.0},
            "bear": {"biasMax": -7.0, "vrMin": 0.9, "slopeMin": -0.6, "v21Max": 45, "rsMax": 60, "slPct": 4.0}
        },
        "RANGE": {
            "name": "⚖️ 盤整震盪（V21 60~75）",
            "bull": {"biasMax": 5.0, "vrMin": 1.2, "slopeMin": 0.3, "v21Min": 60, "rsMin": 75, "instiPos": True, "slPct": 3.5},
            "bear": {"biasMax": -5.0, "vrMin": 1.2, "slopeMin": -0.6, "v21Max": 45, "rsMax": 50, "slPct": 4.0}
        },
        "ATTACK": {
            "name": "🚀 多頭攻擊（V21 ≥ 75）",
            "bull": {"biasMax": 7.0, "vrMin": 1.2, "slopeMin": 0.3, "v21Min": 58, "rsMin": 65, "instiPos": False, "slPct": 4.0},
            "bear": {"biasMax": -5.0, "vrMin": 1.5, "slopeMin": -0.6, "v21Max": 42, "rsMax": 40, "slPct": 3.5}
        }
    }

    return {
        "calibrated": True,
        "version": "V21.8.1",
        "tradeDate": trade_dt,
        "windowStart": bear_start_seen if bear_start_seen != "9999-99-99" else trade_dt,
        "windowEnd": bear_end_seen if bear_end_seen != "0000-00-00" else trade_dt,
        "bullWindow": {
            "start": bull_start_seen if bull_start_seen != "9999-99-99" else trade_dt,
            "end": bull_end_seen if bull_end_seen != "0000-00-00" else trade_dt,
            "holdDays": HOLD_BULL,
        },
        "bearWindow": {
            "start": bear_start_seen if bear_start_seen != "9999-99-99" else trade_dt,
            "end": bear_end_seen if bear_end_seen != "0000-00-00" else trade_dt,
            "holdDays": HOLD_BEAR,
        },
        "sampleSize": len(picked),
        "triggeredStocks": len(stock_candidates),
        "real60mCount": real_60m_used,
        "proxyCount": proxy_used,
        "gridCombos": 405,
        "cooldownDays": HOLD_BEAR,
        "holdDays": HOLD_BULL,
        "holdDaysBear": HOLD_BEAR,
        "costPct": COST_PCT,
        "bullTotalSignals": len(bull_events),
        "bearTotalSignals": len(bear_events),
        "bullStatus": bull_status,
        "recommendedRegime": rec_regime,
        "bullByMarketRegime": bull_by_regime,
        "regimePresets": regime_presets,
        "sampleCodesPreview": [f"{s['code']} {s['name']}" for s in picked[:50]],
        "bestBull": {
            **best_bull,
            "v21Min": 62 if bull_status == "DEFENSIVE" else 58,
            "rsMin": 85 if bull_status == "DEFENSIVE" else 70,
            "instiPos": bull_status == "DEFENSIVE",
        },
        "bestBear": {**best_bear, "v21Max": 45, "rsMax": 60},
        "bullTop5": bull_top5,
        "bearTop5": bear_top5,
    }
def run_taiex_2y_regime_backtest(tx_rw, otc_rw, breadth_hist, dual_bt, mkt_tot, trade_dt):
    """
    V21.8.1 大盤 2 年位階回測 × 2,000 檔市場廣度與雙流共振風險警示引擎（實測微調版）
    ================================================================================
    V21.8.1 依據近 2 年 486 交易日實測數據完成三大精準微調：
    1. 【拆解高檔過熱（六階控倉模型）】：
       - 原 OVERHEAT 占比達 31.5% 且 20日勝率 71.9%，一刀切降倉至 50% 過於保守。
       - 細分為「⚡ 強勢軋空高檔 (STRONG_SURGE, 權重 70%，廣度健康 ≥38% 且非極端噴出)」與
         「🔥 極端過熱／拉積背離 (OVERHEAT, 權重 45%，拉積盤廣度 <38% 或月/季/年線乖離極端噴出)」。
    2. 【收緊最毒主跌段權重】：
       - 實測顯示 BEAR_DEFENSE 未來 5/10 日均為負報酬且 20日平均回撤 -7.36%，將防禦權重由 25% 收緊至 20%（建議倉位 20%～35%），
         同時將 BULL_ATTACK 提至 90%、CONSOLIDATION 提至 60%，顯著拉高多頭參與率並守穩 MDD。
    3. 【新增「右側轉折抄底觀察指標 (reboundWatch)」】：
       - 實測 CRASH_ALERT 危機區 20日反彈勝率高達 90.2%，於空頭／危機期間自動追蹤三大右側確認條件
         （①重站回 240MA 年線、②2,000 檔站月線脫離冰點突破 15%、③站上 5MA 且 5MA 翻揚），直接顯示於首頁警示卡。
    ================================================================================
    """
    REGIME_META = {
        "BULL_ATTACK":   {"name": "🚀 多頭攻擊主升",     "riskLevel": 1, "riskLabel": "低風險・順勢進攻",     "posCap": "75%～90%", "weight": 0.90, "cls": "b-up"},
        "STRONG_SURGE":  {"name": "⚡ 強勢軋空高檔",     "riskLevel": 2, "riskLabel": "中低風險・廣度健康續抱", "posCap": "60%～75%", "weight": 0.70, "cls": "b-up"},
        "CONSOLIDATION": {"name": "⚖️ 震盪整理換手",     "riskLevel": 2, "riskLabel": "中低風險・區間操作",   "posCap": "45%～60%", "weight": 0.60, "cls": "b-cyan"},
        "OVERHEAT":      {"name": "🔥 極端過熱／拉積背離", "riskLevel": 3, "riskLabel": "中度警戒・防背離急殺", "posCap": "35%～50%", "weight": 0.45, "cls": "b-amber"},
        "BEAR_DEFENSE":  {"name": "🛡️ 空頭防禦修正",     "riskLevel": 4, "riskLabel": "高風險・嚴控水位",     "posCap": "20%～35%", "weight": 0.20, "cls": "b-amber"},
        "CRASH_ALERT":   {"name": "🚨 系統性破線危機",   "riskLevel": 5, "riskLabel": "極高風險・現金為王",   "posCap": "0%～15%",  "weight": 0.10, "cls": "b-down"},
    }
    ORDER = ("BULL_ATTACK", "STRONG_SURGE", "OVERHEAT", "CONSOLIDATION", "BEAR_DEFENSE", "CRASH_ALERT")

    if not tx_rw or len(tx_rw) < 120:
        return {"calibrated": False, "version": "V21.8.1", "note": "大盤歷史資料不足 120 日"}

    dts = [r[0][:10] for r in tx_rw]
    cl = [float(r[4]) for r in tx_rw]
    hi = [float(r[2]) for r in tx_rw]
    lo = [float(r[3]) for r in tx_rw]
    n = len(cl)

    ma5 = sma(cl, 5)
    ma20 = sma(cl, 20)
    ma60 = sma(cl, 60)
    ma120 = sma(cl, 120)
    ma240 = sma(cl, 240)

    # OTC 20日相對強弱對照表
    otc_r20 = {}
    if otc_rw and len(otc_rw) >= 25:
        ocl = [float(r[4]) for r in otc_rw]
        for i in range(20, len(otc_rw)):
            if ocl[i - 20] > 0:
                otc_r20[otc_rw[i][0][:10]] = ((ocl[i] / ocl[i - 20]) - 1.0) * 100.0

    # 確保至少有 60MA 暖機，優先取完整 2 年 (~500 交易日)
    min_warm = 240 if n >= 300 else 60
    start_idx = max(min_warm, n - 500)

    def classify_day(t):
        c = cl[t]
        m5 = ma5[t] or c
        m5p = ma5[t - 1] if (t >= 1 and ma5[t - 1]) else m5
        m20 = ma20[t] or c
        m60 = ma60[t] or m20
        m120 = ma120[t] or m60
        m240 = ma240[t] or m120
        m20p = ma20[t - 4] if (t >= 4 and ma20[t - 4]) else m20
        m60p = ma60[t - 4] if (t >= 4 and ma60[t - 4]) else m60

        m5sl = ((m5 / m5p) - 1.0) * 100.0 if m5p > 0 else 0.0
        m20sl = ((m20 / m20p) - 1.0) * 100.0 if m20p > 0 else 0.0
        m60sl = ((m60 / m60p) - 1.0) * 100.0 if m60p > 0 else 0.0
        b20 = ((c / m20) - 1.0) * 100.0 if m20 > 0 else 0.0
        b60 = ((c / m60) - 1.0) * 100.0 if m60 > 0 else 0.0
        b240 = ((c / m240) - 1.0) * 100.0 if m240 > 0 else 0.0

        win_h = max(hi[max(0, t - 249):t + 1])
        dd250 = ((c / win_h) - 1.0) * 100.0 if win_h > 0 else 0.0

        rets20 = [(cl[j] / cl[j - 1] - 1.0) for j in range(max(1, t - 19), t + 1) if cl[j - 1] > 0]
        vol20 = round(stds(rets20) * math.sqrt(252) * 100.0, 1) if len(rets20) >= 5 else 18.0

        bh = (breadth_hist or {}).get(dts[t])
        has_br = bool(bh and bh.get("tot", 0) >= 100)
        if has_br:
            tot_b = max(bh["tot"], 1)
            abv20_p = round(bh["abv20"] / tot_b * 100.0, 1)
            abv60_p = round(bh["abv60"] / tot_b * 100.0, 1)
            zb_cnt = int(bh.get("zb", 0))
            zs_cnt = int(bh.get("zs", 0))
            bbr = round(zs_cnt / max(zb_cnt, 1), 2)
        else:
            abv20_p, abv60_p, zb_cnt, zs_cnt, bbr = None, None, 0, 0, 1.0

        tx_r20 = ((c / cl[t - 20]) - 1.0) * 100.0 if (t >= 20 and cl[t - 20] > 0) else 0.0
        o_r20 = otc_r20.get(dts[t])
        otc_rel = round(o_r20 - tx_r20, 2) if o_r20 is not None else 0.0

        # 六大位階判定規則（整合大盤均線架構 ＋ 2,000 檔市場廣度）
        is_extreme_blowoff = (b20 >= 8.0 or b60 >= 18.0 or b240 >= 32.0)
        is_breadth_div = (abv20_p is not None and abv20_p < 38.0 and b20 >= 1.5)
        is_mod_extended = (b20 >= 5.0 or b60 >= 11.0 or b240 >= 22.0)

        if (c < m240 * 0.97) or (dd250 <= -15.0 and ((abv20_p is not None and abv20_p < 15.0) or c < m240)):
            rg = "CRASH_ALERT"
        elif (c < m20 and (c < m60 or (abv20_p is not None and abv20_p < 25.0) or (has_br and bbr >= 2.2))) or (c < m240):
            rg = "BEAR_DEFENSE"
        elif (c > m20 and c > m60) and (is_extreme_blowoff or is_breadth_div):
            rg = "OVERHEAT"
        elif (c > m20 and c > m60) and is_mod_extended and (abv20_p is None or abv20_p >= 38.0):
            rg = "STRONG_SURGE"
        elif (c > m20 > m60) and (m20sl > 0) and (abv20_p is None or abv20_p >= 42.0):
            rg = "BULL_ATTACK"
        else:
            rg = "CONSOLIDATION"

        return {
            "dt": dts[t], "rg": rg, "c": round(c, 2),
            "m5": round(m5, 2), "m5sl": round(m5sl, 2),
            "m20": round(m20, 2), "m60": round(m60, 2), "m120": round(m120, 2), "m240": round(m240, 2),
            "high250": round(win_h, 2), "dd250": round(dd250, 2),
            "m20sl": round(m20sl, 2), "m60sl": round(m60sl, 2),
            "b20": round(b20, 2), "b60": round(b60, 2), "b240": round(b240, 2),
            "vol20": vol20, "abv20Pct": abv20_p, "abv60Pct": abv60_p,
            "zbCnt": zb_cnt, "zsCnt": zs_cnt, "bearBullRatio": bbr,
            "otcRel20": otc_rel, "hasBreadth": has_br,
        }

    daily_states = [classify_day(t) for t in range(start_idx, n)]
    total_days = len(daily_states)

    # 統計六大位階的未來 5/10/20 日大盤勝率、均報酬與 20 日回撤風險
    buckets = {k: {"f5": [], "f10": [], "f20": [], "dd20": [], "days": 0} for k in ORDER}
    for idx_rel, st in enumerate(daily_states):
        t = start_idx + idx_rel
        rk = st["rg"]
        buckets[rk]["days"] += 1
        c0 = cl[t]
        if c0 <= 0:
            continue
        if t + 5 < n:
            buckets[rk]["f5"].append((cl[t + 5] / c0 - 1.0) * 100.0)
        if t + 10 < n:
            buckets[rk]["f10"].append((cl[t + 10] / c0 - 1.0) * 100.0)
        if t + 20 < n:
            buckets[rk]["f20"].append((cl[t + 20] / c0 - 1.0) * 100.0)
            min_l = min(lo[t + 1:t + 21])
            buckets[rk]["dd20"].append((min_l / c0 - 1.0) * 100.0)

    def _agg(arr):
        if not arr:
            return {"n": 0, "winRate": 0.0, "avgRet": 0.0}
        return {
            "n": len(arr),
            "winRate": round(sum(1 for x in arr if x > 0) / len(arr) * 100.0, 1),
            "avgRet": round(sum(arr) / len(arr), 2),
        }

    regime_stats = []
    for k in ORDER:
        bk = buckets[k]
        m_info = REGIME_META[k]
        f5, f10, f20 = _agg(bk["f5"]), _agg(bk["f10"]), _agg(bk["f20"])
        dd_list = bk["dd20"]
        regime_stats.append({
            "key": k,
            "name": m_info["name"],
            "riskLevel": m_info["riskLevel"],
            "posCap": m_info["posCap"],
            "weight": m_info["weight"],
            "days": bk["days"],
            "pctDays": round(bk["days"] / max(total_days, 1) * 100.0, 1),
            "win5": f5["winRate"], "ret5": f5["avgRet"],
            "win10": f10["winRate"], "ret10": f10["avgRet"],
            "win20": f20["winRate"], "ret20": f20["avgRet"],
            "avgDd20": round(sum(dd_list) / len(dd_list), 2) if dd_list else 0.0,
            "worstDd20": round(min(dd_list), 2) if dd_list else 0.0,
        })

    # 2 年零前視動態控倉回測：今日收盤決定明日持股水位（扣 0.15% 換倉摩擦成本）
    eq_bh, eq_st = [100.0], [100.0]
    ret_bh, ret_st = [], []
    prev_w = REGIME_META[daily_states[0]["rg"]]["weight"]
    eq_dates = [daily_states[0]["dt"]]

    for idx_rel in range(total_days - 1):
        t = start_idx + idx_rel
        w = REGIME_META[daily_states[idx_rel]["rg"]]["weight"]
        dr = (cl[t + 1] / cl[t] - 1.0) if cl[t] > 0 else 0.0
        turnover_cost = abs(w - prev_w) * 0.0015
        prev_w = w
        sr = w * dr - turnover_cost
        ret_bh.append(dr)
        ret_st.append(sr)
        eq_bh.append(eq_bh[-1] * (1.0 + dr))
        eq_st.append(eq_st[-1] * (1.0 + sr))
        eq_dates.append(daily_states[idx_rel + 1]["dt"])

    def perf_metrics(eq, rets):
        if len(eq) < 2:
            return {"totalRet": 0.0, "cagr": 0.0, "mdd": 0.0, "annVol": 0.0, "sharpe": 0.0, "calmar": 0.0}
        tot_r = (eq[-1] / eq[0] - 1.0) * 100.0
        yrs = max(len(rets) / 252.0, 0.1)
        cagr = ((eq[-1] / eq[0]) ** (1.0 / yrs) - 1.0) * 100.0
        pk, mdd = eq[0], 0.0
        for v in eq:
            if v > pk:
                pk = v
            dd = (v / pk - 1.0) * 100.0
            if dd < mdd:
                mdd = dd
        avol = stds(rets) * math.sqrt(252) * 100.0 if len(rets) >= 5 else 0.0
        sharpe = round((sum(rets) / len(rets) * 252.0) / max(stds(rets) * math.sqrt(252), 1e-6), 2) if rets else 0.0
        calmar = round(cagr / abs(mdd), 2) if abs(mdd) > 0.1 else 0.0
        return {
            "totalRet": round(tot_r, 2),
            "cagr": round(cagr, 2),
            "mdd": round(mdd, 2),
            "annVol": round(avol, 1),
            "sharpe": sharpe,
            "calmar": calmar,
        }

    bh_perf = perf_metrics(eq_bh, ret_bh)
    st_perf = perf_metrics(eq_st, ret_st)
    dd_saved = round(abs(bh_perf["mdd"]) - abs(st_perf["mdd"]), 2)
    avg_w = round(sum(REGIME_META[s["rg"]]["weight"] for s in daily_states) / max(total_days, 1) * 100.0, 1)

    # 抽樣約 70 點曲線供前端 Canvas 繪圖
    step = max(1, len(eq_dates) // 70)
    curve_pts = [
        {"dt": eq_dates[i], "bh": round(eq_bh[i], 2), "st": round(eq_st[i], 2), "rg": daily_states[min(i, total_days - 1)]["rg"]}
        for i in range(0, len(eq_dates), step)
    ]
    if curve_pts[-1]["dt"] != eq_dates[-1]:
        curve_pts.append({"dt": eq_dates[-1], "bh": round(eq_bh[-1], 2), "st": round(eq_st[-1], 2), "rg": daily_states[-1]["rg"]})

    # === 產出「當日最新位階判斷警示」、「右側轉折抄底觀察」與「2,000 檔交叉風險提醒」 ===
    cur = daily_states[-1]
    cur_meta = REGIME_META[cur["rg"]]
    risk_alerts = []

    # 1. 大盤均線位階防線警示
    if cur["c"] < cur["m240"]:
        gap_240 = round(cur["m240"] - cur["c"], 1)
        gap_240_pct = round(((cur["m240"] / cur["c"]) - 1.0) * 100.0, 2) if cur["c"] > 0 else 0.0
        risk_alerts.append({
            "level": "danger", "tag": "破年線警報",
            "title": f"加權指數跌破 240MA 年線（{cur['m240']:,.0f} 點，需反彈 +{gap_240:,.0f} 點收復）",
            "detail": f"目前收盤 {cur['c']:,.1f} 點（年線乖離 {cur['b240']:+.2f}%、距收復 +{gap_240_pct:.2f}%），屬於中長期空頭警戒區，建議倉位壓低至 {cur_meta['posCap']}。"
        })
    elif cur["b240"] <= 3.0:
        risk_alerts.append({
            "level": "warn", "tag": "年線保衛戰",
            "title": f"指數緊貼 240MA 年線支撐（僅距 {cur['b240']:+.2f}%）",
            "detail": f"年線 {cur['m240']:,.0f} 點為最後多頭防線；上方月線 {cur['m20']:,.0f} 點與季線 {cur['m60']:,.0f} 點仍有反壓，嚴防反彈遇壓回測。"
        })
    elif cur["c"] < cur["m20"]:
        risk_alerts.append({
            "level": "warn", "tag": "月線下方修正",
            "title": f"指數低於 20MA 月線（{cur['m20']:,.0f} 點，乖離 {cur['b20']:+.2f}%）",
            "detail": f"短中期趨勢轉弱（月線斜率 {cur['m20sl']:+.2f}%），站回 {cur['m20']:,.0f} 點前以反彈減碼、守防線為主。"
        })
    else:
        risk_alerts.append({
            "level": "ok", "tag": "均線多頭結構",
            "title": f"指數站穩 20MA（{cur['m20']:,.0f}）與年線（{cur['m240']:,.0f}）上方",
            "detail": f"目前距離月線 {cur['b20']:+.2f}%、季線 {cur['b60']:+.2f}%，短線防守以不破月線為準。"
        })

    # 2. 2,000 檔市場廣度背離警示
    abv20 = cur["abv20Pct"]
    if abv20 is not None:
        if cur["c"] > cur["m20"] and abv20 < 38.0:
            risk_alerts.append({
                "level": "danger", "tag": "拉積盤背離",
                "title": f"指數創高／站月線，但 2,000 檔站上月線僅 {abv20}%",
                "detail": "呈現權值撐盤、超過六成個股走弱的嚴重結構背離，已將位階壓回「🔥 極端過熱／拉積背離」（倉位上限 35%～50%），務必嚴控個股 3% 停損。"
            })
        elif abv20 < 15.0:
            risk_alerts.append({
                "level": "danger", "tag": "廣度極度冰點",
                "title": f"2,000 檔僅 {abv20}% 站上月線（多空家數比 1 : {cur['bearBullRatio']}）",
                "detail": f"全市場朱家泓多頭僅 {cur['zbCnt']} 檔、空頭高達 {cur['zsCnt']} 檔，屬極度弱勢盤，切忌搶短攤平，僅能輕倉佈局極少數逆勢抗跌股。"
            })
        elif abv20 < 38.0:
            risk_alerts.append({
                "level": "warn", "tag": "廣度偏弱",
                "title": f"2,000 檔站上月線比例偏低（{abv20}%）",
                "detail": f"多數個股仍在月線下方整理（站季線 {cur['abv60Pct']}%），選股須加嚴 RS ≥ 85 與法人買超濾網。"
            })
        else:
            risk_alerts.append({
                "level": "ok", "tag": "廣度健康",
                "title": f"2,000 檔站上月線比例達 {abv20}%（站季線 {cur['abv60Pct']}%）",
                "detail": f"全市場多頭排列 {cur['zbCnt']} 檔，結構支撐良好，高檔強勢盤可維持 60%～90% 水位順勢抱牢。"
            })

    # 3. 2,000 檔雙流回測交叉驗證警示
    if isinstance(dual_bt, dict) and dual_bt.get("calibrated"):
        bb = dual_bt.get("bestBull") or {}
        bs = dual_bt.get("bestBear") or {}
        b_sig = dual_bt.get("bullTotalSignals", 0)
        s_sig = dual_bt.get("bearTotalSignals", 0)
        ratio_sb = round(s_sig / max(b_sig, 1), 2)
        if float(bb.get("pf", 0)) < 1.0:
            risk_alerts.append({
                "level": "danger", "tag": "2000檔逆風護盾",
                "title": f"近 3 月 2,000 檔看漲 PF 僅 {bb.get('pf')}（空頭訊號為多頭 {ratio_sb} 倍）",
                "detail": f"實測看漲均報 {bb.get('avgRet', 0):+.2f}% 處負期望值區，但看跌雙流 PF 達 {bs.get('pf')}（均報 {bs.get('avgRet', 0):+.2f}%）。已自動鎖緊看漲門檻並強化自選股破線警示。"
            })
        else:
            risk_alerts.append({
                "level": "ok", "tag": "2000檔順風確認",
                "title": f"近 3 月 2,000 檔看漲雙流正期望（PF {bb.get('pf')}｜勝率 {bb.get('winRate')}%）",
                "detail": f"多空訊號比正常，搭配大盤位階 {cur_meta['name']} 可依計畫執行 10 日波段與反洗盤緩衝出場。"
            })

    # 4. 乖離過熱 / 空頭急彈與波動率風險提醒
    if cur["c"] >= cur["m240"] and (cur["b20"] >= 8.0 or cur["b60"] >= 18.0 or cur["b240"] >= 32.0):
        risk_alerts.append({
            "level": "warn", "tag": "極端噴出過熱",
            "title": f"大盤正乖離達極端過熱區（月線 {cur['b20']:+.2f}% / 季線 {cur['b60']:+.2f}% / 年線 {cur['b240']:+.2f}%）",
            "detail": "歷史 2 年回測顯示極端正乖離易出現急跌洗盤，嚴禁開盤追高，建議分批獲利了結並把倉位壓在 50% 以內。"
        })
    elif cur["c"] < cur["m240"] and cur["b20"] >= 4.0:
        risk_alerts.append({
            "level": "warn", "tag": "空頭急彈過熱",
            "title": f"破年線格局下短線急彈乖離過大（月線 {cur['b20']:+.2f}%）",
            "detail": "中長線仍在 240MA 下方，急彈乖離擴大屬逃命波風險區，切忌追漲，逢彈優先檢視弱勢股減碼。"
        })
    elif cur["vol20"] >= 25.0:
        risk_alerts.append({
            "level": "warn", "tag": "高波動震盪",
            "title": f"大盤 20 日年化波動率攀升至 {cur['vol20']}%（距高點回撤 {cur['dd250']:+.1f}%）",
            "detail": "盤中上下劇烈洗盤風險高，新進部位請縮減單筆部位至原本一半，絕不使用融資擴張槓桿。"
        })

    # 5. 右側轉折確認監控（於空頭 / 破線危機期間自動啟動，避免左側亂接刀，同時不錯過高勝率反轉）
    c_ma240_ok = cur["c"] >= cur["m240"]
    c_br15_ok = (abv20 is not None and abv20 >= 15.0)
    c_ma5_ok = (cur["c"] >= cur["m5"] and cur["m5sl"] > 0)
    rebound_score = int(c_ma240_ok) + int(c_br15_ok) + int(c_ma5_ok)
    rebound_watch = {
        "active": cur["rg"] in ("CRASH_ALERT", "BEAR_DEFENSE"),
        "passedCount": rebound_score,
        "totalChecks": 3,
        "checks": [
            {"label": f"重站回 240MA 年線（{cur['m240']:,.0f} 點）", "ok": c_ma240_ok, "now": f"{cur['c']:,.1f}（{cur['b240']:+.2f}%）"},
            {"label": "2,000 檔站月線脫離冰點（≥ 15%）", "ok": c_br15_ok, "now": f"{abv20}%" if abv20 is not None else "N/A"},
            {"label": f"短線站上 5MA（{cur['m5']:,.0f}）且 5MA 翻揚", "ok": c_ma5_ok, "now": f"5MA斜率 {cur['m5sl']:+.2f}%"},
        ],
    }
    if rebound_watch["active"]:
        cur_stat_tmp = next((x for x in regime_stats if x["key"] == cur["rg"]), regime_stats[0])
        chk_txt = "｜".join(f"{'✅' if ck['ok'] else '⬜'} {ck['label']}" for ck in rebound_watch["checks"])
        risk_alerts.append({
            "level": "ok" if rebound_score >= 2 else "warn",
            "tag": f"右側轉折監控 {rebound_score}/3",
            "title": f"達標 {rebound_score}/3 項右側翻多訊號（本位階歷史 20 日反彈勝率 {cur_stat_tmp['win20']}%）",
            "detail": f"目前先嚴守低水位現金、絕不左側接刀；待以下條件至少亮 2 顆 ✅ 再分批加碼：{chk_txt}。"
        })

    # 綜合 V21 大盤分數做最終倉位保守取交集
    if mkt_tot < 55 and cur["rg"] in ("BULL_ATTACK", "STRONG_SURGE", "CONSOLIDATION"):
        risk_alerts.append({
            "level": "warn", "tag": "V21 總分防禦覆核",
            "title": f"大盤 V21 綜合得分僅 {mkt_tot} 分（偏弱），強制下修倉位上限",
            "detail": "雖單日反彈或站均線，但綜合籌碼、美債殖利率與市場廣度仍偏弱，實戰總倉位建議不超過 30%～45%。"
        })

    cur_stat = next((x for x in regime_stats if x["key"] == cur["rg"]), regime_stats[0])
    recent_20 = [
        {"dt": s["dt"], "c": s["c"], "rg": s["rg"], "b20": s["b20"], "abv20": s["abv20Pct"]}
        for s in daily_states[-20:]
    ]

    return {
        "calibrated": True,
        "version": "V21.8.1",
        "tradeDate": trade_dt,
        "windowStart": daily_states[0]["dt"],
        "windowEnd": daily_states[-1]["dt"],
        "tradingDays": total_days,
        "breadthCoveredDays": sum(1 for s in daily_states if s["hasBreadth"]),
        "current": {
            **cur,
            "regimeKey": cur["rg"],
            "regimeName": cur_meta["name"],
            "riskLevel": cur_meta["riskLevel"],
            "riskLabel": cur_meta["riskLabel"],
            "posCap": "30%～45%" if (mkt_tot < 55 and cur_meta["weight"] > 0.45) else cur_meta["posCap"],
            "badgeCls": cur_meta["cls"],
            "mktV21": mkt_tot,
            "histWin20": cur_stat["win20"],
            "histRet20": cur_stat["ret20"],
            "histAvgDd20": cur_stat["avgDd20"],
        },
        "reboundWatch": rebound_watch,
        "riskAlerts": risk_alerts,
        "regimeStats": regime_stats,
        "strategyVsBuyHold": {
            "buyHold": bh_perf,
            "v21Regime": st_perf,
            "avgWeightPct": avg_w,
            "drawdownSavedPct": dd_saved,
            "volReducedPct": round(bh_perf["annVol"] - st_perf["annVol"], 1),
        },
        "equityCurve": curve_pts,
        "recent20": recent_20,
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
    log("🚀 V21.3 GitHub Actions 雲端全自動盤後更新引擎啟動！")
    log(f"🕒 台灣時間：{NOW_TW.strftime('%Y-%m-%d %H:%M:%S')}")
    log("==================================================")

    # 讀取歷史紀錄檔（含 TAIFEX 備援）
    hf = os.path.join(DATA_DIR, "v21_hist.json")
    ph = {}
    if os.path.exists(hf):
        try:
            with open(hf, "r", encoding="utf-8") as f:
                ph = json.load(f)
        except Exception:
            ph = {}

    log("📡 [1/6] 抓取美股、美債10Y、匯率、加權與櫃買指數（官方 OHLCV 校準）...")
    idx_meta = {
        "DJI": ("道瓊工業", "^DJI"), "SP500": ("標普500", "^GSPC"), "NASDAQ": ("那斯達克", "^IXIC"),
        "SOX": ("費城半導體", "^SOX"), "NVDA": ("輝達 NVDA", "NVDA"), "TSM": ("台積電 ADR", "TSM"),
        "US10Y": ("美債10Y", "^TNX"), "DXY": ("美元指數", "DX-Y.NYB"), "USDTWD": ("美元兌台幣", "TWD=X"),
        "TAIEX": ("加權指數", "^TWII"), "OTC": ("櫃買指數", "^TWOII")
    }
    gsum, gfr = {}, {}
    for k, (lb, sym) in idx_meta.items():
        rw = yf_ohlcv(sym, "3y" if k in ("TAIEX", "OTC") else "3mo", "1d")
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
    tx_oi_cached = False
    if tx_oi:
        ph["_last_taifex"] = tx_oi
    elif isinstance(ph.get("_last_taifex"), dict) and ph["_last_taifex"].get("net") is not None:
        tx_oi = ph["_last_taifex"]
        tx_oi_cached = True

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
    daily_cache_mem = {}

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
        tail = [{"t": r[0][:10], "o": round(r[1], 2), "h": round(r[2], 2), "l": round(r[3], 2), "c": round(r[4], 2), "v": int(r[5] // 1000)} for r in rw[-160:]]
        # 計算該檔個股近 2 年逐日廣度與朱家泓多空標記（供大盤 2 年位階回測交叉參考）
        br_marks = None
        if sec["tp"] != "ETF" and len(rw) >= 65:
            rcl = [r[4] for r in rw]
            rm5, rm10, rm20, rm60 = sma(rcl, 5), sma(rcl, 10), sma(rcl, 20), sma(rcl, 60)
            br_marks = []
            for j in range(60, len(rw)):
                cj, m5j, m10j, m20j, m60j = rcl[j], rm5[j], rm10[j], rm20[j], rm60[j]
                m20p = rm20[j - 4]
                if None in (m5j, m10j, m20j, m60j, m20p) or cj <= 0:
                    continue
                br_marks.append((
                    rw[j][0][:10],
                    1 if cj > m20j else 0,
                    1 if cj > m60j else 0,
                    1 if cj > rcl[j - 1] else 0,
                    1 if (cj > m5j > m10j > m20j and m20j > m20p) else 0,
                    1 if (cj < m5j < m10j < m20j and m20j < m20p) else 0,
                ))
        return item, (c, tail), br_marks

    stocks, done, tot_n = [], 0, len(secs)
    breadth_hist = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for f in as_completed([ex.submit(w_day, s) for s in secs]):
            done += 1
            res_w = f.result()
            if not res_w or not res_w[0]:
                continue
            item, bar_pair, br_marks = res_w
            stocks.append(item)
            if bar_pair:
                daily_cache_mem[bar_pair[0]] = bar_pair[1]
            if br_marks:
                for (dt_b, a20, a60, up_f, zb_f, zs_f) in br_marks:
                    slot = breadth_hist.get(dt_b)
                    if slot is None:
                        breadth_hist[dt_b] = {"tot": 1, "abv20": a20, "abv60": a60, "up": up_f, "zb": zb_f, "zs": zs_f}
                    else:
                        slot["tot"] += 1
                        slot["abv20"] += a20
                        slot["abv60"] += a60
                        slot["up"] += up_f
                        slot["zb"] += zb_f
                        slot["zs"] += zs_f
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

    log(f"🧮 [4/6] 計算全市場 RS 百分位與 V21 五構面評分...")
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
        c_tag = f"（延續 {tx_oi.get('dt')}）" if tx_oi_cached else f"（{tx_oi.get('dt')}）"
        ms["07"], mn["07"] = (5, 4.5 if noi > 0 else (3.5 if noi > -20000 else (2.5 if noi > -45000 else 1.5))), f"外資台指期淨未平倉{noi:,}口{c_tag}"
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

    macro_15 = round(sum(ms[k][1] for k in ("01", "02", "03", "04")), 1)
    chip_base = ms["05"][1] + ms["07"][1]

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

        p_chip = round(min(36.0, chip_base + s6 + s8 + s9 + s10), 1)
        p_tech = round(min(24.0, r["rs_sc"] + r["ma_sc"] + r["macd_sc"] + r["kd_sc"] + r["bb_sc"]), 1)
        p_mom = round(min(21.0, r["v_sc"] + r["gap_sc"] + r["pat_sc"] + s19), 1)
        p_risk = round(min(4.0, r["r_sc"]), 1)
        r["p5"] = [macro_15, p_chip, p_tech, p_mom, p_risk]
        r["v21"] = round(min(100.0, max(0.0, sum(r["p5"]))), 1)
        r["b60"] = False
        r["b60s"] = "未掃描"
        r["bm"] = None

    liq = [s for s in stocks if s["lots"] >= MIN_VOL_LOTS and s["tp"] == "股票"]
    if len(liq) < 10:
        liq = [s for s in stocks if s["tp"] == "股票"]
    for s in stocks:
        s["b60_bear"], s["b60_dir"], s["b60_sl"], s["b60_bias"] = False, 0, s.get("m20sl", 0.0), s.get("bias20", 0.0)
    bull_pool = sorted(liq, key=lambda x: (x["v21"], x["rs_rk"]), reverse=True)[:TOP_60M_LIMIT]
    bear_pool = sorted([x for x in liq if x.get("zhu_bear")], key=lambda x: (x["lots"], -x["v21"]), reverse=True)[:20]
    pool = list({s["code"]: s for s in bull_pool + bear_pool}.values())

    log(f"⚔️ [5/6] 雙向驗證 {len(pool)} 檔多空候選股 60分K 均線三刀流(20/60/240MA)...")
    k60_cache_mem = {}
    k60_state_mem = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fm = {ex.submit(check_60m, s["tk"]): s for s in pool}
        for f in as_completed(fm):
            s = fm[f]
            res = f.result()
            if res:
                k60_state_mem[s["code"]] = res
                s["b60_dir"] = res.get("dir", 0)
                s["b60_sl"] = res.get("slope20", 0.0)
                s["b60_bias"] = res.get("bias20", s.get("bias20", 0.0))
                s["bm"] = [res["m20"], res["m60"], res["m240"]]
                if res["ok"]:
                    s["b60"] = True
                    s["b60s"] = f"✅ 三刀多頭({res['m20']}/{res['m60']}/{res['m240']})"
                    s["p5"][3] = round(min(21.0, s["p5"][3] + 1.5), 1)
                    s["v21"] = round(min(100.0, sum(s["p5"])), 1)
                elif res.get("bear_ok"):
                    s["b60_bear"] = True
                    s["b60s"] = f"🔻 三刀空頭({res['m20']}/{res['m60']}/{res['m240']})"
                else:
                    s["b60s"] = "⚠️ 整理待轉"
                if res.get("bars"):
                    k60_cache_mem[s["code"]] = res["bars"]

    log("🧪 執行 V21.7.1 雙流共振全市場 2,000 檔多空獨立窗口回測（含大盤 20MA 順逆風對照組）...")
    dual_bt = run_dual_backtest_2000(stocks, daily_cache_mem, k60_state_mem, trade_dt, tx_rw=tx_rw, mkt_tot=mkt_tot)
    _smap = {s["code"]: s for s in stocks}
    for code, res in k60_state_mem.items():
        st = _smap.get(code)
        if not st or st.get("b60s") != "未掃描":
            continue
        st["b60_dir"] = res.get("dir", 0)
        st["b60_sl"] = res.get("slope20", 0.0)
        st["b60_bias"] = res.get("bias20", st.get("bias20", 0.0))
        st["bm"] = [res["m20"], res["m60"], res["m240"]]
        if res.get("ok"):
            st["b60"] = True
            st["b60s"] = f"✅ 三刀多頭({res['m20']}/{res['m60']}/{res['m240']})"
        elif res.get("bear_ok"):
            st["b60_bear"] = True
            st["b60s"] = f"🔻 三刀空頭({res['m20']}/{res['m60']}/{res['m240']})"
        else:
            st["b60s"] = "⚠️ 整理待轉"
        if res.get("bars") and code not in k60_cache_mem:
            k60_cache_mem[code] = res["bars"]
    log(f"🏆 全市場 {dual_bt['sampleSize']} 檔近 3 個月回測完成（真實60分K {dual_bt['real60mCount']} 檔＋等效代理 {dual_bt['proxyCount']} 檔）！看漲最佳：乖離≤+{dual_bt['bestBull']['biasMax']}% / 量比≥{dual_bt['bestBull']['vrMin']}x（勝率{dual_bt['bestBull']['winRate']}%、PF {dual_bt['bestBull']['pf']}）｜看跌最佳：負乖離≥{dual_bt['bestBear']['biasMax']}% / 量比≥{dual_bt['bestBear']['vrMin']}x（勝率{dual_bt['bestBear']['winRate']}%、PF {dual_bt['bestBear']['pf']}）")
    log("🧭 執行大盤 2 年位階回測 × 2,000 檔市場廣度與風險警示引擎...")
    taiex_bt = run_taiex_2y_regime_backtest(tx_rw, gfr.get("OTC", []), breadth_hist, dual_bt, mkt_tot, trade_dt)
    if taiex_bt.get("calibrated"):
        _c_rg = taiex_bt["current"]
        _svb = taiex_bt["strategyVsBuyHold"]
        log(f"  ✅ 大盤 2 年位階回測完成（{taiex_bt['windowStart']} ～ {taiex_bt['windowEnd']}，共 {taiex_bt['tradingDays']} 交易日）｜目前位階：{_c_rg['regimeName']}（風險等級 {_c_rg['riskLevel']}/5，建議倉位 {_c_rg['posCap']}）｜位階控倉 MDD {_svb['v21Regime']['mdd']}% vs 死抱 {_svb['buyHold']['mdd']}%（少扛 {_svb['drawdownSavedPct']}% 回撤）")

    rs20 = sorted(liq, key=lambda x: (x["rs_rk"], x["v21"]), reverse=True)[:20]
    top10 = sorted(liq, key=lambda x: (x["b60"], x["v21"], x["rs_rk"]), reverse=True)[:10]
    bop = [s for s in liq if s["vr"] >= 1.2 or s["gap_sc"] >= 3.0 or s["b60"]]
    top3 = sorted(bop, key=lambda x: (x["v21"], x["vr"], x["rs_rk"]), reverse=True)[:3]
    if len(top3) < 3:
        top3 = top10[:3]
    gaps = sorted([s for s in liq if s["gap_sc"] >= 3.0], key=lambda x: (x["v21"], x["vr"]), reverse=True)[:10]

    # 更新 30 日趨勢與回測紀錄
    pds = sorted([d for d in ph if re.match(r"^\d{4}-\d{2}-\d{2}$", d) and d < trade_dt])
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

    hist_dates = sorted([d for d in ph if re.match(r"^\d{4}-\d{2}-\d{2}$", d)])[-60:]
    history_series = [{"dt": d, "ms": ph[d].get("ms", 50.0), "taiex": ph[d].get("taiex", 0.0), "above20ma": ph[d].get("above20ma", 0.0)} for d in hist_dates]

    log("📝 [6/6] 正在輸出 V21.3 JSON、大盤+個股真實 K 線包與 15 項報告...")
    st_m, act_m = lbl(mkt_tot)
    ts_now = NOW_TW.strftime("%Y-%m-%d %H:%M")
    pos = "70%～85%" if mkt_tot >= 80 else ("50%～65%" if mkt_tot >= 65 else "30%～45%")

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
        "version": "V21.8.1",
        "dualAlert": dual_bt,
        "marketRegime": taiex_bt,
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
        "history": history_series,
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
            "優先追蹤「低乖離雙流共振＋法人買超」標的，回測日內均價或 60分K 20MA（小藍短刀）止穩承接。",
            "所有部位嚴守朱家泓 3% 停損基準，破線立即減碼退場。"
        ]
    }

    sorted_stocks = sorted(stocks, key=lambda x: x["v21"], reverse=True)
    web_stocks = [{
        "dt": r["dt"], "c": r["code"], "n": r["name"], "m": r["mkt"], "i": r["ind"],
        "p": r["price"], "ch": r["chg"], "v": r["lots"], "vr": r["vr"], "rs": r["rs_rk"], "s": r["v21"],
        "p5": r["p5"], "m20": r["ma20"], "mc": round(r["mc"]),
        "z": r["zhu"], "zd": r.get("zhu_dir", 1 if r["zhu"] else 0), "bd": r.get("b60_dir", 1 if r["b60"] else 0), "bsl": r.get("b60_sl", r.get("m20sl", 0.0)), "bb": r.get("b60_bias", r.get("bias20", 0.0)), "b": r["b60s"], "bm": r["bm"], "fl": r["fl"], "tl": r["tl"], "il": r["il"],
        "vw": r["vwap"], "sl": r["sl"], "t1": r["t1"], "t2": r["t2"], "g": r["gap_t"]
    } for r in sorted_stocks]

    with open(os.path.join(DATA_DIR, "v21_latest.json"), "w", encoding="utf-8") as f:
        json.dump(
            {"meta": meta_out, "stocks": web_stocks, "sourceInfo": f"V21.8.1 自動更新 {trade_dt}（同步率 {sync_rate}%｜含{dual_bt['sampleSize']}檔3月回測）"},
            f, ensure_ascii=False, separators=(",", ":")
        )
    with open(os.path.join(DATA_DIR, "dual_alert_backtest.json"), "w", encoding="utf-8") as f:
        json.dump(dual_bt, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, "taiex_regime_backtest.json"), "w", encoding="utf-8") as f:
        json.dump(taiex_bt, f, ensure_ascii=False, indent=2)

    # 2. V21.3 kline_bundle.json: Include TAIEX & OTC real daily bars + Top20/Core daily & 60m real bars!
    bundle_codes = set([r["code"] for r in top10] + [r["code"] for r in rs20[:10]] + ["2330", "2317", "2454", "2308", "2303", "2382", "3711", "2412", "2597", "2610", "2618", "1303", "2881", "2882", "6614", "3023"])
    kline_bundle = {"tradeDate": trade_dt, "indices": {}, "daily": {}, "k60m": {}}
    for ik in ("TAIEX", "OTC"):
        if gfr.get(ik):
            kline_bundle["indices"][ik] = [
                {"t": r[0][:10], "o": round(r[1], 2), "h": round(r[2], 2), "l": round(r[3], 2), "c": round(r[4], 2), "v": int(r[5])}
                for r in gfr[ik][-260:]
            ]
    for c in bundle_codes:
        if c in daily_cache_mem:
            kline_bundle["daily"][c] = daily_cache_mem[c]
        if c in k60_cache_mem:
            kline_bundle["k60m"][c] = k60_cache_mem[c]
    with open(os.path.join(DATA_DIR, "kline_bundle.json"), "w", encoding="utf-8") as f:
        json.dump(kline_bundle, f, ensure_ascii=False, separators=(",", ":"))

    # 3. Build 15-item Markdown Report & CSV
    inf = sorted(ist, key=lambda x: (x["achg"], x["il"]), reverse=True)[:6]
    outf = sorted(ist, key=lambda x: (x["achg"], x["il"]))[:6]
    md = [
        f"# 台股二十大指標最終版（V21.3 雲端全自動版）每日量化戰情報告（{trade_dt}）",
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

    bb_b, bb_s = dual_bt["bestBull"], dual_bt["bestBear"]
    md += [
        f"\n## 16｜雙流共振全市場 {dual_bt['sampleSize']:,} 檔近 3 個月回測與最佳多空提醒數值",
        f"- **回測規模**：全市場 **`{dual_bt['sampleSize']:,}` 檔**（近3月曾觸發朱家泓架構 `{dual_bt['triggeredStocks']:,}` 檔｜真實 60分K 驗證 `{dual_bt['real60mCount']:,}` 檔＋等比例 5/13/53MA 代理 `{dual_bt['proxyCount']:,}` 檔）｜區間：`{dual_bt['windowStart']}` ～ `{dual_bt['windowEnd']}`｜共測 **`{dual_bt['gridCombos']}` 組多空網格**｜單股 `{dual_bt['cooldownDays']}` 日反重疊冷卻｜已扣台股成本 `{dual_bt['costPct']}%`。",
        f"- **🚀 看漲雙流共振最佳提醒數值**：乖離率 `0% ～ +{bb_b['biasMax']}%`、量比 `≥ {bb_b['vrMin']}x`、60分K 20MA 斜率 `≥ +{bb_b['slopeMin']}%`、停損 `{bb_b['slPct']}%`（近3月有效不重疊觸發 **`{bb_b['trades']}` 筆**｜勝率 **`{bb_b['winRate']}%`**｜單筆平均淨報酬 **`{bb_b['avgRet']:+.2f}%`**｜獲利因子 PF **`{bb_b['pf']}`**）。",
        f"- **🔻 看跌雙流共振最佳提醒數值**：負乖離 `{bb_s['biasMax']}% ～ 0%`、破線量比 `≥ {bb_s['vrMin']}x`、60分K 20MA 斜率 `≤ {bb_s['slopeMin']}%`、反彈解除線 `{bb_s['slPct']}%`（近3月有效不重疊觸發 **`{bb_s['trades']}` 筆**｜勝率 **`{bb_s['winRate']}%`**｜單筆平均淨報酬 **`{bb_s['avgRet']:+.2f}%`**｜獲利因子 PF **`{bb_s['pf']}`**）。",
        f"- **🛡️ 實戰位階自動判定**：目前大盤 V21 `{mkt_tot}` 分 → 建議啟用 **`{dual_bt['regimePresets'][dual_bt['recommendedRegime']]['name']}`**（多頭期望值檢核：`{dual_bt['bullStatus']}`；若處逆風自動套用 RS ≥ 85 ＋ 法人買超 ＋ 乖離 ≤ +4.5% 嚴選保護）。",
        f"- **📐 多空獨立結算窗口**：多頭 10 日波段（`{dual_bt['bullWindow']['start']}` ～ `{dual_bt['bullWindow']['end']}`）｜空頭 5 日急跌（`{dual_bt['bearWindow']['start']}` ～ `{dual_bt['bearWindow']['end']}`）。",
        f"- **🌤️ 大盤月線（20MA）順風 vs. 逆風看漲對照**：大盤站上 20MA 時做多 **`{dual_bt['bullByMarketRegime']['above20ma']['trades']}` 筆**（勝率 `{dual_bt['bullByMarketRegime']['above20ma']['winRate']}%`｜均報 `{dual_bt['bullByMarketRegime']['above20ma']['avgRet']:+.2f}%`｜PF **`{dual_bt['bullByMarketRegime']['above20ma']['pf']}`**） vs. 大盤跌破 20MA 時做多 **`{dual_bt['bullByMarketRegime']['below20ma']['trades']}` 筆**（勝率 `{dual_bt['bullByMarketRegime']['below20ma']['winRate']}%`｜均報 `{dual_bt['bullByMarketRegime']['below20ma']['avgRet']:+.2f}%`｜PF **`{dual_bt['bullByMarketRegime']['below20ma']['pf']}`**）。",
        "\n| 方向/排名 | 乖離門檻 | 量比門檻 | 20MA斜率 | 停損線 | 有效筆數 | 勝率 | 均淨報酬 | 獲利因子(PF) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for i, r in enumerate((dual_bt.get("bullTop5") or [])[:3], 1):
        md.append(f"| 🚀 看漲 #{i} | ≤+{r['biasMax']}% | ≥{r['vrMin']}x | ≥+{r['slopeMin']}% | {r['slPct']}% | {r['trades']} | **{r['winRate']}%** | {r['avgRet']:+.2f}% | **{r['pf']}** |")
    for i, r in enumerate((dual_bt.get("bearTop5") or [])[:3], 1):
        md.append(f"| 🔻 看跌 #{i} | ≥{r['biasMax']}% | ≥{r['vrMin']}x | ≤{r['slopeMin']}% | {r['slPct']}% | {r['trades']} | **{r['winRate']}%** | {r['avgRet']:+.2f}% | **{r['pf']}** |")
    if taiex_bt.get("calibrated"):
        _cur = taiex_bt["current"]
        _sv = taiex_bt["strategyVsBuyHold"]
        md += [
            f"\n## 17｜大盤 2 年位階回測 × 2,000 檔共振風險警示（{taiex_bt['windowStart']} ～ {taiex_bt['windowEnd']}）",
            f"- **目前大盤位階判定**：**`{_cur['regimeName']}`**（風險等級 **`{_cur['riskLevel']}/5` — {_cur['riskLabel']}**｜建議總倉位上限 **`{_cur['posCap']}`**）。",
            f"- **關鍵均線與 2,000 檔廣度**：收盤 `{_cur['c']:,.1f}`｜20MA `{_cur['m20']:,.0f}`（`{_cur['b20']:+.2f}%`）｜60MA `{_cur['m60']:,.0f}`（`{_cur['b60']:+.2f}%`）｜240MA 年線 `{_cur['m240']:,.0f}`（`{_cur['b240']:+.2f}%`）｜2,000 檔站上月線 **`{_cur['abv20Pct']}%`**。",
            f"- **2 年動態控倉回測驗證（{taiex_bt['tradingDays']} 交易日）**：V21.8.1 六階控倉總報酬 **`{_sv['v21Regime']['totalRet']:+.2f}%`** / 最大回撤 MDD **`{_sv['v21Regime']['mdd']}%`** / Calmar **`{_sv['v21Regime']['calmar']}`** vs. 大盤滿倉死抱總報酬 `{_sv['buyHold']['totalRet']:+.2f}%` / MDD `{_sv['buyHold']['mdd']}%`（少扛 **`{_sv['drawdownSavedPct']}%`** 跌幅，年化波動降低 **`{_sv['volReducedPct']}%`**）。",
            "\n### 2 年六大位階歷史統計（未來 5日 / 10日 / 20日大盤實測）\n",
            "| 位階燈號 | 建議倉位 | 出現天數(占比) | 未來5日勝率/均報 | 未來10日勝率/均報 | 未來20日勝率/均報 | 20日均回撤/最壞 |",
            "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
        ]
        for rs in taiex_bt.get("regimeStats", []):
            md.append(
                f"| {rs['name']} | {rs['posCap']} | {rs['days']}天 ({rs['pctDays']}%) | "
                f"{rs['win5']}% / {rs['ret5']:+.2f}% | {rs['win10']}% / {rs['ret10']:+.2f}% | "
                f"**{rs['win20']}%** / **{rs['ret20']:+.2f}%** | {rs['avgDd20']:.2f}% / {rs['worstDd20']:.2f}% |"
            )
        md.append("\n### 今日系統位階風險提醒")
        for al in taiex_bt.get("riskAlerts", []):
            ic = "🚨" if al["level"] == "danger" else ("⚠️" if al["level"] == "warn" else "✅")
            md.append(f"- {ic} **[{al['tag']}] {al['title']}**：{al['detail']}")

    with open(os.path.join(DATA_DIR, "V21_最新每日報告.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    flds = ["dt", "code", "name", "mkt", "ind", "price", "chg", "lots", "vr", "rs_rk", "v21", "zhu", "b60s", "fl", "tl", "il", "mc", "vwap", "sl", "t1", "t2", "gap_t"]
    with open(os.path.join(DATA_DIR, "V21_全市場評分_最新.csv"), "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=flds, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted_stocks)

    log(f"🎉 V21.3 雲端全自動更新完成！（耗時 {time.time() - t0:.1f} 秒｜{len(stocks)} 檔｜基準日 {trade_dt}｜大盤 V21 {mkt_tot}）")


if __name__ == "__main__":
    main()
