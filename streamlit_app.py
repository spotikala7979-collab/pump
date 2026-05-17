"""
Fero Radar — Streamlit Edition
Binance Futures pump/radar ve whale dashboard
Streamlit Community Cloud'da ücretsiz çalışır.
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

import httpx
import pandas as pd
import streamlit as st

# ═══════════════════════════════════════════════════════════
# GLOBAL SHARED STATE  (module-level → reruns arasında kalır)
# ═══════════════════════════════════════════════════════════
_lock = threading.RLock()

_radar: dict[str, Any] = {
    "signals": deque(maxlen=100),
    "history": defaultdict(lambda: deque(maxlen=1200)),
    "last_signal_ts": {},
    "stats_4h": defaultdict(lambda: {"PUMP": 0, "DUMP": 0}),
    "last_heartbeat": 0.0,
    "total_pairs": 0,
    "last_error": None,
    "started": False,
    "last_reset_4h_block": datetime.now().hour // 4,
}

_whale: dict[str, Any] = {
    "events": {},
    "market_buffer": defaultdict(list),
    "connected": False,
    "last_sync": 0.0,
    "last_error": None,
    "started": False,
}

_btc_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_btc_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
RADAR_POLL_SECONDS          = 2.0
RADAR_MIN_VOL_3M            = 100_000.0
RADAR_MIN_CHG_3M            = 1.1
RADAR_CONFIRM_CHG_15M       = 2.5
RADAR_FAST_STRIKE_CHG       = 1.5
RADAR_FAST_STRIKE_MIN_VOL_1M= 50_000.0
RADAR_TRI_WINDOW            = 180
RADAR_LONG_WINDOW           = 900
RADAR_SIGNAL_COOLDOWN       = 20

WHALE_MIN_IMPACT            = 0.20
WHALE_MIN_VOL_LIMIT         = 25_000.0
WHALE_LOOKBACK_WINDOW       = 3.0
WHALE_MAX_ROWS              = 50
WHALE_SYMBOLS = [
    "btcusdt","ethusdt","solusdt","adausdt","dogeusdt","avaxusdt",
    "injusdt","fetusdt","flowusdt","banusdt","espusdt","iousdt",
    "bluaiusdt","alchusdt","stousdt","treeusdt","beatusdt","nightusdt",
    "atusdt","musdt","partiusdt","tradoorusdt",
]

TICKER_URL   = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BTC_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

# ═══════════════════════════════════════════════════════════
# RADAR ENGINE
# ═══════════════════════════════════════════════════════════
def _radar_check_resets() -> None:
    block = datetime.now().hour // 4
    if block != _radar["last_reset_4h_block"]:
        _radar["stats_4h"] = defaultdict(lambda: {"PUMP": 0, "DUMP": 0})
        _radar["last_reset_4h_block"] = block


def _radar_process_ticker(data: list) -> None:
    now = time.time()
    with _lock:
        _radar_check_resets()
        _radar["last_heartbeat"] = now
        _radar["total_pairs"] = len(data)
        _radar["last_error"] = None

    for item in data:
        symbol = str(item.get("symbol", ""))
        if not symbol.endswith("USDT"):
            continue
        try:
            price        = float(item["lastPrice"])
            quote_volume = float(item["quoteVolume"])
        except (KeyError, TypeError, ValueError):
            continue

        with _lock:
            _radar["history"][symbol].append((now, price, quote_volume))

        candidate = _radar_evaluate(symbol, now)
        if candidate:
            _radar_add_signal(**candidate)


def _radar_evaluate(symbol: str, now: float) -> dict | None:
    with _lock:
        hist = list(_radar["history"][symbol])

    if len(hist) < 20:
        return None

    current    = hist[-1]
    data_age   = now - hist[0][0]
    past_1m    = next((x for x in hist if now - x[0] <= 60), hist[0])
    past_3m    = next((x for x in hist if now - x[0] <= RADAR_TRI_WINDOW), hist[0])

    if past_1m[1] == 0 or past_3m[1] == 0:
        return None

    c1     = ((current[1] - past_1m[1]) / past_1m[1]) * 100
    c3     = ((current[1] - past_3m[1]) / past_3m[1]) * 100
    vol_1m = current[2] - past_1m[2]
    vol_3m = current[2] - past_3m[2]

    # FLASH sinyal
    if abs(c1) >= RADAR_FAST_STRIKE_CHG and vol_1m >= RADAR_FAST_STRIKE_MIN_VOL_1M:
        return dict(
            symbol=symbol, price=current[1], chg_main=c1, chg_ref=0.0,
            volume=vol_1m, signal_type="PUMP" if c1 > 0 else "DUMP", mode="FLASH",
        )

    if vol_3m < RADAR_MIN_VOL_3M or abs(c3) < RADAR_MIN_CHG_3M:
        return None
    if data_age < RADAR_LONG_WINDOW:
        return None

    past_15m = hist[0]
    if past_15m[1] == 0:
        return None
    c15 = ((current[1] - past_15m[1]) / past_15m[1]) * 100
    consistent = (c3 > 0 and c15 > 0) or (c3 < 0 and c15 < 0)
    if consistent and abs(c15) >= RADAR_CONFIRM_CHG_15M:
        return dict(
            symbol=symbol, price=current[1], chg_main=c3, chg_ref=c15,
            volume=vol_3m, signal_type="PUMP" if c3 > 0 else "DUMP", mode="CONFIRMED",
        )
    return None


def _radar_add_signal(
    *, symbol: str, price: float, chg_main: float, chg_ref: float,
    volume: float, signal_type: str, mode: str,
) -> None:
    now      = time.time()
    sym_clean = symbol.replace("USDT", "")
    key      = (sym_clean, signal_type, mode)
    with _lock:
        last_ts = _radar["last_signal_ts"].get(key, 0.0)
        if now - last_ts < RADAR_SIGNAL_COOLDOWN:
            return
        _radar["last_signal_ts"][key] = now
        _radar["stats_4h"][sym_clean][signal_type] += 1
        pumps = _radar["stats_4h"][sym_clean]["PUMP"]
        dumps = _radar["stats_4h"][sym_clean]["DUMP"]

        sinyal_icon = "🟢 PUMP" if signal_type == "PUMP" else "🔴 DUMP"
        mod_icon    = "⚡ FLASH" if mode == "FLASH" else "✅ CONFIRMED"

        _radar["signals"].appendleft({
            "Zaman":    datetime.now().strftime("%H:%M:%S"),
            "Coin":     sym_clean,
            "Fiyat":    f"${price:.4f}",
            "Değişim":  f"{chg_main:+.2f}%",
            "Ref 15m":  f"{chg_ref:+.2f}%" if chg_ref else "-",
            "Hacim":    f"${volume/1000:.0f}K",
            "Sinyal":   sinyal_icon,
            "Mod":      mod_icon,
            "4s Pump":  pumps,
            "4s Dump":  dumps,
        })


def _radar_loop() -> None:
    while True:
        try:
            with httpx.Client(timeout=10) as client:
                while True:
                    try:
                        resp = client.get(TICKER_URL)
                        resp.raise_for_status()
                        data = resp.json()
                        if isinstance(data, list):
                            _radar_process_ticker(data)
                        else:
                            with _lock:
                                _radar["last_error"] = "Binance ticker beklenmeyen yanıt"
                    except Exception as exc:
                        with _lock:
                            _radar["last_error"] = f"{type(exc).__name__}: {exc}"
                    time.sleep(RADAR_POLL_SECONDS)
        except Exception as exc:
            with _lock:
                _radar["last_error"] = f"Radar crash: {exc}"
            time.sleep(5)


# ═══════════════════════════════════════════════════════════
# WHALE ENGINE
# ═══════════════════════════════════════════════════════════
def _whale_process_trade(data: dict, now: float) -> None:
    try:
        symbol      = str(data["s"]).replace("USDT", "").upper()
        price       = float(data["p"])
        volume_usdt = float(data["p"]) * float(data["q"])
    except (KeyError, TypeError, ValueError):
        return

    with _lock:
        _whale["market_buffer"][symbol].append({"p": price, "v": volume_usdt, "t": now})
        cutoff = now - WHALE_LOOKBACK_WINDOW * 60
        _whale["market_buffer"][symbol] = [
            x for x in _whale["market_buffer"][symbol] if x["t"] > cutoff
        ]
        buffer = list(_whale["market_buffer"][symbol])

    if len(buffer) < 2:
        return
    start_p = buffer[0]["p"]
    end_p   = buffer[-1]["p"]
    if start_p == 0:
        return
    impact    = ((end_p - start_p) / start_p) * 100
    total_vol = sum(x["v"] for x in buffer)
    _whale_process_event(symbol, total_vol, impact, "SWEEP", now)


def _whale_process_force_order(data: Any, now: float) -> None:
    order_list = data if isinstance(data, list) else [data]
    for item in order_list:
        order = item.get("o") if isinstance(item, dict) else None
        if not order:
            continue
        try:
            volume_usdt = float(order["q"]) * float(order["p"])
            impact      = 0.25 if order["S"] == "BUY" else -0.25
            symbol      = str(order["s"]).replace("USDT", "").upper()
        except (KeyError, TypeError, ValueError):
            continue
        _whale_process_event(symbol, volume_usdt, impact, "LIQ", now)


def _whale_process_event(
    symbol: str, volume_usdt: float, impact: float, type_label: str, now: float
) -> None:
    if volume_usdt < WHALE_MIN_VOL_LIMIT or abs(impact) < WHALE_MIN_IMPACT:
        return
    with _lock:
        existing = _whale["events"].get(symbol)
        if existing:
            existing["_hacim_raw"] += volume_usdt
            if (impact > 0) == (existing["_raw_impact"] > 0):
                existing["streak"] = existing.get("streak", 1) + 1
            else:
                existing["streak"] = 1
            existing["_raw_impact"] = impact
            if type_label == "LIQ":
                existing["tip"] = "💥 LIQ"
            existing["Zaman"] = datetime.now().strftime("%H:%M:%S")
        else:
            _whale["events"][symbol] = {
                "Zaman":       datetime.now().strftime("%H:%M:%S"),
                "Coin":        symbol,
                "Tip":         "💥 LIQ" if type_label == "LIQ" else "🌊 SWEEP",
                "Etki":        f"{impact:+.2f}%",
                "_raw_impact": impact,
                "_hacim_raw":  volume_usdt,
                "streak":      1,
            }

        if len(_whale["events"]) > WHALE_MAX_ROWS:
            oldest = min(_whale["events"], key=lambda k: _whale["events"][k]["Zaman"])
            del _whale["events"][oldest]


async def _whale_async_run() -> None:
    import websockets as ws  # noqa: PLC0415

    streams = "/".join([f"{s}@aggTrade" for s in WHALE_SYMBOLS])
    uri = f"wss://fstream.binance.com/market/stream?streams={streams}/!forceOrder@arr"

    while True:
        try:
            async with ws.connect(
                uri, ping_interval=20, ping_timeout=15, close_timeout=3
            ) as websocket:
                with _lock:
                    _whale["connected"] = True
                    _whale["last_error"] = None
                while True:
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    except asyncio.TimeoutError:
                        continue
                    try:
                        packet = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    data   = packet.get("data")
                    stream = packet.get("stream", "")
                    if data is None:
                        continue
                    now = time.time()
                    with _lock:
                        _whale["last_sync"] = now
                    if "@aggTrade" in stream:
                        _whale_process_trade(data, now)
                    elif "!forceOrder" in stream:
                        _whale_process_force_order(data, now)
        except Exception as exc:
            with _lock:
                _whale["connected"] = False
                _whale["last_error"] = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(5.0)


def _whale_loop() -> None:
    loop = (
        asyncio.SelectorEventLoop()
        if sys.platform == "win32"
        else asyncio.new_event_loop()
    )
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_whale_async_run())


# ═══════════════════════════════════════════════════════════
# BTC CHANGE SERVICE
# ═══════════════════════════════════════════════════════════
def get_btc_change() -> dict:
    now = time.time()
    with _btc_lock:
        if now - _btc_cache["ts"] < 8.0 and _btc_cache["data"]:
            return _btc_cache["data"]
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(
                BTC_KLINES_URL,
                params={"symbol": "BTCUSDT", "interval": "1m", "limit": 17},
            )
            resp.raise_for_status()
            payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 17:
            return {}
        current  = float(payload[-2][4])
        ref_5m   = float(payload[-7][4])
        ref_15m  = float(payload[-17][4])
        data = {
            "price":       current,
            "change_5m":   ((current - ref_5m)  / ref_5m)  * 100,
            "change_15m":  ((current - ref_15m) / ref_15m) * 100,
        }
        with _btc_lock:
            _btc_cache["data"] = data
            _btc_cache["ts"]   = time.time()
        return data
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════════
# ENGINE BAŞLATMA (process başına bir kez)
# ═══════════════════════════════════════════════════════════
def _start_engines() -> None:
    with _lock:
        if not _radar["started"]:
            _radar["started"] = True
            threading.Thread(
                target=_radar_loop, name="radar-engine", daemon=True
            ).start()
        if not _whale["started"]:
            _whale["started"] = True
            threading.Thread(
                target=_whale_loop, name="whale-engine", daemon=True
            ).start()


# ═══════════════════════════════════════════════════════════
# STREAMLIT UI
# ═══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Fero Radar",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Özel CSS
st.markdown("""
<style>
    .stApp { background-color: #0d0f14; }
    .block-container { padding-top: 1rem; }
    h1 { color: #e8e8e8 !important; letter-spacing: -1px; }
    .stMetric { background: #161b24; border-radius: 8px; padding: 8px 12px; }
    .stMetric label { color: #888 !important; font-size: 0.75rem !important; }
    .stTabs [data-baseweb="tab-list"] { background: #161b24; border-radius: 8px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { color: #888; }
    .stTabs [aria-selected="true"] { color: #fff !important; background: #252d3d !important; border-radius: 6px; }
    .stDataFrame { border: 1px solid #1e2533; border-radius: 8px; }
    div[data-testid="stStatusWidget"] { display: none; }
    .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
    .dot-green { background: #22c55e; box-shadow: 0 0 6px #22c55e; }
    .dot-red   { background: #ef4444; }
    .dot-orange{ background: #f59e0b; }
</style>
""", unsafe_allow_html=True)

_start_engines()

# ── Header ─────────────────────────────────────
btc = get_btc_change()

col_title, col_btc, col_5m, col_15m, col_time = st.columns([3, 1.5, 1, 1, 1.2])

with col_title:
    st.markdown("## 🎯 Fero Radar")

with col_btc:
    if btc.get("price"):
        st.metric("BTC/USDT", f"${btc['price']:,.0f}")
    else:
        st.metric("BTC/USDT", "—")

with col_5m:
    c5 = btc.get("change_5m")
    if c5 is not None:
        delta_color = "normal" if c5 >= 0 else "inverse"
        st.metric("5 dk", f"{c5:+.2f}%")
    else:
        st.metric("5 dk", "—")

with col_15m:
    c15 = btc.get("change_15m")
    if c15 is not None:
        st.metric("15 dk", f"{c15:+.2f}%")
    else:
        st.metric("15 dk", "—")

with col_time:
    st.metric("Saat", datetime.now().strftime("%H:%M:%S"))

# ── Durum çubuğu ───────────────────────────────
now_ts = time.time()
with _lock:
    r_hb    = _radar["last_heartbeat"]
    r_pairs = _radar["total_pairs"]
    r_err   = _radar["last_error"]
    w_conn  = _whale["connected"]
    w_sync  = _whale["last_sync"]
    w_err   = _whale["last_error"]

radar_live = (now_ts - r_hb < 10) if r_hb else False
whale_live = (now_ts - w_sync < 10) if w_sync else False

sc1, sc2 = st.columns(2)
with sc1:
    if radar_live:
        st.markdown(
            f'<span class="status-dot dot-green"></span>'
            f'<small style="color:#888">Radar: CANLI | {r_pairs} çift izleniyor</small>',
            unsafe_allow_html=True,
        )
    elif r_err:
        st.markdown(
            f'<span class="status-dot dot-orange"></span>'
            f'<small style="color:#888">Radar: Bağlanıyor… {r_err[:60]}</small>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<span class="status-dot dot-red"></span>'
            '<small style="color:#888">Radar: Başlıyor (ilk sinyal ~15 dk sonra)</small>',
            unsafe_allow_html=True,
        )

with sc2:
    if whale_live:
        st.markdown(
            '<span class="status-dot dot-green"></span>'
            '<small style="color:#888">Whale: CANLI | WebSocket bağlı</small>',
            unsafe_allow_html=True,
        )
    elif w_err:
        st.markdown(
            f'<span class="status-dot dot-orange"></span>'
            f'<small style="color:#888">Whale: Yeniden bağlanıyor… {w_err[:60]}</small>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<span class="status-dot dot-red"></span>'
            '<small style="color:#888">Whale: Başlıyor…</small>',
            unsafe_allow_html=True,
        )

st.divider()

# ── Sekmeler ───────────────────────────────────
tab_radar, tab_whale = st.tabs(["🎯 Radar Sinyalleri", "🐳 Whale Takip"])

# ── RADAR sekmesi ──────────────────────────────
with tab_radar:
    with _lock:
        signals = list(_radar["signals"])

    if not signals:
        if radar_live:
            st.info(
                "⏳ Sinyal bekleniyor...\n\n"
                "Radar aktif ve veri topluyor. "
                "CONFIRMED sinyaller için en az 15 dakika veri gerekir. "
                "FLASH sinyaller daha erken gelebilir."
            )
        else:
            st.warning(
                "⏳ Radar başlıyor...\n\n"
                "Binance Futures verisi toplanıyor. "
                "İlk CONFIRMED sinyal yaklaşık 15 dakika sonra gelir."
            )
    else:
        df = pd.DataFrame(signals)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=min(600, 56 + len(df) * 36),
            column_config={
                "Sinyal": st.column_config.TextColumn("Sinyal", width="small"),
                "Mod":    st.column_config.TextColumn("Mod",    width="small"),
                "Zaman":  st.column_config.TextColumn("Zaman",  width="small"),
                "Coin":   st.column_config.TextColumn("Coin",   width="small"),
                "4s Pump": st.column_config.NumberColumn("4s 🟢", width="small"),
                "4s Dump": st.column_config.NumberColumn("4s 🔴", width="small"),
            },
        )
        st.caption(f"Toplam {len(signals)} sinyal gösteriliyor (son 100 kayıt)")

# ── WHALE sekmesi ──────────────────────────────
with tab_whale:
    with _lock:
        events = dict(_whale["events"])

    if not events:
        if whale_live:
            st.info("⏳ Whale hareketi bekleniyor... (WebSocket bağlı, eşiği geçen hareket yok)")
        else:
            st.warning("⏳ Whale motoru başlıyor, Binance WebSocket bağlanıyor...")
    else:
        rows = []
        for sym, ev in sorted(
            events.items(),
            key=lambda x: x[1]["Zaman"],
            reverse=True,
        ):
            hacim_raw = ev.get("_hacim_raw", 0)
            hacim_str = (
                f"${hacim_raw/1_000_000:.2f}M"
                if hacim_raw >= 1_000_000
                else f"${hacim_raw/1000:.0f}K"
            )
            rows.append({
                "Zaman":   ev["Zaman"],
                "Coin":    ev["Coin"],
                "Tip":     ev["Tip"],
                "Etki":    ev["Etki"],
                "Hacim":   hacim_str,
                "Streak":  ev["streak"],
            })

        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=min(600, 56 + len(df) * 36),
            column_config={
                "Streak": st.column_config.NumberColumn("🔥 Streak", width="small"),
                "Tip":    st.column_config.TextColumn("Tip",   width="small"),
                "Zaman":  st.column_config.TextColumn("Zaman", width="small"),
            },
        )
        st.caption(f"{len(events)} coin izleniyor")

# ── Sidebar ayarlar ────────────────────────────
st.sidebar.title("⚙️ Ayarlar")
refresh_rate = st.sidebar.slider("Yenileme süresi (sn)", 2, 15, 4)
st.sidebar.divider()
st.sidebar.caption("**Radar eşikleri**")
st.sidebar.caption(f"• Min hacim 3dk: ${RADAR_MIN_VOL_3M/1000:.0f}K")
st.sidebar.caption(f"• Min değişim 3dk: %{RADAR_MIN_CHG_3M}")
st.sidebar.caption(f"• CONFIRMED onay 15dk: %{RADAR_CONFIRM_CHG_15M}")
st.sidebar.caption(f"• FLASH değişim: %{RADAR_FAST_STRIKE_CHG}")
st.sidebar.divider()
st.sidebar.caption("**Whale eşikleri**")
st.sidebar.caption(f"• Min etki: %{WHALE_MIN_IMPACT}")
st.sidebar.caption(f"• Min hacim: ${WHALE_MIN_VOL_LIMIT/1000:.0f}K")
st.sidebar.divider()
st.sidebar.caption(f"Son güncelleme: {datetime.now().strftime('%H:%M:%S')}")

# ── Otomatik yenileme ──────────────────────────
time.sleep(refresh_rate)
st.rerun()
