
import os
import json
import time
import threading
import logging
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket

# ==========================================
# SMART FUTURES V11.2
# BTC LIQUIDATION + INDEPENDENT DIRECTION
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    force=True
)

log = logging.getLogger("SMART_V11")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

PORT = int(os.getenv("PORT", "10000"))

THRESHOLD = float(
    os.getenv("LIQ_THRESHOLD_USD", "500")
)

LIQ_COOLDOWN = float(
    os.getenv("LIQ_COOLDOWN_SECONDS", "0")
)

DIRECTION_INTERVAL = 60
DIRECTION_REMINDER = 900

BYBIT_WS = (
    "wss://stream.bybit.com/v5/public/linear"
)

BYBIT_TOPIC = "allLiquidation.BTCUSDT"

BINANCE_URL = (
    "https://api.binance.com/api/v3/klines"
)

KRAKEN_URL = (
    "https://api.kraken.com/0/public/OHLC"
)

session = requests.Session()
session.headers.update({
    "User-Agent": "SmartFuturesBTC/11.2"
})

lock = threading.Lock()

status = {
    "liquidation_connected": False,
    "liquidation_events": 0,
    "alerts_sent": 0,
    "last_liquidation": None,
    "direction": "UNKNOWN",
    "direction_source": None,
    "direction_updated": None,
    "last_error": None
}

last_liq_alert = 0


# ==========================================
# TELEGRAM
# ==========================================

def send_telegram(message):

    if not TOKEN or not CHAT_ID:
        log.error("Telegram credentials missing")
        return False

    try:
        response = session.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": message
            },
            timeout=12
        )

        response.raise_for_status()

        result = response.json()

        if not result.get("ok"):
            log.error(
                "Telegram rejected message: %s",
                result.get("description")
            )
            return False

        log.info("TELEGRAM MESSAGE SENT")
        return True

    except Exception as error:
        log.error(
            "TELEGRAM SEND FAILED: %s",
            error
        )
        return False


# ==========================================
# HTTP SERVER FOR RENDER
# ==========================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return

        with lock:
            snapshot = dict(status)

        snapshot["service"] = "SMART FUTURES V11.2"
        snapshot["threshold_usd"] = THRESHOLD

        body = json.dumps(snapshot).encode()

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "application/json"
        )
        self.send_header(
            "Content-Length",
            str(len(body))
        )
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        return


def http_server():

    server = ThreadingHTTPServer(
        ("0.0.0.0", PORT),
        HealthHandler
    )

    log.info(
        "HTTP SERVER READY PORT=%s",
        PORT
    )

    server.serve_forever()


# ==========================================
# LIQUIDATION MONITOR
# FULLY INDEPENDENT
# ==========================================

def process_liquidation(item):

    global last_liq_alert

    try:
        side = str(item.get("S", "")).upper()
        price = float(item.get("p", 0))
        quantity = float(item.get("v", 0))

        value = price * quantity

        if side not in ("BUY", "SELL"):
            return

        if price <= 0 or quantity <= 0:
            return

        with lock:
            status["liquidation_events"] += 1
            status["last_liquidation"] = (
                datetime.now(timezone.utc).isoformat()
            )

        log.info(
            "LIQ EVENT side=%s price=%.2f value=%.2f",
            side,
            price,
            value
        )

        if value < THRESHOLD:
            return

        now = time.monotonic()

        if (
            LIQ_COOLDOWN > 0
            and now - last_liq_alert < LIQ_COOLDOWN
        ):
            return

        if side == "SELL":
            liq_type = "LONG LIQUIDATED"
            market_pressure = "SELL PRESSURE"
            emoji = "🔴"
        else:
            liq_type = "SHORT LIQUIDATED"
            market_pressure = "BUY PRESSURE"
            emoji = "🟢"

        message = (
            "🚨 SMART FUTURES V11.2\n\n"
            f"{emoji} BTC LIQUIDATION ALERT\n\n"
            "Asset: BTCUSDT\n"
            f"Type: {liq_type}\n"
            f"Price: ${price:,.2f}\n"
            f"Quantity: {quantity:.6f} BTC\n"
            f"Liquidation Value: ${value:,.2f}\n\n"
            f"Pressure: {market_pressure}\n"
            "Source: Bybit Futures\n\n"
            "⚡ Independent Liquidation Monitor"
        )

        if send_telegram(message):

            last_liq_alert = now

            with lock:
                status["alerts_sent"] += 1

            log.info(
                "LIQ ALERT SENT value=%.2f",
                value
            )

    except Exception:
        log.exception(
            "LIQUIDATION PROCESSING ERROR"
        )


def on_ws_message(ws, raw_message):

    try:
        message = json.loads(raw_message)

        topic = message.get("topic", "")

        if topic != BYBIT_TOPIC:
            return

        events = message.get("data", [])

        if isinstance(events, dict):
            events = [events]

        for item in events:
            process_liquidation(item)

    except Exception:
        log.exception("WEBSOCKET MESSAGE ERROR")


def on_ws_open(ws):

    with lock:
        status["liquidation_connected"] = True

    log.info("BYBIT WEBSOCKET CONNECTED")

    ws.send(json.dumps({
        "op": "subscribe",
        "args": [BYBIT_TOPIC]
    }))

    log.info(
        "SUBSCRIPTION REQUESTED: %s",
        BYBIT_TOPIC
    )


def on_ws_error(ws, error):

    with lock:
        status["last_error"] = str(error)

    log.error("BYBIT WS ERROR: %s", error)


def on_ws_close(ws, code, message):

    with lock:
        status["liquidation_connected"] = False

    log.warning(
        "BYBIT WS CLOSED code=%s message=%s",
        code,
        message
    )


def liquidation_monitor():

    log.info(
        "LIQUIDATION MONITOR STARTED THRESHOLD=$%.2f",
        THRESHOLD
    )

    while True:

        try:
            ws = websocket.WebSocketApp(
                BYBIT_WS,
                on_open=on_ws_open,
                on_message=on_ws_message,
                on_error=on_ws_error,
                on_close=on_ws_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:
            log.exception(
                "LIQUIDATION MONITOR ERROR"
            )

        with lock:
            status["liquidation_connected"] = False

        log.warning(
            "RECONNECTING BYBIT IN 5 SECONDS"
        )

        time.sleep(5)


# ==========================================
# DIRECTION DATA
# INDEPENDENT FROM LIQUIDATIONS
# ==========================================

def get_binance_candles(interval):

    response = session.get(
        BINANCE_URL,
        params={
            "symbol": "BTCUSDT",
            "interval": f"{interval}m",
            "limit": 120
        },
        timeout=12
    )

    response.raise_for_status()

    rows = response.json()

    if not isinstance(rows, list):
        raise ValueError("Invalid Binance response")

    # Binance: remove still-open candle
    rows = rows[:-1]

    return [float(row[4]) for row in rows]


def get_kraken_candles(interval):

    response = session.get(
        KRAKEN_URL,
        params={
            "pair": "XBTUSD",
            "interval": interval
        },
        timeout=12
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("error"):
        raise ValueError(str(payload["error"]))

    result = payload["result"]

    pair_key = next(
        key for key in result
        if key != "last"
    )

    rows = result[pair_key]

    # Kraken: last candle is still forming
    rows = rows[:-1]

    return [float(row[4]) for row in rows]


def get_candles(interval):

    try:
        values = get_binance_candles(interval)

        if len(values) >= 55:
            return values, "BINANCE"

    except Exception as error:
        log.warning(
            "BINANCE %sm unavailable: %s",
            interval,
            error
        )

    values = get_kraken_candles(interval)

    if len(values) < 55:
        raise ValueError("Not enough candle history")

    return values, "KRAKEN"


# ==========================================
# EMA DIRECTION ANALYSIS
# ==========================================

def ema(values, period):

    alpha = 2 / (period + 1)

    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            value * alpha
            + result * (1 - alpha)
        )

    return result


def analyze_direction():

    closes5, source5 = get_candles(5)
    closes15, source15 = get_candles(15)

    ema9_5 = ema(closes5, 9)
    ema21_5 = ema(closes5, 21)

    ema9_15 = ema(closes15, 9)
    ema21_15 = ema(closes15, 21)

    if (
        ema9_5 > ema21_5
        and ema9_15 > ema21_15
    ):
        direction = "BULLISH"

    elif (
        ema9_5 < ema21_5
        and ema9_15 < ema21_15
    ):
        direction = "BEARISH"

    else:
        direction = "MIXED"

    return (
        direction,
        closes5[-1],
        f"5m:{source5} / 15m:{source15}"
    )


def direction_monitor():

    log.info("DIRECTION MONITOR STARTED")

    previous_direction = None
    last_sent = 0

    while True:

        try:
            direction, price, source = (
                analyze_direction()
            )

            with lock:
                status["direction"] = direction
                status["direction_source"] = source
                status["direction_updated"] = (
                    datetime.now(timezone.utc).isoformat()
                )

            log.info(
                "BTC DIRECTION=%s PRICE=%.2f SOURCE=%s",
                direction,
                price,
                source
            )

            now = time.monotonic()

            should_send = (
                direction in ("BULLISH", "BEARISH")
                and (
                    direction != previous_direction
                    or now - last_sent >= DIRECTION_REMINDER
                )
            )

            if should_send:

                if direction == "BULLISH":
                    icon = "🟢"
                    description = "UPTREND"
                else:
                    icon = "🔴"
                    description = "DOWNTREND"

                message = (
                    "📊 SMART FUTURES V11.2\n\n"
                    f"{icon} BTC DIRECTION\n\n"
                    f"Direction: {description}\n"
                    f"Price: ${price:,.2f}\n"
                    "Timeframes: 5m + 15m\n"
                    f"Source: {source}\n\n"
                    "EMA 9/21 alignment\n"
                    "Not a confirmed trade entry\n\n"
                    "🧭 Independent Direction Analyzer"
                )

                if send_telegram(message):
                    previous_direction = direction
                    last_sent = now

        except Exception as error:

            with lock:
                status["last_error"] = str(error)
                status["direction"] = "UNAVAILABLE"

            log.exception(
                "DIRECTION ANALYZER ERROR"
            )

        time.sleep(DIRECTION_INTERVAL)


# ==========================================
# STARTUP
# ==========================================

def main():

    log.info("SMART FUTURES V11.2 STARTING")
    log.info("BTC ONLY")
    log.info("LIQ THRESHOLD=$%.2f", THRESHOLD)

    # HTTP server starts immediately.
    threading.Thread(
        target=http_server,
        name="http-server",
        daemon=True
    ).start()

    # Liquidation and direction run independently.
    threading.Thread(
        target=liquidation_monitor,
        name="liquidation-monitor",
        daemon=True
    ).start()

    threading.Thread(
        target=direction_monitor,
        name="direction-monitor",
        daemon=True
    ).start()

    send_telegram(
        "✅ SMART FUTURES V11.2 STARTED\n\n"
        "BTC monitoring initialized\n"
        f"Liquidation threshold: ${THRESHOLD:,.0f}\n"
        "Liquidation: Bybit WebSocket\n"
        "Direction: Binance / Kraken\n\n"
        "⚡ Independent monitoring modules\n"
        "Startup test only — not a trading signal"
    )

    # Keep main process alive.
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
