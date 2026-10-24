
import os
import json
import time
import threading
import logging
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
LOG = logging.getLogger("smart-futures")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "10000"))

LIQ_THRESHOLD = float(os.getenv("LIQ_THRESHOLD_USD", "100000"))
LIQ_WINDOW = int(os.getenv("LIQ_WINDOW_SECONDS", "60"))
LIQ_COOLDOWN = int(os.getenv("LIQ_COOLDOWN_SECONDS", "45"))

DIRECTION_INTERVAL = int(os.getenv("DIRECTION_INTERVAL_SECONDS", "60"))
DIRECTION_COOLDOWN = int(os.getenv("DIRECTION_COOLDOWN_SECONDS", "900"))

session = requests.Session()
state_lock = threading.Lock()

state = {
    "ws_connected": False,
    "last_liquidation": None,
    "last_direction": None,
    "direction": "WARMING_UP"
}

recent = deque()
last_alert = {}


# ===== TELEGRAM =====

def telegram(message):
    if not TOKEN or not CHAT_ID:
        LOG.warning("Telegram credentials missing")
        return False

    try:
        r = session.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": message
            },
            timeout=12
        )
        r.raise_for_status()
        return bool(r.json().get("ok"))

    except Exception as exc:
        LOG.error("Telegram error: %s", exc)
        return False


# ===== RENDER HTTP SERVER =====

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return

        with state_lock:
            payload = dict(state)

        body = json.dumps({
            "service": "smart-futures-v11",
            **payload
        }).encode()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


def run_http():
    server = ThreadingHTTPServer(
        ("0.0.0.0", PORT),
        HealthHandler
    )

    LOG.info(
        "HTTP health server listening on port %s",
        PORT
    )
    server.serve_forever()


# ===== INDEPENDENT LIQUIDATION MONITOR =====

def on_liquidation(ws, message):
    try:
        payload = json.loads(message)

        if payload.get("topic") != "allLiquidation.BTCUSDT":
            return

        events = payload.get("data", [])

        if isinstance(events, dict):
            events = [events]

        now = time.time()
        outgoing = []

        with state_lock:
            for item in events:
                side = str(item.get("S", "")).upper()
                price = float(item.get("p", 0))
                qty = float(item.get("v", 0))
                usd = price * qty

                if side not in ("BUY", "SELL") or usd <= 0:
                    continue

                recent.append((now, side, usd))

                state["last_liquidation"] = (
                    datetime.now(timezone.utc).isoformat()
                )

            while recent and recent[0][0] < now - LIQ_WINDOW:
                recent.popleft()

            for side in ("BUY", "SELL"):
                total = sum(
                    x[2] for x in recent if x[1] == side
                )

                if (
                    total >= LIQ_THRESHOLD
                    and now - last_alert.get(side, 0) >= LIQ_COOLDOWN
                ):
                    last_alert[side] = now

                    # Bybit reports the side of the liquidated position.
                    label = (
                        "LONG liquidations"
                        if side == "BUY"
                        else "SHORT liquidations"
                    )

                    outgoing.append(
                        f"🚨 BTC LIQUIDATION\n"
                        f"{label}\n"
                        f"Last {LIQ_WINDOW}s: ${total:,.0f}\n"
                        f"Direction analysis: independent"
                    )

        for text in outgoing:
            telegram(text)

    except Exception:
        LOG.exception("Liquidation processing error")


def on_open(ws):
    with state_lock:
        state["ws_connected"] = True

    LOG.info("Bybit liquidation websocket connected")

    ws.send(json.dumps({
        "op": "subscribe",
        "args": ["allLiquidation.BTCUSDT"]
    }))


def on_close(ws, code, msg):
    with state_lock:
        state["ws_connected"] = False

    LOG.warning("Websocket closed: %s %s", code, msg)


def on_error(ws, err):
    LOG.warning("Websocket error: %s", err)


def liquidation_loop():
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://stream.bybit.com/v5/public/linear",
                on_open=on_open,
                on_message=on_liquidation,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:
            LOG.exception("Websocket reconnecting")

        with state_lock:
            state["ws_connected"] = False

        time.sleep(5)


# ===== INDEPENDENT DIRECTION ANALYZER =====

def candles(interval, limit=120):
    r = session.get(
        "https://api.bybit.com/v5/market/kline",
        params={
            "category": "linear",
            "symbol": "BTCUSDT",
            "interval": interval,
            "limit": limit
        },
        timeout=12
    )

    r.raise_for_status()
    result = r.json()

    if result.get("retCode") != 0:
        raise ValueError(str(result.get("retMsg")))

    rows = result["result"]["list"]

    # Remove the still-forming candle.
    closed = sorted(
        rows,
        key=lambda row: int(row[0])
    )[:-1]

    return [float(row[4]) for row in closed]


def ema(values, period):
    alpha = 2 / (period + 1)
    result = values[0]

    for v in values[1:]:
        result = alpha * v + (1 - alpha) * result

    return result


def direction_signal():
    c5 = candles("5")
    c15 = candles("15")

    if min(len(c5), len(c15)) < 55:
        return "WARMING_UP", None

    f5 = ema(c5, 9)
    s5 = ema(c5, 21)

    f15 = ema(c15, 9)
    s15 = ema(c15, 21)

    if f5 > s5 and f15 > s15:
        return "BULLISH", c5[-1]

    if f5 < s5 and f15 < s15:
        return "BEARISH", c5[-1]

    return "MIXED", c5[-1]


def direction_loop():
    last_sent = None
    last_time = 0

    while True:
        try:
            direction, price = direction_signal()

            with state_lock:
                state["direction"] = direction
                state["last_direction"] = (
                    datetime.now(timezone.utc).isoformat()
                )

            LOG.info(
                "DIRECTION %s | BTC=%s",
                direction,
                price
            )

            now = time.time()

            if (
                direction in ("BULLISH", "BEARISH")
                and (
                    direction != last_sent
                    or now - last_time >= DIRECTION_COOLDOWN
                )
            ):
                message = (
                    f"📊 BTC DIRECTION\n"
                    f"5m + 15m: {direction}\n"
                    f"BTC: ${price:,.2f}\n"
                    f"EMA alignment only; "
                    f"not a trade recommendation"
                )

                if telegram(message):
                    last_sent = direction
                    last_time = now

        except Exception:
            LOG.exception("Direction analyzer error")

        time.sleep(max(30, DIRECTION_INTERVAL))


# ===== START ALL SERVICES =====

if __name__ == "__main__":

    threading.Thread(
        target=liquidation_loop,
        name="liquidation",
        daemon=True
    ).start()

    threading.Thread(
        target=direction_loop,
        name="direction",
        daemon=True
    ).start()

    run_http()
