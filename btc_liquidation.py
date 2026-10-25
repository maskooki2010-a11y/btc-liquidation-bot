
import asyncio
import json
import os
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.error import HTTPError

import websockets


# =====================================
# SMART FUTURES V11 PRO
# BYBIT BTC LIQUIDATION MONITOR
# BYBIT -> RENDER -> TELEGRAM
# =====================================

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
TOPIC = "allLiquidation.BTCUSDT"
MIN_USD = 200

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN", ""
).strip()

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID", ""
).strip()

seen_events = set()
recent_events = deque(maxlen=3000)


# =====================================
# HEALTH SERVER
# =====================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        path = self.path.split("?")[0]

        if path not in ("/", "/health"):
            self.send_error(404)
            return

        body = b"SMART FUTURES V11 BYBIT RUNNING"

        self.send_response(200)
        self.send_header(
            "Content-Type", "text/plain; charset=utf-8"
        )
        self.send_header(
            "Content-Length", str(len(body))
        )
        self.end_headers()
        self.wfile.write(body)


def start_http_server():
    port = int(os.environ.get("PORT", "10000"))

    server = ThreadingHTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    print(
        f"HTTP server listening on port {port}",
        flush=True
    )

    server.serve_forever()


# =====================================
# TELEGRAM
# =====================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(
            "ERROR: Telegram credentials missing",
            flush=True
        )
        return False

    url = (
        "https://api.telegram.org/bot"
        + TELEGRAM_BOT_TOKEN
        + "/sendMessage"
    )

    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        method="POST"
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=15
        ) as response:

            result = json.load(response)

            if result.get("ok") is True:
                print(
                    "Telegram message sent successfully",
                    flush=True
                )
                return True

            print(
                "Telegram API returned an error",
                flush=True
            )

    except HTTPError as error:
        print(
            f"Telegram HTTP error: {error.code}",
            flush=True
        )

    except Exception as error:
        print(
            f"Telegram send error: {type(error).__name__}",
            flush=True
        )

    return False


# =====================================
# STARTUP TEST
# =====================================

def startup_test():

    message = (
        "SMART FUTURES V11 PRO\n\n"
        "BYBIT CONNECTION TEST\n\n"
        "Render application started.\n"
        "Source: BYBIT FUTURES\n"
        "Symbol: BTCUSDT\n"
        f"Minimum liquidation: ${MIN_USD:,}\n\n"
        "Status: TELEGRAM TEST\n\n"
        "Waiting for Bybit connection.\n"
        "Not a trade signal."
    )

    success = send_telegram(message)

    if success:
        print("TELEGRAM TEST SUCCESS", flush=True)
    else:
        print("TELEGRAM TEST FAILED", flush=True)


# =====================================
# LIQUIDATION PROCESSING
# =====================================

def process_liquidation(item):

    if not isinstance(item, dict):
        return None

    if item.get("s") != "BTCUSDT":
        return None

    try:
        quantity = float(item.get("v", 0))
        price = float(item.get("p", 0))
        timestamp_ms = int(item.get("T", 0))
    except (ValueError, TypeError, OverflowError):
        return None

    if quantity <= 0 or price <= 0:
        return None

    usd_value = quantity * price

    if usd_value < MIN_USD:
        return None

    side = item.get("S", "")

    # Bybit reports the liquidated position side.
    if side == "Buy":
        liquidation_type = "LONG LIQUIDATION"
        direction = "🔴 LONG LIQUIDATION"

    elif side == "Sell":
        liquidation_type = "SHORT LIQUIDATION"
        direction = "🟢 SHORT LIQUIDATION"

    else:
        return None

    if timestamp_ms <= 0:
        return None

    event_time = datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=timezone.utc
    ).isoformat()

    return {
        "source": "BYBIT",
        "event": "BTC_LIQUIDATION",
        "symbol": "BTCUSDT",
        "side": side,
        "liquidation_type": liquidation_type,
        "direction": direction,
        "price": price,
        "quantity": quantity,
        "usd_value": round(usd_value, 2),
        "time": event_time,
        "timestamp_ms": timestamp_ms
    }


# =====================================
# LIQUIDATION ALERT
# =====================================

def send_liquidation_alert(payload):

    message = (
        "🚨 SMART FUTURES V11 PRO\n"
        "📊 BTC LIQUIDATION\n\n"
        f"Type: {payload['direction']}\n"
        f"Symbol: {payload['symbol']}\n"
        f"Bankruptcy Price: ${payload['price']:,.2f}\n"
        f"Estimated Value: ${payload['usd_value']:,.2f}\n"
        f"Quantity: {payload['quantity']:.6f} BTC\n\n"
        "Source: BYBIT FUTURES\n"
        f"Time UTC: {payload['time']}\n\n"
        "🧪 TEST MODE: $200 minimum\n"
        "⚠️ Not a trade signal."
    )

    return send_telegram(message)


# =====================================
# WEBSOCKET HEARTBEAT
# =====================================

async def heartbeat(websocket):

    while True:
        await asyncio.sleep(20)

        await websocket.send(
            json.dumps({"op": "ping"})
        )


# =====================================
# BYBIT MONITOR
# =====================================

async def monitor():

    print(
        "Starting BYBIT BTC Liquidation Monitor",
        flush=True
    )

    print(
        f"Minimum liquidation: ${MIN_USD:,}",
        flush=True
    )

    await asyncio.to_thread(startup_test)

    while True:

        ping_task = None

        try:
            print(
                "Connecting to Bybit...",
                flush=True
            )

            async with websockets.connect(
                BYBIT_WS,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30,
                max_size=2_000_000
            ) as websocket:

                print(
                    "Connected to Bybit",
                    flush=True
                )

                subscription = {
                    "op": "subscribe",
                    "args": [TOPIC]
                }

                await websocket.send(
                    json.dumps(subscription)
                )

                print(
                    f"Subscription requested: {TOPIC}",
                    flush=True
                )

                ping_task = asyncio.create_task(
                    heartbeat(websocket)
                )

                async for raw_message in websocket:

                    try:
                        data = json.loads(raw_message)

                        if data.get("op") == "subscribe":

                            if data.get("success") is True:
                                print(
                                    "BYBIT SUBSCRIPTION SUCCESS",
                                    flush=True
                                )
                            else:
                                print(
                                    "BYBIT SUBSCRIPTION FAILED",
                                    flush=True
                                )

                            continue

                        if data.get("topic") != TOPIC:
                            continue

                        events = data.get("data", [])

                        if isinstance(events, dict):
                            events = [events]

                        if not isinstance(events, list):
                            continue

                        for item in events:

                            payload = process_liquidation(item)

                            if payload is None:
                                continue

                            event_key = (
                                payload["timestamp_ms"],
                                payload["side"],
                                payload["price"],
                                payload["quantity"]
                            )

                            if event_key in seen_events:
                                continue

                            if len(recent_events) == recent_events.maxlen:
                                oldest = recent_events.popleft()
                                seen_events.discard(oldest)

                            recent_events.append(event_key)
                            seen_events.add(event_key)

                            print(
                                "BYBIT LIQUIDATION DETECTED",
                                flush=True
                            )

                            print(
                                json.dumps(payload),
                                flush=True
                            )

                            success = await asyncio.to_thread(
                                send_liquidation_alert,
                                payload
                            )

                            if success:
                                print(
                                    "LIQUIDATION ALERT SENT",
                                    flush=True
                                )
                            else:
                                print(
                                    "LIQUIDATION ALERT FAILED",
                                    flush=True
                                )

                    except Exception as error:
                        print(
                            f"Processing error: {error}",
                            flush=True
                        )

        except Exception as error:
            print(
                f"Connection error: {error}",
                flush=True
            )

        finally:
            if ping_task is not None:
                ping_task.cancel()

                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

        print(
            "Reconnecting in 5 seconds...",
            flush=True
        )

        await asyncio.sleep(5)


# =====================================
# MAIN
# =====================================

if __name__ == "__main__":

    Thread(
        target=start_http_server,
        daemon=True
    ).start()

    asyncio.run(monitor())
