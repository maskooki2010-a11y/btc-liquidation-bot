
import asyncio
import json
import os
import time
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
# BYBIT LIQUIDATION + WATCHDOG
# + PIONEX LIVE PRICE
# =====================================

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
TOPIC = "allLiquidation.BTCUSDT"

MIN_USD = 200

CHECK_INTERVAL = 30
PING_TIMEOUT = 15
RECONNECT_DELAY = 5

PIONEX_URL = (
    "https://api.pionex.com/api/v1/market/trades"
    "?symbol=BTC_USDT_PERP&limit=10"
)

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN", ""
).strip()

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID", ""
).strip()

seen_events = set()
recent_events = deque(maxlen=3000)

status = {
    "connected": False,
    "subscribed": False,
    "last_message": None,
    "last_liquidation": None,
    "last_check": None,
    "last_error": None
}

# =====================================
# HEALTH SERVER - UNCHANGED
# =====================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        path = self.path.split("?")[0]

        if path not in ("/", "/health"):
            self.send_error(404)
            return

        if path == "/health":
            body = json.dumps(status).encode("utf-8")
            content_type = "application/json"
        else:
            body = b"SMART FUTURES V11 BYBIT RUNNING"
            content_type = "text/plain; charset=utf-8"

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_http_server():
    port = int(os.environ.get("PORT", "10000"))

    server = ThreadingHTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    print(f"HTTP server on port {port}", flush=True)
    server.serve_forever()


# =====================================
# TELEGRAM - UNCHANGED
# =====================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials missing", flush=True)
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

    except HTTPError as error:
        print(
            f"Telegram HTTP error: {error.code}",
            flush=True
        )

    except Exception as error:
        print(
            f"Telegram error: {error}",
            flush=True
        )

    return False


async def notify(message):
    return await asyncio.to_thread(
        send_telegram, message
    )


# =====================================
# NEW: PIONEX LIVE FUTURES PRICE
# =====================================

def get_pionex_price():
    request_started = time.time()

    try:
        request = urllib.request.Request(
            PIONEX_URL,
            headers={
                "Accept": "application/json",
                "User-Agent": "SmartFuturesV11/1.0"
            }
        )

        with urllib.request.urlopen(
            request,
            timeout=4
        ) as response:
            result = json.load(response)

        if result.get("result") is not True:
            raise ValueError("Pionex response not successful")

        trades = result.get("data", {}).get("trades", [])

        if not trades:
            raise ValueError("No Pionex trades returned")

        latest = max(
            trades,
            key=lambda trade: int(trade["timestamp"])
        )

        price = float(latest["price"])
        market_timestamp = int(latest["timestamp"])

        if price <= 0:
            raise ValueError("Invalid Pionex price")

        received_at = time.time()
        market_age = received_at - market_timestamp / 1000

        if market_age < -5 or market_age > 30:
            raise ValueError(
                f"Stale Pionex trade: {market_age:.1f}s"
            )

        return {
            "price": price,
            "received_at": received_at,
            "market_age": market_age,
            "request_seconds": received_at - request_started
        }

    except Exception as error:
        print(
            f"PIONEX PRICE ERROR: {error}",
            flush=True
        )
        return None


# =====================================
# LIQUIDATION PROCESSING - UNCHANGED
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

    if quantity <= 0 or price <= 0 or timestamp_ms <= 0:
        return None

    usd_value = quantity * price

    if usd_value < MIN_USD:
        return None

    side = item.get("S", "")

    if side == "Buy":
        direction = "🔴 LONG LIQUIDATION"
    elif side == "Sell":
        direction = "🟢 SHORT LIQUIDATION"
    else:
        return None

    event_time = datetime.fromtimestamp(
        timestamp_ms / 1000,
        tz=timezone.utc
    ).isoformat()

    return {
        "symbol": "BTCUSDT",
        "side": side,
        "direction": direction,
        "price": price,
        "quantity": quantity,
        "usd_value": round(usd_value, 2),
        "time": event_time,
        "timestamp_ms": timestamp_ms
    }


# =====================================
# LIQUIDATION ALERT + PIONEX PRICE
# =====================================

async def send_liquidation_alert(payload):

    # Fetch without blocking the event loop.
    pionex = await asyncio.to_thread(get_pionex_price)

    if pionex is not None:
        difference = pionex["price"] - payload["price"]
        difference_pct = (
            difference / payload["price"] * 100
        )

        delay = max(
            0,
            pionex["received_at"]
            - payload["timestamp_ms"] / 1000
        )

        pionex_section = (
            "📈 PIONEX FUTURES\n"
            f"Latest Trade: ${pionex['price']:,.2f}\n"
            f"Price Difference: ${difference:+,.2f}\n"
            f"Difference: {difference_pct:+.3f}%\n"
            f"Event Age: {delay:.1f} seconds\n"
            f"Pionex Trade Age: "
            f"{pionex['market_age']:.1f} seconds\n\n"
        )
    else:
        pionex_section = (
            "📈 PIONEX FUTURES\n"
            "Price: unavailable\n\n"
        )

    message = (
        "🚨 SMART FUTURES V11 PRO\n"
        "📊 BTC LIQUIDATION\n\n"
        f"Type: {payload['direction']}\n"
        f"Symbol: {payload['symbol']}\n"
        f"Bankruptcy Price: ${payload['price']:,.2f}\n"
        f"Estimated Value: ${payload['usd_value']:,.2f}\n"
        f"Quantity: {payload['quantity']:.6f} BTC\n\n"
        + pionex_section +
        "Source: BYBIT FUTURES\n"
        f"Time UTC: {payload['time']}\n\n"
        f"🧪 TEST MODE: ${MIN_USD:,} minimum\n"
        "⚠️ Not a trade signal."
    )

    success = await notify(message)

    if success:
        print("LIQUIDATION ALERT SENT", flush=True)
    else:
        print("LIQUIDATION ALERT FAILED", flush=True)


# =====================================
# WATCHDOG - UNCHANGED
# =====================================

async def watchdog(websocket):

    while True:
        await asyncio.sleep(CHECK_INTERVAL)

        status["last_check"] = datetime.now(
            timezone.utc
        ).isoformat()

        try:
            pong = await websocket.ping()

            await asyncio.wait_for(
                pong,
                timeout=PING_TIMEOUT
            )

            print(
                "WATCHDOG OK - BYBIT CONNECTION ALIVE",
                flush=True
            )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            print(
                f"WATCHDOG FAILED: {error}",
                flush=True
            )

            status["last_error"] = str(error)

            await websocket.close()
            return


# =====================================
# BYBIT MONITOR - UNCHANGED
# =====================================

async def monitor():

    print(
        "Starting SMART FUTURES V11 WATCHDOG",
        flush=True
    )

    await notify(
        "🚀 SMART FUTURES V11 PRO\n\n"
        "Render application started.\n"
        "Watchdog enabled.\n"
        "Source: BYBIT FUTURES\n"
        "Symbol: BTCUSDT\n"
        f"Minimum liquidation: ${MIN_USD:,}\n\n"
        "Waiting for Bybit connection."
    )

    previously_connected = False

    while True:

        watchdog_task = None

        try:
            print("Connecting to Bybit...", flush=True)

            async with websockets.connect(
                BYBIT_WS,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30,
                max_size=2_000_000
            ) as websocket:

                status["connected"] = True
                status["subscribed"] = False
                status["last_error"] = None

                print("Connected to Bybit", flush=True)

                await websocket.send(
                    json.dumps({
                        "op": "subscribe",
                        "args": [TOPIC]
                    })
                )

                watchdog_task = asyncio.create_task(
                    watchdog(websocket)
                )

                async for raw_message in websocket:

                    status["last_message"] = datetime.now(
                        timezone.utc
                    ).isoformat()

                    try:
                        data = json.loads(raw_message)

                        if data.get("op") == "subscribe":

                            if data.get("success") is True:
                                status["subscribed"] = True

                                print(
                                    "BYBIT SUBSCRIPTION SUCCESS",
                                    flush=True
                                )

                                if previously_connected:
                                    await notify(
                                        "🟢 SMART FUTURES V11 PRO\n\n"
                                        "BYBIT CONNECTION RESTORED\n"
                                        "Liquidation monitoring resumed."
                                    )
                                else:
                                    await notify(
                                        "🟢 SMART FUTURES V11 PRO\n\n"
                                        "BYBIT CONNECTED\n"
                                        "Liquidation monitoring active."
                                    )

                                previously_connected = True

                            else:
                                raise RuntimeError(
                                    "Bybit subscription rejected"
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

                            status["last_liquidation"] = (
                                datetime.now(
                                    timezone.utc
                                ).isoformat()
                            )

                            print(
                                "BYBIT LIQUIDATION DETECTED",
                                flush=True
                            )

                            print(
                                json.dumps(payload),
                                flush=True
                            )

                            await send_liquidation_alert(payload)

                    except asyncio.CancelledError:
                        raise

                    except RuntimeError:
                        raise

                    except Exception as error:
                        print(
                            f"Processing error: {error}",
                            flush=True
                        )

        except asyncio.CancelledError:
            raise

        except Exception as error:

            status["last_error"] = str(error)

            print(
                f"Connection error: {error}",
                flush=True
            )

        finally:

            status["connected"] = False
            status["subscribed"] = False

            if watchdog_task is not None:
                watchdog_task.cancel()

                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass

        if previously_connected:
            await notify(
                "🔴 SMART FUTURES V11 PRO\n\n"
                "BYBIT CONNECTION INTERRUPTED\n"
                "Automatic reconnection in progress."
            )

        print(
            "Reconnecting in 5 seconds...",
            flush=True
        )

        await asyncio.sleep(RECONNECT_DELAY)


# =====================================
# MAIN - UNCHANGED
# =====================================

if __name__ == "__main__":

    Thread(
        target=start_http_server,
        daemon=True
    ).start()

    asyncio.run(monitor())
