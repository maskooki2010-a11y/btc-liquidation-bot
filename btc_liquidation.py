import asyncio
import json
import os
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import websockets

# ======================================
# SMART FUTURES V11
# BTC LIQUIDATION ONLY
# ======================================

WS_URL = "wss://stream.bybit.com/v5/public/linear"

TOPIC = "allLiquidation.BTCUSDT"

MIN_USD = float(
    os.getenv("MIN_LIQUIDATION_USD", "500")
)

TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", ""
).strip()

CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID", ""
).strip()


# ======================================
# RENDER HEALTH SERVER
# ======================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.split("?")[0] not in ("/", "/health"):
            self.send_error(404)
            return

        body = b"SMART FUTURES BTC LIQUIDATION RUNNING"

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def health_server():
    port = int(os.getenv("PORT", "10000"))

    server = ThreadingHTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    print("HTTP SERVER STARTED", flush=True)
    server.serve_forever()


# ======================================
# TELEGRAM
# ======================================

def telegram_send(message):

    if not TOKEN or not CHAT_ID:
        print("TELEGRAM CONFIG MISSING", flush=True)
        return False

    url = (
        "https://api.telegram.org/bot"
        + TOKEN
        + "/sendMessage"
    )

    data = urllib.parse.urlencode({
        "chat_id": CHAT_ID,
        "text": message
    }).encode("utf-8")

    try:
        request = urllib.request.Request(
            url,
            data=data,
            method="POST"
        )

        with urllib.request.urlopen(
            request,
            timeout=12
        ) as response:
            result = json.load(response)

        if result.get("ok") is True:
            print("TELEGRAM SENT", flush=True)
            return True

        print("TELEGRAM API ERROR", flush=True)
        return False

    except Exception as error:
        print(
            f"TELEGRAM ERROR: {error}",
            flush=True
        )
        return False


# ======================================
# LIQUIDATION MONITOR
# ======================================

async def liquidation_monitor():

    seen = set()
    seen_order = deque()

    print(
        f"LIQUIDATION MONITOR STARTED: ${MIN_USD}",
        flush=True
    )

    while True:

        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30
            ) as ws:

                print(
                    "BYBIT WEBSOCKET CONNECTED",
                    flush=True
                )

                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [TOPIC]
                }))

                print(
                    "LIQUIDATION SUBSCRIPTION REQUESTED",
                    flush=True
                )

                async for raw in ws:

                    message = json.loads(raw)

                    if message.get("op") == "subscribe":
                        print(
                            "SUBSCRIPTION RESPONSE:",
                            message.get("success"),
                            flush=True
                        )
                        continue

                    if message.get("topic") != TOPIC:
                        continue

                    events = message.get("data", [])

                    if isinstance(events, dict):
                        events = [events]

                    for item in events:

                        try:
                            if item.get("s") != "BTCUSDT":
                                continue

                            price = float(item.get("p", 0))
                            qty = float(item.get("v", 0))
                            side = item.get("S")
                            stamp = int(item.get("T", 0))

                            if price <= 0 or qty <= 0:
                                continue

                            if side not in ("Buy", "Sell"):
                                continue

                            value = price * qty

                            if value < MIN_USD:
                                continue

                            key = (
                                stamp,
                                side,
                                price,
                                qty
                            )

                            if key in seen:
                                continue

                            if len(seen_order) >= 3000:
                                old = seen_order.popleft()
                                seen.discard(old)

                            seen.add(key)
                            seen_order.append(key)

                            # Bybit liquidation side:
                            # Sell = LONG liquidated
                            # Buy  = SHORT liquidated

                            if side == "Sell":
                                kind = "LONG LIQUIDATED"
                                emoji = "🔴"
                            else:
                                kind = "SHORT LIQUIDATED"
                                emoji = "🟢"

                            timestamp = datetime.fromtimestamp(
                                stamp / 1000,
                                tz=timezone.utc
                            ).strftime("%Y-%m-%d %H:%M:%S UTC")

                            alert = (
                                "🚨 SMART FUTURES V11 PRO\n\n"
                                f"{emoji} BTC LIQUIDATION\n\n"
                                f"Type: {kind}\n"
                                "Symbol: BTCUSDT\n"
                                f"Price: ${price:,.2f}\n"
                                f"Quantity: {qty:.6f} BTC\n"
                                f"Value: ${value:,.2f}\n"
                                f"Time: {timestamp}\n\n"
                                "Source: Bybit Futures\n"
                                "⚡ LIQUIDATION ALERT ONLY"
                            )

                            print(
                                f"LIQ EVENT: {kind} ${value:,.2f}",
                                flush=True
                            )

                            await asyncio.to_thread(
                                telegram_send,
                                alert
                            )

                        except Exception as error:
                            print(
                                f"LIQ EVENT ERROR: {error}",
                                flush=True
                            )

        except Exception as error:
            print(
                f"WEBSOCKET ERROR: {error}",
                flush=True
            )

        print(
            "RECONNECTING IN 5 SECONDS",
            flush=True
        )

        await asyncio.sleep(5)


# ======================================
# STARTUP
# ======================================

async def main():

    print(
        "SMART FUTURES V11 STARTING",
        flush=True
    )

    print(
        f"MIN LIQUIDATION: ${MIN_USD}",
        flush=True
    )

    await asyncio.to_thread(
        telegram_send,
        (
            "✅ SMART FUTURES V11 STARTED\n\n"
            "BTC Liquidation Monitor\n"
            f"Minimum: ${MIN_USD:,.0f}\n"
            "Source: Bybit Futures\n\n"
            "Liquidation alerts only"
        )
    )

    await liquidation_monitor()


if __name__ == "__main__":

    Thread(
        target=health_server,
        daemon=True
    ).start()

    asyncio.run(main())
