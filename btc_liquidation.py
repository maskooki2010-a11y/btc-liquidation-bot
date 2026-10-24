import asyncio
import json
import os
import urllib.parse
import urllib.request

from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import websockets


# =====================================
# SMART FUTURES V11 PRO
# BTC LIQUIDATION MONITOR
# DIRECT TELEGRAM CONNECTION
# =====================================

BINANCE_WS = (
    "wss://fstream.binance.com/ws/"
    "btcusdt@forceOrder"
)

# TEST MODE: $200
MIN_USD = 200

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN", ""
)

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID", ""
)


# =====================================
# HEALTH SERVER FOR RENDER
# =====================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path not in ("/", "/health"):
            self.send_error(404)
            return

        body = b"BTC Liquidation Monitor running"

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "text/plain"
        )

        self.send_header(
            "Content-Length",
            str(len(body))
        )

        self.end_headers()
        self.wfile.write(body)


def start_http_server():

    port = int(
        os.environ.get("PORT", "10000")
    )

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
# TELEGRAM MESSAGE SENDER
# =====================================

def send_telegram_message(message):

    if not TELEGRAM_BOT_TOKEN:

        print(
            "ERROR: TELEGRAM_BOT_TOKEN missing",
            flush=True
        )

        return False

    if not TELEGRAM_CHAT_ID:

        print(
            "ERROR: TELEGRAM_CHAT_ID missing",
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
            timeout=10
        ) as response:

            result = json.load(response)

            if result.get("ok"):

                print(
                    "Telegram message sent successfully",
                    flush=True
                )

                return True

            print(
                "Telegram API returned an error",
                flush=True
            )

            return False

    except Exception as error:

        print(
            f"Telegram send error: {error}",
            flush=True
        )

        return False


# =====================================
# STARTUP TELEGRAM TEST
# =====================================

def send_startup_test():

    print(
        "Starting Telegram connection test...",
        flush=True
    )

    message = (
        "✅ SMART FUTURES V11 PRO\n\n"
        "Telegram Connection Test\n\n"
        "Render service started.\n"
        "BTC Liquidation Monitor active.\n"
        "Minimum liquidation: $200\n\n"
        "Source: Binance Futures\n"
        "Status: TEST MESSAGE\n\n"
        "⚠️ This is not a trade signal."
    )

    success = send_telegram_message(message)

    if success:

        print(
            "TELEGRAM TEST SUCCESS",
            flush=True
        )

    else:

        print(
            "TELEGRAM TEST FAILED",
            flush=True
        )


# =====================================
# LIQUIDATION MESSAGE
# =====================================

def send_to_telegram(payload):

    liquidation_type = payload[
        "liquidation_type"
    ]

    if liquidation_type == "LONG LIQUIDATION":

        direction = "🔴 LONG LIQUIDATION"

    else:

        direction = "🟢 SHORT LIQUIDATION"

    message = (
        "🚨 SMART FUTURES V11 PRO\n"
        "📊 BTC LIQUIDATION\n\n"
        f"Type: {direction}\n"
        f"Price: ${payload['price']:,.2f}\n"
        f"Value: ${payload['usd_value']:,.0f}\n"
        f"Quantity: {payload['quantity']:.4f} BTC\n"
        "Source: BINANCE\n"
        f"Time (UTC): {payload['time']}\n\n"
        "⚠️ Market data only - not a trade signal"
    )

    return send_telegram_message(message)


# =====================================
# BINANCE LIQUIDATION MONITOR
# =====================================

async def monitor():

    print(
        "Starting BTC Liquidation Monitor",
        flush=True
    )

    print(
        f"Minimum liquidation: ${MIN_USD}",
        flush=True
    )

    while True:

        try:

            async with websockets.connect(
                BINANCE_WS,
                ping_interval=20,
                ping_timeout=20
            ) as websocket:

                print(
                    "Connected to Binance",
                    flush=True
                )

                async for message in websocket:

                    data = json.loads(message)

                    order = data.get("o", {})

                    symbol = order.get("s", "")

                    side = order.get("S", "")

                    quantity = float(
                        order.get("q", 0)
                    )

                    price = float(
                        order.get("ap", 0)
                    )

                    if price <= 0:

                        price = float(
                            order.get("p", 0)
                        )

                    usd_value = quantity * price

                    if symbol != "BTCUSDT":
                        continue

                    if usd_value < MIN_USD:
                        continue

                    if side == "SELL":

                        liquidation_type = (
                            "LONG LIQUIDATION"
                        )

                    elif side == "BUY":

                        liquidation_type = (
                            "SHORT LIQUIDATION"
                        )

                    else:
                        continue

                    payload = {

                        "source": "BINANCE",

                        "event": "BTC_LIQUIDATION",

                        "symbol": symbol,

                        "side": side,

                        "liquidation_type": liquidation_type,

                        "price": price,

                        "quantity": quantity,

                        "usd_value": round(
                            usd_value, 2
                        ),

                        "time": datetime.now(
                            timezone.utc
                        ).isoformat()

                    }

                    print(
                        json.dumps(payload),
                        flush=True
                    )

                    await asyncio.to_thread(
                        send_to_telegram,
                        payload
                    )

        except Exception as error:

            print(
                f"Connection error: {error}",
                flush=True
            )

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

    # Send one test per application startup
    send_startup_test()

    asyncio.run(monitor())
