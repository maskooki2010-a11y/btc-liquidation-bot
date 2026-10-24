
import asyncio
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import websockets

# SETTINGS
BINANCE_WS = "wss://fstream.binance.com/ws/btcusdt@forceOrder"
MIN_USD = 100_000

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return

        body = b"BTC Liquidation Monitor running"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_http_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"HTTP server listening on port {port}", flush=True)
    server.serve_forever()


def send_to_telegram(payload):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: Telegram credentials missing", flush=True)
        return

    liquidation_type = payload["liquidation_type"]

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

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
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
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.load(response)
            if result.get("ok"):
                print("Telegram message sent successfully", flush=True)
            else:
                print("Telegram API returned an error", flush=True)
    except Exception as error:
        print(f"Telegram send error: {error}", flush=True)


async def monitor():
    print("Starting BTC Liquidation Monitor", flush=True)

    while True:
        try:
            async with websockets.connect(
                BINANCE_WS,
                ping_interval=20,
                ping_timeout=20
            ) as websocket:

                print("Connected to Binance", flush=True)

                async for message in websocket:
                    data = json.loads(message)
                    order = data.get("o", {})

                    symbol = order.get("s", "")
                    side = order.get("S", "")

                    quantity = float(order.get("q", 0))
                    price = float(order.get("ap", 0))

                    if price <= 0:
                        price = float(order.get("p", 0))

                    usd_value = quantity * price

                    if symbol != "BTCUSDT" or usd_value < MIN_USD:
                        continue

                    if side == "SELL":
                        liquidation_type = "LONG LIQUIDATION"
                    elif side == "BUY":
                        liquidation_type = "SHORT LIQUIDATION"
                    else:
                        continue

                    payload = {
                        "source": "BINANCE",
                        "symbol": symbol,
                        "side": side,
                        "liquidation_type": liquidation_type,
                        "price": price,
                        "quantity": quantity,
                        "usd_value": round(usd_value, 2),
                        "time": datetime.now(
                            timezone.utc
                        ).isoformat()
                    }

                    print(json.dumps(payload), flush=True)

                    await asyncio.to_thread(
                        send_to_telegram, payload
                    )

        except Exception as error:
            print(f"Connection error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    Thread(
        target=start_http_server,
        daemon=True
    ).start()

    asyncio.run(monitor())
