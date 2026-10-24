
import asyncio
import json
import os
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import websockets

# SETTINGS
BINANCE_WS = "wss://fstream.binance.com/ws/btcusdt@forceOrder"
MIN_USD = 100_000
MAKE_WEBHOOK_URL = os.environ.get("MAKE_WEBHOOK_URL", "")


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


def send_to_make(payload):
    if not MAKE_WEBHOOK_URL:
        print("ERROR: MAKE_WEBHOOK_URL is missing", flush=True)
        return

    request = urllib.request.Request(
        MAKE_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            print(f"Make response: {response.status}", flush=True)
    except Exception as error:
        print(f"Make webhook error: {error}", flush=True)


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
                        "bot": "SMART FUTURES",
                        "event": "BTC_LIQUIDATION",
                        "symbol": symbol,
                        "side": side,
                        "liquidation_type": liquidation_type,
                        "price": price,
                        "quantity": quantity,
                        "usd_value": round(usd_value, 2),
                        "time": datetime.now(timezone.utc).isoformat()
                    }

                    print(json.dumps(payload), flush=True)

                    await asyncio.to_thread(send_to_make, payload)

        except Exception as error:
            print(f"Connection error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    Thread(target=start_http_server, daemon=True).start()
    asyncio.run(monitor())
