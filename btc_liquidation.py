import asyncio
import json
import os
import urllib.parse
import urllib.request

from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import websockets


# ==========================================
# SMART FUTURES V11 PRO
# BYBIT BTC LIQUIDATION MONITOR
# ==========================================

BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"

SYMBOL = "BTCUSDT"
TOPIC = f"allLiquidation.{SYMBOL}"

# TEST MODE
MIN_USD = 200

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN", ""
)

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID", ""
)


# ==========================================
# RENDER HEALTH SERVER
# ==========================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path not in ("/", "/health"):
            self.send_error(404)
            return

        body = b"Smart Futures V11 PRO - Bybit Monitor"

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


# ==========================================
# TELEGRAM
# ==========================================

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


# ==========================================
# STARTUP TEST
# ==========================================

def send_startup_test():

    print(
        "Starting Telegram connection test...",
        flush=True
    )

    message = (
        "SMART FUTURES V11 PRO\n\n"
        "BYBIT CONNECTION TEST\n\n"
        "Render service started.\n"
        "Source: BYBIT FUTURES\n"
        "Asset: BTCUSDT\n"
        "Minimum liquidation: $200\n\n"
        "Status: TEST MESSAGE\n\n"
        "Not a trading signal
