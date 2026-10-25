import asyncio
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import websockets

WS_URL = 'wss://stream.bybit.com/v5/public/linear'
TOPIC = 'allLiquidation.BTCUSDT'

MIN_USD = float(os.getenv('MIN_LIQUIDATION_USD', '200'))

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split('?')[0] not in ('/', '/health'):
            self.send_error(404)
            return

        body = b'SMART FUTURES V11 BYBIT RUNNING'

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve_health():
    port = int(os.getenv('PORT', '10000'))
    ThreadingHTTPServer(
        ('0.0.0.0', port),
        HealthHandler
    ).serve_forever()


def telegram_send(message):
    if not TOKEN or not CHAT_ID:
        print('TELEGRAM CONFIG MISSING', flush=True)
        return False

    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'

    body = urllib.parse.urlencode({
        'chat_id': CHAT_ID,
        'text': message
    }).encode()

    try:
        request = urllib.request.Request(
            url,
            data=body,
            method='POST'
        )

        with urllib.request.urlopen(request, timeout=12) as response:
            result = json.load(response)

        success = result.get('ok') is True

        print(
            'TELEGRAM SENT' if success else 'TELEGRAM API ERROR',
            flush=True
        )

        return success

    except Exception as exc:
        print(f'TELEGRAM ERROR: {exc}', flush=True)
        return False


def parse_liquidation(item):
    if not isinstance(item, dict):
        return None

    if item.get('s') != 'BTCUSDT':
        return None

    try:
        quantity = float(item.get('v', 0))
        price = float(item.get('p', 0))
        event_ms = int(item.get('T', 0))
    except (ValueError, TypeError):
        return None

    if quantity <= 0 or price <= 0 or event_ms <= 0:
        return None

    usd = quantity * price

    if usd < MIN_USD:
        return None

    side = item.get('S')

    if side == 'Buy':
        liq_type = 'SHORT LIQUIDATED'
        marker = '🟢'
    elif side == 'Sell':
        liq_type = 'LONG LIQUIDATED'
        marker = '🔴'
    else:
        return None

    return liq_type, marker, quantity, price, usd, event_ms


def alert_message(event):
    liq_type, marker, quantity, price, usd, event_ms = event

    stamp = datetime.fromtimestamp(
        event_ms / 1000,
        tz=timezone.utc
    ).strftime('%Y-%m-%d %H:%M:%S UTC')

    return (
        '🚨 SMART FUTURES V11 PRO | BYBIT\n\n'
        f'{marker} {liq_type}\n'
        'Symbol: BTCUSDT\n'
        f'Bankruptcy Price: ${price:,.2f}\n'
        f'Estimated Value: ${usd:,.2f}\n'
        f'Quantity: {quantity:.6f} BTC\n'
        f'Time: {stamp}\n\n'
        f'Minimum Alert: ${MIN_USD:,.0f}'
    )


async def heartbeat(ws):
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps({'op': 'ping'}))


async def monitor():
    print(f'START BYBIT MIN_USD={MIN_USD}', flush=True)

    await asyncio.to_thread(
        telegram_send,
        '✅ SMART FUTURES V11 PRO\n'
        'Bybit connected startup test.\n'
        f'Minimum liquidation: ${MIN_USD:,.0f}'
    )

    while True:
        ping_task = None

        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30,
                max_size=2_000_000
            ) as ws:

                print('Connected to Bybit', flush=True)

                await ws.send(json.dumps({
                    'op': 'subscribe',
                    'args': [TOPIC]
                }))

                ping_task = asyncio.create_task(heartbeat(ws))

                async for raw in ws:
                    try:
                        message = json.loads(raw)

                        if message.get('op') == 'subscribe':
                            print(
                                'BYBIT SUBSCRIPTION SUCCESS'
                                if message.get('success')
                                else 'BYBIT SUBSCRIPTION FAILED',
                                flush=True
                            )
                            continue

                        if message.get('topic') != TOPIC:
                            continue

                        events = message.get('data', [])

                        if isinstance(events, dict):
                            events = [events]

                        for item in events:
                            event = parse_liquidation(item)

                            if event is None:
                                continue

                            print(
                                'BYBIT LIQUIDATION DETECTED',
                                flush=True
                            )

                            sent = await asyncio.to_thread(
                                telegram_send,
                                alert_message(event)
                            )

                            print(
                                'LIQUIDATION ALERT SENT'
                                if sent
                                else 'LIQUIDATION ALERT FAILED',
                                flush=True
                            )

                    except Exception as exc:
                        print(f'MESSAGE ERROR: {exc}', flush=True)

        except Exception as exc:
            print(f'CONNECTION ERROR: {exc}', flush=True)

        finally:
            if ping_task is not None:
                ping_task.cancel()

                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

        await asyncio.sleep(5)


if __name__ == '__main__':
    Thread(target=serve_health, daemon=True).start()
    asyncio.run(monitor())
