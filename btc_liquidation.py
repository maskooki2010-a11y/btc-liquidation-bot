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

import websockets

WS_URL = 'wss://stream.bybit.com/v5/public/linear'
TOPICS = ['allLiquidation.BTCUSDT', 'kline.5.BTCUSDT', 'kline.15.BTCUSDT']
MIN_USD = float(os.getenv('MIN_LIQUIDATION_USD', '200'))
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()

candles = {'5': deque(maxlen=120), '15': deque(maxlen=120)}
seen = set()
seen_order = deque()

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split('?')[0] not in ('/', '/health'):
            self.send_error(404)
            return
        body = b'SMART FUTURES V11 BYBIT RUNNING'
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def serve_health():
    port = int(os.getenv('PORT', '10000'))
    ThreadingHTTPServer(('0.0.0.0', port), HealthHandler).serve_forever()


def telegram_send(message):
    if not TOKEN or not CHAT_ID:
        print('TELEGRAM CONFIG MISSING', flush=True)
        return False
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    body = urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': message}).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=body, method='POST'), timeout=12) as response:
            result = json.load(response)
        success = result.get('ok') is True
        print('TELEGRAM SENT' if success else 'TELEGRAM API ERROR', flush=True)
        return success
    except Exception as exc:
        print(f'TELEGRAM ERROR: {type(exc).__name__}: {exc}', flush=True)
        return False



def ema(values, period):
    if len(values) < period + 2:
        return None
    alpha = 2 / (period + 1)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = alpha * value + (1 - alpha) * result
    return result


def rsi(values, period=7):
    if len(values) < period + 2:
        return None
    gains = []
    losses = []
    for previous, current in zip(values[:-1], values[1:]):
        change = current - previous
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def trend(interval):
    values = [close for _, close in candles[interval]]
    fast = ema(values, 9)
    slow = ema(values, 21)
    strength = rsi(values)
    if fast is None or slow is None or strength is None:
        return 'UNKNOWN', None
    if fast > slow and strength >= 52:
        return 'BULLISH', round(strength, 1)
    if fast < slow and strength <= 48:
        return 'BEARISH', round(strength, 1)
    return 'MIXED', round(strength, 1)


def direction(liq_type):
    t5, r5 = trend('5')
    t15, r15 = trend('15')
    if 'UNKNOWN' in (t5, t15):
        bias = 'WAIT'
        explanation = 'WARMING UP: collecting confirmed candles for EMA9/EMA21/RSI7.'
    elif t5 == 'BEARISH' and t15 == 'BEARISH' and liq_type == 'LONG LIQUIDATION':
        bias = 'SHORT BIAS'
        explanation = 'Long liquidations align with bearish 5m and 15m trends.'
    elif t5 == 'BULLISH' and t15 == 'BULLISH' and liq_type == 'SHORT LIQUIDATION':
        bias = 'LONG BIAS'
        explanation = 'Short liquidations align with bullish 5m and 15m trends.'
    else:
        bias = 'WAIT'
        explanation = 'Liquidation and confirmed trends are not aligned.'
    return bias, explanation, t5, t15, r5, r15


def remember(key):
    if key in seen:
        return False
    if len(seen_order) >= 3000:
        seen.discard(seen_order.popleft())
    seen.add(key)
    seen_order.append(key)
    return True


def parse_liquidation(item):
    if not isinstance(item, dict) or item.get('s') != 'BTCUSDT':
        return None
    try:
        quantity = float(item.get('v', 0))
        bankruptcy_price = float(item.get('p', 0))
        event_ms = int(item.get('T', 0))
    except (ValueError, TypeError, OverflowError):
        return None
    if quantity <= 0 or bankruptcy_price <= 0 or event_ms <= 0:
        return None
    usd = quantity * bankruptcy_price
    if usd < MIN_USD:
        return None
    side = item.get('S')
    if side == 'Buy':
        liq_type = 'LONG LIQUIDATION'
    elif side == 'Sell':
        liq_type = 'SHORT LIQUIDATION'
    else:
        return None
    key = (event_ms, side, quantity, bankruptcy_price)
    if not remember(key):
        return None
    return liq_type, quantity, bankruptcy_price, usd, event_ms


def alert_message(event):
    liq_type, quantity, price, usd, event_ms = event
    bias, reason, t5, t15, r5, r15 = direction(liq_type)
    stamp = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc).isoformat()
    marker = '🔴' if liq_type == 'LONG LIQUIDATION' else '🟢'
    def fmt_rsi(value):
        return str(value) if value is not None else 'N/A'
    return (
        '🚨 SMART FUTURES V11 PRO | BYBIT\n'
        f'{marker} {liq_type}\n'
        'Symbol: BTCUSDT\n'
        f'Bankruptcy Price: ${price:,.2f}\n'
        f'Estimated Value: ${usd:,.2f}\n'
        f'Quantity: {quantity:.6f} BTC\n'
        f'Time UTC: {stamp}\n\n'
        '📊 MARKET DIRECTION (closed candles)\n'
        f'5m: {t5 if t5 != "UNKNOWN" else "WARMING UP"} | RSI7: {fmt_rsi(r5)} | candles: {len(candles["5"])}\n'
        f'15m: {t15 if t15 != "UNKNOWN" else "WARMING UP"} | RSI7: {fmt_rsi(r15)} | candles: {len(candles["15"])}\n'
        f'Suggestion: {bias}\n'
        f'Reason: {reason}\n\n'
        '⚠️ Experimental context, NOT a trade signal. '
        'Liquidation price is bankruptcy price, not live market price.'
    )


async def bootstrap():
    print('KLINE HISTORY: WebSocket only; collecting confirmed candles.', flush=True)
    print('KLINE WARMUP: 5m and 15m trends need at least 23 closed candles.', flush=True)


async def heartbeat(ws):
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps({'op': 'ping'}))


async def monitor():
    print(f'START BYBIT MIN_USD={MIN_USD}', flush=True)
    await asyncio.to_thread(telegram_send, '✅ SMART FUTURES V11 PRO\nBybit startup test: Telegram connected.\nWaiting for Bybit market stream.\nNot a trading signal.')
    await bootstrap()
    while True:
        ping_task = None
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, open_timeout=30, max_size=2_000_000) as ws:
                print('Connected to Bybit', flush=True)
                await ws.send(json.dumps({'op': 'subscribe', 'args': TOPICS}))
                ping_task = asyncio.create_task(heartbeat(ws))
                async for raw in ws:
                    try:
                        message = json.loads(raw)
                        if message.get('op') == 'subscribe':
                            print('BYBIT SUBSCRIPTION SUCCESS' if message.get('success') is True else 'BYBIT SUBSCRIPTION FAILED', flush=True)
                            continue
                        topic = message.get('topic', '')
                        if topic.startswith('kline.'):
                            interval = topic.split('.')[1]
                            if interval not in candles:
                                continue
                            for k in message.get('data', []):
                                if not k.get('confirm'):
                                    continue
                                start, close = int(k['start']), float(k['close'])
                                history = candles[interval]
                                if history and history[-1][0] == start:
                                    history[-1] = (start, close)
                                elif not history or start > history[-1][0]:
                                    history.append((start, close))
                                    print(f'KLINE {interval}m CLOSED: {len(history)} candles', flush=True)
                            continue
                        if topic != TOPICS[0]:
                            continue
                        events = message.get('data', [])
                        if isinstance(events, dict):
                            events = [events]
                        for item in events:
                            event = parse_liquidation(item)
                            if event is None:
                                continue
                            print('BYBIT LIQUIDATION DETECTED', flush=True)
                            sent = await asyncio.to_thread(telegram_send, alert_message(event))
                            print('LIQUIDATION ALERT SENT' if sent else 'LIQUIDATION ALERT FAILED', flush=True)
                    except Exception as exc:
                        print(f'MESSAGE ERROR: {type(exc).__name__}: {exc}', flush=True)
        except Exception as exc:
            print(f'CONNECTION ERROR: {type(exc).__name__}: {exc}', flush=True)
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
