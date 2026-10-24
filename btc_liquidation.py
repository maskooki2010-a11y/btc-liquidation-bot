# ==========================================
# SMART FUTURES V11 PRO
# BTC LIQUIDATION + MARKET DIRECTION
# TWO INDEPENDENT MONITORS
# ==========================================

import asyncio
import json
import os
import time
import logging
import urllib.parse
import urllib.request

from collections import deque

import websockets


# ==========================================
# CONFIGURATION
# ==========================================

SYMBOL = "BTCUSDT"

BYBIT_WS = (
    "wss://stream.bybit.com/v5/public/linear"
)

TELEGRAM_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    os.getenv("BOT_TOKEN", "")
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    os.getenv("CHAT_ID", "")
)

MIN_LIQUIDATION_USD = float(
    os.getenv("MIN_LIQUIDATION_USD", "200")
)

DIRECTION_CANDLES = 250

RECONNECT_DELAY = 5

PING_INTERVAL = 20


# ==========================================
# LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("SMART_FUTURES_V11")


# ==========================================
# TELEGRAM
# ==========================================

def telegram_request(message):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error(
            "TELEGRAM CONFIG MISSING"
        )
        return False

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    data = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
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
            timeout=10
        ) as response:

            result = json.loads(
                response.read().decode("utf-8")
            )

        return result.get("ok", False)

    except Exception:

        logger.exception(
            "TELEGRAM SEND ERROR"
        )

        return False


async def send_telegram(message):

    return await asyncio.to_thread(
        telegram_request,
        message
    )


# ==========================================
# LIQUIDATION MONITOR
# INDEPENDENT SYSTEM
# ==========================================

async def liquidation_monitor():

    logger.info(
        "LIQUIDATION MONITOR STARTED"
    )

    while True:

        try:

            async with websockets.connect(
                BYBIT_WS,
                ping_interval=PING_INTERVAL,
                ping_timeout=20,
                open_timeout=20
            ) as ws:

                logger.info(
                    "LIQUIDATION WS CONNECTED"
                )

                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [
                        "allLiquidation.BTCUSDT"
                    ]
                }))

                logger.info(
                    "LIQUIDATION SUBSCRIBE SENT"
                )

                async for raw_message in ws:

                    try:

                        data = json.loads(
                            raw_message
                        )

                        if data.get("op") == "subscribe":

                            logger.info(
                                "LIQUIDATION SUBSCRIPTION: %s",
                                data
                            )

                            continue

                        if data.get("topic") != (
                            "allLiquidation.BTCUSDT"
                        ):
                            continue

                        events = data.get(
                            "data", []
                        )

                        if isinstance(events, dict):
                            events = [events]

                        for event in events:

                            await process_liquidation(
                                event
                            )

                    except Exception:

                        logger.exception(
                            "LIQUIDATION MESSAGE ERROR"
                        )

        except asyncio.CancelledError:
            raise

        except Exception:

            logger.exception(
                "LIQUIDATION CONNECTION ERROR"
            )

        await asyncio.sleep(
            RECONNECT_DELAY
        )


# ==========================================
# PROCESS LIQUIDATION
# ==========================================

async def process_liquidation(event):

    try:

        if event.get("s") != SYMBOL:
            return

        price = float(
            event.get("p", 0)
        )

        quantity = float(
            event.get("v", 0)
        )

        side = event.get(
            "S", ""
        )

        usd_value = price * quantity

        if usd_value < MIN_LIQUIDATION_USD:
            return

        # Bybit liquidation side:
        # Sell = long position liquidated
        # Buy  = short position liquidated

        if side == "Sell":

            liquidation_type = (
                "LONG LIQUIDATED"
            )

            icon = "🔴"

        elif side == "Buy":

            liquidation_type = (
                "SHORT LIQUIDATED"
            )

            icon = "🟢"

        else:
            return

        logger.info(
            "BYBIT LIQUIDATION DETECTED | %s | $%.2f",
            liquidation_type,
            usd_value
        )

        message = f"""
🚨 SMART FUTURES V11 PRO

{icon} BTC LIQUIDATION ALERT

Asset: BTCUSDT

Type: {liquidation_type}

Price: ${price:,.2f}

Quantity: {quantity:.6f} BTC

Liquidation Value: ${usd_value:,.2f}

Source: Bybit Futures

⚡ Independent Liquidation Monitor
"""

        success = await send_telegram(
            message
        )

        if success:

            logger.info(
                "LIQUIDATION TELEGRAM SENT"
            )

        else:

            logger.error(
                "LIQUIDATION TELEGRAM FAILED"
            )

    except Exception:

        logger.exception(
            "PROCESS LIQUIDATION ERROR"
        )


# ==========================================
# MARKET DIRECTION
# INDEPENDENT SYSTEM
# ==========================================

candles_5m = deque(
    maxlen=DIRECTION_CANDLES
)

candles_15m = deque(
    maxlen=DIRECTION_CANDLES
)

last_direction = None


# ==========================================
# EMA CALCULATION
# ==========================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(
        values[:period]
    ) / period

    for value in values[period:]:

        ema = (
            value * multiplier
            + ema * (1 - multiplier)
        )

    return ema


# ==========================================
# RSI CALCULATION
# ==========================================

def calculate_rsi(values, period=7):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = (
            values[i] - values[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    for i in range(period, len(gains)):

        avg_gain = (
            avg_gain * (period - 1)
            + gains[i]
        ) / period

        avg_loss = (
            avg_loss * (period - 1)
            + losses[i]
        ) / period

    if avg_gain == 0 and avg_loss == 0:
        return 50.0

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (
        100 / (1 + rs)
    )


# ==========================================
# ANALYZE TIMEFRAME
# ==========================================

def analyze_timeframe(candles):

    if len(candles) < 23:

        return {
            "trend": "WARMING UP",
            "rsi": None,
            "ema9": None,
            "ema21": None
        }

    closes = [
        candle["close"]
        for candle in candles
    ]

    ema9 = calculate_ema(
        closes, 9
    )

    ema21 = calculate_ema(
        closes, 21
    )

    rsi = calculate_rsi(
        closes, 7
    )

    price = closes[-1]

    if (
        price > ema9
        and ema9 > ema21
        and rsi > 50
    ):

        trend = "BULLISH"

    elif (
        price < ema9
        and ema9 < ema21
        and rsi < 50
    ):

        trend = "BEARISH"

    else:

        trend = "MIXED"

    return {
        "trend": trend,
        "rsi": rsi,
        "ema9": ema9,
        "ema21": ema21
    }


# ==========================================
# PROCESS CLOSED CANDLES
# ==========================================

async def process_kline(data):

    global last_direction

    topic = data.get(
        "topic", ""
    )

    kline_data = data.get(
        "data", []
    )

    if isinstance(kline_data, dict):
        kline_data = [kline_data]

    for candle in kline_data:

        if not candle.get(
            "confirm", False
        ):
            continue

        close_price = float(
            candle["close"]
        )

        candle_start = int(
            candle["start"]
        )

        item = {
            "start": candle_start,
            "close": close_price
        }

        if topic == "kline.5.BTCUSDT":

            target = candles_5m
            timeframe = "5m"

        elif topic == "kline.15.BTCUSDT":

            target = candles_15m
            timeframe = "15m"

        else:
            continue

        if (
            target
            and target[-1]["start"] == candle_start
        ):

            target[-1] = item

        else:

            target.append(item)

        logger.info(
            "KLINE %s CLOSED | COUNT=%d",
            timeframe,
            len(target)
        )

        result_5m = analyze_timeframe(
            candles_5m
        )

        result_15m = analyze_timeframe(
            candles_15m
        )

        trend_5m = result_5m["trend"]

        trend_15m = result_15m["trend"]

        if (
            trend_5m == "WARMING UP"
            or trend_15m == "WARMING UP"
        ):

            logger.info(
                "DIRECTION WARMING UP | 5m=%d | 15m=%d",
                len(candles_5m),
                len(candles_15m)
            )

            continue

        if (
            trend_5m == "BULLISH"
            and trend_15m == "BULLISH"
        ):

            direction = "BULLISH"

        elif (
            trend_5m == "BEARISH"
            and trend_15m == "BEARISH"
        ):

            direction = "BEARISH"

        else:

            direction = "MIXED"

        if direction == last_direction:
            continue

        last_direction = direction

        if direction == "BULLISH":

            icon = "🟢"

        elif direction == "BEARISH":

            icon = "🔴"

        else:

            icon = "🟡"

        message = f"""
📊 SMART FUTURES V11 PRO

{icon} BTC MARKET DIRECTION

Asset: BTCUSDT

Market Direction: {direction}

5m Trend: {trend_5m}

15m Trend: {trend_15m}

RSI7 5m: {result_5m['rsi']:.2f}

RSI7 15m: {result_15m['rsi']:.2f}

Price: ${close_price:,.2f}

Source: Bybit Futures

⚡ Independent Direction Monitor
"""

        success = await send_telegram(
            message
        )

        if success:

            logger.info(
                "DIRECTION TELEGRAM SENT | %s",
                direction
            )

        else:

            logger.error(
                "DIRECTION TELEGRAM FAILED"
            )


# ==========================================
# DIRECTION WEBSOCKET
# ==========================================

async def direction_monitor():

    logger.info(
        "DIRECTION MONITOR STARTED"
    )

    while True:

        try:

            async with websockets.connect(
                BYBIT_WS,
                ping_interval=PING_INTERVAL,
                ping_timeout=20,
                open_timeout=20
            ) as ws:

                logger.info(
                    "DIRECTION WS CONNECTED"
                )

                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [
                        "kline.5.BTCUSDT",
                        "kline.15.BTCUSDT"
                    ]
                }))

                logger.info(
                    "DIRECTION SUBSCRIBE SENT"
                )

                async for raw_message in ws:

                    try:

                        data = json.loads(
                            raw_message
                        )

                        if data.get("op") == "subscribe":

                            logger.info(
                                "DIRECTION SUBSCRIPTION: %s",
                                data
                            )

                            continue

                        topic = data.get(
                            "topic", ""
                        )

                        if topic.startswith(
                            "kline."
                        ):

                            await process_kline(
                                data
                            )

                    except Exception:

                        logger.exception(
                            "DIRECTION MESSAGE ERROR"
                        )

        except asyncio.CancelledError:
            raise

        except Exception:

            logger.exception(
                "DIRECTION CONNECTION ERROR"
            )

        await asyncio.sleep(
            RECONNECT_DELAY
        )


# ==========================================
# MAIN
# ==========================================

async def main():

    logger.info(
        "SMART FUTURES V11 PRO STARTING"
    )

    logger.info(
        "BTC ONLY"
    )

    logger.info(
        "MIN LIQUIDATION USD = %.2f",
        MIN_LIQUIDATION_USD
    )

    logger.info(
        "LIQUIDATION AND DIRECTION INDEPENDENT"
    )

    await send_telegram(
        "🚀 SMART FUTURES V11 PRO\n\n"
        "BTC Liquidation Monitor: STARTED\n"
        "BTC Market Direction: STARTING\n\n"
        "Independent monitoring enabled."
    )

    await asyncio.gather(
        liquidation_monitor(),
        direction_monitor()
    )


# ==========================================
# RUN
# ==========================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "SMART FUTURES STOPPED"
        )
