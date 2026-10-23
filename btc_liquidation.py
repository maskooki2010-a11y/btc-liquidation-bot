
import asyncio
import json
import os
from datetime import datetime, timezone

import websockets

WS_URL = "wss://fstream.binance.com/ws/btcusdt@forceOrder"
MIN_USD = 100_000


async def monitor():
    while True:
        try:
            print("Connecting to Binance...", flush=True)

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                print("Connected to Binance", flush=True)

                async for message in ws:
                    data = json.loads(message)
                    order = data.get("o", {})

                    price = float(order.get("ap") or order.get("p") or 0)
                    quantity = float(order.get("z") or order.get("q") or 0)
                    usd = price * quantity

                    if usd < MIN_USD:
                        continue

                    side = order.get("S")
                    liquidated = (
                        "LONG" if side == "SELL"
                        else "SHORT" if side == "BUY"
                        else "UNKNOWN"
                    )

                    event_time = datetime.fromtimestamp(
                        data["E"] / 1000, tz=timezone.utc
                    ).isoformat()

                    print(
                        f"BTC LIQUIDATION | {liquidated} | "
                        f"${usd:,.0f} | ${price:,.2f} | {event_time}",
                        flush=True,
                    )

        except Exception as error:
            print(f"Connection error: {error}", flush=True)
            await asyncio.sleep(5)


asyncio.run(monitor())
