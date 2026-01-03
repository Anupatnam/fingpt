import json
import time
import threading
import traceback
from datetime import datetime

import websocket
from sqlalchemy import insert

from src.storage import db as dbmod

# --------------------------------------------------
# CONFIG (UNCHANGED)
# --------------------------------------------------
WS_URL = "wss://ws-feed.exchange.coinbase.com"
SYMBOLS = ["BTC-USD", "ETH-USD", "USDT-USD"]
CHANNEL = "ticker"

RECONNECT_DELAY = 5  # seconds


# --------------------------------------------------
# Message handler (UNCHANGED LOGIC)
# --------------------------------------------------
def handle_message(msg: str):
    try:
        data = json.loads(msg)

        if data.get("type") != "ticker":
            return

        symbol = data["product_id"]
        price = float(data["price"])
        volume = float(data.get("last_size", 0.0))

        ts = datetime.fromisoformat(
            data["time"].replace("Z", "+00:00")
        )

        stmt = insert(dbmod.tickers).values(
            symbol=symbol,
            price=price,
            volume=volume,
            ts=ts
        )

        with dbmod.engine.begin() as conn:
            conn.execute(stmt)

    except Exception:
        print("[coinbase_ws] handle_message error")
        print(traceback.format_exc())


# --------------------------------------------------
# ONE SYMBOL = ONE CONNECTION (SAFE LOOP)
# --------------------------------------------------
def start_ws(symbol: str):
    def on_open(ws):
        print(f"[coinbase_ws] Connected → {symbol}")
        ws.send(json.dumps({
            "type": "subscribe",
            "channels": [{
                "name": CHANNEL,
                "product_ids": [symbol]
            }]
        }))

    def on_message(ws, message):
        handle_message(message)

    def on_error(ws, error):
        print(f"[coinbase_ws] {symbol} error:", error)

    def on_close(ws, code, msg):
        print(f"[coinbase_ws] {symbol} closed:", code, msg)

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:
            print(f"[coinbase_ws] {symbol} fatal error:", e)

        print(f"[coinbase_ws] Reconnecting {symbol} in {RECONNECT_DELAY}s")
        time.sleep(RECONNECT_DELAY)


# --------------------------------------------------
# ENTRY POINT (UNCHANGED BEHAVIOR)
# --------------------------------------------------
if __name__ == "__main__":
    print("Starting Coinbase ingestion (multi-connection mode)")

    for sym in SYMBOLS:
        threading.Thread(
            target=start_ws,
            args=(sym,),
            daemon=True
        ).start()

    while True:
        time.sleep(60)
