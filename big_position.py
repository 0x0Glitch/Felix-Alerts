#!/usr/bin/env python3
import os
import json
import asyncio
import signal
import requests
import contextlib
from datetime import datetime, timezone
import websockets
from dotenv import load_dotenv
from config_loader import load_config

load_dotenv()
config = load_config()

script_config = config.get_script_config('big_position')
NETWORK = os.getenv("NETWORK", "testnet").lower()
COIN_SYMBOL = os.getenv("COIN_SYMBOL", config.symbols['primary_coin'])
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEVELOPER_TELEGRAM_BOT_TOKEN = os.getenv("DEVELOPER_TELEGRAM_BOT_TOKEN")
DEVELOPER_TELEGRAM_CHAT_ID = os.getenv("DEVELOPER_TELEGRAM_CHAT_ID")
THRESHOLD = float(os.getenv("THRESHOLD", script_config.get('threshold', 10000)))

if NETWORK == "mainnet":
    WSS_URL = "wss://api.hyperliquid.xyz/ws"
else:
    WSS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
missing = [v for v in required_vars if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

print(f"Using {NETWORK} network: {WSS_URL}")

SUBSCRIBE_PAYLOAD = {
    "method": "subscribe",
    "subscription": {
        "type": "trades",
        "coin": COIN_SYMBOL
    },
}

PING_INTERVAL_SEC = script_config['ping_interval_seconds']
RECONNECT_BASE_DELAY = script_config['reconnect_base_delay']
RECONNECT_MAX_DELAY = script_config['reconnect_max_delay']
MAX_TRADE_AGE_SECONDS = script_config['max_trade_age_seconds']


def ts_to_iso(ts):
    if ts is None:
        return None
    if ts > 10_000_000_000:
        ts /= 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat()


def send_telegram_alert(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        print("Market alert sent.")
    except Exception as e:
        print("Failed to send market alert:", e)

def send_developer_alert(message: str):
    if not DEVELOPER_TELEGRAM_BOT_TOKEN or not DEVELOPER_TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{DEVELOPER_TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": DEVELOPER_TELEGRAM_CHAT_ID, "text": message}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        print("Developer alert sent.")
    except Exception as e:
        print("Failed to send developer alert:", e)


async def send_heartbeats(ws):
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL_SEC)
            try:
                await ws.ping()
            except Exception:
                return
    except asyncio.CancelledError:
        return


def format_trade(trade):
    try:
        px = float(trade["px"])
        sz = float(trade["sz"])
        notional = px * sz
        side = trade["side"]
        coin = trade.get("coin", COIN_SYMBOL)
        buyer, seller = trade.get("users", ["?", "?"])
        ts = trade.get("time")

        iso_time = ts_to_iso(ts)
        alert_msg = (
            f"ALERT: Large Trade Detected\n\n"
            f"Coin: {coin}\n"
            f"Side: {side}\n"
            f"Price: {px:.2f}\n"
            f"Size: {sz:.2f}\n"
            f"Notional: ${notional:,.2f}\n"
            f"Buyer: {buyer}\n"
            f"Seller: {seller}\n"
            f"Time: {iso_time}\n"
            f"Threshold: ${THRESHOLD:,.2f}"
        )
        return alert_msg, notional
    except Exception as e:
        print("Error formatting trade:", e)
        send_developer_alert(f"big_position.py: Error formatting trade: {e}")
        return None, 0


async def stream_trades():
    reconnect_delay = RECONNECT_BASE_DELAY

    while True:
        try:
            print(f"Connecting to {WSS_URL}...")
            async with websockets.connect(WSS_URL, ping_interval=None, ping_timeout=None) as ws:
                # Subscribe to trade events
                await ws.send(json.dumps(SUBSCRIBE_PAYLOAD))
                print(f"Subscribed: {json.dumps(SUBSCRIBE_PAYLOAD)}")

                # Start heartbeat task
                heartbeat_task = asyncio.create_task(send_heartbeats(ws))
                reconnect_delay = RECONNECT_BASE_DELAY

                try:
                    # Process incoming messages
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            print("Raw non-JSON message:", raw)
                            continue

                        trades = None
                        if isinstance(msg, dict):
                            if "data" in msg and isinstance(msg["data"], list):
                                trades = msg["data"]
                            elif "trades" in msg and isinstance(msg["trades"], list):
                                trades = msg["trades"]

                        if not trades:
                            # Log subscription acknowledgment
                            if msg.get("method") == "subscribe" or msg.get("type") in {"subscribed", "ack"}:
                                print(f"Server ack: {msg}")
                            continue

                        # Check trades against threshold
                        for tr in trades:
                            trade_ts = tr.get("time")
                            if trade_ts is not None:
                                if trade_ts > 10_000_000_000:
                                    trade_ts_sec = trade_ts / 1000.0
                                else:
                                    trade_ts_sec = trade_ts
                                
                                current_ts = datetime.now(tz=timezone.utc).timestamp()
                                age_seconds = current_ts - trade_ts_sec
                                
                                if age_seconds > MAX_TRADE_AGE_SECONDS:
                                    print(f"Skipping stale trade (age: {age_seconds:.1f}s, max: {MAX_TRADE_AGE_SECONDS}s)")
                                    continue
                            
                            alert_msg, notional = format_trade(tr)
                            if alert_msg and notional >= THRESHOLD:
                                send_telegram_alert(alert_msg)
                            elif notional > 0:
                                print(f"Ignored small trade: ${notional:,.2f}")
                finally:
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat_task

        except (websockets.ConnectionClosed, ConnectionError) as e:
            print(f"Connection closed: {e}")
            send_developer_alert(f"big_position.py: WebSocket connection error: {e}")
        except Exception as e:
            print(f"Error: {e}")
            send_developer_alert(f"big_position.py: Unexpected error: {e}")

        print(f"Reconnecting in {reconnect_delay:.1f}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop = asyncio.Event()

    def _handle_signal(*_):
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            send_developer_alert("big_position.py: Signal handler not implemented")
            pass

    async def runner():
        worker = asyncio.create_task(stream_trades())
        await stop.wait()
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker

    try:
        loop.run_until_complete(runner())
    except KeyboardInterrupt:
        send_developer_alert("big_position.py: Stopped using (Ctrl+C)")
    except Exception as e:
        send_developer_alert(f"big_position.py: Crashed with error: {e}")
        raise
    finally:
        loop.close()


if __name__ == "__main__":
    print("Starting Hyperliquid Trade Monitor...")
    main()
