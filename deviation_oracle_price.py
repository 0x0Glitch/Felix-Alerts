import os
import time
import requests
from dotenv import load_dotenv
from config_loader import load_config

load_dotenv()
config = load_config()

NETWORK = os.getenv("NETWORK", "testnet").lower()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEVELOPER_TELEGRAM_BOT_TOKEN = os.getenv("DEVELOPER_TELEGRAM_BOT_TOKEN")
DEVELOPER_TELEGRAM_CHAT_ID = os.getenv("DEVELOPER_TELEGRAM_CHAT_ID")
COIN_SYMBOL = os.getenv("COIN_SYMBOL", config.symbols['primary_coin'])

if NETWORK == "mainnet":
    API_URL = "https://api.hyperliquid.xyz/info"
else:
    API_URL = "https://api.hyperliquid-testnet.xyz/info"

required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
missing = [v for v in required_vars if not os.getenv(v)]
if missing:
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

print(f"Using {NETWORK} network: {API_URL}")

script_config = config.get_script_config('deviation_oracle_price')
body = {"type": "metaAndAssetCtxs", "dex": config.api['dex']}
THRESHOLD_PERCENT = script_config['threshold_percent']  # Alert threshold: percent deviation from oracle price  


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


def fetch_data():
    try:
        resp = requests.post(API_URL, json=body, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("Request failed:", e)
        send_developer_alert(f"API request failed in deviation_oracle_price.py: {e}")
        return None


def find_coin_index(universe, coin_symbol):
    if not isinstance(universe, list):
        return None
    for i, asset in enumerate(universe):
        if isinstance(asset, dict) and asset.get("name") == coin_symbol:
            return i
    return None

def check_price_impact():
    data = fetch_data()
    if not data:
        print("API response failed for price impact check.")
        return

    try:
        if not isinstance(data, list) or len(data) < 2:
            print(f"Unexpected API response structure: {type(data)}")
            send_developer_alert(f"deviation_oracle_price.py: Unexpected API response structure: {type(data)}")
            return
        
        universe = data[0].get("universe") if isinstance(data[0], dict) else None
        coin_index = find_coin_index(universe, COIN_SYMBOL)
        
        if coin_index is None:
            print(f"Coin {COIN_SYMBOL} not found in universe")
            send_developer_alert(f"deviation_oracle_price.py: Coin {COIN_SYMBOL} not found in universe")
            return
        
        if not isinstance(data[1], list) or len(data[1]) <= coin_index:
            print("Market data not available for coin index")
            send_developer_alert(f"deviation_oracle_price.py: Market data not available for coin index {coin_index}")
            return
        
        market_data = data[1][coin_index]
        
        required_fields = ["oraclePx", "impactPxs"]
        for field in required_fields:
            if field not in market_data:
                print(f"Missing field '{field}' in market data")
                send_developer_alert(f"deviation_oracle_price.py: Missing field '{field}' in market data")
                return
        
        oracle_px = float(market_data["oraclePx"])
        impact_pxs = market_data["impactPxs"]
        
        if not isinstance(impact_pxs, list):
            print(f"Invalid impactPxs format (not a list): {type(impact_pxs)}")
            send_developer_alert(f"deviation_oracle_price.py: impactPxs is not a list: {type(impact_pxs)}")
            return
        
        if len(impact_pxs) < 2:
            print(f"Invalid impactPxs format (insufficient elements): {impact_pxs}")
            send_developer_alert(f"deviation_oracle_price.py: impactPxs has only {len(impact_pxs)} elements")
            return
        
        try:
            bid_px = float(impact_pxs[0]) if impact_pxs[0] not in (None, "", "null") else None
            ask_px = float(impact_pxs[1]) if impact_pxs[1] not in (None, "", "null") else None
        except (ValueError, TypeError) as e:
            print(f"Failed to convert impactPxs to float: {impact_pxs}, error: {e}")
            send_developer_alert(f"deviation_oracle_price.py: Failed to convert impactPxs to float: {e}")
            return
        
        if bid_px is None or ask_px is None:
            print(f"Bid or ask price is None or empty: bid={impact_pxs[0]}, ask={impact_pxs[1]}")
            send_developer_alert(f"deviation_oracle_price.py: Bid or ask price is None or empty")
            return
            
    except (KeyError, IndexError, TypeError, ValueError) as e:
        print(f"Failed to extract price data: {e}")
        send_developer_alert(f"deviation_oracle_price.py: Failed to extract price data: {e}")
        return

    impact_px = (bid_px + ask_px) / 2
    print(f"Oracle Price: {oracle_px}, Impact Price (avg of bid/ask): {impact_px}")

    impact_diff_percent = abs((impact_px - oracle_px) / oracle_px) * 100

    if impact_diff_percent > THRESHOLD_PERCENT:
        msg = (
            f"ALERT: Impact price deviated more than {THRESHOLD_PERCENT}% from Oracle Price.\n\n"
            f"Oracle Price: {oracle_px:.2f}\n"
            f"Impact Price: {impact_px:.2f}\n"
            f"Difference: {impact_diff_percent:.2f}%"
        )
        send_telegram_alert(msg)
        print(f"Alert sent: Impact price deviation {impact_diff_percent:.2f}%")
    else:
        print(f"OK: Impact price is within {THRESHOLD_PERCENT}% of Oracle Price.")


if __name__ == "__main__":
    CHECK_INTERVAL = script_config['check_interval_seconds']
    print(f"Starting price impact monitoring loop (interval: {CHECK_INTERVAL} seconds)... Press Ctrl+C to stop.")
    try:
        while True:
            try:
                check_price_impact()
            except Exception as e:
                print(f"Error in check_price_impact: {e}")
                send_developer_alert(f"deviation_oracle_price.py: Error in monitoring loop: {e}")
                time.sleep(60)  # Wait a bit before retrying
                continue
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        send_developer_alert("deviation_oracle_price.py: Stopped by user (Ctrl+C)")
        print("\nStopped by user.")
    except Exception as e:
        send_developer_alert(f"deviation_oracle_price.py: Crashed with error: {e}")
        print(f"Fatal error: {e}")
        raise
