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

script_config = config.get_script_config('funding_rate')
body = {"type": "metaAndAssetCtxs", "dex": config.api['dex']}


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
        send_developer_alert(f"API request failed in funding_rate.py: {e}")
        return None

def find_coin_index(universe, coin_symbol):
    if not isinstance(universe, list):
        return None
    for i, asset in enumerate(universe):
        if isinstance(asset, dict) and asset.get("name") == coin_symbol:
            return i
    return None


def check_funding_rate():
    data = fetch_data()
    if not data:
        print("API response failed for funding rate check.")
        return

    try:
        if not isinstance(data, list) or len(data) < 2:
            print(f"Unexpected API response structure: {type(data)}")
            send_developer_alert(f"funding_rate.py: Unexpected API response structure: {type(data)}")
            return
        
        universe = data[0].get("universe") if isinstance(data[0], dict) else None
        coin_index = find_coin_index(universe, COIN_SYMBOL)
        
        if coin_index is None:
            print(f"Coin {COIN_SYMBOL} not found in universe")
            send_developer_alert(f"funding_rate.py: Coin {COIN_SYMBOL} not found in universe")
            return
        
        if not isinstance(data[1], list) or len(data[1]) <= coin_index:
            print("Market data not available for coin index")
            send_developer_alert(f"funding_rate.py: Market data not available for coin index {coin_index}")
            return
        
        market_data = data[1][coin_index]
        
        if "funding" not in market_data:
            print("Missing 'funding' field in market data")
            send_developer_alert(f"funding_rate.py: Missing 'funding' field in market data")
            return
        
        funding_rate = float(market_data["funding"])
        
    except (KeyError, IndexError, TypeError, ValueError) as e:
        print(f"Failed to extract funding rate: {e}")
        send_developer_alert(f"funding_rate.py: Failed to extract funding rate: {e}")
        return

    annualized_rate = abs(funding_rate * 100 * 3 * 365)
    print(f"Funding rate: {funding_rate}")
    print(f"Annualized (|funding * 100 * 3 * 365|): {annualized_rate}")
    
    threshold = script_config['threshold']
    if annualized_rate > threshold:
        msg = (
            f"ALERT: Funding rate exceeded limit.\n\n"
            f"Funding Rate: {funding_rate * 100:.4f}\n"
            f"Annualized Funding Rate: {annualized_rate:.2f}\n"
            f"Threshold: {threshold}"
        )
        send_telegram_alert(msg)
    else:
        print("OK: Funding rate within acceptable range.")


if __name__ == "__main__":
    CHECK_INTERVAL = script_config['check_interval_seconds']
    print(f"Starting funding rate monitoring loop (interval: {CHECK_INTERVAL/60:.0f} minutes)... Press Ctrl+C to stop.")
    try:
        while True:
            try:
                check_funding_rate()
            except Exception as e:
                print(f"Error in check_funding_rate: {e}")
                send_developer_alert(f"funding_rate.py: Error in monitoring loop: {e}")
                time.sleep(60)  # Wait a bit before retrying
                continue
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        send_developer_alert("funding_rate.py: Stopped by user (Ctrl+C)")    
        print("\nStopped by user.")
    except Exception as e:
        send_developer_alert(f"funding_rate.py: Crashed with error: {e}")
        print(f"Fatal error: {e}")
        raise
