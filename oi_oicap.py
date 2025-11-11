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
SYMBOL = COIN_SYMBOL  # Keep full symbol for compatibility
COIN_PART = COIN_SYMBOL.split(":")[1] if ":" in COIN_SYMBOL else COIN_SYMBOL

if NETWORK == "mainnet":
    API_URL = "https://api.hyperliquid.xyz/info"
else:
    API_URL = "https://api.hyperliquid-testnet.xyz/info"

required_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

print(f"Using {NETWORK} network: {API_URL}")

script_config = config.get_script_config('oi_oicap')
body_meta = {"type": "metaAndAssetCtxs", "dex": config.api['dex']}
body_limits = {"type": "perpDexLimits", "dex": config.api['dex']}


def send_telegram_alert(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        print("Market alert sent.")
    except Exception as e:
        print("Failed to send market alert:", e)

def send_developer_alert(message: str):
    if not DEVELOPER_TELEGRAM_BOT_TOKEN or not DEVELOPER_TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{DEVELOPER_TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": DEVELOPER_TELEGRAM_CHAT_ID, "text": message}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        print("Developer alert sent.")
    except Exception as e:
        print("Failed to send developer alert:", e)


def fetch_data(body: dict):
    try:
        r = requests.post(API_URL, json=body, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed for {body.get('type')}: {e}")
        send_developer_alert(f"API request failed in oi_oicap.py ({body.get('type')}): {e}")
        return None

def find_coin_index(universe, coin_symbol):
    if not isinstance(universe, list):
        return None
    for i, asset in enumerate(universe):
        if isinstance(asset, dict) and asset.get("name") == coin_symbol:
            return i
    return None


def compare_once():
    print("\nFetching data from API...")
    resp_meta = fetch_data(body_meta)
    resp_limits = fetch_data(body_limits)

    if not resp_meta or not resp_limits:
        print("One of the API responses failed.")
        return


    try:
        if not isinstance(resp_meta, list) or len(resp_meta) < 2:
            print(f"Unexpected API response structure: {type(resp_meta)}")
            send_developer_alert(f"oi_oicap.py: Unexpected API response structure: {type(resp_meta)}")
            return
        
        universe = resp_meta[0].get("universe") if isinstance(resp_meta[0], dict) else None
        coin_index = find_coin_index(universe, COIN_PART)
        
        if coin_index is None:
            print(f"Coin {COIN_PART} not found in universe")
            send_developer_alert(f"oi_oicap.py: Coin {COIN_PART} not found in universe")
            return
        
        if not isinstance(resp_meta[1], list) or len(resp_meta[1]) <= coin_index:
            print("Market data not available for coin index")
            send_developer_alert(f"oi_oicap.py: Market data not available for coin index {coin_index}")
            return
        
        data = resp_meta[1][coin_index]
        
        required_fields = ["openInterest", "markPx"]
        for field in required_fields:
            if field not in data:
                print(f"Missing field '{field}' in market data")
                send_developer_alert(f"oi_oicap.py: Missing field '{field}' in market data")
                return
        
        open_interest = float(data["openInterest"])
        mark_px = float(data["markPx"])
        
    except (KeyError, IndexError, TypeError, ValueError) as e:
        print(f"Failed to extract openInterest/markPx: {e}")
        send_developer_alert(f"oi_oicap.py: Failed to extract openInterest/markPx: {e}")
        return

    try:
        coin_caps = dict(resp_limits["coinToOiCap"])
        coin_to_oi_cap_value = coin_caps.get(SYMBOL)
        if coin_to_oi_cap_value is None:
            print(f"Symbol '{SYMBOL}' not found in coinToOiCap")
            send_developer_alert(f"oi_oicap.py: Symbol '{SYMBOL}' not found in coinToOiCap")
            return
        coin_to_oi_cap = float(coin_to_oi_cap_value)
    except Exception as e:
        print(f"Failed to extract coinToOiCap: {e}")
        send_developer_alert(f"oi_oicap.py: Failed to extract coinToOiCap: {e}")
        return

    product = open_interest * mark_px
    threshold_percent = script_config['threshold_percent']
    threshold = threshold_percent * coin_to_oi_cap

    print(f"openInterest = {open_interest}, markPx = {mark_px}, product = {product}")
    print(f"coinToOiCap({SYMBOL}) = {coin_to_oi_cap}")
    print(f"Threshold (85%): {threshold}")

    if product > threshold:
        msg = (
            f"ALERT: OI * MarkPx exceeded {threshold_percent*100:.0f}% of cap limit!\n\n"
            f"Symbol: {SYMBOL}\n"
            f"Open Interest: {open_interest:.2f}\n"
            f"Mark Price: {mark_px:.2f}\n"
            f"OpenInterest_USD: {product:,.2f}\n"
            f"coinToOiCap: {coin_to_oi_cap:,.2f}\n"
            f"Allowed maximum ({threshold_percent*100:.0f}% of cap): {threshold:,.2f}"
        )
        send_telegram_alert(msg)
    else:
        print("OK: product is within acceptable range.")

if __name__ == "__main__":
    CHECK_INTERVAL = script_config['check_interval_seconds']
    print(f"Starting OI cap monitoring (interval: {CHECK_INTERVAL/3600:.1f} hours)... Press Ctrl+C to stop.\n")
    try:
        while True:
            try:
                compare_once()
            except Exception as e:
                print(f"Error in compare_once: {e}")
                send_developer_alert(f"oi_oicap.py: Error in monitoring loop: {e}")
                time.sleep(60)  # Wait a bit before retrying
                continue
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        send_developer_alert("oi_oicap.py: Stopped by user (Ctrl+C)")
        print("\nStopped by user.")
    except Exception as e:
        send_developer_alert(f"oi_oicap.py: Crashed with error: {e}")
        print(f"Fatal error: {e}")
        raise
