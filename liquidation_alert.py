import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import requests
from datetime import datetime
from config_loader import load_config

load_dotenv()
config = load_config()
DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEVELOPER_TELEGRAM_BOT_TOKEN = os.getenv("DEVELOPER_TELEGRAM_BOT_TOKEN")
DEVELOPER_TELEGRAM_CHAT_ID = os.getenv("DEVELOPER_TELEGRAM_CHAT_ID")

script_config = config.get_script_config('liquidation_alert')
CHECK_INTERVAL = script_config['check_interval_seconds']
MIN_POSITION_VALUE = script_config['min_position_value']
MAX_LEVERAGE = script_config['max_leverage']
MARGIN_THRESHOLD_MULTIPLIER = script_config['margin_threshold_multiplier']
MARKET_DATA_TABLE = config.database['market_data_table']
POSITIONS_TABLE = config.database['positions_table']
MARKET_SCHEMA = config.database['market_data_schema']
POSITIONS_SCHEMA = config.database['user_positions_schema']

if not DATABASE_URL or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise EnvironmentError("Missing required environment variables")

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

def check_liquidations():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Query positions from flxn_tsla_positions table
            cur.execute(f'''
                SELECT 
                    address,
                    market,
                    position_size,
                    entry_price,
                    liquidation_price,
                    margin_used,
                    position_value,
                    unrealized_pnl,
                    return_on_equity,
                    leverage_type,
                    leverage_value,
                    leverage_raw_usd,
                    account_value,
                    total_margin_used,
                    withdrawable,
                    last_updated
                FROM user_positions.flxn_tsla_positions
                WHERE position_value IS NOT NULL 
                  AND position_size IS NOT NULL
                  AND position_size != 0
                  AND liquidation_price IS NOT NULL
                  AND ABS(position_value) >= %s
                ORDER BY ABS(position_value) DESC;
            ''', (MIN_POSITION_VALUE,))
            
            positions = cur.fetchall()
            
            if not positions:
                print(f"No positions found with value >= ${MIN_POSITION_VALUE:,}")
                return
            
            print(f"Found {len(positions)} positions with value >= ${MIN_POSITION_VALUE:,}")
            
            alerts = []
            for position in positions:
                address = position['address']
                position_size = float(position['position_size'])
                position_value = float(position['position_value'])
                liquidation_price = float(position['liquidation_price'])
                
                # Calculate mark price from position_value / position_size
                mark_px = abs(position_value / position_size) if position_size != 0 else 0
                
                if mark_px == 0:
                    print(f"Warning: Invalid mark price for address {address[:6]}...{address[-4:]}")
                    continue
                
                # Calculate distance to liquidation price as percentage
                distance_to_liquidation = abs(mark_px - liquidation_price) / mark_px
                
                # Alert if within 10% of liquidation price
                if distance_to_liquidation <= 0.1:
                    distance_percentage = distance_to_liquidation * 100
                    
                    # Get additional position details
                    margin_used = float(position['margin_used']) if position['margin_used'] else 0
                    unrealized_pnl = float(position['unrealized_pnl']) if position['unrealized_pnl'] else 0
                    leverage_value = position['leverage_value']
                    entry_price = float(position['entry_price']) if position['entry_price'] else 0
                    
                    alert_msg = (
                        f"LIQUIDATION WARNING!\n"
                        f"Address: {address}\n"
                        f"Position Value: ${abs(position_value):,.2f}\n"
                        f"Mark Price: ${mark_px:.2f}\n"
                        f"Liquidation Price: ${liquidation_price:.2f}\n"
                        f"Distance to Liquidation: {distance_percentage:.2f}%\n"
                        f"Leverage: {leverage_value}x\n"
                        f"Unrealized PnL: ${unrealized_pnl:+,.2f}"
                    )
                    
                    alerts.append(alert_msg)
                    print(f"\n{alert_msg}")
            
            if alerts:
                header = (
                    f"{len(alerts)} POSITION(S) APPROACHING LIQUIDATION\n"
                    f"Timestamp: {datetime.now()}\n\n"
                )
                full_msg = header + "\n".join(alerts)
                send_telegram_alert(full_msg)
            else:
                print(f"No positions at risk of liquidation.")
            
            print(f"\n--- Summary ---")
            print(f"Total positions monitored: {len(positions)}")
            print(f"Positions at risk: {len(alerts)}")
            print(f"Alert threshold: 10% distance to liquidation")
            print(f"Check completed at {datetime.now()}\n")
            
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        send_developer_alert(f"liquidation_alert.py: Database error: {e}")
    except Exception as e:
        print(f"Error checking liquidations: {e}")
        send_developer_alert(f"liquidation_alert.py: Error checking liquidations: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Starting liquidation monitoring...")
    print(f"Monitoring positions >= ${MIN_POSITION_VALUE:,}")
    print(f"Table: user_positions.flxn_tsla_positions")
    print(f"Alert condition: abs(mark_price - liquidation_price) / mark_price <= 0.1")
    print(f"Mark price formula: position_value / position_size")
    print(f"Alert triggers when position is within 10% of liquidation price")
    print(f"Check interval: {CHECK_INTERVAL} seconds\n")
    
    while True:
        try:
            check_liquidations()
        except KeyboardInterrupt:
            send_developer_alert("liquidation_alert.py: Stopped.")
            print("\nMonitoring stopped by user.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            send_developer_alert(f"liquidation_alert.py: Unexpected error: {e}")
            time.sleep(60)
            continue
        
        print(f"Waiting {CHECK_INTERVAL} seconds until next check...")
        time.sleep(CHECK_INTERVAL)
