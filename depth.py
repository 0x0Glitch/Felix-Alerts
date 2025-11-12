import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import requests
import time
from datetime import datetime, timedelta
from config_loader import load_config

load_dotenv()
config = load_config()
DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_NAME = config.database['market_data_table']
SCHEMA_NAME = config.database['market_data_schema']
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEVELOPER_TELEGRAM_BOT_TOKEN = os.getenv("DEVELOPER_TELEGRAM_BOT_TOKEN")
DEVELOPER_TELEGRAM_CHAT_ID = os.getenv("DEVELOPER_TELEGRAM_CHAT_ID")

script_config = config.get_script_config('depth')
CHECK_INTERVAL = script_config['check_interval_seconds']
DEPTH_THRESHOLD_PERCENT = script_config['threshold_percent']

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

def monitor_liquidity_depth():
    # Rate limiting for 5bps alerts: max 2 per hour
    alert_5bps_timestamps = []
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("Connected to Postgres successfully.\n")

        while True:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f'''
                    SELECT 
                        (COALESCE(bid_depth_5bps, 0) + COALESCE(ask_depth_5bps, 0)) AS total_depth_5bps,
                        (COALESCE(bid_depth_10bps, 0) + COALESCE(ask_depth_10bps, 0)) AS total_depth_10bps,
                        (COALESCE(bid_depth_50bps, 0) + COALESCE(ask_depth_50bps, 0)) AS total_depth_50bps,
                        (COALESCE(bid_depth_100bps, 0) + COALESCE(ask_depth_100bps, 0)) AS total_depth_100bps,
                        timestamp
                    FROM {SCHEMA_NAME}.{TABLE_NAME}
                    ORDER BY timestamp DESC
                    LIMIT 1;
                ''')
                latest_row = cur.fetchone()
                if not latest_row:
                    print("No latest row found.")
                    time.sleep(CHECK_INTERVAL)
                    continue

                latest_ts = latest_row['timestamp']

                one_hour_ago = latest_ts - 3600000  # 1 hour in milliseconds
                cur.execute(f'''
                    SELECT 
                        (COALESCE(bid_depth_5bps, 0) + COALESCE(ask_depth_5bps, 0)) AS total_depth_5bps,
                        (COALESCE(bid_depth_10bps, 0) + COALESCE(ask_depth_10bps, 0)) AS total_depth_10bps,
                        (COALESCE(bid_depth_50bps, 0) + COALESCE(ask_depth_50bps, 0)) AS total_depth_50bps,
                        (COALESCE(bid_depth_100bps, 0) + COALESCE(ask_depth_100bps, 0)) AS total_depth_100bps
                    FROM {SCHEMA_NAME}.{TABLE_NAME}
                    WHERE timestamp >= %s
                    ORDER BY timestamp ASC;
                ''', (one_hour_ago,))
                last_hour_rows = cur.fetchall()
                if not last_hour_rows:
                    print("No rows in the last hour.")
                    time.sleep(CHECK_INTERVAL)
                    continue

                avg_depth_5bps = float(sum(r['total_depth_5bps'] for r in last_hour_rows) / len(last_hour_rows))
                avg_depth_10bps = float(sum(r['total_depth_10bps'] for r in last_hour_rows) / len(last_hour_rows))
                avg_depth_50bps = float(sum(r['total_depth_50bps'] for r in last_hour_rows) / len(last_hour_rows))
                avg_depth_100bps = float(sum(r['total_depth_100bps'] for r in last_hour_rows) / len(last_hour_rows))

                latest_depth_5bps = float(latest_row['total_depth_5bps'])
                latest_depth_10bps = float(latest_row['total_depth_10bps'])
                latest_depth_50bps = float(latest_row['total_depth_50bps'])
                latest_depth_100bps = float(latest_row['total_depth_100bps'])

                timestamp_dt = datetime.fromtimestamp(latest_ts / 1000).replace(microsecond=0)
                print(f"Timestamp: {timestamp_dt}")
                print(f"5bps Depth: Latest = {latest_depth_5bps}, 1h Avg = {avg_depth_5bps:.2f}")
                print(f"10bps Depth: Latest = {latest_depth_10bps}, 1h Avg = {avg_depth_10bps:.2f}")
                print(f"50bps Depth: Latest = {latest_depth_50bps}, 1h Avg = {avg_depth_50bps:.2f}")
                print(f"100bps Depth: Latest = {latest_depth_100bps}, 1h Avg = {avg_depth_100bps:.2f}\n")

                alerts = []
                
                # Check 5bps alert with rate limiting (max 2 per hour)
                if latest_depth_5bps < DEPTH_THRESHOLD_PERCENT * avg_depth_5bps:
                    current_time = datetime.now()
                    # Remove timestamps older than 1 hour
                    alert_5bps_timestamps[:] = [ts for ts in alert_5bps_timestamps 
                                                 if current_time - ts < timedelta(hours=1)]
                    
                    if len(alert_5bps_timestamps) < 2:
                        alerts.append(f"5bps depth dropped below {DEPTH_THRESHOLD_PERCENT*100:.0f}% of last 1h avg")
                        alert_5bps_timestamps.append(current_time)
                    else:
                        print(f"  5bps alert suppressed (rate limit: 2 per hour). Last alerts: {alert_5bps_timestamps}")
                
                if latest_depth_10bps < DEPTH_THRESHOLD_PERCENT * avg_depth_10bps:
                    alerts.append(f"10bps depth dropped below {DEPTH_THRESHOLD_PERCENT*100:.0f}% of last 1h avg")
                if latest_depth_50bps < DEPTH_THRESHOLD_PERCENT * avg_depth_50bps:
                    alerts.append(f"50bps depth dropped below {DEPTH_THRESHOLD_PERCENT*100:.0f}% of last 1h avg")
                if latest_depth_100bps < DEPTH_THRESHOLD_PERCENT * avg_depth_100bps:
                    alerts.append(f"100bps depth dropped below {DEPTH_THRESHOLD_PERCENT*100:.0f}% of last 1h avg")

                if alerts:
                    formatted_time = timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")
                    msg = f"ALERT: Liquidity Depth Alert at {formatted_time}\n" + "\n".join(alerts)
                    print(msg)
                    send_telegram_alert(msg)
                else:
                    print(f"No significant liquidity drop at {timestamp_dt}.\n")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("Monitoring stopped by user")
        send_developer_alert("depth.py: Stopped by user (Ctrl+C)")
    except psycopg2.Error as e:
        print("Database error:", e)
        send_developer_alert(f"depth.py: Database error: {e}")
    except Exception as e:
        print("Error monitoring liquidity:", e)
        send_developer_alert(f"depth.py: Crashed with error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    try:
        monitor_liquidity_depth()
    except Exception as e:
        send_developer_alert(f"depth.py: Fatal error on startup: {e}")
        raise
