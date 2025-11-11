import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta, timezone
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

script_config = config.get_script_config('impact_price_difference')
THRESHOLD_PCT = script_config['threshold_percent']
CHECK_INTERVAL = script_config['check_interval_seconds']

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

def pct_diff(latest, avg):
    if avg is None or avg == 0:
        return 0
    return abs(latest - avg) / avg * 100

def check_volatility():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f'''
                SELECT impactpxs_bid, impactpxs_ask, oraclepx, markpx, timestamp
                FROM {SCHEMA_NAME}.{TABLE_NAME}
                WHERE impactpxs_bid IS NOT NULL 
                  AND impactpxs_ask IS NOT NULL 
                  AND oraclepx IS NOT NULL
                  AND markpx IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1;
            ''')
            latest_row = cur.fetchone()
            if not latest_row:
                print("No latest row found.")
                return

            latest_ts = latest_row['timestamp']

            one_hour_ago = latest_ts - 3600000
            cur.execute(f'''
                SELECT impactpxs_bid, impactpxs_ask, oraclepx, markpx
                FROM {SCHEMA_NAME}.{TABLE_NAME}
                WHERE timestamp >= %s
                  AND impactpxs_bid IS NOT NULL 
                  AND impactpxs_ask IS NOT NULL 
                  AND oraclepx IS NOT NULL
                  AND markpx IS NOT NULL
                ORDER BY timestamp ASC;
            ''', (one_hour_ago,))
            last_hour_rows = cur.fetchall()

            if not last_hour_rows:
                print("No rows in the last hour.")
                return

            avg_impact_px = float(sum((r['impactpxs_bid'] + r['impactpxs_ask']) / 2 for r in last_hour_rows) / len(last_hour_rows))
            avg_oracle = float(sum(r['oraclepx'] for r in last_hour_rows) / len(last_hour_rows))
            avg_mark = float(sum(r['markpx'] for r in last_hour_rows) / len(last_hour_rows))

            latest_impact_px = float((latest_row['impactpxs_bid'] + latest_row['impactpxs_ask']) / 2)
            latest_oracle = float(latest_row['oraclepx'])
            latest_mark = float(latest_row['markpx'])

            impact_diff = pct_diff(latest_impact_px, avg_impact_px)
            oracle_diff = pct_diff(latest_oracle, avg_oracle)
            mark_diff = pct_diff(latest_mark, avg_mark)

            timestamp_dt = datetime.fromtimestamp(latest_ts / 1000)
            print(f"Timestamp: {timestamp_dt}")
            print(f"Latest Impact Price: {latest_impact_px:.8f}, 1h Avg: {avg_impact_px:.8f}, Deviation: {impact_diff:.2f}%")
            print(f"Latest Oracle Price: {latest_oracle:.8f}, 1h Avg: {avg_oracle:.8f}, Deviation: {oracle_diff:.2f}%")
            print(f"Latest Mark Price: {latest_mark:.8f}, 1h Avg: {avg_mark:.8f}, Deviation: {mark_diff:.2f}%\n")

            alerts_sent = []
            
            if impact_diff > THRESHOLD_PCT:
                msg = (
                    f"ALERT: Impact Price deviation exceeds {THRESHOLD_PCT}%\n\n"
                    f"Timestamp: {timestamp_dt}\n"
                    f"Impact Price (avg of 1h): {avg_impact_px:.8f}\n"
                    f"current Impact Price: {latest_impact_px:.8f}\n"
                    f"Deviation: {impact_diff:.2f}%"
                )
                print(f"Sending Impact price alert")
                send_telegram_alert(msg)
                alerts_sent.append("Impact")
            
            if oracle_diff > THRESHOLD_PCT:
                msg = (
                    f"ALERT: Oracle Price deviation exceeds {THRESHOLD_PCT}%\n\n"
                    f"Timestamp: {timestamp_dt}\n"
                    f"Oracle Price (avg of 1h): {avg_oracle:.8f}\n"
                    f"current Oracle Price: {latest_oracle:.8f}\n"
                    f"Deviation: {oracle_diff:.2f}%"
                )
                print(f"Sending Oracle price alert")
                send_telegram_alert(msg)
                alerts_sent.append("Oracle")
            
            if mark_diff > THRESHOLD_PCT:
                msg = (
                    f"ALERT: Mark Price deviation exceeds {THRESHOLD_PCT}%\n\n"
                    f"Timestamp: {timestamp_dt}\n"
                    f"Mark Price (avg of 1h): {avg_mark:.8f}\n"
                    f"current Mark Price: {latest_mark:.8f}\n"
                    f"Deviation: {mark_diff:.2f}%"
                )
                print(f"Sending Mark price alert")
                send_telegram_alert(msg)
                alerts_sent.append("Mark")
            
            if not alerts_sent:
                print(f"No significant deviation detected.\n")
            else:
                print(f"Alerts sent for: {', '.join(alerts_sent)}\n")

    except psycopg2.Error as e:
        print("Database error:", e)
        send_developer_alert(f"impact_price_difference.py: Database error: {e}")
    except Exception as e:
        print("Error connecting or processing rows:", e)
        send_developer_alert(f"impact_price_difference.py: Error processing: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Starting volatility monitoring...\n")
    while True:
        check_volatility()
        time.sleep(CHECK_INTERVAL)
