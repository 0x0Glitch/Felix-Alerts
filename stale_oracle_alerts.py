#!/usr/bin/env python3
import os, time, json, traceback
from collections import deque
from datetime import datetime
from urllib import request, parse
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from dotenv import load_dotenv
from config_loader import load_config

load_dotenv()
config = load_config()

script_config = config.get_script_config('stale_oracle_alerts')
ROOT = os.path.expanduser(os.getenv("HIP3_HOURLY_ROOT", "~/hl/data/hip3_oracle_updates_by_block/hourly"))
THRESHOLD_SECONDS = script_config['threshold_seconds']
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEVELOPER_TELEGRAM_BOT_TOKEN = os.getenv("DEVELOPER_TELEGRAM_BOT_TOKEN")
DEVELOPER_TELEGRAM_CHAT_ID = os.getenv("DEVELOPER_TELEGRAM_CHAT_ID")

MARKETS_STR = os.getenv("MARKETS", config.symbols['primary_coin'])
MARKETS = set(s.strip() for s in MARKETS_STR.split(",") if s.strip()) if MARKETS_STR else None

READ_EXISTING_AT_START = script_config['read_existing_at_start']
READ_NEW_FILES_FROM_START = script_config['read_new_files_from_start']

def _now():
    return time.strftime("%H:%M:%S")

def send_telegram_alert(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[{_now()}] TELEGRAM creds missing; skipping alert.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = parse.urlencode({"chat_id": TELEGRAM_CHAT_ID, "text": text}).encode()
    try:
        with request.urlopen(request.Request(url, data=data), timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as e:
        print(f"[{_now()}] Market alert send failed: {e}")
        return False

def send_developer_alert(text: str) -> bool:
    if not DEVELOPER_TELEGRAM_BOT_TOKEN or not DEVELOPER_TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{DEVELOPER_TELEGRAM_BOT_TOKEN}/sendMessage"
    data = parse.urlencode({"chat_id": DEVELOPER_TELEGRAM_CHAT_ID, "text": text}).encode()
    try:
        with request.urlopen(request.Request(url, data=data), timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as e:
        print(f"[{_now()}] Developer alert send failed: {e}")
        return False

def parse_iso_ts(ts: str) -> datetime:
    s = ts
    if s.endswith("Z"): s = s[:-1] + "+00:00"
    if "." in s:
        head, tail = s.split(".", 1); tz = ""
        for i, ch in enumerate(tail):
            if ch in "+-": tz = tail[i:]; frac = tail[:i]; break
        else: frac = tail
        frac = ("".join(ch for ch in frac if ch.isdigit()) + "000000")[:6]
        s = f"{head}.{frac}{tz}"
    return datetime.fromisoformat(s)

def collect_last_update_times(record: dict):
    out = []
    events = record.get("events") or []
    if not events: return out
    oracle = events[0].get("oracle_pxs", {})
    for key in ("coin_to_mark_px","coin_to_oracle_px","coin_to_external_perp_px"):
        for sym, obj in oracle.get(key, []):
            if MARKETS is not None and sym not in MARKETS:
                continue
            ts = obj.get("last_update_time") or obj.get("last_updated_time")
            if isinstance(ts, str) and "T" in ts: out.append((sym, ts))
    return out

def check_record(record: dict, source_path: str, alerted_blocks: set):
    block_time = record.get("block_time")
    if not block_time: return
    block_number = record.get("block_number", "unknown")
    if block_number in alerted_blocks: return
    ups = collect_last_update_times(record)
    if not ups: return
    block_dt = parse_iso_ts(block_time)
    offenders = []
    for sym, lut in ups:
        try:
            dsec = abs((block_dt - parse_iso_ts(lut)).total_seconds())
            if dsec > THRESHOLD_SECONDS: offenders.append((sym, dsec, lut))
        except: pass
    if not offenders: return
    offenders.sort(key=lambda x: x[1], reverse=True)
    worst = offenders[0][1]
    lines = [
        "ALERT: HIP3 Oracle Update Skew Detected\n",
        f"Block: {block_number}",
        f"Block Time: {block_time}",
        f"Threshold: {THRESHOLD_SECONDS:.3f}s",
        f"Worst Skew: {worst:.3f}s",
        f"Total Stale Markets: {len(offenders)}\n",
        "Market Name:",
    ]
    for i, (symbol, dsec, lut) in enumerate(offenders[:20], 1):
        lines.append(f"  {i}. {symbol} - Skew: {dsec:.3f}s (last_update: {lut})")
    if len(offenders) > 20:
        lines.append(f"\n... and {len(offenders) - 20} more")
    msg = "\n".join(lines)
    ok = send_telegram_alert(msg)
    if ok: 
        alerted_blocks.add(block_number)
    print(f"[{_now()}] alert {'sent' if ok else 'suppressed'} for block {block_number} ({source_path})")

def find_latest_hour_path(root):
    """Find the most recent hourly directory path."""
    print(f"[{_now()}] scan date dirs under {root}")
    date_dirs = [d for d in os.listdir(root) if len(d)==8 and d.isdigit() and os.path.isdir(os.path.join(root,d))]
    if not date_dirs: return None
    date_dirs.sort()
    latest_date = os.path.join(root, date_dirs[-1])
    print(f"[{_now()}] latest date -> {latest_date}")
    hour_dirs = [h for h in os.listdir(latest_date) if len(h)==2 and h.isdigit() and os.path.isdir(os.path.join(latest_date,h))]
    if not hour_dirs: return None
    hour_dirs.sort()
    latest_hour = os.path.join(latest_date, hour_dirs[-1])
    print(f"[{_now()}] latest hour -> {latest_hour}")
    return latest_hour

def find_latest_file_in(dir_path):
    """Find the most recently modified file in directory."""
    latest_p, latest_mt = None, -1.0
    for fn in os.listdir(dir_path):
        p = os.path.join(dir_path, fn)
        if not os.path.isfile(p): continue
        try:
            mt = os.path.getmtime(p)
            if mt > latest_mt: latest_p, latest_mt = p, mt
        except FileNotFoundError:
            pass
    if latest_p:
        print(f"[{_now()}] latest file in {dir_path} = {latest_p}")
    else:
        print(f"[{_now()}] no files in {dir_path}")
    return latest_p

class TailHandler(PatternMatchingEventHandler):
    """File watcher handler that monitors and processes oracle update files."""
    def __init__(self, root: str):
        super().__init__(patterns=["*"], ignore_patterns=None, ignore_directories=True, case_sensitive=True)
        self.root = os.path.abspath(root)
        self.offsets, self.buffers = {}, {}
        self.alerted_blocks = set()
        self.alerted_order = deque(maxlen=1000)
        print(f"[{_now()}] register existing files (tail-only={not READ_EXISTING_AT_START})")
        for dp, _, files in os.walk(self.root):
            print(f"[{_now()}] DIR {dp}")
            for fn in files:
                p = os.path.join(dp, fn)
                if not os.path.isfile(p): continue
                try: size = os.path.getsize(p)
                except FileNotFoundError: continue
                start = 0 if READ_EXISTING_AT_START else size
                self.offsets[p] = start; self.buffers[p] = ""
                print(f"[{_now()}]  + {p} size={size} offset={start}")
        latest_hour = find_latest_hour_path(self.root)
        self.focus_dir = latest_hour
        self.focus_file = find_latest_file_in(latest_hour) if latest_hour else None
        if self.focus_file:
            size = os.path.getsize(self.focus_file)
            self.offsets[self.focus_file] = 0 if READ_EXISTING_AT_START else size
            self.buffers.setdefault(self.focus_file, "")
            print(f"[{_now()}] focus file {self.focus_file} start_offset={self.offsets[self.focus_file]}")

    def _read_new_lines(self, path: str):
        try:
            last_off = self.offsets.get(path, 0)
            size = os.path.getsize(path)
            if size < last_off:
                print(f"[{_now()}] rotation/truncate {path} {last_off}->{size}; reset to 0")
                last_off = 0
            with open(path, "r") as f:
                f.seek(last_off); chunk = f.read(); new_off = f.tell()
                self.offsets[path] = new_off
                print(f"[{_now()}] READ {path}: {last_off}->{new_off} bytes={len(chunk)}")
        except FileNotFoundError:
            print(f"[{_now()}] disappeared before read: {path}")
            return
        buf = self.buffers.get(path, ""); data = buf + (chunk or "")
        if not data:
            print(f"[{_now()}] no data for {path}"); return
        lines = data.splitlines()
        if not data.endswith(("\n", "\r")):
            self.buffers[path] = lines[-1] if lines else data
            print(f"[{_now()}] keep partial buffer for {path}: {len(self.buffers[path])} chars")
            lines = lines[:-1] if lines else []
        else:
            self.buffers[path] = ""
        for line in lines:
            yield line

    def _process_path(self, path: str):
        if not os.path.isfile(path):
            return
        for line in self._read_new_lines(path):
            s = line.strip()
            if not s:
                continue
            if not (s.startswith("{") or s.startswith("[")):
                print(f"[{_now()}] skip non-JSON line in {path}: {s[:80]!r}")
                continue
            try:
                record = json.loads(s)
            except json.JSONDecodeError:
                self.buffers[path] = self.buffers.get(path, "") + s
                print(f"[{_now()}] mid-write JSON at {path}; buffer size={len(self.buffers[path])}")
                continue
            try:
                check_record(record, path, self.alerted_blocks)
            except Exception:
                print(f"[{_now()}] error in check_record for {path}")
                traceback.print_exc()

    def on_created(self, event):
        size = os.path.getsize(event.src_path) if os.path.exists(event.src_path) else 0
        start = 0 if READ_NEW_FILES_FROM_START else size
        self.offsets[event.src_path] = start; self.buffers[event.src_path] = ""
        print(f"[{_now()}] NEW file {event.src_path} size={size} offset={start}")
        if READ_NEW_FILES_FROM_START:
            self._process_path(event.src_path)

    def on_modified(self, event):
        if event.src_path not in self.offsets:
            size = os.path.getsize(event.src_path) if os.path.exists(event.src_path) else 0
            self.offsets[event.src_path] = size; self.buffers[event.src_path] = ""
            print(f"[{_now()}] late-register {event.src_path} size={size} offset={size}")
        self._process_path(event.src_path)

if __name__ == "__main__":
    if not os.path.isdir(ROOT):
        print(f"[{_now()}] Not found: {ROOT}"); raise SystemExit(1)
    print(f"[{_now()}] Monitoring recursively under: {ROOT}")
    if MARKETS:
        print(f"[{_now()}] Filtering for markets: {', '.join(sorted(MARKETS))}")
    else:
        print(f"[{_now()}] Monitoring all markets")
    handler = TailHandler(ROOT)
    observer = Observer()
    observer.schedule(handler, path=ROOT, recursive=True)
    observer.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        send_developer_alert("stale_oracle_alerts.py: Stopped by user (Ctrl+C)")
        print(f"[{_now()}] stopping"); observer.stop()
    except Exception as e:
        send_developer_alert(f"stale_oracle_alerts.py: Crashed with error: {e}")
        print(f"[{_now()}] Fatal error: {e}"); observer.stop()
        raise
    observer.join()
