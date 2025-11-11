#!/usr/bin/env python3
import os, sys, time, json, traceback
from collections import deque
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib import request, parse
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from dotenv import load_dotenv
from config_loader import load_config

load_dotenv()
config = load_config()

script_config = config.get_script_config('big_liquidation')
ROOT = os.path.expanduser(os.getenv("HIP_FILLS_ROOT", "~/hl/data/node_fills_by_block/hourly"))
USD_THRESHOLD = Decimal(str(script_config['usd_threshold']))
READ_EXISTING_AT_START = script_config['read_existing_at_start']
READ_NEW_FILES_FROM_START = script_config['read_new_files_from_start']

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEVELOPER_TELEGRAM_BOT_TOKEN = os.getenv("DEVELOPER_TELEGRAM_BOT_TOKEN")
DEVELOPER_TELEGRAM_CHAT_ID = os.getenv("DEVELOPER_TELEGRAM_CHAT_ID")

ALLOWED_COINS = set(config.symbols['allowed_liquidation_coins'])

def now():
    return time.strftime("%H:%M:%S")

def log(msg):
    print(f"[{now()}] {msg}", flush=True)

def send_telegram_alert(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("TELEGRAM credentials missing; skipping alert.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = parse.urlencode({"chat_id": TELEGRAM_CHAT_ID, "text": text}).encode()
    try:
        with request.urlopen(request.Request(url, data=data), timeout=10) as resp:
            ok = (200 <= resp.status < 300)
            if not ok:
                log(f"Telegram response status: {resp.status}")
            return ok
    except Exception as e:
        log(f"Market alert send failed: {e}")
        return False

def send_developer_alert(text: str) -> bool:
    if not DEVELOPER_TELEGRAM_BOT_TOKEN or not DEVELOPER_TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{DEVELOPER_TELEGRAM_BOT_TOKEN}/sendMessage"
    data = parse.urlencode({"chat_id": DEVELOPER_TELEGRAM_CHAT_ID, "text": text}).encode()
    try:
        with request.urlopen(request.Request(url, data=data), timeout=10) as resp:
            return (200 <= resp.status < 300)
    except Exception as e:
        log(f"Developer alert send failed: {e}")
        return False

def _to_decimal(x):
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None

def _fmt_usd(d: Decimal) -> str:
    q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"${q:,.2f}"

def extract_liquidations_from_record(rec):
    if isinstance(rec, dict) and "liquidatedUser" in rec and "method" in rec:
        yield {
            "time": rec.get("time"),
            "local_time": rec.get("local_time"),
            "block_time": rec.get("block_time"),
            "block_number": rec.get("block_number"),
            "hash": rec.get("hash"),
            "coin": rec.get("coin"),
            "px": rec.get("px"),
            "sz": rec.get("sz"),
            "side": rec.get("side"),
            "method": rec.get("method"),
            "liquidatedUser": rec.get("liquidatedUser"),
        }
        return

    # case (1): raw node record with events
    evs = rec.get("events") or []
    for ev in evs:
        try:
            e1 = ev[1] if isinstance(ev, (list, tuple)) and len(ev) > 1 else None
            if not isinstance(e1, dict):
                continue
            liq = e1.get("liquidation")
            if not isinstance(liq, dict):
                continue
            yield {
                "time": e1.get("time"),
                "local_time": rec.get("local_time"),
                "block_time": rec.get("block_time"),
                "block_number": rec.get("block_number"),
                "hash": e1.get("hash"),
                "coin": e1.get("coin"),
                "px": e1.get("px"),
                "sz": e1.get("sz"),
                "side": e1.get("side"),
                "method": liq.get("method"),
                "liquidatedUser": liq.get("liquidatedUser"),
            }
        except Exception:
            continue

def should_alert(liq):
    method = (liq.get("method") or "").lower()
    log(f"      should_alert check: method='{method}'")

    if method == "backstop":
        log(f"      Backstop liquidation detected - will alert")
        return True, "backstop"

    if method == "market":
        px = _to_decimal(liq.get("px"))
        sz = _to_decimal(liq.get("sz"))
        log(f"      Market liquidation: px={px}, sz={sz}")

        if px is None or sz is None:
            log(f"      Invalid px/sz values - no alert")
            return False, None

        notional = px * sz
        threshold_met = notional > USD_THRESHOLD
        log(f"      Notional: {_fmt_usd(notional)} vs threshold {_fmt_usd(USD_THRESHOLD)} - threshold_met={threshold_met}")
        return threshold_met, "market"

    log(f"      Unknown method '{method}' - no alert")
    return False, None

def compose_message(liq, kind):
    # required fields
    px = liq.get("px")
    sz = liq.get("sz")
    user = liq.get("liquidatedUser")
    txh = liq.get("hash")
    coin = liq.get("coin")
    block = liq.get("block_number")
    btime = liq.get("block_time")
    method = liq.get("method")

    # compute notional if possible
    notional = None
    px_d = _to_decimal(px)
    sz_d = _to_decimal(sz)
    if px_d is not None and sz_d is not None:
        notional = px_d * sz_d

    lines = []
    if kind == "backstop":
        lines.append("Liquidation alert: backstop (ADL occurred on the exchange)")
    else:
        lines.append("Liquidation alert")

    if method:
        lines.append(f"method: {method}")
    if coin:
        lines.append(f"coin: {coin}")
    lines.append(f"px: {px}")
    lines.append(f"sz: {sz}")
    if notional is not None:
        lines.append(f"notional: {_fmt_usd(notional)} (threshold {_fmt_usd(USD_THRESHOLD)})")
    if user:
        lines.append(f"user: {user}")
    if txh:
        lines.append(f"hash: {txh}")
    if block is not None:
        lines.append(f"block: {block}")
    if btime:
        lines.append(f"block_time: {btime}")
    return "\n".join(lines)

class TailHandler(PatternMatchingEventHandler):
    def __init__(self, root: str):
        super().__init__(patterns=["*"], ignore_patterns=None, ignore_directories=True, case_sensitive=True)
        self.root = os.path.abspath(root)
        self.offsets = {}
        self.buffers = {}
        self.seen_keys = set()
        self.seen_order = deque(maxlen=20000)  # LRU for (hash, method)

        log(f"initial scan under {self.root}")
        for dp, _, files in os.walk(self.root):
            log(f"DIR {dp}")
            for fn in files:
                p = os.path.join(dp, fn)
                if not os.path.isfile(p):
                    continue
                try:
                    size = os.path.getsize(p)
                except FileNotFoundError:
                    continue
                start = 0 if READ_EXISTING_AT_START else size
                self.offsets[p] = start
                self.buffers[p] = ""
                log(f"  register {p} size={size} start_offset={start}")

    def _read_new_lines(self, path: str):
        try:
            last_off = self.offsets.get(path, 0)
            try:
                size = os.path.getsize(path)
            except FileNotFoundError:
                log(f"  disappeared: {path}")
                return
            if size < last_off:
                log(f"  truncate/rotation detected {path} {last_off}->{size}; reset to 0")
                last_off = 0
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(last_off)
                chunk = f.read()
                new_off = f.tell()
                self.offsets[path] = new_off
                if new_off != last_off:
                    log(f"  read {path}: {last_off}->{new_off} bytes={len(chunk)}")
        except Exception as e:
            log(f"  read error {path}: {e}")
            return

        buf = self.buffers.get(path, "")
        data = buf + (chunk or "")
        if not data:
            return

        lines = data.splitlines()
        if not data.endswith(("\n", "\r")):
            self.buffers[path] = lines[-1] if lines else data
            lines = lines[:-1] if lines else []
        else:
            self.buffers[path] = ""

        for line in lines:
            yield line

    def _maybe_alert(self, liq, source_path):
        do, kind = should_alert(liq)
        log(f"    Alert decision: should_alert={do}, kind={kind}")

        if not do:
            log(f"    No alert needed for liquidation (method={liq.get('method')}, threshold check failed or not backstop)")
            return
        
        coin = liq.get("coin")
        if coin not in ALLOWED_COINS:
            log(f"    Skipping liquidation for coin '{coin}' (not in allowed list)")
            return

        log(f"    Composing alert message for {kind} liquidation")
        msg = compose_message(liq, kind)
        log(f"    Sending telegram alert: {msg[:100]}...")

        ok = send_telegram_alert(msg)
        log(f"alert {'sent' if ok else 'FAILED'} ({source_path})")

        # keep LRU size in sync with set
        while len(self.seen_keys) > self.seen_order.maxlen:
            old = self.seen_order.popleft()
            self.seen_keys.discard(old)

    def _process_path(self, path: str):
        if not os.path.isfile(path):
            return
        for line in self._read_new_lines(path):
            s = line.strip()
            if not s:
                continue
            if not (s.startswith("{") or s.startswith("[")):
                # logs that are not JSON lines are skipped
                continue
            try:
                rec = json.loads(s)
            except json.JSONDecodeError:
                # accumulate for multi-line JSON
                self.buffers[path] = self.buffers.get(path, "") + s
                continue

            # Log block number for every record processed
            block_num = rec.get("block_number")
            if block_num is not None:
                log(f"Processing block {block_num}")

            try:
                liquidations_found = 0
                for liq in extract_liquidations_from_record(rec):
                    liquidations_found += 1
                    log(f"  Found liquidation: user={liq.get('liquidatedUser')}, method={liq.get('method')}, px={liq.get('px')}, sz={liq.get('sz')}, coin={liq.get('coin')}")
                    self._maybe_alert(liq, path)
                if liquidations_found > 0:
                    log(f"  Total liquidations found in record: {liquidations_found}")
            except Exception:
                log(f"  error while extracting/alerting from {path}")
                traceback.print_exc()

    def on_created(self, event):
        size = os.path.getsize(event.src_path) if os.path.exists(event.src_path) else 0
        start = 0 if READ_NEW_FILES_FROM_START else size
        self.offsets[event.src_path] = start
        self.buffers[event.src_path] = ""
        log(f"NEW file {event.src_path} size={size} offset={start}")
        if READ_NEW_FILES_FROM_START:
            self._process_path(event.src_path)

    def on_modified(self, event):
        if event.src_path not in self.offsets:
            size = os.path.getsize(event.src_path) if os.path.exists(event.src_path) else 0
            self.offsets[event.src_path] = size
            self.buffers[event.src_path] = ""
            log(f"late-register {event.src_path} size={size} offset={size}")
        self._process_path(event.src_path)

    def on_moved(self, event):
        old = event.src_path
        new = event.dest_path
        off = self.offsets.pop(old, 0)
        buf = self.buffers.pop(old, "")
        self.offsets[new] = off
        self.buffers[new] = buf
        log(f"moved {old} -> {new} carry_offset={off} carry_buf={len(buf)}")

def main():
    root = ROOT
    if not os.path.isdir(root):
        log(f"Not found: {root}")
        sys.exit(1)
    log(f"Monitoring recursively under: {root}")
    log(f"Tail existing files from {'start' if READ_EXISTING_AT_START else 'EOF'}; new files from {'start' if READ_NEW_FILES_FROM_START else 'EOF'}")
    handler = TailHandler(root)
    observer = Observer()
    observer.schedule(handler, path=root, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        send_developer_alert("big_liquidation.py: Stopped by user (Ctrl+C)")
        log("stopping")
        observer.stop()
    except Exception as e:
        send_developer_alert(f"big_liquidation.py: Crashed with error: {e}")
        log(f"Fatal error: {e}")
        observer.stop()
        raise
    observer.join()

if __name__ == "__main__":
    main()