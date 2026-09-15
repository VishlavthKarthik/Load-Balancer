#!/usr/bin/env python3
"""
loadgen6/loadgen.py
Load generator for Lab 6. Sends POST /message and GET /feed requests
with variable concurrency, message lengths, and intervals.

Usage:
  python3 loadgen.py --url http://10.1.75.51:3318 --users 50 --duration 60
"""
import argparse, time, random, string, threading, statistics, json, sys
import urllib.request, urllib.parse, urllib.error
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Lab 6 Load Generator")
parser.add_argument("--url",       default="http://10.1.75.51:3318", help="LB base URL")
parser.add_argument("--users",     type=int, default=20,  help="Concurrent users")
parser.add_argument("--duration",  type=int, default=30,  help="Test duration (seconds)")
parser.add_argument("--msg-min",   type=int, default=5,   help="Min message length")
parser.add_argument("--msg-max",   type=int, default=100, help="Max message length")
parser.add_argument("--interval-min", type=float, default=0.05, help="Min sleep between reqs (s)")
parser.add_argument("--interval-max", type=float, default=0.5,  help="Max sleep between reqs (s)")
parser.add_argument("--read-ratio",   type=float, default=0.3,  help="Fraction of GET /feed requests")
args = parser.parse_args()

BASE = args.url.rstrip("/")

NAMES = ["Alice","Bob","Carol","Dave","Eve","Frank","Grace","Heidi",
         "Ivan","Judy","Karthik","Liam","Mia","Nora","Oscar","Priya",
         "Quinn","Raj","Sara","Tom","Uma","Vijay","Wren","Xena","Yash","Zara"]

def rand_msg(min_len, max_len):
    n = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_letters + string.digits + " .,!?", k=n))

# ── Shared metrics ─────────────────────────────────────────────
lock = threading.Lock()
results = defaultdict(list)  # route -> list of (latency_ms, status_code)
errors  = 0
stop_event = threading.Event()

def send_message(name, msg):
    global errors
    data = urllib.parse.urlencode({"client-name": name, "msg": msg}).encode()
    req = urllib.request.Request(f"{BASE}/message", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            ms = (time.perf_counter()-t0)*1000
            with lock:
                results["POST /message"].append((ms, resp.status))
    except Exception as e:
        ms = (time.perf_counter()-t0)*1000
        with lock:
            results["POST /message"].append((ms, 0))
            errors += 1

def get_feed():
    global errors
    req = urllib.request.Request(f"{BASE}/feed", method="GET")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            ms = (time.perf_counter()-t0)*1000
            with lock:
                results["GET /feed"].append((ms, resp.status))
    except Exception as e:
        ms = (time.perf_counter()-t0)*1000
        with lock:
            results["GET /feed"].append((ms, 0))
            errors += 1

def user_worker(uid):
    name = NAMES[uid % len(NAMES)] + str(uid)
    while not stop_event.is_set():
        if random.random() < args.read_ratio:
            get_feed()
        else:
            msg = rand_msg(args.msg_min, args.msg_max)
            send_message(name, msg)
        sleep = random.uniform(args.interval_min, args.interval_max)
        time.sleep(sleep)

# ── Run ────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  Lab 6 Load Generator")
print(f"  URL      : {BASE}")
print(f"  Users    : {args.users}")
print(f"  Duration : {args.duration}s")
print(f"  Msg size : {args.msg_min}-{args.msg_max} chars")
print(f"  Interval : {args.interval_min}-{args.interval_max}s")
print(f"  Read %   : {int(args.read_ratio*100)}%")
print(f"{'='*60}\n")

threads = [threading.Thread(target=user_worker, args=(i,), daemon=True)
           for i in range(args.users)]
t_start = time.time()
for t in threads: t.start()

# Progress bar
while time.time() - t_start < args.duration:
    elapsed = time.time() - t_start
    pct = int(elapsed / args.duration * 40)
    bar = "█"*pct + "░"*(40-pct)
    with lock:
        total = sum(len(v) for v in results.values())
    print(f"\r  [{bar}] {int(elapsed)}s  {total} reqs", end="", flush=True)
    time.sleep(0.5)

stop_event.set()
for t in threads: t.join(timeout=3)
elapsed = time.time() - t_start

print(f"\n\n{'='*60}")
print(f"  RESULTS")
print(f"{'='*60}")

for route in sorted(results):
    lats = [x[0] for x in results[route]]
    codes = [x[1] for x in results[route]]
    ok = sum(1 for c in codes if 200 <= c < 300)
    err = len(codes) - ok
    if not lats: continue
    lats.sort()
    n = len(lats)
    print(f"\n  {route}")
    print(f"    Requests   : {n}")
    print(f"    Errors     : {err} ({100*err//max(n,1)}%)")
    print(f"    Throughput : {n/elapsed:.1f} req/s")
    print(f"    Min        : {min(lats):.1f} ms")
    print(f"    Mean       : {statistics.mean(lats):.1f} ms")
    print(f"    Median     : {statistics.median(lats):.1f} ms")
    print(f"    P95        : {lats[min(int(n*0.95),n-1)]:.1f} ms")
    print(f"    P99        : {lats[min(int(n*0.99),n-1)]:.1f} ms")
    print(f"    Max        : {max(lats):.1f} ms")

total_reqs = sum(len(v) for v in results.values())
print(f"\n  Total requests : {total_reqs}")
print(f"  Total errors   : {errors}")
print(f"  Overall RPS    : {total_reqs/elapsed:.1f}")
print(f"  Duration       : {elapsed:.1f}s")
print(f"{'='*60}")
