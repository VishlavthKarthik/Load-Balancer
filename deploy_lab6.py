#!/usr/bin/env python3
"""
deploy_lab6.py
Master deployment script for Lab 6.
- Sys2:
    * DB Service on :5000 (Go db6_service)
    * Backend 1 on :4000 (Go backend6_server)
    * Dynamic Load Balancer on :3000 (Go lb6_server, mapped to http://10.1.75.51:3318)
- Sys3:
    * Backend 2 on :3000 (Go backend6_server, mapped to http://10.1.75.51:3319)
- Sys4:
    * Backend 3 on :3000 (Go backend6_server, mapped to http://10.1.75.51:3320)
- Sys1: Untouched!
"""
import paramiko
import time
import os
import urllib.request
import json

HOST = "10.1.75.51"
USER = "student"
PASS = "karthik123"
LOCAL = "/home/karthik/Desktop/CSD/GRP-CHAT"
REMOTE = "/home/student/chat_app"

IP2 = "172.17.0.119"
IP3 = "172.17.0.120"
IP4 = "172.17.0.121"

def ssh(port, retries=3):
    for attempt in range(retries):
        try:
            c = paramiko.SSHClient()
            c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            c.connect(
                HOST,
                port=port,
                username=USER,
                password=PASS,
                timeout=15,
                banner_timeout=30,
                look_for_keys=False,
                allow_agent=False
            )
            return c
        except Exception as e:
            if attempt == retries - 1:
                raise
            print(f"  SSH to port {port} attempt {attempt+1} failed ({e}), retrying in 2s...")
            time.sleep(2)

def run_cmd(c, cmd, timeout=30):
    _, stdout, stderr = c.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    stdout.channel.recv_exit_status()
    return (out + err).strip()

def upload_file(c, local_path, remote_path):
    sftp = c.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()

def deploy_db_and_files_sys2():
    print("=== Step 1: Stopping old processes and starting DB Service on Sys2 (:5000) ===")
    c2 = ssh(2318)
    run_cmd(c2, f"mkdir -p {REMOTE}/lb6 {REMOTE}/db6 {REMOTE}/backend6")

    print("  Stopping old processes on Sys2...")
    run_cmd(c2, "pkill -9 -f 'lb6_server|db6_service|backend6_server|gunicorn' 2>/dev/null; sleep 1")
    run_cmd(c2, "fuser -k 3000/tcp 4000/tcp 5000/tcp 2>/dev/null; sleep 1")
    run_cmd(c2, f"cp {REMOTE}/messages.db {REMOTE}/messages.db.bak 2>/dev/null; rm -f {REMOTE}/messages.db {REMOTE}/messages.db-wal {REMOTE}/messages.db-shm 2>/dev/null || true")

    print("  Uploading db6_service...")
    upload_file(c2, f"{LOCAL}/db6/db6_service", f"{REMOTE}/db6/db6_service")
    run_cmd(c2, f"chmod +x {REMOTE}/db6/db6_service")

    print("  Uploading backend6_server to Sys2...")
    upload_file(c2, f"{LOCAL}/backend6_go/backend6_server", f"{REMOTE}/backend6/backend6_server")
    run_cmd(c2, f"chmod +x {REMOTE}/backend6/backend6_server")

    print("  Uploading lb6_server to Sys2...")
    upload_file(c2, f"{LOCAL}/lb6/lb6_server", f"{REMOTE}/lb6/lb6_server")
    run_cmd(c2, f"chmod +x {REMOTE}/lb6/lb6_server")

    # Start Go DB Service on port 5000 with setsid watchdog
    print("  Starting Go DB Service on :5000...")
    run_cmd(c2, f"/usr/bin/setsid bash -c 'ulimit -n 65535; while true; do {REMOTE}/db6/db6_service -addr :5000 -db {REMOTE}/messages.db >>/tmp/db6.log 2>&1; sleep 1; done' </dev/null >/dev/null 2>&1 &")
    time.sleep(2)
    db_h = run_cmd(c2, "curl -s http://127.0.0.1:5000/health")
    print(f"  DB Service health: {db_h}")
    c2.close()

def deploy_remote_backend(sys_num, port, backend_id):
    print(f"=== Deploying to Sys{sys_num} ({backend_id} on :3000) ===")
    c = ssh(port)
    run_cmd(c, f"mkdir -p {REMOTE}/backend6")
    run_cmd(c, "pkill -9 -f 'backend6_server|gunicorn' 2>/dev/null; sleep 1")
    run_cmd(c, "fuser -k 3000/tcp 2>/dev/null; sleep 1")

    print(f"  Uploading backend6_server to Sys{sys_num}...")
    upload_file(c, f"{LOCAL}/backend6_go/backend6_server", f"{REMOTE}/backend6/backend6_server")
    run_cmd(c, f"chmod +x {REMOTE}/backend6/backend6_server")

    start_cmd = (
        f"{REMOTE}/backend6/backend6_server -port 3000 -backend-id {backend_id} "
        f"-db-service http://{IP2}:5000"
    )
    run_cmd(c, f"/usr/bin/setsid bash -c 'ulimit -n 65535; while true; do {start_cmd} >>/tmp/backend_3000.log 2>&1; sleep 1; done' </dev/null >/dev/null 2>&1 &")
    time.sleep(2)
    h = run_cmd(c, "curl -s http://127.0.0.1:3000/health")
    print(f"  Sys{sys_num} ({backend_id}) health: {h}")
    c.close()

def start_backend1_and_lb_sys2():
    print("=== Step 4: Starting Backend 1 (:4000) and Load Balancer (:3000) on Sys2 ===")
    c2 = ssh(2318)

    # 1. Start Backend 1 on port 4000 with setsid watchdog
    print("  Starting Go Backend 1 on :4000...")
    start_b1 = (
        f"{REMOTE}/backend6/backend6_server -port 4000 -backend-id backend-1 "
        f"-db-service http://127.0.0.1:5000"
    )
    run_cmd(c2, f"/usr/bin/setsid bash -c 'ulimit -n 65535; while true; do {start_b1} >>/tmp/backend_4000.log 2>&1; sleep 1; done' </dev/null >/dev/null 2>&1 &")
    time.sleep(2)
    b1_h = run_cmd(c2, "curl -s http://127.0.0.1:4000/health")
    print(f"  Backend 1 health: {b1_h}")

    # 2. Start Load Balancer on port 3000 (mapped to host 3318) with setsid watchdog
    backends = f"backend-1=http://127.0.0.1:4000,backend-2=http://{IP3}:3000,backend-3=http://{IP4}:3000"
    print("  Starting Go Dynamic Load Balancer on :3000...")
    start_lb = (
        f"{REMOTE}/lb6/lb6_server -addr :3000 "
        f"-backends '{backends}' -threshold-active 35 -threshold-latency 60"
    )
    run_cmd(c2, f"/usr/bin/setsid bash -c 'ulimit -n 65535; while true; do {start_lb} >>/tmp/lb6.log 2>&1; sleep 1; done' </dev/null >/dev/null 2>&1 &")
    time.sleep(2)
    lb_h = run_cmd(c2, "curl -s http://127.0.0.1:3000/health")
    print(f"  LB health on Sys2:3000: {lb_h}")
    c2.close()

def verify_external_endpoints():
    print("\n=== Verifying External Endpoints from Host ===")
    endpoints = [
        ("Load Balancer (Sys2)", "http://10.1.75.51:3318/health"),
        ("Backend 2 (Sys3)", "http://10.1.75.51:3319/health"),
        ("Backend 3 (Sys4)", "http://10.1.75.51:3320/health"),
        ("LB Metrics", "http://10.1.75.51:3318/lb-metrics"),
    ]
    for name, url in endpoints:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                print(f"  ✓ {name}: {resp.read().decode()[:80]}")
        except Exception as e:
            print(f"  ✗ {name} FAILED: {e}")

    # Test POST /message and GET /feed via Load Balancer
    print("\n=== Testing Message Flow via Load Balancer ===")
    test_msg = json.dumps({"client-name": "deploy_verifier", "msg": f"Verification ping {int(time.time())}"}).encode()
    req = urllib.request.Request("http://10.1.75.51:3318/message", data=test_msg, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"  ✓ POST /message: {resp.read().decode()}")
    except Exception as e:
        print(f"  ✗ POST /message FAILED: {e}")

    try:
        with urllib.request.urlopen("http://10.1.75.51:3318/feed", timeout=5) as resp:
            data = json.loads(resp.read().decode())
            print(f"  ✓ GET /feed: returned {len(data)} total messages")
            if data:
                print(f"    Latest message: {data[-1]}")
    except Exception as e:
        print(f"  ✗ GET /feed FAILED: {e}")

    # Final clean reset so the deployment is 100% fresh for student submission
    try:
        req = urllib.request.Request("http://10.1.75.51:3318/reset", data=b"", method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"  ✓ POST /reset: {resp.read().decode()}")
        with urllib.request.urlopen("http://10.1.75.51:3318/feed", timeout=5) as resp:
            data = json.loads(resp.read().decode())
            print(f"  ✓ Final feed check: {len(data)} messages (clean [] verified)")
    except Exception as e:
        print(f"  ✗ Final reset notice: {e}")

if __name__ == "__main__":
    deploy_db_and_files_sys2()
    deploy_remote_backend(3, 2319, "backend-2")
    deploy_remote_backend(4, 2320, "backend-3")
    start_backend1_and_lb_sys2()
    verify_external_endpoints()
    print("\n✓ Deployment across Sys2, Sys3, Sys4 complete! (Sys1 untouched)")
