#!/usr/bin/env python3
"""
db6/db_service.py — Shared SQLite DB service (Python fallback)
Runs on Sys2 port 5318. All backends call this to insert/fetch messages.
WAL mode + connection pool for high concurrency.
"""
import sqlite3, json, threading, uuid, time, os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

DB_PATH = os.environ.get("DB_PATH", "/home/student/chat_app/messages.db")
PORT    = int(os.environ.get("PORT", 5318))

# ── Connection pool (WAL allows concurrent reads) ──────────────
_local = threading.local()

def get_conn():
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id  TEXT    UNIQUE NOT NULL,
                client_name TEXT    NOT NULL,
                msg         TEXT    NOT NULL,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        _local.conn = conn
    return _local.conn

# Pre-init the DB
_init_lock = threading.Lock()
with _init_lock:
    get_conn()

# Write lock (only one writer at a time with SQLite)
_write_lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence access logs

    def send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            self.send_json(200, {"status": "ok"})
        elif path == "/db/feed":
            try:
                conn = get_conn()
                rows = conn.execute(
                    "SELECT id, message_id, client_name, msg, created_at FROM messages ORDER BY id ASC"
                ).fetchall()
                result = [dict(r) for r in rows]
                self.send_json(200, result)
            except Exception as e:
                self.send_json(500, {"error": str(e)})
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/db/insert":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                message_id  = data.get("message_id") or str(uuid.uuid4())
                client_name = data.get("client_name", "anonymous")
                msg         = data.get("msg", "")

                conn = get_conn()
                with _write_lock:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO messages (message_id, client_name, msg) VALUES (?,?,?)",
                        (message_id, client_name, msg)
                    )
                    conn.commit()
                    duplicate = cur.rowcount == 0

                self.send_json(200, {"ok": True, "duplicate": duplicate, "message_id": message_id})
            except Exception as e:
                self.send_json(500, {"error": str(e)})
        else:
            self.send_json(404, {"error": "not found"})

class ThreadedHTTPServer(HTTPServer):
    def process_request(self, request, client_address):
        t = threading.Thread(target=self._process, args=(request, client_address))
        t.daemon = True
        t.start()
    def _process(self, req, addr):
        self.finish_request(req, addr)
        self.shutdown_request(req)

if __name__ == "__main__":
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    server = ThreadedHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"DB service listening on :{PORT}, DB={DB_PATH}")
    server.serve_forever()
