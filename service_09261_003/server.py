"""标准库 HTTP 入口：python3 -m service_09261_003.server

环境变量：
    CASE_DB_PATH  SQLite 文件路径（默认 cases.db，重启历史不丢；
                  设为 :memory: 则仅用于测试）
    PORT          监听端口（默认 8080）
"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .api import handle_json
from .store import SQLiteEventStore
from .workflow import Workflow


def build_app(db_path: str | None = None):
    db_path = db_path or os.environ.get("CASE_DB_PATH", "cases.db")
    store = SQLiteEventStore(db_path)
    return Workflow(store), store


class Handler(BaseHTTPRequestHandler):
    flow: Workflow = None  # 由 main 注入到类上

    def _read_json(self) -> bytes | None:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else None

    def _serve(self, method: str):
        raw = self._read_json() if method == "POST" else None
        status, payload = handle_json(self.flow, method, self.path, raw)
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._serve("GET")

    def do_POST(self):
        self._serve("POST")

    def log_message(self, fmt, *args):  # 简洁日志
        print(f"[{self.log_date_time_string()}] {fmt % args}")


def main():
    flow, _store = build_app()
    Handler.flow = flow
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"脱敏交付服务已启动: http://0.0.0.0:{port}  (db={_store.path})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
