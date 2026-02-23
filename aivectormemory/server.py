import sys
from aivectormemory import __version__
from aivectormemory.protocol import (
    read_message, write_message, make_result, make_error,
    METHOD_NOT_FOUND, INVALID_PARAMS, INTERNAL_ERROR, SERVER_ERROR
)
from aivectormemory.db import ConnectionManager, init_db
from aivectormemory.embedding.engine import EmbeddingEngine
from aivectormemory.tools import TOOL_DEFINITIONS, TOOL_HANDLERS


class MCPServer:
    def __init__(self, project_dir: str | None = None):
        self.cm = ConnectionManager(project_dir=project_dir)
        self.engine = EmbeddingEngine()
        self._session_id = 0
        self._initialized = False

    def _init_db(self):
        init_db(self.cm.conn, engine=self.engine)
        row = self.cm.conn.execute(
            "SELECT last_session_id FROM session_state WHERE project_dir=?",
            (self.cm.project_dir,)
        ).fetchone()
        if row:
            self._session_id = row["last_session_id"]
        else:
            # 首次：从 memories 表兼容恢复，并创建 session_state 行
            row = self.cm.conn.execute(
                "SELECT MAX(session_id) as max_sid FROM memories WHERE project_dir=?",
                (self.cm.project_dir,)
            ).fetchone()
            self._session_id = (row["max_sid"] or 0)
            from datetime import datetime
            now = datetime.now().astimezone().isoformat()
            self.cm.conn.execute(
                "INSERT OR IGNORE INTO session_state (project_dir, last_session_id, updated_at) VALUES (?,?,?)",
                (self.cm.project_dir, self._session_id, now)
            )
            self.cm.conn.commit()

    def handle_initialize(self, req_id, params):
        if not self._initialized:
            self._init_db()
            self._initialized = True
        # 从 DB 读取最新值再递增，避免多实例冲突
        row = self.cm.conn.execute(
            "SELECT last_session_id FROM session_state WHERE project_dir=?",
            (self.cm.project_dir,)
        ).fetchone()
        self._session_id = (row["last_session_id"] if row else self._session_id) + 1
        self.cm.conn.execute(
            "UPDATE session_state SET last_session_id=? WHERE project_dir=?",
            (self._session_id, self.cm.project_dir)
        )
        self.cm.conn.commit()
        print(f"[aivectormemory] Session {self._session_id} started, project={self.cm.project_dir}", file=sys.stderr)
        write_message(make_result(req_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "aivectormemory", "version": __version__}
        }))

    def handle_tools_list(self, req_id, params):
        write_message(make_result(req_id, {"tools": TOOL_DEFINITIONS}))

    def handle_tools_call(self, req_id, params):
        name = params.get("name", "")
        args = params.get("arguments", {})
        handler = TOOL_HANDLERS.get(name)
        if not handler:
            write_message(make_error(req_id, METHOD_NOT_FOUND, f"Unknown tool: {name}"))
            return
        try:
            result = handler(args, cm=self.cm, engine=self.engine, session_id=self._session_id)
            text = str(result)
            if len(text) > 30000:
                text = text[:30000] + "\n[truncated: output too large, use specific query to narrow results]"
            write_message(make_result(req_id, {
                "content": [{"type": "text", "text": text}]
            }))
        except ValueError as e:
            write_message(make_error(req_id, INVALID_PARAMS, str(e)))
        except Exception as e:
            print(f"[aivectormemory] Error in {name}: {e}", file=sys.stderr)
            write_message(make_error(req_id, SERVER_ERROR, str(e)))

    def handle_notifications_initialized(self, req_id, params):
        pass

    def run(self):
        handlers = {
            "initialize": self.handle_initialize,
            "notifications/initialized": self.handle_notifications_initialized,
            "tools/list": self.handle_tools_list,
            "tools/call": self.handle_tools_call,
        }
        print("[aivectormemory] MCP Server started (stdio)", file=sys.stderr)
        while True:
            msg = read_message()
            if msg is None:
                break
            method = msg.get("method", "")
            req_id = msg.get("id")
            params = msg.get("params", {})
            handler = handlers.get(method)
            try:
                if handler:
                    handler(req_id, params)
                elif req_id is not None:
                    write_message(make_error(req_id, METHOD_NOT_FOUND, f"Unknown method: {method}"))
            except Exception as e:
                print(f"[aivectormemory] Unhandled error in {method}: {e}", file=sys.stderr)
                if req_id is not None:
                    write_message(make_error(req_id, INTERNAL_ERROR, str(e)))
        self.cm.close()
        print("[aivectormemory] MCP Server stopped", file=sys.stderr)


def run_server(project_dir: str | None = None):
    server = MCPServer(project_dir=project_dir)
    server.run()
