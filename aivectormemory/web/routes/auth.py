"""Local authentication routes for web dashboard."""
import hashlib
import os
import time
from datetime import datetime

# In-memory session store: {token: {"username": str, "created_at": float}}
_sessions: dict[str, dict] = {}


def _hash_password(password: str, salt: bytes | None = None) -> tuple[str, bytes]:
    if salt is None:
        salt = os.urandom(32)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return (salt + h).hex(), salt


def _verify_password(password: str, stored_hex: str) -> bool:
    raw = bytes.fromhex(stored_hex)
    salt, stored_hash = raw[:32], raw[32:]
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return h == stored_hash


def _generate_token() -> str:
    return os.urandom(32).hex()


def register(handler, cm, read_body):
    body = read_body(handler)
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    if not username or not password:
        return {"error": "username and password required"}
    if len(password) < 6:
        return {"error": "password must be at least 6 characters"}

    conn = cm.conn
    existing = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        return {"error": "username already exists"}

    pw_hash, _ = _hash_password(password)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
        (username, pw_hash, now),
    )
    conn.commit()
    return {"success": True}


def login(handler, cm, read_body):
    body = read_body(handler)
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    if not username or not password:
        return {"error": "username and password required"}

    conn = cm.conn
    row = conn.execute(
        "SELECT id, password_hash FROM users WHERE username = ?", (username,)
    ).fetchone()

    if not row or not _verify_password(password, row[1]):
        return {"error": "invalid username or password"}

    token = _generate_token()
    _sessions[token] = {"username": username, "created_at": time.time()}

    conn.execute(
        "UPDATE users SET last_login = ? WHERE id = ?",
        (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), row[0]),
    )
    conn.commit()

    return {"token": token, "username": username}


def logout(handler, read_body):
    body = read_body(handler)
    token = body.get("token") or ""
    _sessions.pop(token, None)
    return {"success": True}


def verify_token(token: str) -> str | None:
    """Return username if token is valid, else None."""
    session = _sessions.get(token)
    if not session:
        return None
    return session["username"]


def get_current_user(params):
    token = params.get("token", [None])[0]
    if not token:
        return {"error": "token required"}
    username = verify_token(token)
    if not username:
        return {"error": "invalid token"}
    return {"username": username}


def change_password(handler, cm, read_body):
    body = read_body(handler)
    token = body.get("token") or ""
    current_pw = body.get("current_password") or ""
    new_pw = body.get("new_password") or ""

    username = verify_token(token)
    if not username:
        return {"error": "invalid token"}
    if not current_pw or not new_pw:
        return {"error": "current and new password required"}
    if len(new_pw) < 6:
        return {"error": "new password must be at least 6 characters"}

    conn = cm.conn
    row = conn.execute(
        "SELECT id, password_hash FROM users WHERE username = ?", (username,)
    ).fetchone()
    if not row or not _verify_password(current_pw, row[1]):
        return {"error": "current password is incorrect"}

    pw_hash, _ = _hash_password(new_pw)
    conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pw_hash, row[0]))
    conn.commit()
    return {"success": True}
