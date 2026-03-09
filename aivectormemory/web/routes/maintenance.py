"""Maintenance routes: health check, DB stats, backup/restore."""
import json
import os
import shutil
from datetime import datetime
from pathlib import Path


def health_check(cm):
    """Health check: count memories vs vec embeddings, find missing/orphans."""
    c = cm.conn
    r = {}
    for key, sql in [
        ("memories_total", "SELECT COUNT(*) FROM memories"),
        ("vec_memories_total", "SELECT COUNT(*) FROM vec_memories"),
        ("memories_missing", "SELECT COUNT(*) FROM memories WHERE id NOT IN (SELECT id FROM vec_memories)"),
        ("user_memories_total", "SELECT COUNT(*) FROM user_memories"),
        ("vec_user_memories_total", "SELECT COUNT(*) FROM vec_user_memories"),
        ("user_memories_missing", "SELECT COUNT(*) FROM user_memories WHERE id NOT IN (SELECT id FROM vec_user_memories)"),
        ("orphan_vec", "SELECT COUNT(*) FROM vec_memories WHERE id NOT IN (SELECT id FROM memories)"),
        ("orphan_user_vec", "SELECT COUNT(*) FROM vec_user_memories WHERE id NOT IN (SELECT id FROM user_memories)"),
    ]:
        try:
            r[key] = c.execute(sql).fetchone()[0]
        except Exception:
            r[key] = 0
    return r


def db_stats(cm):
    """Database statistics: file size, table counts, embedding dim."""
    c = cm.conn
    db_path = str(cm._db_path)
    stats = {"db_path": db_path, "table_counts": {}}

    try:
        info = os.stat(db_path)
        stats["file_size_bytes"] = info.st_size
        stats["file_size_mb"] = round(info.st_size / 1024 / 1024, 2)
    except OSError:
        stats["file_size_bytes"] = 0
        stats["file_size_mb"] = 0

    for table in ["memories", "user_memories", "vec_memories", "vec_user_memories",
                   "issues", "issues_archive", "tasks", "tasks_archive", "session_state"]:
        try:
            stats["table_counts"][table] = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            stats["table_counts"][table] = 0

    try:
        row = c.execute("SELECT embedding FROM vec_memories LIMIT 1").fetchone()
        if row and row[0]:
            stats["embedding_dim"] = len(row[0]) // 4
        else:
            stats["embedding_dim"] = 0
    except Exception:
        stats["embedding_dim"] = 0

    return stats


def repair_missing(cm):
    """Delete orphan vectors and report. Re-embedding requires MCP tools."""
    c = cm.conn
    deleted = 0
    try:
        cur = c.execute("DELETE FROM vec_memories WHERE id NOT IN (SELECT id FROM memories)")
        deleted += cur.rowcount
        cur = c.execute("DELETE FROM vec_user_memories WHERE id NOT IN (SELECT id FROM user_memories)")
        deleted += cur.rowcount
        c.commit()
    except Exception as e:
        return {"error": str(e)}
    return {"success": True, "orphans_deleted": deleted}


def backup_db(cm):
    """Create a timestamped backup of the database file."""
    db_path = Path(str(cm._db_path))
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"avm_backup_{ts}.db"
    backup_path = backup_dir / backup_name
    try:
        shutil.copy2(str(db_path), str(backup_path))
        size = backup_path.stat().st_size
        return {"success": True, "path": str(backup_path), "name": backup_name,
                "size_mb": round(size / 1024 / 1024, 2)}
    except Exception as e:
        return {"error": str(e)}


def list_backups(cm):
    """List existing backups."""
    db_path = Path(str(cm._db_path))
    backup_dir = db_path.parent / "backups"
    if not backup_dir.exists():
        return {"backups": []}
    backups = []
    for f in sorted(backup_dir.glob("avm_backup_*.db"), reverse=True):
        backups.append({
            "name": f.name,
            "path": str(f),
            "size_mb": round(f.stat().st_size / 1024 / 1024, 2),
            "created": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    return {"backups": backups}
