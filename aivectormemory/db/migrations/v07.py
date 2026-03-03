"""v7: 用户记忆拆分、auto_save 清理、踩坑迁移、archive embedding"""
import json
from datetime import datetime
from aivectormemory.config import USER_SCOPE_DIR
from aivectormemory.db.schema import USER_MEMORIES_TABLE, VEC_USER_MEMORIES_TABLE, VEC_ISSUES_ARCHIVE_TABLE
from aivectormemory.log import log


def upgrade(conn, engine=None, **_):
    conn.execute(USER_MEMORIES_TABLE)
    conn.execute(VEC_USER_MEMORIES_TABLE)
    conn.execute(VEC_ISSUES_ARCHIVE_TABLE)

    # scope=user 记录从 memories 迁移到 user_memories
    user_rows = conn.execute(
        "SELECT * FROM memories WHERE project_dir=?", (USER_SCOPE_DIR,)
    ).fetchall()
    for r in user_rows:
        conn.execute(
            "INSERT INTO user_memories (id, content, tags, source, session_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
            (r["id"], r["content"], r["tags"], r["source"] or "manual", r["session_id"], r["created_at"], r["updated_at"])
        )
        vec_row = conn.execute("SELECT embedding FROM vec_memories WHERE id=?", (r["id"],)).fetchone()
        if vec_row:
            conn.execute("INSERT INTO vec_user_memories (id, embedding) VALUES (?,?)", (r["id"], vec_row["embedding"]))
            conn.execute("DELETE FROM vec_memories WHERE id=?", (r["id"],))
        conn.execute("DELETE FROM memories WHERE id=?", (r["id"],))

    # 删除 auto_save 碎片
    conn.execute(
        "DELETE FROM vec_memories WHERE id IN ("
        "  SELECT id FROM memories WHERE source='auto_save' AND tags NOT LIKE '%\"preference\"%'"
        ")"
    )
    conn.execute(
        "DELETE FROM memories WHERE source='auto_save' AND tags NOT LIKE '%\"preference\"%'"
    )

    # 踩坑记录迁移到 issues_archive
    pitfall_rows = conn.execute(
        "SELECT * FROM memories WHERE tags LIKE '%\"踩坑\"%'"
    ).fetchall()
    now_ts = datetime.now().astimezone().isoformat()
    for r in pitfall_rows:
        content = r["content"]
        first_line = content.split("\n")[0].lstrip("# ").strip()[:100]
        created_date = r["created_at"][:10]
        tags_raw = r["tags"]
        tags_list = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
        has_project_knowledge = "项目知识" in tags_list

        conn.execute(
            """INSERT INTO issues_archive (project_dir, issue_number, date, title, content, description,
               root_cause, solution, status, archived_at, created_at)
               VALUES (?,0,?,?,?,?,?,?,?,?,?)""",
            (r["project_dir"], created_date, first_line, "", content, "", "", "completed", now_ts, r["created_at"])
        )

        if has_project_knowledge:
            new_tags = [t for t in tags_list if t != "踩坑"]
            conn.execute("UPDATE memories SET tags=? WHERE id=?",
                         (json.dumps(new_tags, ensure_ascii=False), r["id"]))
        else:
            conn.execute("DELETE FROM vec_memories WHERE id=?", (r["id"],))
            conn.execute("DELETE FROM memories WHERE id=?", (r["id"],))

    # 删除 ISSUE_STEPS 产生的系统任务
    conn.execute(
        "DELETE FROM tasks WHERE task_type='system' AND feature_id LIKE 'issue/%'"
    )

    # issues_archive 批量生成 embedding
    archive_count = conn.execute("SELECT COUNT(*) FROM issues_archive").fetchone()[0]
    if engine and archive_count <= 50:
        archives = conn.execute(
            "SELECT id, title, description, root_cause, solution FROM issues_archive"
        ).fetchall()
        gen_count = 0
        for a in archives:
            existing = conn.execute(
                "SELECT id FROM vec_issues_archive WHERE id=?", (a["id"],)
            ).fetchone()
            if existing:
                continue
            text = f"{a['title']} {a['description'] or ''} {a['root_cause'] or ''} {a['solution'] or ''}"
            emb = engine.encode(text)
            conn.execute(
                "INSERT INTO vec_issues_archive (id, embedding) VALUES (?,?)",
                (a["id"], json.dumps(emb))
            )
            gen_count += 1
        log.info("v7 migration: generated embeddings for %d archived issues", gen_count)
    elif archive_count > 50:
        log.info("v7 migration: skipped embedding generation for %d archived issues (>50, lazy loading)", archive_count)
