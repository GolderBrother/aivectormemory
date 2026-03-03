# AIVectorMemory 代码优化设计文档

> 版本：v1.0 | 日期：2026-02-27 | 基于需求：requirements.md v1.1

---

## 1. 设计总览

### 1.1 实施策略
按 P0 → P1 → P2 顺序实施，每个需求独立提交，确保可回滚。P0 为架构基础，后续优化依赖 P0 完成。

### 1.2 影响范围

| 模块 | 涉及需求 | 变更程度 |
|------|---------|---------|
| `db/` | F1, F3-F6, F8, F11 | 重构 |
| `web/` | F2, F5, F13 | 拆分 |
| `tools/` | F8 | 小改 |
| `embedding/` | F10 | 小改 |
| `server.py` | F12, F15 | 小改 |
| `config.py` | F16 | 小改 |
| 新增 `utils.py` | F1 | 新建 |

---

## 2. P0 - 架构基础

### 2.1 F1. Repository 层重构

#### 2.1.1 当前结构
```
db/
├── memory_repo.py      # 172 行，MemoryRepo
├── user_memory_repo.py # 143 行，UserMemoryRepo
├── issue_repo.py       # 236 行，IssueRepo
├── task_repo.py        # 207 行，TaskRepo
├── state_repo.py       # 54 行，StateRepo
└── connection.py       # 46 行
```

重复方法对照：

| 方法 | MemoryRepo | UserMemoryRepo | 差异 |
|------|-----------|---------------|------|
| `_now()` | 有 | 有 | 无差异（5 个 Repo 都有） |
| `insert()` | 有 | 有 | 表名不同，MemoryRepo 多 scope/project_dir 字段 |
| `update()` / `_update()` | 有 | 有 | 表名不同 |
| `find_duplicate()` / `_find_duplicate()` | 有 | 有 | MemoryRepo 多 project_dir 过滤 |
| `search_by_vector()` | 有 | 有 | MemoryRepo 多 scope/project_dir/source 过滤 |
| `search_by_vector_with_tags()` | 有 | 有 | MemoryRepo 多 scope/project_dir/source 参数 |
| `list_by_tags()` | 有 | 有 | MemoryRepo 多 scope/project_dir 条件 |
| `get_tag_counts()` | 有 | 有 | MemoryRepo 多 project_dir 过滤 |
| `delete()` | 有 | 有 | 表名不同 |
| `get_by_id()` | 有 | 有 | 表名不同 |

#### 2.1.2 目标结构
```
db/
├── base.py             # 新建：BaseRepo + BaseMemoryRepo
├── memory_repo.py      # 精简：继承 BaseMemoryRepo
├── user_memory_repo.py # 精简：继承 BaseMemoryRepo
├── issue_repo.py       # 继承 BaseRepo
├── task_repo.py        # 继承 BaseRepo
├── state_repo.py       # 继承 BaseRepo
└── connection.py       # 不变
```

新增 `utils.py`（项目根包下）：
```
aivectormemory/
├── utils.py            # 新建：now() 公共函数
```

#### 2.1.3 BaseRepo 设计

```python
# db/base.py
from datetime import datetime


class BaseRepo:
    """所有 Repo 的基类"""
    def __init__(self, conn, project_dir: str = ""):
        self.conn = conn
        self.project_dir = project_dir

    def _now(self) -> str:
        return datetime.now().astimezone().isoformat()
```

#### 2.1.4 BaseMemoryRepo 设计

```python
# db/base.py (续)
class BaseMemoryRepo(BaseRepo):
    """MemoryRepo 和 UserMemoryRepo 的公共基类"""
    # 子类必须覆盖
    TABLE = ""           # "memories" / "user_memories"
    VEC_TABLE = ""       # "vec_memories" / "vec_user_memories"
    HAS_PROJECT_DIR = False  # MemoryRepo=True, UserMemoryRepo=False

    def insert(self, content, tags, session_id, embedding,
               dedup_threshold=0.95, source="manual", **extra) -> dict:
        dup = self._find_duplicate(embedding, dedup_threshold)
        if dup:
            return self._update_existing(dup["id"], content, tags, session_id, embedding)
        now = self._now()
        mid = uuid.uuid4().hex[:12]
        cols, vals = self._build_insert(mid, content, tags, source, session_id, now, extra)
        self.conn.execute(f"INSERT INTO {self.TABLE} ({cols}) VALUES ({','.join('?' * len(vals))})", vals)
        self.conn.execute(f"INSERT INTO {self.VEC_TABLE} (id, embedding) VALUES (?,?)", (mid, json.dumps(embedding)))
        self.conn.commit()
        return {"id": mid, "action": "created"}

    def _build_insert(self, mid, content, tags, source, session_id, now, extra):
        """子类覆盖以添加额外字段（scope, project_dir）"""
        raise NotImplementedError

    def _update_existing(self, mid, content, tags, session_id, embedding) -> dict:
        now = self._now()
        self.conn.execute(
            f"UPDATE {self.TABLE} SET content=?, tags=?, session_id=?, updated_at=? WHERE id=?",
            (content, json.dumps(tags, ensure_ascii=False), session_id, now, mid))
        self.conn.execute(f"DELETE FROM {self.VEC_TABLE} WHERE id=?", (mid,))
        self.conn.execute(f"INSERT INTO {self.VEC_TABLE} (id, embedding) VALUES (?,?)", (mid, json.dumps(embedding)))
        self.conn.commit()
        return {"id": mid, "action": "updated"}

    def _find_duplicate(self, embedding, threshold) -> dict | None:
        rows = self.conn.execute(
            f"SELECT id, distance FROM {self.VEC_TABLE} WHERE embedding MATCH ? AND k = 5",
            (json.dumps(embedding),)).fetchall()
        for r in rows:
            if self._is_same_scope(r["id"]):
                similarity = 1 - (r["distance"] ** 2) / 2
                if similarity >= threshold:
                    return dict(r)
        return None

    def _is_same_scope(self, mid: str) -> bool:
        """子类覆盖：MemoryRepo 需检查 project_dir，UserMemoryRepo 直接返回 True"""
        return True

    def search_by_vector(self, embedding, top_k=5, **filters) -> list[dict]:
        k = top_k * 3
        rows = self.conn.execute(
            f"SELECT id, distance FROM {self.VEC_TABLE} WHERE embedding MATCH ? AND k = ?",
            (json.dumps(embedding), k)).fetchall()
        results = []
        for r in rows:
            mem = self.conn.execute(f"SELECT * FROM {self.TABLE} WHERE id=?", (r["id"],)).fetchone()
            if not mem:
                continue
            if not self._match_filters(mem, filters):
                continue
            d = dict(mem)
            d["distance"] = r["distance"]
            results.append(d)
            if len(results) >= top_k:
                break
        return results

    def _match_filters(self, mem, filters) -> bool:
        """子类覆盖：MemoryRepo 需过滤 scope/project_dir/source"""
        source = filters.get("source")
        return not source or mem.get("source", "manual") == source

    def search_by_vector_with_tags(self, embedding, tags, top_k=5, **filters) -> list[dict]:
        import numpy as np
        candidates = self.list_by_tags(tags, limit=1000, **filters)
        if not candidates:
            return []
        query_vec = np.array(embedding, dtype=np.float32)
        final = []
        for mem in candidates:
            row = self.conn.execute(f"SELECT embedding FROM {self.VEC_TABLE} WHERE id=?", (mem["id"],)).fetchone()
            if not row:
                continue
            raw = row["embedding"]
            vec = np.frombuffer(raw, dtype=np.float32) if isinstance(raw, (bytes, memoryview)) else np.array(json.loads(raw), dtype=np.float32)
            cos_sim = float(np.dot(query_vec, vec) / (np.linalg.norm(query_vec) * np.linalg.norm(vec) + 1e-9))
            d = dict(mem)
            d["distance"] = 1 - cos_sim
            final.append(d)
        final.sort(key=lambda x: x["distance"])
        return final[:top_k]

    def delete(self, mid: str) -> bool:
        cur = self.conn.execute(f"DELETE FROM {self.TABLE} WHERE id=?", (mid,))
        self.conn.execute(f"DELETE FROM {self.VEC_TABLE} WHERE id=?", (mid,))
        self.conn.commit()
        return cur.rowcount > 0

    def get_by_id(self, mid: str) -> dict | None:
        row = self.conn.execute(f"SELECT * FROM {self.TABLE} WHERE id=?", (mid,)).fetchone()
        return dict(row) if row else None

    def list_by_tags(self, tags, limit=100, **filters) -> list[dict]:
        """子类覆盖以添加 scope/project_dir 条件"""
        raise NotImplementedError

    def get_tag_counts(self, **filters) -> dict[str, int]:
        """子类覆盖以添加 project_dir 过滤"""
        raise NotImplementedError
```

#### 2.1.5 子类精简后示例

```python
# memory_repo.py（精简后约 60 行）
class MemoryRepo(BaseMemoryRepo):
    TABLE = "memories"
    VEC_TABLE = "vec_memories"
    HAS_PROJECT_DIR = True

    def _build_insert(self, mid, content, tags, source, session_id, now, extra):
        scope = extra.get("scope", "project")
        cols = "id, content, tags, scope, source, project_dir, session_id, created_at, updated_at"
        vals = [mid, content, json.dumps(tags, ensure_ascii=False), scope, source, self.project_dir, session_id, now, now]
        return cols, vals

    def _is_same_scope(self, mid):
        mem = self.conn.execute("SELECT project_dir FROM memories WHERE id=?", (mid,)).fetchone()
        return mem and mem["project_dir"] == self.project_dir

    def _match_filters(self, mem, filters):
        scope = filters.get("scope", "all")
        if scope == "project" and mem["project_dir"] != filters.get("project_dir", self.project_dir):
            return False
        source = filters.get("source")
        if source and mem.get("source", "manual") != source:
            return False
        return True

    def list_by_tags(self, tags, scope="all", project_dir="", limit=100, source=None, **_):
        sql, params = "SELECT * FROM memories WHERE 1=1", []
        if scope == "project":
            sql += " AND project_dir=?"; params.append(project_dir or self.project_dir)
        if source:
            sql += " AND source=?"; params.append(source)
        for tag in tags:
            sql += " AND tags LIKE ?"; params.append(f'%"{tag}"%')
        sql += " ORDER BY created_at DESC LIMIT ?"; params.append(limit)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    # get_all, count, get_ids_with_tag, get_tag_counts 保留（MemoryRepo 特有逻辑）
```

```python
# user_memory_repo.py（精简后约 40 行）
class UserMemoryRepo(BaseMemoryRepo):
    TABLE = "user_memories"
    VEC_TABLE = "vec_user_memories"
    HAS_PROJECT_DIR = False

    def __init__(self, conn):
        super().__init__(conn, project_dir="")

    def _build_insert(self, mid, content, tags, source, session_id, now, extra):
        cols = "id, content, tags, source, session_id, created_at, updated_at"
        vals = [mid, content, json.dumps(tags, ensure_ascii=False), source, session_id, now, now]
        return cols, vals

    def list_by_tags(self, tags, limit=100, source=None, **_):
        sql, params = "SELECT * FROM user_memories WHERE 1=1", []
        if source:
            sql += " AND source=?"; params.append(source)
        for tag in tags:
            sql += " AND tags LIKE ?"; params.append(f'%"{tag}"%')
        sql += " ORDER BY created_at DESC LIMIT ?"; params.append(limit)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
```

#### 2.1.6 公共工具函数

```python
# aivectormemory/utils.py
from datetime import datetime

def now_iso() -> str:
    """返回当前时间的 ISO 格式字符串（带时区）"""
    return datetime.now().astimezone().isoformat()
```

`api.py` 中所有 `repo._now()` 调用替换为 `now_iso()`。

#### 2.1.7 迁移策略
- 保持所有公开方法签名不变
- 保持所有现有测试不变（仅需通过）
- 先写基类 → 改 MemoryRepo → 改 UserMemoryRepo → 改其他 3 个 Repo → 改 api.py

---

### 2.2 F2. Web API 层拆分

#### 2.2.1 当前结构
```
web/
├── __init__.py
├── app.py              # HTTP Server
└── api.py              # 799 行，所有路由
```

#### 2.2.2 目标结构
```
web/
├── __init__.py
├── app.py              # HTTP Server（不变）
├── api.py              # 精简：路由分发 + 公共工具（约 80 行）
└── routes/
    ├── __init__.py
    ├── memories.py     # 记忆 CRUD + 导入导出 + 搜索（约 250 行）
    ├── issues.py       # 问题追踪 CRUD（约 100 行）
    ├── tasks.py        # 任务管理 CRUD（约 80 行）
    ├── tags.py         # 标签管理（约 120 行）
    └── projects.py     # 项目管理 + 浏览 + 统计（约 150 行）
```

#### 2.2.3 api.py 精简后职责

```python
# web/api.py（精简后约 80 行）
from web.routes import memories, issues, tasks, tags, projects

def handle_api_request(handler, cm):
    parsed = urlparse(handler.path)
    path = parsed.path
    params = parse_qs(parsed.query)
    pdir = _resolve_project(cm, params)
    method = handler.command

    # 资源路由分发
    if path.startswith("/api/memories"):
        return memories.handle(handler, cm, pdir, path, params, method)
    if path.startswith("/api/issues"):
        return issues.handle(handler, cm, pdir, path, params, method)
    if path.startswith("/api/tasks"):
        return tasks.handle(handler, cm, pdir, path, params, method)
    if path.startswith("/api/tags"):
        return tags.handle(handler, cm, pdir, path, params, method)
    if path.startswith("/api/projects") or path == "/api/browse":
        return projects.handle(handler, cm, pdir, path, params, method)
    if path == "/api/stats":
        return _json_response(handler, projects.get_stats(cm, pdir))
    if path == "/api/status":
        return status_route(handler, cm, pdir, method)
    handler.send_error(404, "API not found")

# 保留公共工具：_read_body, _json_response, _resolve_project
```

#### 2.2.4 路由模块接口规范

每个路由模块导出 `handle(handler, cm, pdir, path, params, method)` 函数，内部处理该资源的所有 HTTP 方法和子路径。

公共工具从 `api.py` 导入：
```python
from aivectormemory.web.api import _read_body, _json_response
```

#### 2.2.5 迁移策略
- 函数平移，不改逻辑
- 逐个模块拆分：memories → issues → tasks → tags → projects
- 每拆一个模块运行全部 API 测试

---

## 3. P1 - 性能优化

### 3.1 F3. 标签查询优化

#### 3.1.1 方案
新增 Schema v10 迁移：创建 `memory_tags` 和 `user_memory_tags` 关联表。

```sql
CREATE TABLE IF NOT EXISTS memory_tags (
    memory_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (memory_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_memory_tags_tag ON memory_tags(tag);

CREATE TABLE IF NOT EXISTS user_memory_tags (
    memory_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (memory_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_user_memory_tags_tag ON user_memory_tags(tag);
```

#### 3.1.2 迁移数据
v10 迁移时从 `memories.tags`（JSON 数组）解析填充关联表。保留原 `tags` 字段作为冗余（读写双写），确保向后兼容。

#### 3.1.3 查询优化
```python
# 优化前
sql += " AND tags LIKE ?"
params.append(f'%"{tag}"%')

# 优化后
sql += " AND id IN (SELECT memory_id FROM memory_tags WHERE tag=?)"
params.append(tag)
```

多标签交集：
```python
for tag in tags:
    sql += " AND id IN (SELECT memory_id FROM memory_tags WHERE tag=?)"
    params.append(tag)
```

#### 3.1.4 写入同步
insert/update/delete 时同步维护关联表。在 `BaseMemoryRepo` 中实现：

```python
def _sync_tags(self, mid: str, tags: list[str]):
    tag_table = "user_memory_tags" if self.TABLE == "user_memories" else "memory_tags"
    self.conn.execute(f"DELETE FROM {tag_table} WHERE memory_id=?", (mid,))
    if tags:
        self.conn.executemany(
            f"INSERT INTO {tag_table} (memory_id, tag) VALUES (?,?)",
            [(mid, t) for t in tags])
```

---

### 3.2 F4. 向量搜索预过滤

#### 3.2.1 调研结论
sqlite-vec 的 `vec0` 虚拟表不支持 WHERE 预过滤，只支持 `embedding MATCH ? AND k = ?` 语法。

#### 3.2.2 替代方案
增大 k 倍数并动态调整：当过滤率高时自动重试更大的 k。

```python
def search_by_vector(self, embedding, top_k=5, **filters):
    multiplier = 3
    while multiplier <= 10:
        k = top_k * multiplier
        rows = self.conn.execute(
            f"SELECT id, distance FROM {self.VEC_TABLE} WHERE embedding MATCH ? AND k = ?",
            (json.dumps(embedding), k)).fetchall()
        results = self._filter_and_collect(rows, top_k, filters)
        if len(results) >= top_k or multiplier >= 10:
            return results
        multiplier += 2  # 重试更大范围
    return results
```

---

### 3.3 F5. N+1 查询优化

#### 3.3.1 当前问题
`api.py:294-309` 对每个 issue 循环调用 `task_repo.list_by_feature(feature_id=fid)`。

#### 3.3.2 方案
一次查询获取所有 feature_id 的任务统计：

```python
def get_task_progress_batch(self, feature_ids: list[str]) -> dict[str, dict]:
    """批量获取任务进度 {feature_id: {total: n, done: n}}"""
    if not feature_ids:
        return {}
    placeholders = ",".join("?" * len(feature_ids))
    rows = self.conn.execute(
        f"SELECT feature_id, status, parent_id FROM tasks WHERE project_dir=? AND feature_id IN ({placeholders})",
        [self.project_dir] + feature_ids
    ).fetchall()
    # 按 feature_id 分组计算
    ...
```

api.py 调用改为：
```python
fids = list({i.get("feature_id", "") for i in issues if i.get("feature_id")})
progress_map = task_repo.get_task_progress_batch(fids) if fids else {}
for issue in issues:
    fid = issue.get("feature_id", "")
    if fid and fid in progress_map:
        issue["task_progress"] = progress_map[fid]
```

---

### 3.4 F6. 批量插入去重优化

#### 3.4.1 方案
batch_create 开头一次性加载该 feature_id 下所有已有任务到内存：

```python
def batch_create(self, feature_id, tasks, task_type="manual"):
    # 一次查询加载所有已有记录
    existing_rows = self.conn.execute(
        "SELECT id, title, parent_id, sort_order FROM tasks WHERE project_dir=? AND feature_id=?",
        (self.project_dir, feature_id)
    ).fetchall()
    # 构建查重集合
    title_key_set = {(r["title"], r["parent_id"]) for r in existing_rows}
    sort_key_set = {r["sort_order"] for r in existing_rows if r["parent_id"] == 0}
    # 后续用 set 判重代替数据库查询
```

---

### 3.5 F11. 事务粒度优化

#### 3.5.1 方案
在 `ConnectionManager` 中添加事务上下文管理器：

```python
# connection.py
from contextlib import contextmanager

@contextmanager
def transaction(self):
    """批量事务：块内不自动 commit，退出时统一 commit 或 rollback"""
    self._in_transaction = True
    try:
        yield
        self.conn.commit()
    except Exception:
        self.conn.rollback()
        raise
    finally:
        self._in_transaction = False
```

Repo 层的 `commit()` 调用改为检查事务状态：
```python
def _commit(self):
    if not getattr(self.conn, '_in_transaction', False):
        self.conn.commit()
```

> 注：此优化影响所有 Repo，需在 F1 的 BaseRepo 中实现。

---

### 3.6 F14. 输入验证增强

#### 3.6.1 方案
在 `utils.py` 中定义验证函数：

```python
MAX_CONTENT_LENGTH = 50000
MAX_TAGS_COUNT = 20
MAX_TAG_LENGTH = 50
MAX_TITLE_LENGTH = 200

def validate_content(content: str) -> str:
    if len(content) > MAX_CONTENT_LENGTH:
        raise ValueError(f"content exceeds {MAX_CONTENT_LENGTH} chars")
    return content

def validate_tags(tags: list) -> list[str]:
    if len(tags) > MAX_TAGS_COUNT:
        raise ValueError(f"tags exceeds {MAX_TAGS_COUNT} items")
    for t in tags:
        if len(t) > MAX_TAG_LENGTH:
            raise ValueError(f"tag '{t[:20]}...' exceeds {MAX_TAG_LENGTH} chars")
    return tags
```

在 `handle_remember`、`handle_track` 等入口调用验证。

---

## 4. P2 - 代码质量

### 4.1 F7. Schema 迁移拆分

#### 4.1.1 方案
```
db/
├── schema.py           # 保留表定义 + init_db 框架
└── migrations/
    ├── __init__.py      # MIGRATIONS 注册表
    ├── v01.py           # v1 迁移
    ├── v02.py           # v2 迁移
    └── ...
```

```python
# migrations/__init__.py
from .v01 import upgrade as v01
from .v02 import upgrade as v02
# ...
MIGRATIONS = {1: v01, 2: v02, ...}
```

```python
# schema.py init_db 改为：
from .migrations import MIGRATIONS
for ver_num in range(current_ver + 1, CURRENT_SCHEMA_VERSION + 1):
    fn = MIGRATIONS.get(ver_num)
    if fn:
        fn(conn, engine=engine)
```

---

### 4.2 F8. 统一异常处理

#### 4.2.1 方案
```python
# errors.py（扩展现有文件）
class AIVectorMemoryError(Exception):
    pass

class NotFoundError(AIVectorMemoryError):
    def __init__(self, resource: str, identifier):
        super().__init__(f"{resource} {identifier} not found")

class DuplicateError(AIVectorMemoryError):
    def __init__(self, resource: str, identifier):
        super().__init__(f"{resource} {identifier} already exists")
```

Repo 层可选使用（渐进式，不强制一次全改）。

---

### 4.3 F9. 类型注解补全

逐模块补全：db/ → tools/ → web/ → server.py。添加 `py.typed` 标记文件。pyproject.toml 中添加 mypy 配置。

---

### 4.4 F10. Embedding 缓存优化

将 `maxsize=1024` 改为 `maxsize=4096`。MCP Server 进程生命周期较短，4096 条缓存内存占用约 6MB（384 维 * float32 * 4096），可接受。

---

### 4.5 F12. 输出截断优化

```python
# server.py
def _smart_truncate(text: str, max_len: int = 30000) -> str:
    if len(text) <= max_len:
        return text
    # 尝试按 JSON 结构截断
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            for key in ("memories", "issues", "tasks", "results"):
                if key in data and isinstance(data[key], list) and len(data[key]) > 1:
                    while len(json.dumps(data, ensure_ascii=False)) > max_len and len(data[key]) > 1:
                        data[key].pop()
                    data["_truncated"] = True
                    return json.dumps(data, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        pass
    return text[:max_len] + "\n[truncated]"
```

---

### 4.6 F13. 动态 SQL 防御性编程

```python
# api.py 中添加
_TABLE_WHITELIST = {"memories", "user_memories"}

def _safe_table(name: str) -> str:
    if name not in _TABLE_WHITELIST:
        raise ValueError(f"Invalid table: {name}")
    return name
```

所有 `f"UPDATE {table}"` 替换为 `f"UPDATE {_safe_table(table)}"`。

---

### 4.7 F15. 日志系统统一

```python
# aivectormemory/log.py（新建）
import logging
import sys

def setup_logger(level: str = "WARNING") -> logging.Logger:
    logger = logging.getLogger("aivectormemory")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.WARNING))
    return logger

log = setup_logger()
```

`__main__.py` 中根据 `--quiet` / `--verbose` 设置级别。所有 `print(..., file=sys.stderr)` 替换为 `log.info(...)` / `log.error(...)`。

---

### 4.8 F16. 配置外部化

```python
# config.py 修改
import os

DB_DIR = Path(os.getenv("AIVM_DB_DIR", str(Path.home() / ".aivectormemory")))
DB_NAME = os.getenv("AIVM_DB_NAME", "memory.db")
MODEL_NAME = os.getenv("AIVM_MODEL_NAME", "intfloat/multilingual-e5-small")
MODEL_DIMENSION = int(os.getenv("AIVM_MODEL_DIMENSION", "384"))
DEDUP_THRESHOLD = float(os.getenv("AIVM_DEDUP_THRESHOLD", "0.95"))
DEFAULT_TOP_K = int(os.getenv("AIVM_DEFAULT_TOP_K", "5"))
WEB_DEFAULT_PORT = int(os.getenv("AIVM_WEB_PORT", "9080"))
```

---

## 5. 实施顺序与依赖

```
F1 (Repo 重构) ──┬──→ F3 (标签优化，依赖 BaseMemoryRepo)
                 ├──→ F4 (向量搜索，依赖 BaseMemoryRepo)
                 ├──→ F6 (批量插入，独立)
                 ├──→ F11 (事务，依赖 BaseRepo)
                 └──→ F8 (异常，依赖 BaseRepo)

F2 (API 拆分) ───┬──→ F5 (N+1，改 issues 路由)
                 └──→ F13 (SQL 安全，改路由模块)

独立：F7, F9, F10, F12, F14, F15, F16
```

---

## 6. 测试策略

| 阶段 | 测试方式 |
|------|---------|
| F1 完成后 | `python -m pytest tests/test_db.py tests/test_tools_integration.py` |
| F2 完成后 | `python -m pytest tests/test_api_v4.py` |
| 每个 P1/P2 完成后 | 运行全量测试 `python -m pytest` |
| 最终验收 | 全量测试 + Web 看板手动验证 |
