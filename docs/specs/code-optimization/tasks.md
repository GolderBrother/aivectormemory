# AIVectorMemory 代码优化任务文档

> 基于：requirements.md v1.1 + design.md v1.0

---

## P0 - 架构基础

### F1. Repository 层重构

- [x] 1.1 创建 `aivectormemory/utils.py`，实现 `now_iso()` 公共函数
- [x] 1.2 创建 `aivectormemory/db/base.py`，实现 `BaseRepo` 基类（`__init__`, `_now`）
- [x] 1.3 在 `base.py` 中实现 `BaseMemoryRepo`（继承 `BaseRepo`）
  - [x] 1.3.1 类属性：`TABLE`, `VEC_TABLE`, `HAS_PROJECT_DIR`
  - [x] 1.3.2 通用方法：`insert`, `_build_insert`(抽象), `_update_existing`, `_find_duplicate`, `_is_same_scope`
  - [x] 1.3.3 搜索方法：`search_by_vector`, `_match_filters`, `search_by_vector_with_tags`
  - [x] 1.3.4 CRUD 方法：`delete`, `get_by_id`, `list_by_tags`(抽象), `get_tag_counts`(抽象)
- [x] 1.4 重构 `MemoryRepo` 继承 `BaseMemoryRepo`，覆盖差异方法
- [x] 1.5 运行测试验证 `MemoryRepo`：`python -m pytest tests/test_db.py`
- [x] 1.6 重构 `UserMemoryRepo` 继承 `BaseMemoryRepo`，覆盖差异方法
- [x] 1.7 运行测试验证 `UserMemoryRepo`：`python -m pytest tests/test_db.py`
- [x] 1.8 重构 `IssueRepo` 继承 `BaseRepo`，删除 `_now()` 重复
- [x] 1.9 重构 `TaskRepo` 继承 `BaseRepo`，删除 `_now()` 重复
- [x] 1.10 重构 `StateRepo` 继承 `BaseRepo`，删除 `_now()` 重复
- [x] 1.11 替换 `api.py` 中所有 `repo._now()` 调用为 `now_iso()`
- [x] 1.12 运行全量测试：`python -m pytest`

### F2. Web API 层拆分

- [x] 2.1 创建 `web/routes/` 目录和 `__init__.py`
- [x] 2.2 在 `api.py` 中提取公共工具函数（`_read_body`, `_json_response`, `_resolve_project`）为公开接口
- [x] 2.3 拆分 `routes/memories.py`（记忆 CRUD + 导入导出 + 搜索）
- [x] 2.4 运行 API 测试验证 memories 路由
- [x] 2.5 拆分 `routes/issues.py`（问题追踪 CRUD）
- [x] 2.6 运行 API 测试验证 issues 路由
- [x] 2.7 拆分 `routes/tasks.py`（任务管理 CRUD）
- [x] 2.8 运行 API 测试验证 tasks 路由
- [x] 2.9 拆分 `routes/tags.py`（标签管理）
- [x] 2.10 运行 API 测试验证 tags 路由
- [x] 2.11 拆分 `routes/projects.py`（项目管理 + 浏览 + 统计）
- [x] 2.12 运行 API 测试验证 projects 路由
- [x] 2.13 精简 `api.py` 为路由分发（~80 行）
- [x] 2.14 运行全量测试：`python -m pytest`

---

## P1 - 性能优化

### F3. 标签查询优化（依赖 F1）

- [x] 3.1 在 `schema.py` 中添加 `memory_tags` 和 `user_memory_tags` 表定义和索引
- [x] 3.2 编写 v10 迁移：从 `memories.tags` JSON 解析填充 `memory_tags`
- [x] 3.3 编写 v10 迁移：从 `user_memories.tags` JSON 解析填充 `user_memory_tags`
- [x] 3.4 在 `BaseMemoryRepo` 中实现 `_sync_tags()` 方法
- [x] 3.5 在 `insert` / `_update_existing` 中调用 `_sync_tags()` 双写
- [x] 3.6 在 `delete` 中同步删除关联表记录
- [x] 3.7 修改 `list_by_tags` 查询：`LIKE` → `IN (SELECT memory_id FROM memory_tags WHERE tag=?)`
- [x] 3.8 运行全量测试：`python -m pytest`

### F4. 向量搜索预过滤（依赖 F1）

- [x] 4.1 在 `BaseMemoryRepo.search_by_vector` 中实现动态 k 倍数重试逻辑
- [x] 4.2 抽取 `_filter_and_collect()` 方法
- [x] 4.3 运行测试验证：`python -m pytest tests/test_db.py`

### F5. N+1 查询优化（依赖 F2）

- [x] 5.1 在 `TaskRepo` 中新增 `get_task_progress_batch()` 方法
- [x] 5.2 修改 `routes/issues.py`（或 `api.py`）中 get_issues 调用为批量查询
- [x] 5.3 运行 API 测试验证

### F6. 批量插入去重优化

- [x] 6.1 重构 `TaskRepo.batch_create`：开头一次性加载已有任务到内存
- [x] 6.2 构建 `title_key_set` 和 `sort_key_set` 用于内存判重
- [x] 6.3 替换逐条 SELECT 查重为 set 查重
- [x] 6.4 运行测试验证：`python -m pytest tests/test_task_repo.py`

### F11. 事务粒度优化（依赖 F1）

- [x] 11.1 在 `ConnectionManager` 中添加 `transaction()` 上下文管理器
- [x] 11.2 在 `BaseRepo` 中添加 `_commit()` 方法（检查事务状态）
- [x] 11.3 替换所有 Repo 中的 `self.conn.commit()` 为 `self._commit()`
- [x] 11.4 在 `handle_auto_save` 等高频场景使用 `with cm.transaction()`
- [x] 11.5 运行全量测试：`python -m pytest`

### F14. 输入验证增强

- [x] 14.1 在 `utils.py` 中定义验证函数（`validate_content`, `validate_tags`, `validate_title`）
- [x] 14.2 在 `handle_remember` 入口调用验证
- [x] 14.3 在 `handle_track` 入口调用验证
- [x] 14.4 在 `handle_task` 入口调用验证
- [x] 14.5 运行全量测试：`python -m pytest`

---

## P2 - 代码质量与可维护性

### F7. Schema 迁移拆分

- [x] 7.1 创建 `db/migrations/` 目录和 `__init__.py`
- [x] 7.2 拆分 v1-v3 迁移到独立文件
- [x] 7.3 拆分 v4-v6 迁移到独立文件
- [x] 7.4 拆分 v7-v9 迁移到独立文件
- [x] 7.5 重构 `init_db` 为循环调用 `MIGRATIONS` 注册表
- [x] 7.6 保留表定义在 `schema.py` 中
- [x] 7.7 运行全量测试 + 验证新安装流程

### F8. 统一异常处理

- [x] 8.1 在 `errors.py` 中定义 `AIVectorMemoryError`, `NotFoundError`, `DuplicateError`
- [x] 8.2 在 Repo 层渐进式使用业务异常（优先改 IssueRepo）
- [x] 8.3 在 Tools 层捕获新异常并转换
- [x] 8.4 运行全量测试

### F9. 类型注解补全

- [x] 9.1 补全 `db/` 模块类型注解
- [x] 9.2 补全 `tools/` 模块类型注解
- [x] 9.3 补全 `web/` 模块类型注解
- [x] 9.4 添加 `py.typed` 标记文件
- [x] 9.5 配置 mypy 并验证通过

### F10. Embedding 缓存优化

- [x] 10.1 将 `lru_cache(maxsize=1024)` 改为 `maxsize=4096`
- [x] 10.2 运行测试验证

### F12. 输出截断优化

- [x] 12.1 在 `server.py` 中实现 `_smart_truncate()` 函数
- [x] 12.2 替换硬编码截断逻辑
- [x] 12.3 运行测试验证 JSON 完整性

### F13. 动态 SQL 防御性编程（依赖 F2）

- [x] 13.1 在路由模块中添加 `_TABLE_WHITELIST` 和 `_safe_table()`
- [x] 13.2 替换所有 `f"UPDATE {table}"` 为安全调用
- [x] 13.3 运行 API 测试验证

### F15. 日志系统统一

- [x] 15.1 创建 `aivectormemory/log.py`，实现 `setup_logger()`
- [x] 15.2 在 `__main__.py` 中添加 `--quiet` / `--verbose` 参数解析
- [x] 15.3 替换所有 `print(..., file=sys.stderr)` 为 logging 调用
- [x] 15.4 验证 MCP stdio 通信不受影响

### F16. 配置外部化

- [x] 16.1 修改 `config.py`：所有常量支持环境变量覆盖
- [x] 16.2 运行测试验证默认值不变
