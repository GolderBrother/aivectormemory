# AIVectorMemory 代码优化需求文档

> 版本：v1.1 | 日期：2026-02-27 | 状态：待实施

---

## 1. 背景与目标

### 1.1 背景
AIVectorMemory 经过多个版本迭代，代码库已具备完整功能，但在架构设计、代码质量、性能优化、安全性、可维护性等方面存在优化空间。通过代码审查发现 16 个优化点。

### 1.2 目标
- 提升代码可维护性和可读性
- 减少重复代码，提高复用性
- 优化数据库查询性能
- 增强系统安全性和可扩展性

---

## 2. 功能需求

### 2.1 架构设计

#### F1. Repository 层重构
**问题描述**：
1. `MemoryRepo` 与 `UserMemoryRepo` 存在大量重复代码（insert/update/search_by_vector/search_by_vector_with_tags/list_by_tags/get_tag_counts）
2. `_now()` 方法在 5 个 Repo（MemoryRepo、UserMemoryRepo、IssueRepo、TaskRepo、StateRepo）中重复定义
3. `api.py` 多处直接调用 `repo._now()` 获取时间戳，API 层不应调用 Repo 私有方法

**解决方案**：
1. 抽取 `BaseRepo` 基类，封装 `_now()` 和通用初始化逻辑
2. 抽取 `BaseMemoryRepo` 继承 `BaseRepo`，封装向量搜索等通用 CRUD 逻辑
3. `MemoryRepo` 和 `UserMemoryRepo` 继承 `BaseMemoryRepo`，仅处理表名和字段差异
4. 将 `_now()` 抽取为公共工具函数，供 API 层使用

**验收标准**：
- [ ] 所有现有测试通过
- [ ] 功能行为不变
- [ ] 减少约 100 行重复代码

---

#### F2. Web API 层拆分
**问题描述**：`web/api.py` 单文件达 799 行，路由、业务逻辑、响应格式混在一起

**解决方案**：
1. 拆分为独立路由模块：
   - `routes/memories.py` - 记忆相关 API
   - `routes/issues.py` - 问题追踪 API
   - `routes/tasks.py` - 任务管理 API
   - `routes/tags.py` - 标签管理 API
   - `routes/projects.py` - 项目管理 API
2. 统一响应格式中间件

**验收标准**：
- [ ] 所有 API 端点功能不变
- [ ] 单文件代码量控制在 300 行以内

---

### 2.2 数据库性能

#### F3. 标签查询优化
**问题描述**：`list_by_tags` 使用 `LIKE '%"tag"%'`，无法利用索引，全表扫描

**解决方案**：
1. 建立 `memory_tags` 关联表（多对多关系）
2. 支持高效索引查询
3. 提供数据迁移脚本

**验收标准**：
- [ ] 标签查询响应时间 < 100ms（万级数据）
- [ ] 现有标签功能正常
- [ ] 提供数据迁移脚本

---

#### F4. 向量搜索预过滤
**问题描述**：`search_by_vector` 取 `k * 3` 条再在 Python 过滤 scope/project_dir，浪费计算

**解决方案**：
1. 调研 sqlite-vec 是否支持 WHERE 条件预过滤
2. 或使用虚拟列/CTE 优化
3. 减少无效向量计算

**验收标准**：
- [ ] 向量搜索减少无效计算
- [ ] 搜索结果准确性不变

---

#### F5. N+1 查询优化
**问题描述**：`get_issues` 对每个 issue 单独查询 task 进度（`api.py:294-309`）

**解决方案**：
1. 使用批量 JOIN 或子查询聚合
2. 一次查询获取所有 issue 的 task 进度

**验收标准**：
- [ ] get_issues 接口响应时间显著降低
- [ ] 返回数据结构不变

---

#### F6. 批量插入去重优化
**问题描述**：`TaskRepo.batch_create` 逐条 INSERT，每条插入前需按 title+parent_id 和 sort_order 两次查重，子任务还依赖父任务的 `lastrowid`

**注意**：由于去重逻辑和父子依赖，无法使用 `executemany`。当前已是单次 commit，性能瓶颈在逐条查重。

**解决方案**：
1. 在内存中构建已存在记录的 Set，减少重复 SELECT 查询
2. 一次性加载该 feature_id 下所有已有任务，内存判重代替数据库查重

**验收标准**：
- [ ] batch_create 减少数据库查询次数
- [ ] 功能行为不变（去重逻辑不变）

---

### 2.3 代码质量

#### F7. Schema 迁移拆分
**问题描述**：`schema.py` 总计 390 行，其中 `init_db` 函数约 210 行，9 个版本迁移逻辑堆在一起

**解决方案**：
1. 拆分为独立迁移文件：
   ```
   aivectormemory/migrations/
   ├── __init__.py
   ├── v1_initial.py
   ├── v2_session_state.py
   ├── ...
   └── v9_tasks_archive.py
   ```
2. 每个迁移文件包含 `upgrade(conn)` 函数

**验收标准**：
- [ ] 迁移逻辑按版本独立文件
- [ ] 现有数据库升级无数据丢失
- [ ] 新安装流程正常

---

#### F8. 统一异常处理
**问题描述**：tools 层用 `raise ValueError`（被 `server.py` 捕获转 MCP 错误码），API 层返回 `{"error": "xxx"}`。两者服务不同协议（MCP stdio vs HTTP REST），当前处理方式各自合理，但 Repo 层缺少统一的业务异常定义。

**解决方案**：
1. 定义 Repo 层业务异常类：
   ```
   AIVectorMemoryError
   ├── NotFoundError (资源不存在)
   └── DuplicateError (重复资源)
   ```
2. Tools 层和 API 层各自捕获并转换为对应协议的错误格式
3. 减少 Repo 层返回 None 需要调用方判断的模式

**验收标准**：
- [ ] Repo 层异常语义明确
- [ ] Tools 和 API 层错误处理代码简化

---

#### F9. 类型注解补全
**问题描述**：部分函数缺少类型注解，影响 IDE 支持和代码可读性

**解决方案**：
1. 补全所有公开函数的类型注解
2. 加入 mypy 静态检查到 CI
3. 配置 `py.typed` 支持

**验收标准**：
- [ ] `mypy aivectormemory` 无错误
- [ ] 所有公开 API 有完整类型注解

---

### 2.4 功能优化

#### F10. Embedding 缓存优化
**问题描述**：`lru_cache(maxsize=1024)` 限制可能不足，热点数据可能超限

**解决方案**：
1. 增大缓存至 4096 或动态调整
2. 或持久化热门 embedding 到 SQLite（可选）

**验收标准**：
- [ ] 热点内容重复编码率降低
- [ ] 内存占用可控

---

#### F11. 事务粒度优化
**问题描述**：每次操作立即 `commit()`，高频调用场景性能差

**解决方案**：
1. 支持上下文管理器批量事务：
   ```python
   with cm.transaction():
       repo.insert(...)
       repo.insert(...)
   ```
2. auto_save 等高频场景使用批量事务

**验收标准**：
- [ ] 高频操作性能提升
- [ ] 事务原子性保证

---

#### F12. 输出截断优化
**问题描述**：`server.py:78` 硬编码 30000 字符截断，可能截断 JSON 结构

**解决方案**：
1. 基于结构化输出智能截断
2. 保证 JSON 完整性
3. 添加截断标记

**验收标准**：
- [ ] 截断后输出仍是合法 JSON
- [ ] 截断信息有明确提示

---

### 2.5 安全性

#### F13. 动态 SQL 表名防御性编程
**问题描述**：`api.py` 中 `f"UPDATE {table} SET ..."` 的 `table` 变量来源于内部逻辑判断（非用户输入），当前无实际注入风险，但缺少防御性校验

**解决方案**：
1. 使用表名白名单映射：
   ```python
   TABLE_WHITELIST = {
       "memories": "memories",
       "user_memories": "user_memories",
   }
   table = TABLE_WHITELIST.get(table_name)
   ```
2. 无效表名直接拒绝

**验收标准**：
- [ ] 表名经过白名单校验
- [ ] 无效表名有明确错误提示

---

#### F14. 输入验证增强
**问题描述**：部分参数无长度限制，tags 数组、content 仅截断不报错

**解决方案**：
1. 定义输入验证规则：
   - content: 最大 5000 字符，超出报错
   - tags: 最多 20 个，每个最长 50 字符
   - title: 最大 200 字符
2. 实现统一验证装饰器

**验收标准**：
- [ ] 所有输入有明确限制
- [ ] 超限返回友好错误提示

---

### 2.6 可维护性

#### F15. 日志系统统一
**问题描述**：`print(..., file=sys.stderr)` 散落各处，无法控制日志级别。注：MCP Server 通过 stdio 通信，stderr 日志是 MCP 协议推荐方式，改动需确保不影响 MCP 通信。

**解决方案**：
1. 使用 Python `logging` 模块统一日志，handler 输出到 stderr
2. 支持命令行参数控制日志级别：
   - `--quiet` - 只输出错误
   - `--verbose` - 输出调试信息
   - 默认 - 输出警告和错误
3. 统一日志格式：`[时间] [级别] [模块] 消息`

**验收标准**：
- [ ] 所有 `print(..., file=sys.stderr)` 替换为 logging
- [ ] 支持 `--quiet` / `--verbose` 参数
- [ ] MCP stdio 通信不受影响

---

#### F16. 配置外部化
**问题描述**：`MODEL_NAME`、`DEDUP_THRESHOLD` 等写死在 `config.py`

**解决方案**：
1. 支持环境变量覆盖：
   ```python
   MODEL_NAME = os.getenv("AIVM_MODEL_NAME", "intfloat/multilingual-e5-small")
   DEDUP_THRESHOLD = float(os.getenv("AIVM_DEDUP_THRESHOLD", "0.95"))
   ```
2. 创建配置文档说明可配置项

**验收标准**：
- [ ] 核心配置支持环境变量覆盖
- [ ] 配置文档完整

---

## 3. 非功能需求

### N1. 性能要求
- 标签查询响应时间 < 100ms（万级数据）
- 向量搜索响应时间 < 500ms（万级数据）
- API 响应时间 < 200ms（P95）

### N2. 兼容性要求
- 保持现有 API 接口不变
- 数据库结构变更需提供迁移脚本
- 支持向后兼容一个版本

### N3. 可测试性要求
- 新增代码需有单元测试
- 核心功能测试覆盖率 > 80%

---

## 4. 实施优先级

### P0 - 必须完成（架构基础）
| 需求 | 预计工时 | 风险 |
|-----|---------|-----|
| F1. Repository 层重构 | 4h | 低 |
| F2. Web API 层拆分 | 8h | 中 |

### P1 - 重要（性能）
| 需求 | 预计工时 | 风险 |
|-----|---------|-----|
| F3. 标签查询优化 | 6h | 中 |
| F4. 向量搜索预过滤 | 4h | 中 |
| F5. N+1 查询优化 | 2h | 低 |
| F6. 批量插入去重优化 | 2h | 低 |
| F11. 事务粒度优化 | 3h | 低 |
| F14. 输入验证增强 | 3h | 低 |

### P2 - 可选（代码质量与可维护性）
| 需求 | 预计工时 | 风险 |
|-----|---------|-----|
| F7. Schema 迁移拆分 | 6h | 高 |
| F8. 统一异常处理 | 4h | 低 |
| F9. 类型注解补全 | 4h | 低 |
| F10. Embedding 缓存优化 | 2h | 低 |
| F12. 输出截断优化 | 2h | 低 |
| F13. 动态 SQL 防御性编程 | 2h | 低 |
| F15. 日志系统统一 | 3h | 低 |
| F16. 配置外部化 | 2h | 低 |

---

## 5. 风险与依赖

### 5.1 风险
| 风险项 | 影响 | 缓解措施 |
|-------|-----|---------|
| F3 标签查询优化数据迁移 | 可能需要停机 | 提供增量迁移脚本 |
| F7 Schema 迁移重构 | 涉及核心逻辑 | 充分测试 + 灰度发布 |

### 5.2 依赖
- 无外部依赖变更
- 需 Python 3.10+ 环境

---

## 6. 验收清单

- [ ] 所有功能需求完成
- [ ] 单元测试通过
- [ ] 集成测试通过
- [ ] 性能指标达标
- [ ] 代码审查通过
- [ ] 文档更新完成
