# 更新日志归档（v0.1.x ~ v0.2.8）

### v0.2.8

**Web 看板**
- 📋 归档问题详情弹窗：点击归档卡片弹出只读详情（含所有结构化字段：排查过程/根因/解决方案/自测结果/修改文件），底部红色删除按钮支持永久删除归档记录

**Steering 规则强化**
- 📝 `track create` 强制要求 `content` 必填（简述问题现象和背景），禁止只传 title 留空
- 📝 排查后 `track update` 强制要求填充 `investigation`（排查过程）和 `root_cause`（根本原因）
- 📝 修复后 `track update` 强制要求填充 `solution`（解决方案）、`files_changed`（修改文件）、`test_result`（自测结果）
- 📝 第 4 节新增「字段填充规范」子节，明确各阶段必填字段
- 📝 第 5 节从「代码修改检查」扩展为「操作前检查」，新增看板启动/PyPI 发布/服务重启等操作前 recall 踩坑记录规则
- 📝 `install.py` STEERING_CONTENT 同步全部变更

**工具优化**
- 🔧 `track` 工具 `content` 字段描述从「排查内容」改为「问题描述（create 时必填，简述问题现象和背景）」

### v0.2.7

**自动关键词提取**
- 🔑 `remember`/`auto_save` 自动从内容提取关键词补充到 tags，无需 AI 手动传递完整标签
- 🔑 采用 jieba 中文分词 + 英文正则提取，中英文混合内容均能准确提取高质量关键词
- 🔑 新增 `jieba>=0.42` 依赖

### v0.2.6

**Steering 规则重构**
- 📝 Steering 规则文档从旧的 3 节结构重写为 9 节结构（新会话启动/处理流程/阻塞规则/问题追踪/代码检查/Spec任务管理/记忆质量/工具速查/开发规范）
- 📝 `install.py` STEERING_CONTENT 模板同步更新，新项目安装即用新规则
- 📝 tags 从固定列表改为动态提取（从内容提取关键词标签），提升记忆检索精度

**Bug 修复**
- 🐛 `readme` 工具 `handle_readme()` 缺少 `**_` 导致 MCP 调用报错 `unexpected keyword argument 'engine'`
- 🐛 Web 看板记忆搜索分页修复（有搜索词时先全量过滤再分页，解决搜索结果不完整问题）

**文档更新**
- 📖 README 工具数量 7→8、架构图 `digest`→`task`、工具描述新增 `task`/`readme`
- 📖 `auto_save` 参数从旧的 `decisions[]/modifications[]/pitfalls[]/todos[]` 更新为 `preferences[]/extra_tags[]`
- 📖 Steering 规则范例从 3 节格式更新为 9 节结构摘要
- 📖 同步更新 6 个语言版本（繁體中文/English/Español/Deutsch/Français/日本語）

### v0.2.5

**任务驱动开发模式**
- 🔗 问题跟踪（track）与任务管理（task）通过 `feature_id` 打通成完整链路：发现问题 → 创建任务 → 执行任务 → 状态自动同步 → 联动归档
- 🔄 `task update` 更新任务状态时自动同步关联问题状态（全部完成→completed，有进行中→in_progress）
- 📦 `track archive` 归档问题时自动归档关联任务（最后一个活跃问题归档时联动）
- 📦 `task` 工具新增 `archive` action，将功能组所有任务移入 `tasks_archive` 归档表
- 📊 问题卡片显示关联任务进度（如 `5/10`），任务页面支持归档筛选

**新增工具**
- 🆕 `task` 工具 — 任务管理（batch_create/update/list/delete/archive），支持树形子任务，通过 feature_id 关联 spec 文档
- 🆕 `readme` 工具 — 从 TOOL_DEFINITIONS/pyproject.toml 自动生成 README 内容，支持多语言和差异对比

**工具增强**
- 🔧 `track` 新增 delete action、9 个结构化字段（description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id）、list 按 issue_id 查单条
- 🔧 `recall` 新增 source 参数过滤（manual/auto_save）和 brief 精简模式（只返回 content+tags，节省上下文）
- 🔧 `auto_save` 写入记忆标记 source="auto_save"，区分手动记忆和自动保存

**知识库拆表重构**
- 🗃️ project_memories + user_memories 独立表，消除 scope/filter_dir 混合查询，查询性能提升
- 📊 DB Schema v4→v6：issues 新增 9 个结构化字段 + tasks/tasks_archive 表 + memories.source 字段

**Web 看板**
- 📊 首页新增阻塞状态卡片（红色阻塞警告/绿色正常运行），点击跳转会话状态页
- 📊 新增任务管理页面（功能组折叠/展开、状态筛选、搜索、CRUD）
- 📊 侧边栏导航顺序优化（会话状态、问题跟踪、任务管理提前至核心位置）
- 📊 记忆列表新增 source 过滤和 exclude_tags 排除过滤

**稳定性与规范**
- 🛡️ Server 主循环全局异常捕获，单条消息错误不再导致 server 退出
- 🛡️ Protocol 层空行跳过和 JSON 解析异常容错
- 🕐 时间戳从 UTC 改为本地时区
- 🧹 清理冗余代码（删除无调用方法、冗余导入、备份文件）
- 📝 Steering 模板新增 Spec 流程与任务管理章节、context transfer 续接规则

### v0.2.4

- 🔇 Stop hook prompt 改为直接指令，消除 Claude Code 重复回复
- 🛡️ Steering 规则 auto_save 规范增加短路防护，会话结束场景跳过其他规则
- 🐛 `_copy_check_track_script` 幂等性修复（返回变更状态避免误报"已同步"）
- 🐛 issue_repo delete 中 `row.get()` 对 `sqlite3.Row` 不兼容修复（改用 `row.keys()` 判断）
- 🐛 Web 看板项目选择页面滚动修复（项目多时无法滚动）
- 🐛 Web 看板 CSS 污染修复（strReplace 全局替换导致 6 处样式异常）
- 🔄 Web 看板所有 confirm() 弹窗替换为自定义 showConfirm 模态框（记忆/问题/标签/项目删除）
- 🔄 Web 看板删除操作增加 API 错误响应处理（toast 提示替代 alert）
- 🧹 `.gitignore` 补充 `.devmemory/` 旧版残留目录忽略规则
- 🧪 pytest 临时项目数据库残留自动清理（conftest.py session fixture）

### v0.2.3

- 🛡️ PreToolUse Hook：Edit/Write 前强制检查 track issue，无活跃问题则拒绝执行（Claude Code / Kiro / OpenCode 三端支持）
- 🔌 OpenCode 插件升级为 `@opencode-ai/plugin` SDK 格式（tool.execute.before hook）
- 🔧 `run install` 自动部署 check_track.sh 检查脚本并动态填充路径
- 🐛 issue_repo archive/delete 中 `row.get()` 对 `sqlite3.Row` 不兼容修复
- 🐛 session_id 从 DB 读取最新值再递增，避免多实例竞态
- 🐛 track date 参数格式校验（YYYY-MM-DD）+ issue_id 类型校验
- 🐛 Web API 请求解析安全加固（Content-Length 校验 + 10MB 上限 + JSON 异常捕获）
- 🐛 Tag 过滤 scope 逻辑修复（`filter_dir is not None` 替代 falsy 判断）
- 🐛 Export 向量数据 struct.unpack 字节长度校验
- 🐛 Schema 版本化迁移（schema_version 表 + v1/v2/v3 增量迁移）
- 🐛 `__init__.py` 版本号同步修复

### v0.2.2

- 🔇 Web 看板 `--quiet` 参数屏蔽请求日志
- 🔄 Web 看板 `--daemon` 参数后台运行（macOS/Linux）
- 🔧 `run install` MCP 配置生成修复（sys.executable + 完整字段）
- 📋 问题跟踪增删改归档（Web 看板添加/编辑/归档/删除 + 记忆关联）
- 👆 全部列表页点击行任意位置弹出编辑弹窗（记忆/问题/标签）
- 🔒 会话延续/上下文转移时阻塞规则强制生效（跨会话必须重新确认）

### v0.2.1

- ➕ Web 看板前端添加项目（目录浏览器 + 手动输入）
- 🏷️ 标签跨项目污染修复（标签操作限定当前项目 + 全局记忆范围）
- 📐 弹窗分页省略号截断 + 弹窗宽度 80%
- 🔌 OpenCode install 自动生成 auto_save 插件（session.idle 事件触发）
- 🔗 Claude Code / Cursor / Windsurf install 自动生成 Hooks 配置（会话结束自动保存）
- 🎯 Web 看板交互体验补全（Toast 操作反馈、空状态引导、导出/导入工具栏）
- 🔧 统计概览卡片点击跳转（点击记忆数/问题数直接弹窗查看）
- 🏷️ 标签管理页区分项目/全局标签来源（📁/🌐 标记）
- 🏷️ 项目卡片标签数合并全局记忆标签

### v0.2.0

- 🔐 Web 看板 Token 认证机制
- ⚡ Embedding 向量缓存，相同内容不重复计算
- 🔍 recall 支持 query + tags 组合查询
- 🗑️ forget 支持批量删除（memory_ids 参数）
- 📤 记忆导出/导入（JSON 格式）
- 🔎 Web 看板语义搜索
- 🗂️ Web 看板项目删除按钮
- 📊 Web 看板性能优化（消除全表扫描）
- 🧠 digest 智能压缩
- 💾 session_id 持久化
- 📏 content 长度限制保护
- 🏷️ version 动态引用（不再硬编码）

### v0.1.x

- 初始版本：7 个 MCP 工具、Web 看板、3D 向量可视化、多语言支持
