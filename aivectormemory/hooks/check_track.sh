#!/usr/bin/env bash
# AIVectorMemory PreToolUse Hook (Edit|Write)
# 检查1：当前项目是否有活跃的 track issue（没有则阻断文件修改）
# 检查2：如果活跃 issue 关联了 spec 任务且有待执行子任务，必须有 in_progress 的子任务

DB_PATH="$HOME/.aivectormemory/memory.db"

# 数据库不存在则放行（首次使用）
if [ ! -f "$DB_PATH" ]; then
  exit 0
fi

PROJECT_DIR="${PROJECT_DIR:-$(pwd)}"

# === 检查1：是否有活跃的 track issue ===
COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM issues WHERE project_dir='$PROJECT_DIR' AND status IN ('pending','in_progress');" 2>/dev/null)

if [ $? -ne 0 ] || [ -z "$COUNT" ]; then
  exit 0
fi

if [ "$COUNT" -eq 0 ]; then
  echo "⚠️ 当前项目没有活跃的 track issue。请先调用 track(action: create) 记录问题后再修改代码。" >&2
  exit 2
fi

# === 检查2：spec 任务 in_progress 检查 ===
# 仅当活跃 issue 有 feature_id 且该 feature 有 pending 子任务时生效
FEATURE_ID=$(sqlite3 "$DB_PATH" "SELECT feature_id FROM issues WHERE project_dir='$PROJECT_DIR' AND status IN ('pending','in_progress') AND feature_id != '' AND feature_id IS NOT NULL LIMIT 1;" 2>/dev/null)

if [ -n "$FEATURE_ID" ]; then
  PENDING_TASKS=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM tasks WHERE project_dir='$PROJECT_DIR' AND feature_id='$FEATURE_ID' AND status='pending' AND parent_id!=0;" 2>/dev/null)

  if [ "$PENDING_TASKS" -gt 0 ] 2>/dev/null; then
    IN_PROGRESS=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM tasks WHERE project_dir='$PROJECT_DIR' AND feature_id='$FEATURE_ID' AND status='in_progress' AND parent_id!=0;" 2>/dev/null)

    if [ "$IN_PROGRESS" -eq 0 ] 2>/dev/null; then
      echo "⚠️ spec 任务 [$FEATURE_ID] 有待执行的子任务但没有 in_progress 的子任务。请先调用 task(action: update, task_id: X, status: in_progress) 标记当前正在执行的子任务后再修改代码。" >&2
      exit 2
    fi
  fi
fi

exit 0
