import json
import re
from pathlib import Path
from aivectormemory.db.task_repo import TaskRepo
from aivectormemory.db.issue_repo import IssueRepo
from aivectormemory.errors import success_response, NotFoundError
from aivectormemory.utils import validate_title

_SPEC_DIRS = [".kiro/specs", ".cursor/specs", ".windsurf/specs", ".trae/specs", "docs/specs"]


def _sync_tasks_md(project_dir: str, feature_id: str, title: str, completed: bool):
    root = Path(project_dir)
    old = "- [ ]" if completed else "- [x]"
    new = "- [x]" if completed else "- [ ]"
    escaped = re.escape(title)
    pattern = re.compile(rf"^(- \[[ x]\] ){escaped}\s*$", re.MULTILINE)
    for spec_dir in _SPEC_DIRS:
        tasks_md = root / spec_dir / feature_id / "tasks.md"
        if not tasks_md.is_file():
            continue
        text = tasks_md.read_text(encoding="utf-8")
        match = pattern.search(text)
        if match:
            line = match.group(0)
            if old in line:
                tasks_md.write_text(text.replace(line, line.replace(old, new), 1), encoding="utf-8")


def handle_task(args, *, cm, **_):
    action = args.get("action")
    if not action:
        raise ValueError("action is required")

    repo = TaskRepo(cm.conn, cm.project_dir)

    if action == "batch_create":
        feature_id = args.get("feature_id", "").strip()
        if not feature_id:
            raise ValueError("feature_id is required for batch_create")
        tasks = args.get("tasks", [])
        if not tasks:
            raise ValueError("tasks array is required for batch_create")
        for t in tasks:
            if t.get("title", "").strip():
                validate_title(t["title"].strip())
            for child in t.get("children", []):
                if child.get("title", "").strip():
                    validate_title(child["title"].strip())
        result = repo.batch_create(feature_id, tasks, task_type=args.get("task_type", "manual"))
        return json.dumps(success_response(**result))

    elif action == "update":
        task_id = args.get("task_id")
        if not task_id:
            raise ValueError("task_id is required for update")
        fields = {k: args[k] for k in ("status", "title") if k in args}
        result = repo.update(int(task_id), **fields)
        if not result:
            raise NotFoundError("Task", task_id)
        if "status" in fields:
            _sync_tasks_md(cm.project_dir, result["feature_id"], result["title"], fields["status"] == "completed")
        feature_id = result.get("feature_id", "")
        if feature_id and "status" in fields:
            new_status = repo.get_feature_status(feature_id)
            issue_repo = IssueRepo(cm.conn, cm.project_dir)
            for issue in issue_repo.list_by_feature_id(feature_id):
                if issue["status"] != new_status:
                    issue_repo.update(issue["id"], status=new_status)
        return json.dumps(success_response(task=result))

    elif action == "list":
        feature_id = args.get("feature_id")
        if not feature_id:
            raise ValueError("feature_id is required for list")
        status = args.get("status")
        tasks = repo.list_by_feature(feature_id=feature_id, status=status)
        return json.dumps(success_response(tasks=tasks))

    elif action == "archive":
        feature_id = args.get("feature_id", "").strip()
        if not feature_id:
            raise ValueError("feature_id is required for archive")
        result = repo.archive_by_feature(feature_id)
        return json.dumps(success_response(**result))

    elif action == "delete":
        task_id = args.get("task_id")
        if not task_id:
            raise ValueError("task_id is required for delete")
        result = repo.delete(int(task_id))
        if not result:
            raise NotFoundError("Task", task_id)
        return json.dumps(success_response(deleted=result))

    else:
        raise ValueError(f"Unknown action: {action}")
