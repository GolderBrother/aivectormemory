import re
from datetime import date
from aivectormemory.db.issue_repo import IssueRepo
from aivectormemory.db.task_repo import TaskRepo
from aivectormemory.errors import success_response, NotFoundError
from aivectormemory.i18n.responses import fmt, to_json
from aivectormemory.utils import validate_title, validate_content

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_date(d: str) -> str:
    if not _DATE_RE.match(d):
        raise ValueError(f"Invalid date format: {d}, expected YYYY-MM-DD")
    return d


def _resolve_issue(repo, val) -> dict:
    try:
        num = int(val)
    except (TypeError, ValueError):
        raise ValueError(f"issue_id must be an integer, got: {val}")
    row = repo.get_by_number(num)
    if not row:
        row = repo.get_archived_by_number(num)
    if not row:
        raise NotFoundError("Issue", f"#{num}")
    return row


def handle_track(args, *, cm, engine=None, **_):
    action = args.get("action")
    if not action:
        raise ValueError("action is required")

    repo = IssueRepo(cm.conn, cm.project_dir, engine=engine)
    today = date.today().isoformat()

    if action == "create":
        title = validate_title(args.get("title", ""))
        content = args.get("content", "")
        if content:
            validate_content(content)
        d = _validate_date(args.get("date", today))
        result = repo.create(d, title, content, args.get("memory_id", ""), args.get("parent_id", 0))
        key = "track.create.dedup" if result.get("deduplicated") else "track.create"
        return fmt(key, issue_number=result["issue_number"], date=result["date"])

    elif action == "update":
        row = _resolve_issue(repo, args.get("issue_id"))
        issue_id = row["id"]
        fields = {k: args[k] for k in ("title", "status", "content", "memory_id",
                  "description", "investigation", "root_cause", "solution",
                  "files_changed", "test_result", "notes", "feature_id") if k in args}
        result = repo.update(issue_id, **fields)
        if not result:
            raise ValueError(f"Issue #{row['issue_number']} not found")
        return fmt("track.update", issue_number=result["issue_number"], status=result.get("status", ""))

    elif action == "archive":
        row = _resolve_issue(repo, args.get("issue_id"))
        issue_id = row["id"]
        content = args.get("content")
        if content:
            repo.update(issue_id, content=content)
        result = repo.archive(issue_id)
        if not result:
            raise ValueError(f"Issue #{row['issue_number']} not found")
        feature_id = row.get("feature_id", "")
        if feature_id:
            remaining = repo.count_active_by_feature(feature_id)
            if remaining == 0:
                task_repo = TaskRepo(cm.conn, cm.project_dir)
                task_repo.archive_by_feature(feature_id)
        return fmt("track.archive", archived_at=result.get("archived_at", ""))

    elif action == "delete":
        row = _resolve_issue(repo, args.get("issue_id"))
        issue_id = row["id"]
        result = repo.delete(issue_id)
        if not result:
            raise ValueError(f"Issue #{row['issue_number']} not found")
        return fmt("track.delete")

    elif action == "list":
        issue_id = args.get("issue_id")
        if issue_id is not None:
            row = _resolve_issue(repo, issue_id)
            return to_json(success_response(issues=[row]))

        d = args.get("date")
        if d:
            _validate_date(d)
        status = args.get("status")
        brief = args.get("brief", True)
        limit = args.get("limit", 50)
        issues, total = repo.list_by_date(date=d, status=status, brief=brief, limit=limit)
        return to_json(success_response(issues=issues, total=total))

    else:
        raise ValueError(f"Unknown action: {action}")
