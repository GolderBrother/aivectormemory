from aivectormemory.db.task_repo import TaskRepo


def get_tasks(cm, params, pdir):
    repo = TaskRepo(cm.conn, pdir)
    feature_id = params.get("feature_id", [None])[0]
    status = params.get("status", [None])[0]
    tasks = repo.list_by_feature(feature_id=feature_id, status=status)
    return {"tasks": tasks}


def post_tasks(handler, cm, pdir):
    from aivectormemory.web.api import _read_body
    body = _read_body(handler)
    repo = TaskRepo(cm.conn, pdir)
    feature_id = body.get("feature_id", "").strip()
    if not feature_id:
        return {"error": "feature_id is required"}
    tasks = body.get("tasks", [])
    if not tasks:
        return {"error": "tasks array is required"}
    result = repo.batch_create(feature_id, tasks, task_type=body.get("task_type", "manual"))
    return result


def put_task(handler, cm, tid, pdir):
    from aivectormemory.web.api import _read_body
    body = _read_body(handler)
    repo = TaskRepo(cm.conn, pdir)
    fields = {k: body[k] for k in ("status", "title") if k in body}
    result = repo.update(tid, **fields)
    if not result:
        return {"error": "not found"}
    return {"task": result}


def delete_task(cm, tid, pdir):
    repo = TaskRepo(cm.conn, pdir)
    result = repo.delete(tid)
    return result if result else {"error": "not found"}


def delete_tasks_by_feature(handler, cm, pdir, params):
    feature_id = params.get("feature_id", [None])[0]
    if not feature_id:
        return {"error": "feature_id is required"}
    repo = TaskRepo(cm.conn, pdir)
    count = repo.delete_by_feature(feature_id)
    return {"deleted": count, "feature_id": feature_id}


def get_archived_tasks(cm, params, pdir):
    repo = TaskRepo(cm.conn, pdir)
    feature_id = params.get("feature_id", [None])[0]
    tasks = repo.list_archived(feature_id=feature_id)
    return {"tasks": tasks}
