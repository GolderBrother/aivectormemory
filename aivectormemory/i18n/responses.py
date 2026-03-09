"""工具返回值多语言格式化"""
import json
from aivectormemory.settings import get_language

STATUS_MAP = {
    "zh-CN": {"pending": "待处理", "in_progress": "进行中", "completed": "已完成"},
    "zh-TW": {"pending": "待處理", "in_progress": "進行中", "completed": "已完成"},
    "en": {"pending": "Pending", "in_progress": "In Progress", "completed": "Completed"},
    "ja": {"pending": "保留中", "in_progress": "進行中", "completed": "完了"},
    "es": {"pending": "Pendiente", "in_progress": "En progreso", "completed": "Completado"},
    "de": {"pending": "Ausstehend", "in_progress": "In Bearbeitung", "completed": "Abgeschlossen"},
    "fr": {"pending": "En attente", "in_progress": "En cours", "completed": "Terminé"},
}

TEMPLATES = {
    "zh-CN": {
        "remember.created": "已创建记忆 {id}，标签：{tags}",
        "remember.updated": "已更新记忆 {id}，标签：{tags}",
        "forget": "已删除 {deleted_count} 条记忆",
        "forget.not_found": "（{not_found_count} 条未找到）",
        "track.create": "已创建问题 #{issue_number}（{date}）",
        "track.create.dedup": "问题已存在 #{issue_number}（已去重）",
        "track.update": "已更新问题 #{issue_number}（状态：{status}）",
        "track.archive": "已归档问题（archived_at: {archived_at}）",
        "track.delete": "已删除问题",
        "task.batch_create": "已创建 {created} 个任务（{feature_id}），跳过 {skipped} 个",
        "task.update": "已更新任务「{title}」（状态：{status}）",
        "task.archive": "已归档任务组 {feature_id}（共 {archived} 个）",
        "task.delete": "已删除任务",
        "auto_save": "已保存 {count} 条偏好",
        "auto_save.empty": "无偏好需要保存",
    },
    "zh-TW": {
        "remember.created": "已建立記憶 {id}，標籤：{tags}",
        "remember.updated": "已更新記憶 {id}，標籤：{tags}",
        "forget": "已刪除 {deleted_count} 條記憶",
        "forget.not_found": "（{not_found_count} 條未找到）",
        "track.create": "已建立問題 #{issue_number}（{date}）",
        "track.create.dedup": "問題已存在 #{issue_number}（已去重）",
        "track.update": "已更新問題 #{issue_number}（狀態：{status}）",
        "track.archive": "已歸檔問題（archived_at: {archived_at}）",
        "track.delete": "已刪除問題",
        "task.batch_create": "已建立 {created} 個任務（{feature_id}），跳過 {skipped} 個",
        "task.update": "已更新任務「{title}」（狀態：{status}）",
        "task.archive": "已歸檔任務組 {feature_id}（共 {archived} 個）",
        "task.delete": "已刪除任務",
        "auto_save": "已儲存 {count} 條偏好",
        "auto_save.empty": "無偏好需要儲存",
    },
    "en": {
        "remember.created": "Memory created {id}, tags: {tags}",
        "remember.updated": "Memory updated {id}, tags: {tags}",
        "forget": "Deleted {deleted_count} memories",
        "forget.not_found": " ({not_found_count} not found)",
        "track.create": "Issue #{issue_number} created ({date})",
        "track.create.dedup": "Issue #{issue_number} already exists (deduplicated)",
        "track.update": "Issue #{issue_number} updated (status: {status})",
        "track.archive": "Issue archived (archived_at: {archived_at})",
        "track.delete": "Issue deleted",
        "task.batch_create": "Created {created} tasks ({feature_id}), skipped {skipped}",
        "task.update": "Task \"{title}\" updated (status: {status})",
        "task.archive": "Task group {feature_id} archived ({archived} total)",
        "task.delete": "Task deleted",
        "auto_save": "Saved {count} preferences",
        "auto_save.empty": "No preferences to save",
    },
    "ja": {
        "remember.created": "メモリ作成 {id}、タグ：{tags}",
        "remember.updated": "メモリ更新 {id}、タグ：{tags}",
        "forget": "{deleted_count} 件のメモリを削除",
        "forget.not_found": "（{not_found_count} 件見つからず）",
        "track.create": "問題 #{issue_number} を作成（{date}）",
        "track.create.dedup": "問題 #{issue_number} は既に存在（重複排除済み）",
        "track.update": "問題 #{issue_number} を更新（状態：{status}）",
        "track.archive": "問題をアーカイブ（archived_at: {archived_at}）",
        "track.delete": "問題を削除",
        "task.batch_create": "{created} 件のタスクを作成（{feature_id}）、{skipped} 件スキップ",
        "task.update": "タスク「{title}」を更新（状態：{status}）",
        "task.archive": "タスクグループ {feature_id} をアーカイブ（{archived} 件）",
        "task.delete": "タスクを削除",
        "auto_save": "{count} 件の設定を保存",
        "auto_save.empty": "保存する設定なし",
    },
    "es": {
        "remember.created": "Memoria creada {id}, etiquetas: {tags}",
        "remember.updated": "Memoria actualizada {id}, etiquetas: {tags}",
        "forget": "{deleted_count} memorias eliminadas",
        "forget.not_found": " ({not_found_count} no encontradas)",
        "track.create": "Problema #{issue_number} creado ({date})",
        "track.create.dedup": "Problema #{issue_number} ya existe (deduplicado)",
        "track.update": "Problema #{issue_number} actualizado (estado: {status})",
        "track.archive": "Problema archivado (archived_at: {archived_at})",
        "track.delete": "Problema eliminado",
        "task.batch_create": "{created} tareas creadas ({feature_id}), {skipped} omitidas",
        "task.update": "Tarea \"{title}\" actualizada (estado: {status})",
        "task.archive": "Grupo de tareas {feature_id} archivado ({archived} en total)",
        "task.delete": "Tarea eliminada",
        "auto_save": "{count} preferencias guardadas",
        "auto_save.empty": "Sin preferencias para guardar",
    },
    "de": {
        "remember.created": "Erinnerung erstellt {id}, Tags: {tags}",
        "remember.updated": "Erinnerung aktualisiert {id}, Tags: {tags}",
        "forget": "{deleted_count} Erinnerungen gelöscht",
        "forget.not_found": " ({not_found_count} nicht gefunden)",
        "track.create": "Problem #{issue_number} erstellt ({date})",
        "track.create.dedup": "Problem #{issue_number} existiert bereits (dedupliziert)",
        "track.update": "Problem #{issue_number} aktualisiert (Status: {status})",
        "track.archive": "Problem archiviert (archived_at: {archived_at})",
        "track.delete": "Problem gelöscht",
        "task.batch_create": "{created} Aufgaben erstellt ({feature_id}), {skipped} übersprungen",
        "task.update": "Aufgabe \"{title}\" aktualisiert (Status: {status})",
        "task.archive": "Aufgabengruppe {feature_id} archiviert ({archived} insgesamt)",
        "task.delete": "Aufgabe gelöscht",
        "auto_save": "{count} Einstellungen gespeichert",
        "auto_save.empty": "Keine Einstellungen zu speichern",
    },
    "fr": {
        "remember.created": "Mémoire créée {id}, tags : {tags}",
        "remember.updated": "Mémoire mise à jour {id}, tags : {tags}",
        "forget": "{deleted_count} mémoires supprimées",
        "forget.not_found": " ({not_found_count} non trouvées)",
        "track.create": "Problème #{issue_number} créé ({date})",
        "track.create.dedup": "Problème #{issue_number} existe déjà (dédupliqué)",
        "track.update": "Problème #{issue_number} mis à jour (statut : {status})",
        "track.archive": "Problème archivé (archived_at: {archived_at})",
        "track.delete": "Problème supprimé",
        "task.batch_create": "{created} tâches créées ({feature_id}), {skipped} ignorées",
        "task.update": "Tâche \"{title}\" mise à jour (statut : {status})",
        "task.archive": "Groupe de tâches {feature_id} archivé ({archived} au total)",
        "task.delete": "Tâche supprimée",
        "auto_save": "{count} préférences enregistrées",
        "auto_save.empty": "Aucune préférence à enregistrer",
    },
}

_CJK_SEPARATOR = {"zh-CN", "zh-TW", "ja"}


def _translate_status(status: str, lang: str) -> str:
    return STATUS_MAP.get(lang, STATUS_MAP["zh-CN"]).get(status, status)


def _join_tags(tags: list, lang: str) -> str:
    sep = "、" if lang in _CJK_SEPARATOR else ", "
    return sep.join(str(t) for t in tags)


def fmt(key: str, lang: str = None, **kwargs) -> str:
    lang = lang or get_language()
    tpl = TEMPLATES.get(lang, TEMPLATES["zh-CN"]).get(key)
    if not tpl:
        tpl = TEMPLATES["zh-CN"].get(key, key)
    if "status" in kwargs:
        kwargs["status"] = _translate_status(kwargs["status"], lang)
    if "tags" in kwargs and isinstance(kwargs["tags"], list):
        kwargs["tags"] = _join_tags(kwargs["tags"], lang)
    return tpl.format(**kwargs)


def to_json(data, **kwargs) -> str:
    return json.dumps(data, ensure_ascii=False, **kwargs)
