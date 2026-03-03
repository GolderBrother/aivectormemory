from datetime import datetime

MAX_CONTENT_LENGTH = 50000
MAX_TAGS_COUNT = 20
MAX_TAG_LENGTH = 50
MAX_TITLE_LENGTH = 200


def now_iso() -> str:
    """返回当前时间的 ISO 格式字符串（带时区）"""
    return datetime.now().astimezone().isoformat()


def validate_content(content: str) -> str:
    if not content or not isinstance(content, str):
        raise ValueError("content is required and must be a string")
    if len(content) > MAX_CONTENT_LENGTH:
        raise ValueError(f"content exceeds {MAX_CONTENT_LENGTH} chars")
    return content


def validate_tags(tags) -> list[str]:
    if not isinstance(tags, list):
        raise ValueError("tags must be a list")
    if len(tags) > MAX_TAGS_COUNT:
        raise ValueError(f"tags exceeds {MAX_TAGS_COUNT} items")
    for t in tags:
        if not isinstance(t, str):
            raise ValueError(f"each tag must be a string, got: {type(t).__name__}")
        if len(t) > MAX_TAG_LENGTH:
            raise ValueError(f"tag '{t[:20]}...' exceeds {MAX_TAG_LENGTH} chars")
    return tags


_TABLE_WHITELIST = {"memories", "user_memories", "vec_memories", "vec_user_memories",
                    "memory_tags", "user_memory_tags"}


def safe_table(name: str) -> str:
    if name not in _TABLE_WHITELIST:
        raise ValueError(f"Invalid table: {name}")
    return name


def validate_title(title: str) -> str:
    if not title or not isinstance(title, str):
        raise ValueError("title is required and must be a string")
    if len(title) > MAX_TITLE_LENGTH:
        raise ValueError(f"title exceeds {MAX_TITLE_LENGTH} chars")
    return title
