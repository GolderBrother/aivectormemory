"""v9: 创建 tasks_archive 表"""
from aivectormemory.db.schema import TASKS_ARCHIVE_TABLE


def upgrade(conn, **_):
    conn.execute(TASKS_ARCHIVE_TABLE)
