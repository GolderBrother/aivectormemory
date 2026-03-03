"""v3: user scope 记忆的 project_dir 从空字符串改为 @user@"""
from aivectormemory.config import USER_SCOPE_DIR


def upgrade(conn, **_):
    conn.execute(
        "UPDATE memories SET project_dir=? WHERE project_dir='' AND scope='user'",
        (USER_SCOPE_DIR,)
    )
