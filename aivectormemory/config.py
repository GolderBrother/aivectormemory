import os
from pathlib import Path

DB_DIR = Path(os.getenv("AIVM_DB_DIR", str(Path.home() / ".aivectormemory")))
DB_NAME = os.getenv("AIVM_DB_NAME", "memory.db")
MODEL_NAME = os.getenv("AIVM_MODEL_NAME", "intfloat/multilingual-e5-small")
MODEL_DIMENSION = int(os.getenv("AIVM_MODEL_DIMENSION", "384"))
DEDUP_THRESHOLD = float(os.getenv("AIVM_DEDUP_THRESHOLD", "0.95"))
USER_SCOPE_DIR = "@user@"
DEFAULT_TOP_K = int(os.getenv("AIVM_DEFAULT_TOP_K", "5"))
WEB_DEFAULT_PORT = int(os.getenv("AIVM_WEB_PORT", "9080"))


OLD_DB_DIR = Path.home() / ".devmemory"


def get_db_path() -> Path:
    db_path = DB_DIR / DB_NAME
    old_path = OLD_DB_DIR / DB_NAME
    if old_path.exists() and (not db_path.exists() or db_path.stat().st_size < 8192):
        import shutil
        DB_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(old_path, db_path)
    return db_path


def get_project_dir(project_dir: str | None = None) -> str:
    return str(Path(project_dir).resolve()) if project_dir else str(Path.cwd().resolve())
