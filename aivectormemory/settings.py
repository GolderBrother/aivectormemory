"""全局语言配置：读写 ~/.aivectormemory/settings.json"""
import json
from pathlib import Path

SETTINGS_PATH = Path.home() / ".aivectormemory" / "settings.json"
DEFAULT_LANG = "zh-CN"
SUPPORTED_LANGS = ("zh-CN", "zh-TW", "en", "es", "de", "fr", "ja")


def get_language() -> str:
    """读取全局语言设置，不存在或无效则返回默认值"""
    try:
        data = json.loads(SETTINGS_PATH.read_text("utf-8"))
        lang = data.get("language", "")
        return lang if lang in SUPPORTED_LANGS else DEFAULT_LANG
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return DEFAULT_LANG


def set_language(lang: str) -> None:
    """写入语言设置到 settings.json（保留其他字段）"""
    if lang not in SUPPORTED_LANGS:
        raise ValueError(f"Unsupported language: {lang}. Supported: {SUPPORTED_LANGS}")
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {}
    try:
        data = json.loads(SETTINGS_PATH.read_text("utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    data["language"] = lang
    SETTINGS_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
