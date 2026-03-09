"""i18n 模块：按语言加载 STEERING_CONTENT 和 DEV_WORKFLOW_PROMPT"""
import importlib
from aivectormemory.settings import get_language, DEFAULT_LANG

LANG_MODULE_MAP = {
    "zh-CN": "zh_CN",
    "zh-TW": "zh_TW",
    "en": "en",
    "es": "es",
    "de": "de",
    "fr": "fr",
    "ja": "ja",
}


def _load_module(lang: str):
    """加载指定语言的规则模块，失败则回退到默认语言"""
    module_name = LANG_MODULE_MAP.get(lang, LANG_MODULE_MAP[DEFAULT_LANG])
    try:
        return importlib.import_module(f"aivectormemory.i18n.rules.{module_name}")
    except (ImportError, ModuleNotFoundError):
        return importlib.import_module(f"aivectormemory.i18n.rules.{LANG_MODULE_MAP[DEFAULT_LANG]}")


def get_steering(lang: str = None) -> str:
    """返回指定语言的 STEERING_CONTENT，lang 为 None 时从 settings.json 读取"""
    lang = lang or get_language()
    return _load_module(lang).STEERING_CONTENT


def get_workflow_prompt(lang: str = None) -> str:
    """返回指定语言的 DEV_WORKFLOW_PROMPT"""
    lang = lang or get_language()
    return _load_module(lang).DEV_WORKFLOW_PROMPT
