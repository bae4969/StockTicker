import json
from pathlib import Path


def _load_settings() -> dict:
    settings_path = Path(__file__).resolve().parent.parent / "config" / "settings.json"

    if not settings_path.exists():
        raise FileNotFoundError(
            f"설정 파일이 없습니다: {settings_path}. config/settings.json 파일을 생성해 주세요."
        )

    with settings_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


_CONFIG = _load_settings()

SQL_HOST = _CONFIG["SQL_HOST"]
SQL_PORT = _CONFIG["SQL_PORT"]
SQL_ID = _CONFIG["SQL_ID"]
SQL_PW = _CONFIG["SQL_PW"]
SQL_CHARSET = _CONFIG["SQL_CHARSET"]

SQL_LOG_DB = _CONFIG["SQL_LOG_DB"]
SQL_BH_DB = _CONFIG["SQL_BH_DB"]
SQL_KI_DB = _CONFIG["SQL_KI_DB"]

KI_API_KEY_LIST = _CONFIG["KI_API_KEY_LIST"]
