import json
from pathlib import Path


# ============================================================================
# 런타임 설정 (config/settings.json)
# ============================================================================

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


# ============================================================================
# SQL 재시도 / 세션 타임아웃 상수
# ============================================================================

SQL_RETRYABLE_ERRORS = (2003, 2006, 2013, 1213, 1205)
#   2003: Can't connect to MySQL server
#   2006: MySQL server has gone away
#   2013: Lost connection to MySQL server
#   1213: Deadlock found
#   1205: Lock wait timeout exceeded

SQL_MAX_RETRY = 5
SQL_RETRY_BACKOFF_MAX = 30

# SQL 세션 타임아웃 (초)
SQL_SESSION_NET_READ_TIMEOUT = 3600
SQL_SESSION_NET_WRITE_TIMEOUT = 3600
SQL_SESSION_WAIT_TIMEOUT = 3600


def set_session_timeouts(conn) -> None:
    """pymysql 커넥션에 세션 타임아웃 설정 적용"""
    with conn.cursor() as cur:
        cur.execute(f"SET SESSION net_read_timeout={SQL_SESSION_NET_READ_TIMEOUT}")
        cur.execute(f"SET SESSION net_write_timeout={SQL_SESSION_NET_WRITE_TIMEOUT}")
        cur.execute(f"SET SESSION wait_timeout={SQL_SESSION_WAIT_TIMEOUT}")


def is_retryable_error(ex) -> bool:
    """MySQL 예외가 재시도 가능한 에러인지 판별"""
    err_code = ex.args[0] if hasattr(ex, "args") and ex.args else None
    return err_code in SQL_RETRYABLE_ERRORS
