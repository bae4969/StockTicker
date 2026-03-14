# SQL 재시도 관련 설정
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
