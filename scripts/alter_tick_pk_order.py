#!/usr/bin/env python3
"""
alter_tick_pk_order.py - tick 스키마의 PK 순서를 (execution_datetime, execution_id)로 변경

동작 방식:
  1) tick 스키마의 모든 베이스 테이블 조회
  2) 각 테이블의 PRIMARY KEY 컬럼 순서 점검
  3) 현재 PK가 (execution_id, execution_datetime)인 경우에만 ALTER 실행

사용법:
    python scripts/alter_tick_pk_order.py
    python scripts/alter_tick_pk_order.py --dry-run
    python scripts/alter_tick_pk_order.py --table cBTC
"""

import argparse
import os
import sys
import time

import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import config


def get_connection():
    last_ex = None
    for attempt in range(1, config.SQL_MAX_RETRY + 1):
        try:
            conn = pymysql.connect(
                host=config.SQL_HOST,
                port=config.SQL_PORT,
                user=config.SQL_ID,
                passwd=config.SQL_PW,
                charset=config.SQL_CHARSET,
                autocommit=True,
                connect_timeout=30,
                read_timeout=config.SQL_SESSION_NET_READ_TIMEOUT,
                write_timeout=config.SQL_SESSION_NET_WRITE_TIMEOUT,
            )
            config.set_session_timeouts(conn)
            return conn
        except pymysql.err.OperationalError as ex:
            last_ex = ex
            if not config.is_retryable_error(ex) or attempt >= config.SQL_MAX_RETRY:
                break

            wait_sec = min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX)
            err_code = ex.args[0] if ex.args else "unknown"
            print(f"DB 연결 실패({err_code}): {wait_sec}초 후 재시도 ({attempt}/{config.SQL_MAX_RETRY})")
            time.sleep(wait_sec)

    if last_ex is not None:
        raise last_ex

    raise RuntimeError("DB 연결 시도 중 알 수 없는 오류가 발생했습니다.")


def reconnect_and_get_cursor(conn):
    conn.ping(reconnect=True)
    config.set_session_timeouts(conn)
    return conn.cursor()


def safe_execute(cursor, query, params=None, dry_run=False):
    if dry_run:
        one_line = " ".join(query.split())
        print(f"  [DRY-RUN] {one_line[:300]}")
        return

    params = params or ()
    for attempt in range(1, config.SQL_MAX_RETRY + 1):
        try:
            cursor.execute(query, params)
            return
        except pymysql.err.OperationalError as ex:
            if not config.is_retryable_error(ex) or attempt >= config.SQL_MAX_RETRY:
                raise
            wait_sec = min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX)
            print(f"    재시도 가능한 오류({ex.args[0]}): {wait_sec}초 후 재시도 ({attempt}/{config.SQL_MAX_RETRY})")
            time.sleep(wait_sec)
            cursor = reconnect_and_get_cursor(cursor.connection)


def fetch_tick_tables(cursor, table_filter=None):
    query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'tick'
          AND TABLE_TYPE = 'BASE TABLE'
    """
    params = ()

    if table_filter:
        query += " AND TABLE_NAME = %s"
        params = (table_filter,)

    query += " ORDER BY TABLE_NAME"
    safe_execute(cursor, query, params=params, dry_run=False)
    return [row[0] for row in cursor.fetchall()]


def fetch_primary_key_columns(cursor, table_name):
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = 'tick'
          AND TABLE_NAME = %s
          AND INDEX_NAME = 'PRIMARY'
        ORDER BY SEQ_IN_INDEX
    """
    safe_execute(cursor, query, params=(table_name,), dry_run=False)
    return [row[0] for row in cursor.fetchall()]


def has_non_primary_leading_execution_id_index(cursor, table_name):
    query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = 'tick'
          AND TABLE_NAME = %s
                    AND INDEX_NAME <> 'PRIMARY'
          AND SEQ_IN_INDEX = 1
          AND COLUMN_NAME = 'execution_id'
    """
    safe_execute(cursor, query, params=(table_name,), dry_run=False)
    return cursor.fetchone()[0] > 0


def alter_pk_order(cursor, table_name, dry_run=False):
    full_name = f"`tick`.`{table_name}`"

    # AUTO_INCREMENT 제약을 위해 PRIMARY 외 execution_id 선두 인덱스가 없으면 함께 추가
    need_execution_id_index = not has_non_primary_leading_execution_id_index(cursor, table_name)

    if need_execution_id_index:
        alter_query = (
            f"ALTER TABLE {full_name} "
            "DROP PRIMARY KEY, "
            "ADD PRIMARY KEY (`execution_datetime`, `execution_id`) USING BTREE, "
            "ADD INDEX `idx_execution_id` (`execution_id`) USING BTREE"
        )
    else:
        alter_query = (
            f"ALTER TABLE {full_name} "
            "DROP PRIMARY KEY, "
            "ADD PRIMARY KEY (`execution_datetime`, `execution_id`) USING BTREE"
        )

    safe_execute(cursor, alter_query, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="tick 스키마 PK 순서 변경 스크립트")
    parser.add_argument("--dry-run", action="store_true", help="SQL만 출력하고 실행하지 않음")
    parser.add_argument("--table", type=str, default=None, help="특정 테이블만 처리 (예: cBTC)")
    args = parser.parse_args()

    try:
        conn = get_connection()
    except pymysql.err.OperationalError as ex:
        err_code = ex.args[0] if ex.args else "unknown"
        err_msg = ex.args[1] if len(ex.args) > 1 else str(ex)
        print("DB 연결에 실패했습니다.")
        print(f"  에러 코드: {err_code}")
        print(f"  상세: {err_msg}")
        print("  확인 사항: SQL_HOST/SQL_PORT, DB 서버 기동 상태, 방화벽/네트워크")
        sys.exit(1)

    cursor = conn.cursor()

    try:
        tables = fetch_tick_tables(cursor, table_filter=args.table)
        if not tables:
            print("처리할 tick 테이블이 없습니다.")
            return

        print(f"대상 tick 테이블 수: {len(tables)}")

        changed = 0
        skipped = 0

        for table_name in tables:
            pk_cols = fetch_primary_key_columns(cursor, table_name)

            if pk_cols == ["execution_datetime", "execution_id"]:
                print(f"  [tick.{table_name}] 이미 변경됨, 건너뜀")
                skipped += 1
                continue

            if pk_cols != ["execution_id", "execution_datetime"]:
                print(f"  [tick.{table_name}] 예상 외 PK 구조({pk_cols}), 건너뜀")
                skipped += 1
                continue

            print(f"  [tick.{table_name}] PK 순서 변경 시작")
            alter_pk_order(cursor, table_name, dry_run=args.dry_run)
            print(f"  [tick.{table_name}] PK 순서 변경 완료")
            changed += 1

        print("\n완료")
        print(f"  변경: {changed}")
        print(f"  건너뜀: {skipped}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
