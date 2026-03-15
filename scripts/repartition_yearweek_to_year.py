#!/usr/bin/env python3
"""
repartition_yearweek_to_year.py — tick/candle 테이블의 YEARWEEK 파티션을 YEAR 파티션으로 변환

동작 방식:
  1. tick.*, candle.* DB에서 YEARWEEK 파티션(파티션명이 6자리 pYYYYWW 형태)을 사용하는 테이블을 탐색
  2. 각 테이블에 대해:
     a) ALTER TABLE REMOVE PARTITIONING (파티션 해제, 데이터 유지)
     b) tick 테이블인 경우 execution_id 컬럼 추가
     c) ALTER TABLE PARTITION BY RANGE (YEAR(...)) (연도별 파티션 재설정)

사용법:
    python scripts/repartition_yearweek_to_year.py                  # 변환 실행
    python scripts/repartition_yearweek_to_year.py --dry-run        # SQL만 출력
    python scripts/repartition_yearweek_to_year.py --only-tick      # tick DB만 변환
    python scripts/repartition_yearweek_to_year.py --only-candle    # candle DB만 변환
"""

import argparse
import os
import sys
import pymysql
import re
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import config
import doc.settings as Settings


def get_connection():
    conn = pymysql.connect(
        host=Settings.SQL_HOST,
        port=Settings.SQL_PORT,
        user=Settings.SQL_ID,
        passwd=Settings.SQL_PW,
        charset=Settings.SQL_CHARSET,
        autocommit=True,
        connect_timeout=30,
        read_timeout=config.SQL_SESSION_NET_READ_TIMEOUT,
        write_timeout=config.SQL_SESSION_NET_WRITE_TIMEOUT,
    )
    config.set_session_timeouts(conn)
    return conn


def execute(cursor, query, dry_run=False):
    if dry_run:
        print(f"  [DRY-RUN] {query[:300]}")
        return
    cursor.execute(query)


def get_yearweek_partitioned_tables(cursor, schema):
    """YEARWEEK 파티션을 사용하는 테이블 목록 반환 (파티션명이 pYYYYWW 6자리 형태)"""
    cursor.execute(f"""
        SELECT DISTINCT TABLE_NAME
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE TABLE_SCHEMA = %s
          AND PARTITION_NAME IS NOT NULL
          AND PARTITION_NAME REGEXP '^p[0-9]{{6}}$'
    """, (schema,))
    return [row[0] for row in cursor.fetchall()]


def get_partition_years(cursor, schema, table_name):
    """테이블의 기존 YEARWEEK 파티션에서 연도 목록 추출"""
    cursor.execute(f"""
        SELECT DISTINCT PARTITION_NAME
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND PARTITION_NAME IS NOT NULL
          AND PARTITION_NAME != 'pmax'
    """, (schema, table_name))
    years = set()
    for (p_name,) in cursor.fetchall():
        m = re.match(r'^p(\d{4})\d{2}$', p_name)
        if m:
            years.add(int(m.group(1)))
    return sorted(years)


def has_column(cursor, schema, table_name, column_name):
    """테이블에 특정 컬럼이 있는지 확인"""
    cursor.execute(f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (schema, table_name, column_name))
    return cursor.fetchone()[0] > 0


def convert_table(cursor, schema, table_name, dry_run=False):
    """단일 테이블의 YEARWEEK → YEAR 파티션 변환 (ALTER 방식)"""
    full_name = f"`{schema}`.`{table_name}`"

    print(f"  [{schema}.{table_name}] 변환 시작")

    # 1. 연도 목록 추출
    years = get_partition_years(cursor, schema, table_name)
    if not years:
        print(f"    파티션 연도를 찾을 수 없음, 건너뜀")
        return
    print(f"    대상 연도: {years}")

    # 2. 파티션 제거 (데이터 유지)
    execute(cursor, f"ALTER TABLE {full_name} REMOVE PARTITIONING", dry_run)
    print(f"    YEARWEEK 파티션 제거 완료")

    # 3. tick 테이블이면 execution_id 컬럼 추가
    if schema == "tick" and not dry_run and not has_column(cursor, schema, table_name, "execution_id"):
        execute(cursor,
            f"ALTER TABLE {full_name} "
            f"ADD COLUMN `execution_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT FIRST, "
            f"ADD PRIMARY KEY (`execution_datetime`, `execution_id`) USING BTREE",
            dry_run)
        print(f"    execution_id 컬럼 추가 완료")
    elif schema == "tick" and dry_run:
        execute(cursor,
            f"ALTER TABLE {full_name} "
            f"ADD COLUMN `execution_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT FIRST, "
            f"ADD PRIMARY KEY (`execution_datetime`, `execution_id`) USING BTREE",
            dry_run)

    # 4. YEAR 파티션 재설정
    partition_defs = []
    for y in years:
        partition_defs.append(f"PARTITION p{y:04d} VALUES LESS THAN ({y + 1:04d})")
    partition_defs.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
    partition_sql = ",\n    ".join(partition_defs)

    execute(cursor,
        f"ALTER TABLE {full_name} PARTITION BY RANGE (YEAR(`execution_datetime`)) (\n    {partition_sql}\n)",
        dry_run)
    print(f"    YEAR 파티션 설정 완료 ({len(years)}개 연도 + pmax)")


def process_schema(cursor, schema, dry_run=False):
    """하나의 DB(tick 또는 candle)의 모든 YEARWEEK 테이블을 변환"""
    tables = get_yearweek_partitioned_tables(cursor, schema)
    if not tables:
        print(f"[{schema}] YEARWEEK 파티션 테이블 없음")
        return

    print(f"[{schema}] 변환 대상: {len(tables)}개 테이블")
    for table_name in sorted(tables):
        conn = cursor.connection
        retry_count = 0
        while True:
            try:
                convert_table(cursor, schema, table_name, dry_run)
                break
            except pymysql.err.OperationalError as e:
                if not config.is_retryable_error(e):
                    raise
                retry_count += 1
                print(f"    연결 끊김: 재연결 재시도 #{retry_count}")
                time.sleep(min(2 ** retry_count, config.SQL_RETRY_BACKOFF_MAX))
                conn.ping(reconnect=True)
                config.set_session_timeouts(conn)
                cursor = conn.cursor()
    print()


def main():
    parser = argparse.ArgumentParser(description="tick/candle 테이블의 YEARWEEK 파티션을 YEAR 파티션으로 변환")
    parser.add_argument("--dry-run", action="store_true", help="SQL만 출력, 실행하지 않음")
    parser.add_argument("--only-tick", action="store_true", help="tick DB만 변환")
    parser.add_argument("--only-candle", action="store_true", help="candle DB만 변환")
    args = parser.parse_args()

    conn = get_connection()
    cursor = conn.cursor()

    schemas = []
    if not args.only_candle:
        schemas.append("tick")
    if not args.only_tick:
        schemas.append("candle")

    for schema in schemas:
        process_schema(cursor, schema, dry_run=args.dry_run)

    cursor.close()
    conn.close()
    print("완료")


if __name__ == "__main__":
    main()
