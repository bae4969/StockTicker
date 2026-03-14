#!/usr/bin/env python3
"""
repartition_yearweek_to_year.py — tick/candle 테이블의 YEARWEEK 파티션을 YEAR 파티션으로 변환

동작 방식:
  1. tick.*, candle.* DB에서 YEARWEEK 파티션(파티션명이 6자리 pYYYYWW 형태)을 사용하는 테이블을 탐색
  2. 각 테이블에 대해:
     a) YEAR 파티션으로 새 임시 테이블(_tmp) 생성
     b) 기존 테이블 → 임시 테이블로 데이터 복사
     c) 기존 테이블을 _old로 리네임, 임시 테이블을 원래 이름으로 리네임
     d) _old 테이블 삭제 (--keep-old 옵션 시 유지)

사용법:
    python scripts/repartition_yearweek_to_year.py                  # 변환 실행
    python scripts/repartition_yearweek_to_year.py --dry-run        # SQL만 출력
    python scripts/repartition_yearweek_to_year.py --keep-old       # 기존 테이블(_old) 유지
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
        WHERE TABLE_SCHEMA = '{schema}'
          AND PARTITION_NAME IS NOT NULL
          AND PARTITION_NAME REGEXP '^p[0-9]{{6}}$'
    """)
    return [row[0] for row in cursor.fetchall()]


def get_partition_years(cursor, schema, table_name):
    """테이블의 기존 YEARWEEK 파티션에서 연도 목록 추출"""
    cursor.execute(f"""
        SELECT DISTINCT PARTITION_NAME
        FROM INFORMATION_SCHEMA.PARTITIONS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name}'
          AND PARTITION_NAME IS NOT NULL
          AND PARTITION_NAME != 'pmax'
    """)
    years = set()
    for (p_name,) in cursor.fetchall():
        m = re.match(r'^p(\d{4})\d{2}$', p_name)
        if m:
            years.add(int(m.group(1)))
    return sorted(years)


def is_tick_table(schema):
    return schema == "tick"


def build_create_tmp_table_sql(cursor, schema, table_name, years, dry_run=False):
    """기존 테이블 구조를 기반으로 YEAR 파티션 임시 테이블 CREATE SQL 생성"""
    cursor.execute(f"SHOW CREATE TABLE `{schema}`.`{table_name}`")
    create_sql = cursor.fetchone()[1]

    # PARTITION BY 이하를 제거
    partition_idx = create_sql.upper().find("PARTITION BY")
    if partition_idx == -1:
        raise RuntimeError(f"PARTITION BY 절을 찾을 수 없음: {schema}.{table_name}")

    base_sql = create_sql[:partition_idx].rstrip()

    # tick 테이블이면 execution_id 컬럼 추가 (없는 경우)
    if is_tick_table(schema) and "execution_id" not in base_sql:
        # 첫 번째 컬럼 앞에 execution_id 추가 + PRIMARY KEY 변경
        # "CREATE TABLE `xxx` (\n  `execution_datetime` ..." 형태
        # ( 뒤 첫 줄에 execution_id 삽입
        paren_idx = base_sql.index("(", base_sql.upper().index("CREATE TABLE"))
        before = base_sql[:paren_idx + 1]
        after = base_sql[paren_idx + 1:]
        base_sql = before + "\n  `execution_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT," + after

        # PRIMARY KEY가 없으면 닫는 괄호 앞에 추가
        if "PRIMARY KEY" not in base_sql.upper():
            last_paren = base_sql.rfind(")")
            base_sql = base_sql[:last_paren] + ",\n  PRIMARY KEY (`execution_id`, `execution_datetime`)\n" + base_sql[last_paren:]
        else:
            # 기존 PK를 (execution_id, execution_datetime)로 교체
            base_sql = re.sub(
                r'PRIMARY KEY\s*\([^)]*\)',
                'PRIMARY KEY (`execution_id`, `execution_datetime`)',
                base_sql,
                flags=re.IGNORECASE
            )

    # 테이블명을 _tmp로 변경
    base_sql = base_sql.replace(f"`{table_name}`", f"`{table_name}_tmp`", 1)

    # YEAR 파티션 구성
    partition_defs = []
    for y in years:
        partition_defs.append(f"PARTITION p{y:04d} VALUES LESS THAN ({y + 1:04d})")
    partition_defs.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
    partition_clause = f"PARTITION BY RANGE (YEAR(`execution_datetime`)) (\n  " + ",\n  ".join(partition_defs) + "\n)"

    return base_sql + "\n" + partition_clause


def convert_table(cursor, schema, table_name, dry_run=False, keep_old=False):
    """단일 테이블의 YEARWEEK → YEAR 파티션 변환"""
    full_name = f"`{schema}`.`{table_name}`"
    tmp_name = f"`{schema}`.`{table_name}_tmp`"
    old_name = f"`{schema}`.`{table_name}_old`"

    print(f"  [{schema}.{table_name}] 변환 시작")

    # 1. 연도 목록 추출
    years = get_partition_years(cursor, schema, table_name)
    if not years:
        print(f"    파티션 연도를 찾을 수 없음, 건너뜀")
        return
    print(f"    대상 연도: {years}")

    # 2. 기존 행 수 확인
    if not dry_run:
        cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
        src_count = cursor.fetchone()[0]
        print(f"    원본 행 수: {src_count:,}")
    else:
        src_count = 0

    # 3. 기존 _tmp, _old 테이블 제거
    execute(cursor, f"DROP TABLE IF EXISTS {tmp_name}", dry_run)
    execute(cursor, f"DROP TABLE IF EXISTS {old_name}", dry_run)

    # 4. YEAR 파티션 임시 테이블 생성
    create_sql = build_create_tmp_table_sql(cursor, schema, table_name, years, dry_run)
    execute(cursor, create_sql, dry_run)

    # 5. 데이터 복사
    if is_tick_table(schema):
        # tick: execution_id 제외하고 나머지 컬럼만 복사 (AUTO_INCREMENT 자동 할당)
        execute(cursor,
            f"INSERT INTO {tmp_name} (execution_datetime, execution_price, execution_non_volume, execution_ask_volume, execution_bid_volume) "
            f"SELECT execution_datetime, execution_price, execution_non_volume, execution_ask_volume, execution_bid_volume "
            f"FROM {full_name} ORDER BY execution_datetime",
            dry_run)
    else:
        execute(cursor, f"INSERT INTO {tmp_name} SELECT * FROM {full_name}", dry_run)

    # 6. 행 수 검증
    if not dry_run:
        cursor.execute(f"SELECT COUNT(*) FROM {tmp_name}")
        dst_count = cursor.fetchone()[0]
        print(f"    복사된 행 수: {dst_count:,}")
        if dst_count != src_count:
            print(f"    [경고] 행 수 불일치! 원본={src_count:,}, 복사={dst_count:,}")
            print(f"    변환 중단, _tmp 테이블 유지")
            return

    # 7. RENAME으로 원자적 교체
    execute(cursor,
        f"RENAME TABLE {full_name} TO {old_name}, {tmp_name} TO {full_name}",
        dry_run)
    print(f"    교체 완료")

    # 8. _old 삭제 (옵션)
    if not keep_old:
        execute(cursor, f"DROP TABLE IF EXISTS {old_name}", dry_run)
        print(f"    _old 테이블 삭제 완료")
    else:
        print(f"    _old 테이블 유지: {old_name}")


def process_schema(cursor, schema, dry_run=False, keep_old=False):
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
                convert_table(cursor, schema, table_name, dry_run, keep_old)
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
    parser.add_argument("--keep-old", action="store_true", help="기존 테이블을 _old로 보존")
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
        process_schema(cursor, schema, dry_run=args.dry_run, keep_old=args.keep_old)

    cursor.close()
    conn.close()
    print("완료")


if __name__ == "__main__":
    main()
