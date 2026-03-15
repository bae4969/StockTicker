#!/usr/bin/env python3
"""
migrate_db.py - Z_Coin_*/Z_Stock_* DB → tick/candle DB 마이그레이션 스크립트

기존 구조:
  Z_Coin{code}.Raw{YYYY}    (ARCHIVE) → tick.c{code}   (InnoDB, YEAR 파티션)
  Z_Coin{code}.Candle{YYYY} (InnoDB)  → candle.c{code} (InnoDB, YEAR 파티션)
  Z_Stock{code}.Raw{YYYY}   (ARCHIVE) → tick.s{code}   (InnoDB, YEAR 파티션)
  Z_Stock{code}.Candle{YYYY}(InnoDB)  → candle.s{code} (InnoDB, YEAR 파티션)

사용법:
    python migrate_db.py                  # 마이그레이션 실행 (기존 DB 유지)
    python migrate_db.py --workers 8      # 종목별 병렬 마이그레이션
    python migrate_db.py --drop-old       # 마이그레이션 후 기존 DB 삭제
    python migrate_db.py --run-prepare    # 대상 테이블/파티션만 구성
    python migrate_db.py --run-migrate    # 데이터 이관만 실행
    python migrate_db.py --run-drop       # 기존 DB 삭제만 실행
    python migrate_db.py --dry-run        # SQL만 출력, 실행하지 않음
    python migrate_db.py --resume-from Z_CoinETH  # 해당 종목부터 재개 (이전 건너뜀)
    python migrate_db.py --only-coin              # 코인만 마이그레이션
    python migrate_db.py --only-stock             # 주식만 마이그레이션
"""

import argparse
import os
import sys
import pymysql
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 프로젝트 루트를 sys.path에 추가 (scripts/ 하위에서 실행 시)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import config
import doc.settings as Settings


def reconnect_and_get_cursor(conn):
    conn.ping(reconnect=True)
    config.set_session_timeouts(conn)
    return conn.cursor()


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
        print(f"  [DRY-RUN] {query[:200]}")
        return
    cursor.execute(query)


def get_old_databases(cursor):
    """Z_Coin*, Z_Stock* DB 목록 조회"""
    conn = cursor.connection
    retry_count = 0
    while True:
        try:
            cursor.execute("SHOW DATABASES")
            all_dbs = [row[0] for row in cursor.fetchall()]
            break
        except pymysql.err.OperationalError as e:
            if not config.is_retryable_error(e):
                raise

            retry_count += 1
            print(f"  SHOW DATABASES 연결 끊김: 재연결 재시도 #{retry_count}")
            time.sleep(2)
            cursor = reconnect_and_get_cursor(conn)

    coin_dbs = [db for db in all_dbs if db.startswith("Z_Coin")]
    stock_dbs = [db for db in all_dbs if db.startswith("Z_Stock")]
    return coin_dbs, stock_dbs


def get_tables(cursor, db_name):
    """특정 DB의 Raw/Candle 테이블 목록 및 연도 추출"""
    conn = cursor.connection
    retry_count = 0
    while True:
        try:
            cursor.execute(f"SHOW TABLES IN `{db_name}`")
            tables = [row[0] for row in cursor.fetchall()]
            break
        except pymysql.err.OperationalError as e:
            if not config.is_retryable_error(e):
                raise

            retry_count += 1
            print(f"  [{db_name}] SHOW TABLES 연결 끊김: 재연결 재시도 #{retry_count}")
            time.sleep(2)
            cursor = reconnect_and_get_cursor(conn)

    raw_tables = []
    candle_tables = []
    for t in tables:
        m = re.match(r'^Raw(\d{4})$', t)
        if m:
            raw_tables.append((t, int(m.group(1))))
            continue
        m = re.match(r'^Candle(\d{4})$', t)
        if m:
            candle_tables.append((t, int(m.group(1))))

    return raw_tables, candle_tables


def build_group_plan(cursor, db_list):
    """DB별 Raw/Candle 테이블 및 연도 정보를 사전 수집"""
    plan_map = {}
    for db_name in sorted(db_list):
        raw_tables, candle_tables = get_tables(cursor, db_name)
        plan_map[db_name] = {
            "raw_tables": sorted(raw_tables, key=lambda x: x[1]),
            "candle_tables": sorted(candle_tables, key=lambda x: x[1]),
        }
    return plan_map


def prepare_database_targets(cursor, db_name, prefix, id_prefix, plan_entry, dry_run=False):
    """DB 1개에 대한 대상 테이블/파티션을 선구성"""
    code = extract_code(db_name, prefix)
    new_id = f"{id_prefix}{code}"
    tick_table = f"tick.`{new_id}`"
    candle_table = f"candle.`{new_id}`"

    raw_tables = plan_entry.get("raw_tables", []) if plan_entry else []
    candle_tables = plan_entry.get("candle_tables", []) if plan_entry else []

    if raw_tables:
        raw_years = sorted({year for _, year in raw_tables})
        create_tick_table(cursor, tick_table, dry_run)
        for year in raw_years:
            ensure_year_partitions(cursor, tick_table, year, dry_run)

    if candle_tables:
        candle_years = sorted({year for _, year in candle_tables})
        create_candle_table(cursor, candle_table, dry_run)
        for year in candle_years:
            ensure_year_partitions(cursor, candle_table, year, dry_run)


def prepare_group_targets(cursor, db_list, prefix, id_prefix, title, plan_map, dry_run=False):
    """그룹 전체 DB의 대상 테이블/파티션을 사전에 구성"""
    if not db_list:
        return

    conn = cursor.connection
    print(f"=== {title} 대상 테이블/파티션 선구성 ===")
    for db_name in sorted(db_list):
        plan_entry = plan_map.get(db_name, {})
        raw_count = len(plan_entry.get("raw_tables", []))
        candle_count = len(plan_entry.get("candle_tables", []))
        if raw_count == 0 and candle_count == 0:
            print(f"  [{db_name}] 대상 테이블 없음, 건너뜀")
            continue

        print(f"  [{db_name}] 준비 중 (Raw={raw_count}, Candle={candle_count})")
        retry_count = 0
        while True:
            try:
                prepare_database_targets(cursor, db_name, prefix, id_prefix, plan_entry, dry_run)
                break
            except pymysql.err.OperationalError as e:
                if not config.is_retryable_error(e):
                    raise

                err_code = e.args[0] if e.args else None
                retry_count += 1
                print(f"      연결 끊김 감지({err_code}): 재연결 후 준비 재시도 #{retry_count}")
                time.sleep(2)
                cursor = reconnect_and_get_cursor(conn)
    print()


def extract_code(db_name, prefix):
    """DB명에서 코드 추출: Z_CoinBTC → BTC, Z_Stock005930 → 005930"""
    return db_name[len(prefix):]


def build_year_partition_str(year):
    """해당 연도의 YEAR 파티션 정의 문자열 생성"""
    return f"PARTITION p{year:04d} VALUES LESS THAN ({year+1:04d})"


def create_tick_table(cursor, table_name, dry_run=False):
    execute(cursor, f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            execution_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            execution_datetime DATETIME NOT NULL,
            execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            PRIMARY KEY (execution_datetime, execution_id) USING BTREE
        ) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB
        PARTITION BY RANGE (YEAR(execution_datetime)) (
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )
    """, dry_run)


def create_candle_table(cursor, table_name, dry_run=False):
    execute(cursor, f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            execution_datetime DATETIME NOT NULL,
            execution_open DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_close DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_min DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_max DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_non_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_ask_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_bid_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            PRIMARY KEY (execution_datetime) USING BTREE
        ) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB
        PARTITION BY RANGE (YEAR(execution_datetime)) (
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )
    """, dry_run)


def ensure_year_partitions(cursor, table_name, year, dry_run=False):
    """해당 연도의 YEAR 파티션이 없으면 REORGANIZE pmax로 추가"""
    partition_name = f"p{year:04d}"
    if not dry_run:
        schema = table_name.split('.')[0]
        tbl = table_name.split('.')[1].strip('`')
        cursor.execute(f"""
            SELECT PARTITION_NAME, PARTITION_DESCRIPTION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{tbl}'
              AND PARTITION_NAME IS NOT NULL
        """)
        partitions = cursor.fetchall()

        # 이미 해당 연도 파티션이 있으면 스킵
        if any(p[0] == partition_name for p in partitions):
            return

        # 기존에 더 큰 범위의 파티션이 있으면, 과거 연도 추가는 정렬 오류(1493)를 유발할 수 있음.
        # RANGE 파티션은 연속 구간이므로 과거 값은 기존 첫 파티션에 이미 포함됨.
        max_less_than = None
        for p_name, p_desc in partitions:
            if p_name == 'pmax' or p_desc in (None, 'MAXVALUE'):
                continue
            try:
                p_val = int(p_desc)
            except (TypeError, ValueError):
                continue
            if max_less_than is None or p_val > max_less_than:
                max_less_than = p_val

        target_start = year + 1
        if max_less_than is not None and target_start <= max_less_than:
            print(
                f"      파티션 추가 생략: {table_name} 은(는) 이미 YEAR < {max_less_than} 범위를 포함"
            )
            return

    year_partition = build_year_partition_str(year)
    try:
        execute(cursor, f"""
            ALTER TABLE {table_name} REORGANIZE PARTITION pmax INTO (
                {year_partition},
                PARTITION pmax VALUES LESS THAN MAXVALUE
            )
        """, dry_run)
    except pymysql.err.OperationalError as e:
        if e.args and e.args[0] == 1493:
            print(f"      파티션 추가 생략(정렬 조건 충돌): {table_name}, year={year}")
            return
        raise


def get_row_count(cursor, full_table_name):
    conn = cursor.connection
    retry_count = 0
    while True:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            return cursor.fetchone()[0]
        except pymysql.err.OperationalError as e:
            if not config.is_retryable_error(e):
                raise

            retry_count += 1
            print(f"      COUNT 연결 끊김: 재연결 재시도 #{retry_count}")
            time.sleep(2)
            cursor = reconnect_and_get_cursor(conn)


def get_year_row_count(cursor, full_table_name, year):
    conn = cursor.connection
    retry_count = 0
    query = f"SELECT COUNT(*) FROM {full_table_name} WHERE YEAR(execution_datetime) = {year}"
    while True:
        try:
            cursor.execute(query)
            return cursor.fetchone()[0], cursor
        except pymysql.err.OperationalError as e:
            if not config.is_retryable_error(e):
                raise

            retry_count += 1
            print(f"      검증 COUNT 연결 끊김: 재연결 재시도 #{retry_count}")
            time.sleep(2)
            cursor = reconnect_and_get_cursor(conn)


def clean_target_table(cursor, table_name, dry_run=False):
    """종목 마이그레이션 시작 전 대상 테이블의 기존 데이터를 모두 삭제"""
    conn = cursor.connection
    retry_count = 0
    while True:
        try:
            execute(cursor, f"TRUNCATE TABLE {table_name}", dry_run)
            return cursor
        except pymysql.err.OperationalError as e:
            if not config.is_retryable_error(e):
                raise
            retry_count += 1
            print(f"      TRUNCATE 연결 끊김: 재연결 재시도 #{retry_count}")
            time.sleep(2)
            cursor = reconnect_and_get_cursor(conn)


def prepare_target_year(cursor, target_table, year, dry_run=False):
    """재실행/재시도 시 중복 누적을 막기 위해 대상 연도 데이터를 먼저 비움"""
    execute(cursor, f"DELETE FROM {target_table} WHERE YEAR(execution_datetime) = {year}", dry_run)


def copy_year_rows(conn, cursor, target_table, source_table, year, dry_run=False):
    """연도 단위 복사를 수행하고, 2006/2013 연결 오류 시 무제한 재연결 재시도"""
    insert_sql = f"INSERT INTO {target_table} SELECT * FROM {source_table}"

    if dry_run:
        print(f"[DRY-RUN] {insert_sql}")
        return cursor

    retry_count = 0
    while True:
        try:
            execute(cursor, insert_sql)
            return cursor
        except pymysql.err.OperationalError as e:
            if not config.is_retryable_error(e):
                raise

            err_code = e.args[0] if e.args else None
            retry_count += 1
            print(f"\n      연결 끊김 감지({err_code}): 재연결 후 재시도 #{retry_count}")
            time.sleep(2)
            new_cursor = reconnect_and_get_cursor(conn)
            prepare_target_year(new_cursor, target_table, year, dry_run=False)
            cursor = new_cursor


def migrate_database(conn, cursor, db_name, prefix, id_prefix, dry_run=False, plan_entry=None):
    """단일 기존 DB를 tick/candle로 마이그레이션"""
    code = extract_code(db_name, prefix)
    new_id = f"{id_prefix}{code}"
    tick_table = f"tick.`{new_id}`"
    candle_table = f"candle.`{new_id}`"

    if plan_entry is None:
        raw_tables, candle_tables = get_tables(cursor, db_name)
    else:
        raw_tables = plan_entry.get("raw_tables", [])
        candle_tables = plan_entry.get("candle_tables", [])

    if not raw_tables and not candle_tables:
        print(f"  [{db_name}] 테이블 없음, 건너뜀")
        return cursor

    print(f"  [{db_name}] → tick.{new_id} / candle.{new_id}")
    print(f"    Raw 테이블: {[t[0] for t in raw_tables]}")
    print(f"    Candle 테이블: {[t[0] for t in candle_tables]}")

    # 대상 테이블 정리 (기존 부분 데이터 제거)
    if raw_tables:
        print(f"    대상 정리: {tick_table} ...", end=" ", flush=True)
        cursor = clean_target_table(cursor, tick_table, dry_run)
        print("OK")
    if candle_tables:
        print(f"    대상 정리: {candle_table} ...", end=" ", flush=True)
        cursor = clean_target_table(cursor, candle_table, dry_run)
        print("OK")

    # tick 테이블 마이그레이션
    if raw_tables:
        for table_name, year in sorted(raw_tables, key=lambda x: x[1]):
            old_full = f"`{db_name}`.`{table_name}`"
            print(f"    복사 중: {old_full} → {tick_table} ...", end=" ", flush=True)

            if not dry_run:
                old_count = get_row_count(cursor, old_full)
                if old_count == 0:
                    print("(빈 테이블, 건너뜀)")
                    continue

                prepare_target_year(cursor, tick_table, year, dry_run=False)
                cursor = copy_year_rows(conn, cursor, tick_table, old_full, year, dry_run=False)
                new_count, cursor = get_year_row_count(cursor, tick_table, year)
                if new_count >= old_count:
                    print(f"OK ({old_count} rows)")
                else:
                    print(f"WARNING: 원본 {old_count}, 복사 {new_count}")
            else:
                print(f"[DRY-RUN] INSERT INTO {tick_table} SELECT * FROM {old_full}")

    # candle 테이블 마이그레이션
    if candle_tables:
        for table_name, year in sorted(candle_tables, key=lambda x: x[1]):
            old_full = f"`{db_name}`.`{table_name}`"
            print(f"    복사 중: {old_full} → {candle_table} ...", end=" ", flush=True)

            if not dry_run:
                old_count = get_row_count(cursor, old_full)
                if old_count == 0:
                    print("(빈 테이블, 건너뜀)")
                    continue

                prepare_target_year(cursor, candle_table, year, dry_run=False)
                cursor = copy_year_rows(conn, cursor, candle_table, old_full, year, dry_run=False)
                new_count, cursor = get_year_row_count(cursor, candle_table, year)
                if new_count >= old_count:
                    print(f"OK ({old_count} rows)")
                else:
                    print(f"WARNING: 원본 {old_count}, 복사 {new_count}")
            else:
                print(f"[DRY-RUN] INSERT INTO {candle_table} SELECT * FROM {old_full}")

    return cursor


def drop_old_databases(cursor, db_list, dry_run=False):
    print("\n=== 기존 DB 삭제 ===")
    conn = cursor.connection
    for db_name in db_list:
        print(f"  DROP DATABASE `{db_name}`")
        retry_count = 0
        while True:
            try:
                execute(cursor, f"DROP DATABASE IF EXISTS `{db_name}`", dry_run)
                break
            except pymysql.err.OperationalError as e:
                if not config.is_retryable_error(e):
                    raise

                retry_count += 1
                print(f"    연결 끊김 감지({e.args[0]}): 재연결 후 삭제 재시도 #{retry_count}")
                time.sleep(2)
                cursor = reconnect_and_get_cursor(conn)
    print("  완료.")


def parse_args():
    cpu_workers = os.cpu_count() or 2
    default_workers = min(8, max(2, cpu_workers))

    parser = argparse.ArgumentParser(description="Z_Coin/Z_Stock DB를 tick/candle로 마이그레이션")
    parser.add_argument("--dry-run", action="store_true", help="SQL만 출력하고 실행하지 않음")
    parser.add_argument("--drop-old", action="store_true", help="마이그레이션 완료 후 기존 DB 삭제")
    parser.add_argument("--run-prepare", action="store_true", help="대상 테이블/파티션 구성 단계만 실행")
    parser.add_argument("--run-migrate", action="store_true", help="데이터 이관 단계만 실행")
    parser.add_argument("--run-drop", action="store_true", help="기존 DB 삭제 단계만 실행")
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=f"종목별 병렬 워커 수 (기본값: {default_workers}, 1이면 순차 실행)",
    )
    parser.add_argument(
        "--resume-from",
        type=str,
        default=None,
        help="지정한 DB명(예: Z_CoinETH, Z_Stock005930)부터 마이그레이션 재개 (이전 종목 건너뜀)",
    )
    parser.add_argument("--only-coin", action="store_true", help="코인 DB만 마이그레이션")
    parser.add_argument("--only-stock", action="store_true", help="주식 DB만 마이그레이션")
    return parser.parse_args()


def resolve_steps(args):
    """실행 단계 결정: 기본은 prepare+migrate, --drop-old 시 drop 추가"""
    explicit = args.run_prepare or args.run_migrate or args.run_drop
    if explicit:
        steps = set()
        if args.run_prepare:
            steps.add("prepare")
        if args.run_migrate:
            steps.add("migrate")
        if args.run_drop:
            steps.add("drop")
        # 하위 호환: 명시 모드에서도 --drop-old는 drop 단계 추가로 동작
        if args.drop_old:
            steps.add("drop")
        return steps

    steps = {"prepare", "migrate"}
    if args.drop_old:
        steps.add("drop")
    return steps


def filter_dbs_for_resume(coin_dbs, stock_dbs, resume_from):
    """--resume-from 기준으로 DB 목록 필터링 (이전 종목 건너뜀)"""
    if not resume_from:
        return coin_dbs, stock_dbs

    if resume_from.startswith("Z_Coin"):
        coin_dbs = [db for db in sorted(coin_dbs) if db >= resume_from]
        # stock_dbs는 전체 유지 (코인 이후 실행)
    elif resume_from.startswith("Z_Stock"):
        coin_dbs = []  # 코인은 이미 완료된 것으로 간주
        stock_dbs = [db for db in sorted(stock_dbs) if db >= resume_from]
    else:
        print(f"  WARNING: --resume-from 값이 Z_Coin 또는 Z_Stock으로 시작하지 않음: {resume_from}")

    return coin_dbs, stock_dbs


def migrate_one_database(db_name, prefix, id_prefix, dry_run=False, plan_entry=None):
    """병렬 실행용: DB 1개를 독립 연결로 마이그레이션"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        migrate_database(conn, cursor, db_name, prefix, id_prefix, dry_run, plan_entry=plan_entry)
    finally:
        cursor.close()
        conn.close()


def run_migration_group(db_list, prefix, id_prefix, title, dry_run=False, workers=1, plan_map=None):
    if not db_list:
        return

    ordered = sorted(db_list)
    print(f"=== {title} ===")

    if workers <= 1 or len(ordered) == 1:
        for db_name in ordered:
            plan_entry = plan_map.get(db_name) if plan_map else None
            migrate_one_database(db_name, prefix, id_prefix, dry_run, plan_entry=plan_entry)
        print()
        return

    print(f"  병렬 실행: {len(ordered)}개 DB, workers={workers}")
    errors = []
    with ThreadPoolExecutor(max_workers=min(workers, len(ordered))) as executor:
        futures = {
            executor.submit(
                migrate_one_database,
                db_name,
                prefix,
                id_prefix,
                dry_run,
                plan_map.get(db_name) if plan_map else None,
            ): db_name
            for db_name in ordered
        }
        for future in as_completed(futures):
            db_name = futures[future]
            try:
                future.result()
            except Exception as e:
                errors.append((db_name, e))

    if errors:
        print("\n  실패한 DB 목록:")
        for db_name, err in errors:
            print(f"    - {db_name}: {err}")
        raise RuntimeError(f"총 {len(errors)}개 DB 마이그레이션 실패")

    print()


def main():
    args = parse_args()
    dry_run = args.dry_run
    workers = max(1, args.workers)
    resume_from = args.resume_from
    steps = resolve_steps(args)

    if dry_run:
        print("=== DRY-RUN 모드 (SQL 출력만, 실행 안 함) ===\n")
    print(
        f"=== 실행 옵션: workers={workers}, dry_run={dry_run}, "
        f"resume_from={resume_from}, steps={sorted(steps)} ===\n"
    )

    conn = get_connection()
    cursor = conn.cursor()

    # 1. tick/candle DB 생성 (prepare/migrate 단계에서 필요)
    if "prepare" in steps or "migrate" in steps:
        print("=== tick/candle DB 생성 ===")
        execute(cursor, "CREATE DATABASE IF NOT EXISTS tick CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'", dry_run)
        execute(cursor, "CREATE DATABASE IF NOT EXISTS candle CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'", dry_run)
        print("  완료.\n")

    # 2. 기존 DB 탐색
    print("=== 기존 DB 탐색 ===")
    coin_dbs, stock_dbs = get_old_databases(cursor)
    print(f"  코인 DB: {coin_dbs}")
    print(f"  주식 DB: {stock_dbs}\n")

    # --only-coin / --only-stock 필터링
    if args.only_coin:
        stock_dbs = []
    elif args.only_stock:
        coin_dbs = []

    # --resume-from 필터링
    if resume_from:
        coin_dbs, stock_dbs = filter_dbs_for_resume(coin_dbs, stock_dbs, resume_from)
        print(f"  resume-from 적용 후 코인 DB: {coin_dbs}")
        print(f"  resume-from 적용 후 주식 DB: {stock_dbs}\n")

    if not coin_dbs and not stock_dbs:
        print("마이그레이션 대상이 없습니다.")
        cursor.close()
        conn.close()
        return

    coin_plan = {}
    stock_plan = {}
    if "prepare" in steps or "migrate" in steps:
        # 2-1. 전체 테이블 스캔(사전 계획)
        print("=== 마이그레이션 계획 수립(전체 테이블 스캔) ===")
        coin_plan = build_group_plan(cursor, coin_dbs)
        stock_plan = build_group_plan(cursor, stock_dbs)

        coin_table_count = sum(len(v["raw_tables"]) + len(v["candle_tables"]) for v in coin_plan.values())
        stock_table_count = sum(len(v["raw_tables"]) + len(v["candle_tables"]) for v in stock_plan.values())
        print(f"  코인 대상 테이블 수: {coin_table_count}")
        print(f"  주식 대상 테이블 수: {stock_table_count}\n")

    # 2-2. 전체 대상 테이블/파티션 선구성
    if "prepare" in steps:
        prepare_group_targets(cursor, coin_dbs, "Z_Coin", "c", "코인", coin_plan, dry_run=dry_run)
        prepare_group_targets(cursor, stock_dbs, "Z_Stock", "s", "주식", stock_plan, dry_run=dry_run)

    # 3. 코인/주식 마이그레이션
    if "migrate" in steps:
        run_migration_group(
            coin_dbs,
            "Z_Coin",
            "c",
            "코인 마이그레이션",
            dry_run=dry_run,
            workers=workers,
            plan_map=coin_plan,
        )

        run_migration_group(
            stock_dbs,
            "Z_Stock",
            "s",
            "주식 마이그레이션",
            dry_run=dry_run,
            workers=workers,
            plan_map=stock_plan,
        )

    # 4. 기존 DB 삭제
    all_old_dbs = coin_dbs + stock_dbs
    if "drop" in steps and all_old_dbs:
        if dry_run:
            drop_old_databases(cursor, all_old_dbs, dry_run)
        else:
            print(f"\n{len(all_old_dbs)}개 기존 DB를 삭제하시겠습니까?")
            for db in all_old_dbs:
                print(f"  - {db}")
            confirm = input("\n삭제하려면 'YES'를 입력하세요: ")
            if confirm == "YES":
                drop_old_databases(cursor, all_old_dbs)
            else:
                print("  삭제 취소됨.")

    print("\n=== 마이그레이션 완료 ===")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()