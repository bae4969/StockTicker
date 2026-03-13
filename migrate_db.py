#!/usr/bin/env python3
"""
migrate_db.py - Z_Coin_*/Z_Stock_* DB → tick/candle DB 마이그레이션 스크립트

기존 구조:
  Z_Coin{code}.Raw{YYYY}    (ARCHIVE) → tick.c{code}   (InnoDB, YEARWEEK 파티션)
  Z_Coin{code}.Candle{YYYY} (InnoDB)  → candle.c{code} (InnoDB, YEARWEEK 파티션)
  Z_Stock{code}.Raw{YYYY}   (ARCHIVE) → tick.s{code}   (InnoDB, YEARWEEK 파티션)
  Z_Stock{code}.Candle{YYYY}(InnoDB)  → candle.s{code} (InnoDB, YEARWEEK 파티션)

사용법:
  python migrate_db.py                  # 마이그레이션 실행 (기존 DB 유지)
  python migrate_db.py --drop-old       # 마이그레이션 후 기존 DB 삭제
  python migrate_db.py --dry-run        # SQL만 출력, 실행하지 않음
"""

import pymysql
import sys
import re
import doc.Define as Define


def get_connection():
    return pymysql.connect(
        host=Define.SQL_HOST,
        port=3306,
        user=Define.SQL_ID,
        passwd=Define.SQL_PW,
        charset='utf8',
        autocommit=True,
    )


def execute(cursor, query, dry_run=False):
    if dry_run:
        print(f"  [DRY-RUN] {query[:200]}")
        return
    cursor.execute(query)


def get_old_databases(cursor):
    """Z_Coin*, Z_Stock* DB 목록 조회"""
    cursor.execute("SHOW DATABASES")
    all_dbs = [row[0] for row in cursor.fetchall()]

    coin_dbs = [db for db in all_dbs if db.startswith("Z_Coin")]
    stock_dbs = [db for db in all_dbs if db.startswith("Z_Stock")]
    return coin_dbs, stock_dbs


def get_tables(cursor, db_name):
    """특정 DB의 Raw/Candle 테이블 목록 및 연도 추출"""
    cursor.execute(f"SHOW TABLES IN `{db_name}`")
    tables = [row[0] for row in cursor.fetchall()]

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


def extract_code(db_name, prefix):
    """DB명에서 코드 추출: Z_CoinBTC → BTC, Z_Stock005930 → 005930"""
    return db_name[len(prefix):]


def build_yearweek_partition_str(year):
    """해당 연도의 YEARWEEK 파티션 정의 문자열 생성 (53개)"""
    parts = ""
    for i in range(1, 53):
        parts += f"PARTITION p{year:04d}{i:02d} VALUES LESS THAN ({year:04d}{i+1:02d}),"
    parts += f"PARTITION p{year:04d}{53:02d} VALUES LESS THAN ({year+1:04d}{1:02d})"
    return parts


def create_tick_table(cursor, table_name, dry_run=False):
    execute(cursor, f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            execution_datetime DATETIME NOT NULL,
            execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
            execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0'
        ) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB
        PARTITION BY RANGE (YEARWEEK(execution_datetime)) (
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
        PARTITION BY RANGE (YEARWEEK(execution_datetime)) (
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )
    """, dry_run)


def ensure_year_partitions(cursor, table_name, year, dry_run=False):
    """해당 연도의 YEARWEEK 파티션이 없으면 REORGANIZE pmax로 53개 추가"""
    partition_name = f"p{year:04d}01"
    if not dry_run:
        schema = table_name.split('.')[0]
        tbl = table_name.split('.')[1].strip('`')
        cursor.execute(f"""
            SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{tbl}'
              AND PARTITION_NAME = '{partition_name}'
        """)
        if cursor.fetchone():
            return  # 해당 연도 파티션이 이미 존재

    year_partitions = build_yearweek_partition_str(year)
    execute(cursor, f"""
        ALTER TABLE {table_name} REORGANIZE PARTITION pmax INTO (
            {year_partitions},
            PARTITION pmax VALUES LESS THAN MAXVALUE
        )
    """, dry_run)


def get_row_count(cursor, full_table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
    return cursor.fetchone()[0]


def migrate_database(cursor, db_name, prefix, id_prefix, dry_run=False):
    """단일 기존 DB를 tick/candle로 마이그레이션"""
    code = extract_code(db_name, prefix)
    new_id = f"{id_prefix}{code}"
    tick_table = f"tick.`{new_id}`"
    candle_table = f"candle.`{new_id}`"

    raw_tables, candle_tables = get_tables(cursor, db_name)

    if not raw_tables and not candle_tables:
        print(f"  [{db_name}] 테이블 없음, 건너뜀")
        return

    print(f"  [{db_name}] → tick.{new_id} / candle.{new_id}")
    print(f"    Raw 테이블: {[t[0] for t in raw_tables]}")
    print(f"    Candle 테이블: {[t[0] for t in candle_tables]}")

    # tick 테이블 마이그레이션
    if raw_tables:
        create_tick_table(cursor, tick_table, dry_run)
        for table_name, year in sorted(raw_tables, key=lambda x: x[1]):
            ensure_year_partitions(cursor, tick_table, year, dry_run)

            old_full = f"`{db_name}`.`{table_name}`"
            print(f"    복사 중: {old_full} → {tick_table} ...", end=" ", flush=True)

            if not dry_run:
                old_count = get_row_count(cursor, old_full)
                if old_count == 0:
                    print("(빈 테이블, 건너뜀)")
                    continue

                execute(cursor, f"INSERT INTO {tick_table} SELECT * FROM {old_full}")
                new_count_query = f"SELECT COUNT(*) FROM {tick_table} WHERE YEAR(execution_datetime) = {year}"
                cursor.execute(new_count_query)
                new_count = cursor.fetchone()[0]
                if new_count >= old_count:
                    print(f"OK ({old_count} rows)")
                else:
                    print(f"WARNING: 원본 {old_count}, 복사 {new_count}")
            else:
                print(f"[DRY-RUN] INSERT INTO {tick_table} SELECT * FROM {old_full}")

    # candle 테이블 마이그레이션
    if candle_tables:
        create_candle_table(cursor, candle_table, dry_run)
        for table_name, year in sorted(candle_tables, key=lambda x: x[1]):
            ensure_year_partitions(cursor, candle_table, year, dry_run)

            old_full = f"`{db_name}`.`{table_name}`"
            print(f"    복사 중: {old_full} → {candle_table} ...", end=" ", flush=True)

            if not dry_run:
                old_count = get_row_count(cursor, old_full)
                if old_count == 0:
                    print("(빈 테이블, 건너뜀)")
                    continue

                execute(cursor, f"INSERT INTO {candle_table} SELECT * FROM {old_full}")
                new_count_query = f"SELECT COUNT(*) FROM {candle_table} WHERE YEAR(execution_datetime) = {year}"
                cursor.execute(new_count_query)
                new_count = cursor.fetchone()[0]
                if new_count >= old_count:
                    print(f"OK ({old_count} rows)")
                else:
                    print(f"WARNING: 원본 {old_count}, 복사 {new_count}")
            else:
                print(f"[DRY-RUN] INSERT INTO {candle_table} SELECT * FROM {old_full}")


def drop_old_databases(cursor, db_list, dry_run=False):
    print("\n=== 기존 DB 삭제 ===")
    for db_name in db_list:
        print(f"  DROP DATABASE `{db_name}`")
        execute(cursor, f"DROP DATABASE `{db_name}`", dry_run)
    print("  완료.")


def main():
    dry_run = "--dry-run" in sys.argv
    drop_old = "--drop-old" in sys.argv

    if dry_run:
        print("=== DRY-RUN 모드 (SQL 출력만, 실행 안 함) ===\n")

    conn = get_connection()
    cursor = conn.cursor()

    # 1. tick/candle DB 생성
    print("=== tick/candle DB 생성 ===")
    execute(cursor, "CREATE DATABASE IF NOT EXISTS tick CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'", dry_run)
    execute(cursor, "CREATE DATABASE IF NOT EXISTS candle CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'", dry_run)
    print("  완료.\n")

    # 2. 기존 DB 탐색
    print("=== 기존 DB 탐색 ===")
    coin_dbs, stock_dbs = get_old_databases(cursor)
    print(f"  코인 DB: {coin_dbs}")
    print(f"  주식 DB: {stock_dbs}\n")

    if not coin_dbs and not stock_dbs:
        print("마이그레이션 대상이 없습니다.")
        cursor.close()
        conn.close()
        return

    # 3. 코인 마이그레이션
    if coin_dbs:
        print("=== 코인 마이그레이션 ===")
        for db_name in sorted(coin_dbs):
            migrate_database(cursor, db_name, "Z_Coin", "c", dry_run)
        print()

    # 4. 주식 마이그레이션
    if stock_dbs:
        print("=== 주식 마이그레이션 ===")
        for db_name in sorted(stock_dbs):
            migrate_database(cursor, db_name, "Z_Stock", "s", dry_run)
        print()

    # 5. 기존 DB 삭제 (옵션)
    all_old_dbs = coin_dbs + stock_dbs
    if drop_old and all_old_dbs:
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