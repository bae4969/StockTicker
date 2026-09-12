#!/usr/bin/env python3
"""stock_info에 마스터 기반 대분류를 추가하고 기존 행을 채운다.

이 스크립트는 종목 마스터 파일만 다운로드하며 KIS REST API나 토큰을 사용하지 않는다.
마스터 파서가 `./temp`를 작업 폴더로 쓰므로 정규 주간 싱크와 동시에 실행하지 않는다.

사용법:
    python scripts/add_stock_categories.py --dry-run
    python scripts/add_stock_categories.py --apply
"""

import argparse
import os
import sys
import time
from collections import Counter
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api.korea_invest import _master as master
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
                db=config.SQL_KI_DB,
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
            print(f"DB 연결 실패({ex.args[0]}): {wait_sec}초 후 재시도")
            time.sleep(wait_sec)
    if last_ex is not None:
        raise last_ex
    raise RuntimeError("DB 연결 시도 중 알 수 없는 오류가 발생했습니다.")


def run_db(conn, action):
    """짧은 DB 작업 하나를 프로젝트 공통 재시도 규칙으로 실행한다."""
    for attempt in range(1, config.SQL_MAX_RETRY + 1):
        cursor = conn.cursor()
        try:
            return action(cursor)
        except Exception as ex:
            if not config.is_retryable_error(ex) or attempt >= config.SQL_MAX_RETRY:
                raise
            wait_sec = min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX)
            print(f"DB 작업 실패({ex.args[0]}): {wait_sec}초 후 재시도")
            time.sleep(wait_sec)
            conn.ping(reconnect=True)
            config.set_session_timeouts(conn)
        finally:
            cursor.close()


def build_catalog() -> dict:
    """`(시장, 종목코드) -> (카테고리코드, 카테고리명)` 마스터."""
    print("종목 마스터 다운로드·분류 중...")
    kr_category_names = dict(master.get_kr_index_list())
    markets = (
        ("KOSPI", master.get_kospi_stock_list(kr_category_names)),
        ("KOSDAQ", master.get_kosdaq_stock_list(kr_category_names)),
        ("KONEX", master.get_konex_stock_list(kr_category_names)),
        ("NASDAQ", master.get_nasdaq_stock_list()),
        ("NYSE", master.get_nyse_stock_list()),
        ("AMEX", master.get_amex_stock_list()),
    )

    catalog = {}
    for market, stock_types in markets:
        # STOCK과 ETF가 겹치는 마스터 행은 뒤의 ETF/ETN 분류가 이긴다.
        for entries in stock_types.values():
            for stock_code, category_code, category_name in entries:
                catalog[(market, stock_code)] = (category_code, category_name)
    return catalog


def category_columns(cursor) -> set:
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='stock_info'",
        (config.SQL_KI_DB,),
    )
    return {row[0] for row in cursor.fetchall()}


def alter_sql(columns: set) -> str | None:
    parts = []
    if "stock_category_code" not in columns:
        parts.append(
            "ADD COLUMN stock_category_code VARCHAR(16) NOT NULL DEFAULT '' "
            "COLLATE 'utf8mb4_general_ci' AFTER stock_type"
        )
    if "stock_category_name" not in columns:
        parts.append(
            "ADD COLUMN stock_category_name VARCHAR(64) NOT NULL DEFAULT '' "
            "COLLATE 'utf8mb4_general_ci' AFTER stock_category_code"
        )
    if not parts:
        return None
    return f"ALTER TABLE `{config.SQL_KI_DB}`.`stock_info` " + ", ".join(parts)


def print_coverage(cursor, catalog: dict, subscribed_only: bool) -> None:
    if subscribed_only:
        cursor.execute(
            "SELECT DISTINCT I.stock_code, I.stock_market, I.stock_type "
            "FROM stock_info AS I JOIN stock_last_ws_query AS Q ON Q.stock_code=I.stock_code"
        )
        label = "현재 구독"
    else:
        cursor.execute("SELECT stock_code, stock_market, stock_type FROM stock_info")
        label = "stock_info 전체"

    rows = cursor.fetchall()
    counts = Counter()
    missing = 0
    for stock_code, market, _stock_type in rows:
        category_code, category_name = catalog.get((market, stock_code), ("", ""))
        name = category_name or "미분류"
        counts[name] += 1
        if not category_code or not category_name:
            missing += 1

    print(f"{label}: {len(rows):,}행 / 분류 {len(rows) - missing:,} / 미분류 {missing:,}")
    if subscribed_only:
        print("  " + ", ".join(f"{name} {count}" for name, count in counts.most_common()))


def apply_backfill(cursor, catalog: dict) -> int:
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_stock_category")
    cursor.execute(
        "CREATE TEMPORARY TABLE tmp_stock_category ("
        "stock_market VARCHAR(32) COLLATE utf8mb4_general_ci NOT NULL, "
        "stock_code VARCHAR(16) COLLATE utf8mb4_general_ci NOT NULL, "
        "category_code VARCHAR(16) COLLATE utf8mb4_general_ci NOT NULL, "
        "category_name VARCHAR(64) COLLATE utf8mb4_general_ci NOT NULL, "
        "PRIMARY KEY (stock_market, stock_code)) ENGINE=InnoDB "
        "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
    )
    rows = [
        (market, stock_code, category_code, category_name)
        for (market, stock_code), (category_code, category_name) in catalog.items()
    ]
    cursor.executemany(
        "INSERT INTO tmp_stock_category "
        "(stock_market, stock_code, category_code, category_name) VALUES (%s, %s, %s, %s)",
        rows,
    )
    cursor.execute(
        f"UPDATE `{config.SQL_KI_DB}`.`stock_info` AS I "
        "JOIN tmp_stock_category AS C "
        "ON C.stock_market=I.stock_market AND C.stock_code=I.stock_code "
        "SET I.stock_category_code=C.category_code, I.stock_category_name=C.category_name "
        "WHERE I.stock_category_code<>C.category_code OR I.stock_category_name<>C.category_name"
    )
    return cursor.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="stock_info 종목 카테고리 컬럼 추가·백필")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="현황과 SQL만 확인")
    mode.add_argument("--apply", action="store_true", help="ALTER와 백필을 실제 실행")
    args = parser.parse_args()

    os.chdir(ROOT)
    catalog = build_catalog()
    classified = sum(bool(name) for _, name in catalog.values())
    print(f"마스터: {len(catalog):,}종목 / 분류 {classified:,} / 미분류 {len(catalog) - classified:,}")

    conn = get_connection()
    try:
        columns = run_db(conn, category_columns)
        ddl = alter_sql(columns)
        run_db(conn, lambda cursor: print_coverage(cursor, catalog, subscribed_only=False))
        run_db(conn, lambda cursor: print_coverage(cursor, catalog, subscribed_only=True))

        if ddl:
            print("적용할 DDL:")
            print("  " + ddl)
        else:
            print("카테고리 컬럼: 이미 존재")

        print(f"백필 후보: 마스터 {len(catalog):,}행을 임시 테이블로 넣어 시장+종목코드로 갱신")
        if args.dry_run:
            print("[dry-run] 실제 DB 변경 없이 종료합니다.")
            return

        if ddl:
            run_db(conn, lambda cursor: cursor.execute(ddl))
            print("카테고리 컬럼 추가 완료")
        changed = run_db(conn, lambda cursor: apply_backfill(cursor, catalog))
        print(f"카테고리 백필 완료: {changed:,}행 변경")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
