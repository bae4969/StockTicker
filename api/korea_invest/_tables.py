from datetime import datetime as DateTime


def create_stock_info_table(sql_client) -> None:
    try:
        query = (
            "CREATE TABLE IF NOT EXISTS stock_info ("
            + "stock_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "stock_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "stock_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "stock_market VARCHAR(32) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "stock_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "stock_category_code VARCHAR(16) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "stock_category_name VARCHAR(64) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "stock_count BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',"
            + "stock_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
            + "stock_capitalization DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
            + "stock_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + "PRIMARY KEY (stock_code) USING BTREE,"
            + "UNIQUE INDEX stock_code (stock_code) USING BTREE,"
            + "INDEX stock_name (stock_name_kr, stock_name_en) USING BTREE"
            + ")COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
        )
        sql_client.execute_sync(query)
    except Exception as e:
        raise Exception(f"Fail to create stock info table | {e}")


def create_last_ws_query_table(sql_client) -> None:
    try:
        query = (
            "CREATE TABLE IF NOT EXISTS stock_last_ws_query ("
            + "stock_query VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "stock_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "query_type VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "stock_api_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "stock_api_stock_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "PRIMARY KEY (stock_query) USING BTREE,"
            + "INDEX stock_code (stock_code) USING BTREE,"
            + "CONSTRAINT FK_stock_list_last_query_stock_info FOREIGN KEY (stock_code) REFERENCES stock_info (stock_code) ON UPDATE CASCADE ON DELETE CASCADE"
            + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
        )
        sql_client.execute_sync(query)
    except Exception as e:
        raise Exception(f"Fail to create last websocket query table | {e}")


def create_quote_info_table(sql_client) -> None:
    # 지수·환율 카탈로그. 한투 마스터 파일에서 주간 갱신된다 (수집 대상이 아니라 '고를 수 있는 목록').
    try:
        query = (
            "CREATE TABLE IF NOT EXISTS quote_info ("
            + "quote_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "quote_api_code VARCHAR(32) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "quote_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "quote_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "quote_category VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "quote_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + "PRIMARY KEY (quote_code) USING BTREE,"
            + "INDEX quote_category (quote_category) USING BTREE,"
            + "INDEX quote_name (quote_name_kr, quote_name_en) USING BTREE"
            + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
        )
        sql_client.execute_sync(query)
    except Exception as e:
        raise Exception(f"Fail to create quote info table | {e}")


def create_last_rest_query_table(sql_client) -> None:
    # 실제 수집 대상. quote_query 가 저장 식별자이자 테이블명이 된다 (tick.iKOSPI, tick.fKRWEUR).
    #
    # 주식과 달리 카탈로그 코드를 그대로 테이블명에 쓰지 않는다. 원/유로처럼 두 소스를 조합해
    # 만드는 합성 대상은 카탈로그에 대응 행이 없기 때문이다.
    #   quote_code      : 주 소스 (quote_info 참조)
    #   quote_base_code : 환산 기준 소스. 안 쓰면 빈 문자열이라 FK 를 걸지 않는다.
    #   quote_operator  : NONE / MUL / DIV — 원/유로=MUL, 원/위안=DIV
    try:
        query = (
            "CREATE TABLE IF NOT EXISTS quote_last_rest_query ("
            + "quote_query VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "quote_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "quote_base_code VARCHAR(32) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "quote_operator VARCHAR(8) NOT NULL DEFAULT 'NONE' COLLATE 'utf8mb4_general_ci',"
            + "query_type VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "PRIMARY KEY (quote_query) USING BTREE,"
            + "INDEX quote_code (quote_code) USING BTREE,"
            + "CONSTRAINT FK_quote_last_rest_query_quote_info FOREIGN KEY (quote_code) REFERENCES quote_info (quote_code) ON UPDATE CASCADE ON DELETE CASCADE"
            + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
        )
        sql_client.execute_sync(query)
    except Exception as e:
        raise Exception(f"Fail to create last rest query table | {e}")


def enqueue_update_quote_info(sql_client, sql_main_db: str, quote_info_dict: dict) -> None:
    sql_client.enqueue(
        f"INSERT INTO {sql_main_db}.quote_info ("
        "quote_code, quote_api_code, quote_name_kr, quote_name_en, quote_category"
        ") VALUES (%s, %s, %s, %s, %s"
        ") ON DUPLICATE KEY UPDATE "
        "quote_api_code=%s, quote_name_kr=%s, quote_name_en=%s, quote_category=%s",
        (
            quote_info_dict['quote_code'],
            quote_info_dict['quote_api_code'],
            quote_info_dict['quote_name_kr'],
            quote_info_dict['quote_name_en'],
            quote_info_dict['quote_category'],
            quote_info_dict['quote_api_code'],
            quote_info_dict['quote_name_kr'],
            quote_info_dict['quote_name_en'],
            quote_info_dict['quote_category'],
        )
    )


def create_quote_execution_tables(sql_client, quote_id: str, year: int) -> None:
    # 주식 체결 테이블과 동일한 스키마를 쓴다. 지수·환율은 매수/매도 구분이 없어
    # 거래량은 execution_non_volume 한 곳에만 담는다.
    tick_table_name = f"tick.{quote_id}"
    candle_table_name = f"candle.{quote_id}"

    create_tick_db_query = "CREATE DATABASE IF NOT EXISTS tick CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'"
    create_candle_db_query = "CREATE DATABASE IF NOT EXISTS candle CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'"

    create_tick_table_query = (
        f"""CREATE TABLE IF NOT EXISTS {tick_table_name} (
        execution_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        execution_datetime DATETIME NOT NULL,
        execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        PRIMARY KEY (execution_datetime, execution_id) USING BTREE,
        INDEX idx_execution_id (execution_id) USING BTREE
        ) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB
        PARTITION BY RANGE (YEAR(execution_datetime)) (
        PARTITION pmax VALUES LESS THAN MAXVALUE)"""
    )
    create_candle_table_query = (
        f"""CREATE TABLE IF NOT EXISTS {candle_table_name} (
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
        PARTITION pmax VALUES LESS THAN MAXVALUE)"""
    )

    reorganize_partitions = (
        f"PARTITION p{year:04d} VALUES LESS THAN ({year+1:04d}),"
        "PARTITION pmax VALUES LESS THAN MAXVALUE"
    )

    sql_client.execute_sync(create_tick_db_query)
    sql_client.execute_sync(create_candle_db_query)
    sql_client.execute_sync(create_tick_table_query)
    sql_client.execute_sync(create_candle_table_query)

    partition_name = f"p{year:04d}"
    check_query = (
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.PARTITIONS "
        "WHERE PARTITION_NAME = %s AND ("
        "(TABLE_SCHEMA = 'tick' AND TABLE_NAME = %s) OR "
        "(TABLE_SCHEMA = 'candle' AND TABLE_NAME = %s))"
    )
    cursor = sql_client.execute_sync(check_query, (partition_name, quote_id, quote_id))
    if cursor is None:
        existing = set()
    else:
        existing = {(r[0], r[1]) for r in cursor.fetchall()}

    if ("tick", quote_id) not in existing:
        sql_client.execute_sync(
            f"ALTER TABLE {tick_table_name} REORGANIZE PARTITION pmax INTO ({reorganize_partitions})"
        )
    if ("candle", quote_id) not in existing:
        sql_client.execute_sync(
            f"ALTER TABLE {candle_table_name} REORGANIZE PARTITION pmax INTO ({reorganize_partitions})"
        )


def enqueue_update_quote_execution(sql_client, quote_id: str, dt: DateTime, price: float, volume: float) -> None:
    # 지수·환율은 매수/매도 구분이 없으므로 ask/bid 는 0 으로 두고 non 만 채운다.
    raw_table_name = f"tick.{quote_id}"
    candle_table_name = f"candle.{quote_id}"

    datetime_00_min = dt
    datetime_10_min = dt.replace(minute=dt.minute // 10 * 10, second=0)
    price_str = str(price)
    volume_str = str(volume)
    amount_str = str(price * volume)

    sql_client.enqueue(
        f"INSERT INTO {raw_table_name} "
        "(execution_datetime, execution_price, execution_non_volume, execution_ask_volume, execution_bid_volume) "
        "VALUES (%s, %s, %s, 0, 0)",
        (datetime_00_min.strftime("%Y-%m-%d %H:%M:%S"), price_str, volume_str)
    )
    sql_client.enqueue(
        f"INSERT INTO {candle_table_name} VALUES ("
        "%s, %s, %s, %s, %s, %s, 0, 0, %s, 0, 0"
        ") ON DUPLICATE KEY UPDATE "
        "execution_close=%s,"
        "execution_min=LEAST(execution_min,%s),"
        "execution_max=GREATEST(execution_max,%s),"
        "execution_non_volume=execution_non_volume+%s,"
        "execution_non_amount=execution_non_amount+%s",
        (
            datetime_10_min.strftime("%Y-%m-%d %H:%M:%S"),
            price_str, price_str, price_str, price_str,
            volume_str, amount_str,
            price_str, price_str, price_str,
            volume_str, amount_str,
        )
    )


def create_stock_execution_tables(sql_client, stock_code: str, year: int):
    stock_id = "s" + stock_code.replace("/", "_")
    tick_table_name = f"tick.{stock_id}"
    candle_table_name = f"candle.{stock_id}"

    create_tick_db_query = "CREATE DATABASE IF NOT EXISTS tick CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'"
    create_candle_db_query = "CREATE DATABASE IF NOT EXISTS candle CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'"

    create_tick_table_query = (
        f"""CREATE TABLE IF NOT EXISTS {tick_table_name} (
        execution_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        execution_datetime DATETIME NOT NULL,
        execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
        PRIMARY KEY (execution_datetime, execution_id) USING BTREE,
        INDEX idx_execution_id (execution_id) USING BTREE
        ) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB
        PARTITION BY RANGE (YEAR(execution_datetime)) (
        PARTITION pmax VALUES LESS THAN MAXVALUE)"""
    )
    create_candle_table_query = (
        f"""CREATE TABLE IF NOT EXISTS {candle_table_name} (
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
        PARTITION pmax VALUES LESS THAN MAXVALUE)"""
    )

    reorganize_partitions = (
        f"PARTITION p{year:04d} VALUES LESS THAN ({year+1:04d}),"
        "PARTITION pmax VALUES LESS THAN MAXVALUE"
    )

    add_tick_partition_query = (
        f"ALTER TABLE {tick_table_name} REORGANIZE PARTITION pmax INTO ({reorganize_partitions})"
    )
    add_candle_partition_query = (
        f"ALTER TABLE {candle_table_name} REORGANIZE PARTITION pmax INTO ({reorganize_partitions})"
    )

    sql_client.execute_sync(create_tick_db_query)
    sql_client.execute_sync(create_candle_db_query)
    sql_client.execute_sync(create_tick_table_query)
    sql_client.execute_sync(create_candle_table_query)

    partition_name = f"p{year:04d}"
    tick_db, tick_tbl = tick_table_name.split(".")
    candle_db, candle_tbl = candle_table_name.split(".")

    check_query = (
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.PARTITIONS "
        "WHERE PARTITION_NAME = %s AND ("
        "(TABLE_SCHEMA = %s AND TABLE_NAME = %s) OR "
        "(TABLE_SCHEMA = %s AND TABLE_NAME = %s))"
    )
    cursor = sql_client.execute_sync(
        check_query, (partition_name, tick_db, tick_tbl, candle_db, candle_tbl)
    )
    if cursor is None:
        existing = set()
    else:
        existing = {(r[0], r[1]) for r in cursor.fetchall()}

    if (tick_db, tick_tbl) not in existing:
        sql_client.execute_sync(add_tick_partition_query)
    if (candle_db, candle_tbl) not in existing:
        sql_client.execute_sync(add_candle_partition_query)


def create_stock_orderbook_tables(sql_client, stock_code: str, year: int):
    # TODO
    return


def enqueue_update_stock_info(sql_client, sql_main_db: str, stock_info_dict: dict) -> None:
    sql_client.enqueue(
        f"INSERT INTO {sql_main_db}.stock_info ("
        "stock_code, stock_name_kr, stock_name_en, stock_market, stock_type, "
        "stock_category_code, stock_category_name, stock_count, stock_price, stock_capitalization"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        ") ON DUPLICATE KEY UPDATE "
        "stock_name_kr=%s, stock_name_en=%s, stock_market=%s, stock_type=%s, "
        "stock_category_code=%s, stock_category_name=%s, "
        "stock_count=%s, stock_price=%s, stock_capitalization=%s",
        (
            stock_info_dict['stock_code'],
            stock_info_dict['stock_name_kr'],
            stock_info_dict['stock_name_en'],
            stock_info_dict['stock_market'],
            stock_info_dict['stock_type'],
            stock_info_dict['stock_category_code'],
            stock_info_dict['stock_category_name'],
            stock_info_dict['stock_count'],
            stock_info_dict['stock_price'],
            stock_info_dict['stock_cap'],
            stock_info_dict['stock_name_kr'],
            stock_info_dict['stock_name_en'],
            stock_info_dict['stock_market'],
            stock_info_dict['stock_type'],
            stock_info_dict['stock_category_code'],
            stock_info_dict['stock_category_name'],
            stock_info_dict['stock_count'],
            stock_info_dict['stock_price'],
            stock_info_dict['stock_cap'],
        )
    )


def enqueue_update_stock_execution(
    sql_client,
    stock_code: str,
    dt: DateTime,
    price: float,
    non_volume: float,
    ask_volume: float,
    bid_volume: float,
) -> None:
    stock_id = "s" + stock_code.replace("/", "_")
    raw_table_name = f"tick.{stock_id}"
    candle_table_name = f"candle.{stock_id}"

    datetime_00_min = dt
    datetime_10_min = dt.replace(minute=dt.minute // 10 * 10, second=0)
    price_str = str(price)
    non_volume_str = str(non_volume)
    ask_volume_str = str(ask_volume)
    bid_volume_str = str(bid_volume)
    non_amount_str = str(price * non_volume)
    ask_amount_str = str(price * ask_volume)
    bid_amount_str = str(price * bid_volume)

    sql_client.enqueue(
        f"INSERT INTO {raw_table_name} "
        "(execution_datetime, execution_price, execution_non_volume, execution_ask_volume, execution_bid_volume) "
        "VALUES (%s, %s, %s, %s, %s)",
        (datetime_00_min.strftime("%Y-%m-%d %H:%M:%S"), price_str, non_volume_str, ask_volume_str, bid_volume_str)
    )
    sql_client.enqueue(
        f"INSERT INTO {candle_table_name} VALUES ("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        ") ON DUPLICATE KEY UPDATE "
        "execution_close=%s,"
        "execution_min=LEAST(execution_min,%s),"
        "execution_max=GREATEST(execution_max,%s),"
        "execution_non_volume=execution_non_volume+%s,"
        "execution_ask_volume=execution_ask_volume+%s,"
        "execution_bid_volume=execution_bid_volume+%s,"
        "execution_non_amount=execution_non_amount+%s,"
        "execution_ask_amount=execution_ask_amount+%s,"
        "execution_bid_amount=execution_bid_amount+%s",
        (
            datetime_10_min.strftime("%Y-%m-%d %H:%M:%S"),
            price_str, price_str, price_str, price_str,
            non_volume_str, ask_volume_str, bid_volume_str,
            non_amount_str, ask_amount_str, bid_amount_str,
            price_str, price_str, price_str,
            non_volume_str, ask_volume_str, bid_volume_str,
            non_amount_str, ask_amount_str, bid_amount_str,
        )
    )


def update_stock_orderbook(sql_client, stock_code: str, dt: DateTime, data) -> None:
    # TODO
    #무엇을 저장할지, 어떤 방식으로 저장할지 안 정해짐
    return

    table_name = (
        stock_code.replace("/", "_")
        + "_"
        + dt.strftime("%Y%V")
    )

    create_orderbook_table_query_str = (
        "CREATE TABLE IF NOT EXISTS stock_orderbook_" + table_name + " ("
        + "execution_datetime DATETIME NOT NULL,"
        + "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
        + "execution_volume BIGINT(20) UNSIGNED NOT NULL DEFAULT '0'"
        + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
    )
    insert_orderbook_table_query_str = (
        "INSERT INTO stock_orderbook_" + table_name + " VALUES ("
        + "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
        + "'" + data + "'"
        + ")"
    )

    sql_client.execute_sync(create_orderbook_table_query_str)
    sql_client.execute_sync(insert_orderbook_table_query_str)
