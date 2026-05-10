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
        "stock_code, stock_name_kr, stock_name_en, stock_market, stock_type, stock_count, stock_price, stock_capitalization"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s"
        ") ON DUPLICATE KEY UPDATE "
        "stock_name_kr=%s, stock_name_en=%s, stock_market=%s, stock_type=%s, "
        "stock_count=%s, stock_price=%s, stock_capitalization=%s",
        (
            stock_info_dict['stock_code'],
            stock_info_dict['stock_name_kr'],
            stock_info_dict['stock_name_en'],
            stock_info_dict['stock_market'],
            stock_info_dict['stock_type'],
            stock_info_dict['stock_count'],
            stock_info_dict['stock_price'],
            stock_info_dict['stock_cap'],
            stock_info_dict['stock_name_kr'],
            stock_info_dict['stock_name_en'],
            stock_info_dict['stock_market'],
            stock_info_dict['stock_type'],
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
