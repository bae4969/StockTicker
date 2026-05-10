from datetime import datetime as DateTime


def create_coin_info_table(sql_client) -> None:
    try:
        query = (
            "CREATE TABLE IF NOT EXISTS coin_info ("
            + "coin_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "coin_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "coin_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
            + "coin_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
            + "coin_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
            + "coin_order INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
            + "coin_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
            + "PRIMARY KEY (coin_code) USING BTREE,"
            + "UNIQUE INDEX coin_code (coin_code) USING BTREE,"
            + "INDEX coin_name (coin_name_kr, coin_name_en) USING BTREE,"
            + "INDEX coin_order (coin_order) USING BTREE"
            + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
        )
        sql_client.execute_sync(query)
    except Exception as e:
        raise Exception(f"Fail to create coin info table | {e}")


def create_last_ws_query_table(sql_client) -> None:
    try:
        query = (
            "CREATE TABLE IF NOT EXISTS coin_last_ws_query ("
            + "coin_query VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "coin_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "query_type VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "coin_api_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "coin_api_coin_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
            + "PRIMARY KEY (coin_query) USING BTREE,"
            + "INDEX coin_code (coin_code) USING BTREE,"
            + "CONSTRAINT FK_coin_list_last_query_coin_info FOREIGN KEY (coin_code) REFERENCES coin_info (coin_code) ON UPDATE CASCADE ON DELETE CASCADE"
            + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
        )
        sql_client.execute_sync(query)
    except Exception as e:
        raise Exception(f"Fail to create last websocket query table | {e}")


def create_coin_execution_tables(sql_client, coin_code: str, year: int) -> None:
    coin_id = "c" + coin_code.replace("/", "_")
    tick_table_name = f"tick.{coin_id}"
    candle_table_name = f"candle.{coin_id}"

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


def create_coin_orderbook_tables(sql_client, coin_code: str, year: int) -> None:
    # TODO
    return


def enqueue_update_coin_info(sql_client, sql_main_db: str, coin_info_dict: dict) -> None:
    sql_client.enqueue(
        f"INSERT INTO {sql_main_db}.coin_info ("
        "coin_code, coin_name_kr, coin_name_en, coin_price, coin_amount"
        ") VALUES (%s, %s, %s, %s, %s"
        ") ON DUPLICATE KEY UPDATE "
        "coin_name_kr=%s, coin_name_en=%s, coin_price=%s, coin_amount=%s",
        (
            coin_info_dict['coin_code'],
            coin_info_dict['coin_name_kr'],
            coin_info_dict['coin_name_en'],
            coin_info_dict['coin_price'],
            coin_info_dict['coin_amount'],
            coin_info_dict['coin_name_kr'],
            coin_info_dict['coin_name_en'],
            coin_info_dict['coin_price'],
            coin_info_dict['coin_amount'],
        )
    )


def enqueue_update_coin_execution(
    sql_client,
    coin_code: str,
    dt: DateTime,
    price: float,
    non_volume: float,
    ask_volume: float,
    bid_volume: float,
) -> None:
    coin_id = "c" + coin_code.replace("/", "_")
    raw_table_name = f"tick.{coin_id}"
    candle_table_name = f"candle.{coin_id}"

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


def update_coin_orderbook(sql_client, coin_code: str, dt: DateTime, data) -> None:
    # TODO
    #무엇을 저장할지, 어떤 방식으로 저장할지 안 정해짐
    return

    table_name = (
        coin_code.replace("/", "_")
        + "_"
        + dt.strftime("%Y%V")
    )
    orderbook_table_query_str = (
        "CREATE TABLE IF NOT EXISTS coin_orderbook_" + table_name + " ("
        + "execution_datetime DATETIME NOT NULL,"
        + "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
        + "execution_volume BIGINT(20) UNSIGNED NOT NULL DEFAULT '0'"
        + ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
    )

    sql_client.execute_sync(orderbook_table_query_str)

    table_name = coin_code.replace("/", "_")
    orderbook_table_query_str = (
        "INSERT INTO coin_orderbook_" + table_name + " VALUES ("
        + "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
        + "'" + data + "'"
        + ")"
    )

    sql_client.execute_sync(orderbook_table_query_str)
