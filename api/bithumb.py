from core import config
from core import util
import requests
from websocket import WebSocketApp
import pymysql
import json
import asyncio
import aiomysql
from threading import Thread, Event
import time
from datetime import datetime as DateTime


class ApiBithumbType:
    __sql_main_db:str
    __sql_common_connection:pymysql.Connection = None

    __API_BASE_URL:str = "https://api.bithumb.com/public"

    __WS_BASE_URL:str = "wss://pubwss.bithumb.com/pub/ws"
    __ws_app:WebSocketApp = None
    __ws_thread:Thread = None
    __ws_is_opened = False
    __ws_keep_connect = True
    __ws_query_list = []

    __ws_query_datetime = DateTime.min


    ##########################################################################


    def __init__(self, sql_host:str, sql_port:int, sql_id:str, sql_pw:str, sql_db:str, sql_charset:str):
        self.__sql_main_db = sql_db
        self.__sql_query_config = {
            'host': sql_host,
            'port': sql_port,
            'user': sql_id,
            'password': sql_pw,
            'charset': sql_charset,
            'autocommit': True,
        }
        self.__sql_common_connection = pymysql.connect(
            host = sql_host,
            port = sql_port,
            user = sql_id,
            passwd = sql_pw,
            db = sql_db,
            charset = sql_charset,
            autocommit=True
        )
        config.set_session_timeouts(self.__sql_common_connection)

        self.__create_coin_info_table()
        self.__create_last_ws_query_table()
        self.__start_dequeue_sql_query()
  
        self.__create_ws_app_info()

    def __del__(self):
        self.__ws_keep_connect = False
        self.__ws_app.close()
        self.__stop_dequeue_sql_query()

    def __enqueue_sql(self, query:str, params=None) -> None:
        if params is not None:
            self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, (query, params))
        else:
            self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, query)

    def __reconnect(self) -> None:
        self.__sql_common_connection.ping(reconnect=True)
        config.set_session_timeouts(self.__sql_common_connection)

    def __execute_sync_query(self, query:str):
        for attempt in range(1, config.SQL_MAX_RETRY + 1):
            try:
                self.__reconnect()
                cursor = self.__sql_common_connection.cursor()
                cursor.execute(query)
                return cursor
            except Exception as ex:
                if config.is_retryable_error(ex) and attempt < config.SQL_MAX_RETRY:
                    util.InsertLog("ApiBithumb", "E", f"Sync query retry #{attempt} [ {ex.args[0]} ]")
                    time.sleep(min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX))
                    continue
                raise


    def __create_coin_info_table(self) -> None:
        try:
            table_query_str = (
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

            self.__execute_sync_query(table_query_str)

        except: raise Exception("Fail to create coin info table")

    def __create_last_ws_query_table(self) -> None:
        try:
            create_table_query = (
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
            
            self.__execute_sync_query(create_table_query)

        except: raise Exception("Fail to create last websocket query table")


    def __create_ws_app_info(self) -> None:
        self.__ws_app = WebSocketApp(
            url = self.__WS_BASE_URL,
            on_message= self.__on_ws_recv_message,
            on_open= self.__on_ws_open,
            on_close= self.__on_ws_close,
            )
        self.__ws_thread = Thread(
            name="Bithumb_WS",
            target=self.__ws_app.run_forever
        )
        self.__ws_is_opened = False
        self.__ws_thread.daemon = True
        self.__ws_thread.start()
        self.__ws_keep_connect = True
        self.__ws_query_list = []


    def __get_coin_name_dict(self) -> dict:
        # 항목은 빗썸에서 한글 이름은 업비트에서 가져옴
        upbit_rep = requests.get(url = "https://api.upbit.com/v1/market/all?isDetails=true")
        upbit_rep_json = json.loads(upbit_rep.text)
  
        coin_name_dict = {}
        for data in upbit_rep_json:
            try:
                from_to = data["market"].split("-")
                if from_to[0] != "KRW": continue
                coin_name_dict[from_to[1]] = [
                    data["korean_name"],
                       data["english_name"],
                  ]
            except:
                continue

        return coin_name_dict

    def __get_coin_price_dict(self) -> dict:
        bithumb_rep = requests.get(url = "https://api.bithumb.com/public/ticker/ALL_KRW")
        bithumb_rep_json = json.loads(bithumb_rep.text)
        if bithumb_rep_json["status"] != "0000":
            raise Exception("BITHUMB REQUEST ERROR")
        data_list = bithumb_rep_json["data"]

        coin_price_dict = {}
        for key, val in data_list.items():
            if key == "date": continue
   
            coin_price_dict[key] = [
                val["closing_price"],
                val["acc_trade_value_24H"],
            ]
   
        return coin_price_dict


    ##########################################################################
 

    def __start_dequeue_sql_query(self) -> None:
        self.__ready_event = Event()
        self.__loop = asyncio.new_event_loop()
        self.__async_thread = Thread(name="Bithumb_Async", target=self.__run_async_loop, daemon=True)
        self.__async_thread.start()
        self.__ready_event.wait()
        
    def __stop_dequeue_sql_query(self) -> None:
        self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, None)
        self.__async_thread.join()

    def __run_async_loop(self) -> None:
        asyncio.set_event_loop(self.__loop)
        self.__loop.run_until_complete(self.__async_main())

    async def __async_main(self) -> None:
        self.__sql_query_queue = asyncio.Queue()
        retry_count = 0
        while True:
            try:
                self.__sql_pool = await aiomysql.create_pool(**self.__sql_query_config)
                break
            except Exception as ex:
                retry_count += 1
                util.InsertLog("ApiBithumb", "E", f"Fail to create sql pool, retry #{retry_count} [ {ex.__str__()} ]")
                await asyncio.sleep(2)
        self.__ready_event.set()
        await self.__async_dequeue()
        self.__sql_pool.close()
        await self.__sql_pool.wait_closed()

    async def __async_dequeue(self) -> None:
        while True:
            item = await self.__sql_query_queue.get()
            if item is None:
                break

            if isinstance(item, tuple):
                query, params = item
            else:
                query, params = item, None

            for attempt in range(1, config.SQL_MAX_RETRY + 1):
                try:
                    async with self.__sql_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            await cursor.execute(query, params)
                    break
                except Exception as ex:
                    if config.is_retryable_error(ex) and attempt < config.SQL_MAX_RETRY:
                        util.InsertLog("ApiBithumb", "E", f"DB query retry #{attempt} [ {ex.args[0]} ]")
                        await asyncio.sleep(min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX))
                        continue

                    util.InsertLog("ApiBithumb", "E", f"Fail to execute sql query [ {ex.__str__()} ]")
                    break


    def __update_coin_info_table(self, coin_info_dict:dict) -> None:
        self.__enqueue_sql(
            f"INSERT INTO {self.__sql_main_db}.coin_info ("
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
  
    def __update_coin_execution_table(self, coin_code:str, dt:DateTime, price:float, non_volume:float, ask_volume:float, bid_volume:float) -> None:
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
        
        self.__enqueue_sql(
            "INSERT INTO " + raw_table_name + " VALUES ("
            + "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
            + "'" + price_str + "',"
            + "'" + non_volume_str + "',"
            + "'" + ask_volume_str + "',"
            + "'" + bid_volume_str + "' "
            + ")"
        )
        self.__enqueue_sql(
            "INSERT INTO " + candle_table_name + " VALUES ("
            + "'" + datetime_10_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
            + "'" + price_str + "',"
            + "'" + price_str + "',"
            + "'" + price_str + "',"
            + "'" + price_str + "',"
            + "'" + non_volume_str + "',"
            + "'" + ask_volume_str + "',"
            + "'" + bid_volume_str + "',"
            + "'" + non_amount_str + "',"
            + "'" + ask_amount_str + "',"
            + "'" + bid_amount_str + "' "
            + ") ON DUPLICATE KEY UPDATE "
            + "execution_close='" + price_str + "',"
            + "execution_min=LEAST(execution_min,'" + price_str + "'),"
            + "execution_max=GREATEST(execution_max,'" + price_str + "'),"
            + "execution_non_volume=execution_non_volume+'" + non_volume_str + "',"
            + "execution_ask_volume=execution_ask_volume+'" + ask_volume_str + "',"
            + "execution_bid_volume=execution_bid_volume+'" + bid_volume_str + "',"
            + "execution_non_amount=execution_non_amount+'" + non_amount_str + "',"
            + "execution_ask_amount=execution_ask_amount+'" + ask_amount_str + "',"
            + "execution_bid_amount=execution_bid_amount+'" + bid_amount_str + "'"
        )

    def __update_coin_orderbook_table(self, coin_code:str, dt:DateTime, data) -> None:
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

        self.__execute_sync_query(orderbook_table_query_str)

        table_name = coin_code.replace("/", "_")
        orderbook_table_query_str = (
            "INSERT INTO coin_orderbook_" + table_name + " VALUES ("
            + "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
            + "'" + data + "'"
            + ")"
        )

        self.__execute_sync_query(orderbook_table_query_str)


    def __create_coin_execution_table(self, coin_code:str, year:int):
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
            PRIMARY KEY (execution_id, execution_datetime)
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

        self.__enqueue_sql(create_tick_db_query)
        self.__enqueue_sql(create_candle_db_query)
        self.__enqueue_sql(create_tick_table_query)
        self.__enqueue_sql(create_candle_table_query)
        self.__enqueue_sql(add_tick_partition_query)
        self.__enqueue_sql(add_candle_partition_query)
    
    def __create_coin_orderbook_table(self, coin_code:str, year:int):
        # TODO
        return


    ##########################################################################
  

    def __on_recv_coin_execution(self, msg_json:json) -> None:
        for data in msg_json["content"]["list"]:
            try:
                from_to = data["symbol"].split("_")
                dt_str = data["contDtm"].split(".")

                coin_code = from_to[0]
                dt = DateTime.strptime(dt_str[0], "%Y-%m-%d %H:%M:%S")
                price = float(data["contPrice"])
                volume = float(data["contQty"])

                if data["buySellGb"] == "1":
                    self.__update_coin_execution_table(coin_code, dt, price, 0, volume, 0)
                elif data["buySellGb"] == "2":
                    self.__update_coin_execution_table(coin_code, dt, price, 0, 0, volume)
                else:
                    self.__update_coin_execution_table(coin_code, dt, price, volume, 0, 0)

            except Exception as e:
                raise Exception("[ coin execution ][ %s ][ %s ]"%(coin_code, e.__str__()))

    def __on_recv_coin_orderbook(self, msg_json:json) -> None:
        return
        # {
        # 	"type": "orderbooksnapshot",
        # 	"content": {			//매수,매도 30호가 제공
        # 		"symbol": "BTC_KRW",
        # 		"datetime": "1680082623245840",
        # 		"asks": [
        # 			[
        # 				"37458000",	//호가
        # 				"0.9986"	//잔량
        # 			],
        # 			["37461000","0.0487"],
        # 			["37462000","0.9896"],
        # 			["37464000","0.2296"],
        # 			["37466000","0.1075"]
        # 		],
        # 		"bids": [
        # 			[
        # 				"37452000",
        # 				"0.0115"
        # 			],
        # 			["37450000","0.0614"],
        # 			["37448000","0.0991"],
        # 			["37447000","0.0002"],
        # 			["37445000","0.1249"]
        # 		]
        # 	}
        # }
    

    def __on_ws_recv_message(self, ws:WebSocketApp, msg:str) -> None:
        try:
            msg_json = json.loads(msg)

            if "status" in msg_json:
                if msg_json["status"] != "0000":
                    status = msg_json["status"]
                    msg = msg_json["resmsg"]
                    util.InsertLog("ApiBithumb", "E", f"Fail msg : [ {status}:{msg} ]")

            elif "type" in msg_json:
                if msg_json["type"] == "transaction":
                    Thread(name="Bithumb_Execution", target=self.__on_recv_coin_execution, args=(msg_json,)).start()
                elif msg_json["type"] == "orderbooksnapshot":
                    Thread(name="Bithumb_Orderbook", target=self.__on_recv_coin_orderbook, args=(msg_json,)).start()

        except Exception as e:
            util.InsertLog("ApiBithumb", "E", "Fail to process ws recv msg : " + e.__str__())
    
    def __on_ws_open(self, ws:WebSocketApp) -> None:
        self.__ws_is_opened = True
        
        buf_dict = {}
        for ws_query in self.__ws_query_list:
            if ws_query["coin_api_type"] in buf_dict:
                buf_dict[ws_query["coin_api_type"]].append(ws_query["coin_api_coin_code"])
            else:
                buf_dict[ws_query["coin_api_type"]] = [ws_query["coin_api_coin_code"]]
            
        for key, val in buf_dict.items():
            try:
                msg = {
                    "type" : key,
                    "symbols" : val,
                }
                self.__ws_app.send(json.dumps(msg))

            except Exception as e:
                util.InsertLog("ApiBithumb", "E", f"Fail to process ws send msg [ Bithumb_WS | {e.__str__()} ]")

            time.sleep(0.25)

        util.InsertLog("ApiBithumb", "N", "Opened bithumb websocket [ Bithumb_WS ]")

    def __on_ws_close(self, ws:WebSocketApp, close_code, close_msg) -> None:
        self.__ws_is_opened = False
  
        while self.__ws_keep_connect == True:
            time.sleep(1)
            try:
                self.__ws_app = WebSocketApp(
                    url = self.__WS_BASE_URL,
                    on_message= self.__on_ws_recv_message,
                    on_open= self.__on_ws_open,
                    on_close= self.__on_ws_close,
                    )
                self.__ws_thread = Thread(
                    name="Bithumb_WS",
                    target=self.__ws_app.run_forever
                )
                self.__ws_is_opened = False
                self.__ws_thread.daemon = True
                self.__ws_thread.start()
                self.__ws_keep_connect = True
    
                util.InsertLog("ApiBithumb", "N", f"Reconnected bithumb websocket [ Bithumb_WS ]")
                break
    
            except Exception as ex:
                util.InsertLog("ApiBithumb", "E", f"Fail to reconnect korea bithumb websocket [ Bithumb_WS | {ex.__str__()} ]")
    
        util.InsertLog("ApiBithumb", "N", "Closed bithumb websocket [ Bithumb_WS ]")


    ##########################################################################


    def __sync_coin_info_table(self) -> None:
        try:
            coin_name_dict = self.__get_coin_name_dict()
            coin_price_dict = self.__get_coin_price_dict()
   
            for key, price_data in coin_price_dict.items():
                if key in coin_name_dict:
                    name_kr = coin_name_dict[key][0]
                    name_en = coin_name_dict[key][1]
                else:
                    name_kr = key
                    name_en = key
    
                coin_info_dict = {
                    'coin_code' : key,
                    'coin_name_kr' : name_kr,
                    'coin_name_en' : name_en,
                    'coin_price' : price_data[0],
                    'coin_amount' : price_data[1]
                }
    
                self.__update_coin_info_table(coin_info_dict)

                
            util.InsertLog("ApiBithumb", "N", "Success to update coin info")

        except Exception as e: 
            util.InsertLog("ApiBithumb", "E", "Fail to update coin info : " + e.__str__())

    def __sync_ws_query_list(self) -> None:
        try:
            select_query = "SELECT coin_code, coin_api_type, coin_api_coin_code FROM coin_last_ws_query"
            cursor = self.__execute_sync_query(select_query)
            sql_query_list = cursor.fetchall()
   
            this_year = DateTime.now().year
            for sql_query in sql_query_list:
                if sql_query[1] == "transaction":
                    self.__create_coin_execution_table(sql_query[0], this_year)
                    self.__create_coin_execution_table(sql_query[0], this_year + 1)
     
                elif sql_query[1] == "orderbooksnapshot":
                    self.__create_coin_orderbook_table(sql_query[0], this_year)
                    self.__create_coin_orderbook_table(sql_query[0], this_year + 1)
     
                else:
                    continue

            temp_list = []
            for sql_query in sql_query_list:
                temp_list.append({
                    "coin_code" : sql_query[0],
                    "coin_api_type" : sql_query[1],
                    "coin_api_coin_code" : sql_query[2]
                })
    
            self.__ws_query_list = temp_list
            self.__ws_app.close()

        except: raise Exception("Fail to load last websocket query table")
 

    ##########################################################################


    def GetCurrentCollectingDateTime(self) -> DateTime:
        return self.__ws_query_datetime


    def SyncDailyInfo(self) -> None:
        try:
            self.__ws_query_datetime = DateTime.now().replace(hour=0, minute=0, second=0)
            self.__sync_ws_query_list()
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync daily info for bithumb api [ {ex.__str__()} ] ")
   
    def SyncWeeklyInfo(self) -> None:
        try:
            self.__sync_coin_info_table()
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync weekly info for bithumb api [ {ex.__str__()} ] ")




