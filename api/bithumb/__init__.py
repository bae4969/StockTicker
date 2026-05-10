from core import util
from datetime import datetime as DateTime

from ._sql import BithumbSqlClient
from ._rest import BithumbRestClient
from ._websocket import BithumbWsClient
from . import _tables as tables


class ApiBithumbType:
    __API_BASE_URL: str = "https://api.bithumb.com/public"

    def __init__(self, sql_host: str, sql_port: int, sql_id: str, sql_pw: str, sql_db: str, sql_charset: str):
        self.__sql_main_db = sql_db
        self.__sql = BithumbSqlClient(sql_host, sql_port, sql_id, sql_pw, sql_db, sql_charset)

        tables.create_coin_info_table(self.__sql)
        tables.create_last_ws_query_table(self.__sql)
        self.__sql.start()

        self.__rest = BithumbRestClient()
        self.__ws_query_datetime = DateTime.min
        self.__ws = BithumbWsClient(self.__sql)
        self.__ws.start()

    def StopCollecting(self) -> None:
        self.__ws.stop()
        self.__sql.stop()


    ##########################################################################


    def __sync_coin_info_table(self) -> None:
        try:
            coin_name_dict = self.__rest.get_coin_name_dict()
            coin_price_dict = self.__rest.get_coin_price_dict()

            for key, price_data in coin_price_dict.items():
                if key in coin_name_dict:
                    name_kr = coin_name_dict[key][0]
                    name_en = coin_name_dict[key][1]
                else:
                    name_kr = key
                    name_en = key

                coin_info_dict = {
                    'coin_code': key,
                    'coin_name_kr': name_kr,
                    'coin_name_en': name_en,
                    'coin_price': price_data[0],
                    'coin_amount': price_data[1],
                }

                tables.enqueue_update_coin_info(self.__sql, self.__sql_main_db, coin_info_dict)

            util.InsertLog("ApiBithumb", "N", "Success to update coin info")

        except Exception as e:
            util.InsertLog("ApiBithumb", "E", "Fail to update coin info : " + e.__str__())

    def __sync_partitions(self) -> None:
        select_query = "SELECT coin_code, coin_api_type FROM coin_last_ws_query"
        cursor = self.__sql.execute_sync(select_query)
        sql_query_list = cursor.fetchall()

        this_year = DateTime.now().year
        for sql_query in sql_query_list:
            if sql_query[1] == "transaction":
                tables.create_coin_execution_tables(self.__sql, sql_query[0], this_year)
                tables.create_coin_execution_tables(self.__sql, sql_query[0], this_year + 1)

            elif sql_query[1] == "orderbooksnapshot":
                tables.create_coin_orderbook_tables(self.__sql, sql_query[0], this_year)
                tables.create_coin_orderbook_tables(self.__sql, sql_query[0], this_year + 1)

    def __sync_ws_query_list(self) -> None:
        try:
            select_query = "SELECT coin_code, coin_api_type, coin_api_coin_code FROM coin_last_ws_query"
            cursor = self.__sql.execute_sync(select_query)
            sql_query_list = cursor.fetchall()

            temp_list = []
            for sql_query in sql_query_list:
                temp_list.append({
                    "coin_code": sql_query[0],
                    "coin_api_type": sql_query[1],
                    "coin_api_coin_code": sql_query[2],
                })

            self.__ws.update_subscriptions(temp_list)

        except Exception as e:
            raise Exception(f"Fail to load last websocket query table | {e}")


    ##########################################################################


    def GetCurrentCollectingDateTime(self) -> DateTime:
        return self.__ws_query_datetime


    def SyncPartitions(self) -> None:
        try:
            self.__sync_partitions()
        except Exception as ex:
            util.InsertLog("ApiBithumb", "E", f"Fail to sync partitions for bithumb api [ {ex.__str__()} ] ")

    def SyncDailyInfo(self) -> None:
        try:
            self.__ws_query_datetime = DateTime.now().replace(hour=0, minute=0, second=0)
            self.__sync_ws_query_list()
        except Exception as ex:
            util.InsertLog("ApiBithumb", "E", f"Fail to sync daily info for bithumb api [ {ex.__str__()} ] ")

    def SyncWeeklyInfo(self) -> None:
        try:
            self.__sync_coin_info_table()
        except Exception as ex:
            util.InsertLog("ApiBithumb", "E", f"Fail to sync weekly info for bithumb api [ {ex.__str__()} ] ")
