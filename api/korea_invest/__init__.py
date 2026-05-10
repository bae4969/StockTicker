from core import util
from datetime import datetime as DateTime
from threading import Thread
import os
import glob
import time

from ._sql import KoreaInvestSqlClient
from ._rest import KoreaInvestRestClient
from ._websocket import KoreaInvestWsClient
from . import _tables as tables
from . import _master as master


class ApiKoreaInvestType:
    def __init__(self, sql_host: str, sql_port: int, sql_id: str, sql_pw: str, sql_db: str, sql_charset: str, api_key_list: list):
        self.__sql_main_db = sql_db
        self.__sql = KoreaInvestSqlClient(sql_host, sql_port, sql_id, sql_pw, sql_db, sql_charset)

        tables.create_stock_info_table(self.__sql)
        tables.create_last_ws_query_table(self.__sql)
        self.__sql.start()

        self.__rest = KoreaInvestRestClient(api_key_list)
        self.__ws_query_type: str = ""
        self.__ws = KoreaInvestWsClient(self.__sql, self.__rest, api_key_list)
        self.__ws.start()

    def StopCollecting(self) -> None:
        self.__ws.stop()
        self.__sql.stop()


    ##########################################################################


    def __sync_stock_info_table(self) -> None:
        try:
            if not os.path.exists("./temp"):
                os.makedirs("./temp")
            else:
                files = glob.glob('./temp/*')
                for f in files:
                    os.remove(f)

            stock_code_list = {
                "KOSPI" : master.get_kospi_stock_list(),
                "KOSDAQ" : master.get_kosdaq_stock_list(),
                "KONEX" : master.get_konex_stock_list(),
                "NASDAQ" : master.get_nasdaq_stock_list(),
                "NYSE" : master.get_nyse_stock_list(),
                "AMEX" : master.get_amex_stock_list(),
            }

            rest_api_token_list = self.__rest.get_token_list()
            temp_market_code_list = [[] for _ in range(len(rest_api_token_list))]
            token_idx = 0
            for stock_market, stock_code_infos in stock_code_list.items():
                for stock_type, stock_code_list in stock_code_infos.items():
                    for stock_code in stock_code_list:
                        temp_market_code_list[token_idx].append([stock_market, stock_type, stock_code])
                        token_idx += 1
                        if token_idx >= len(rest_api_token_list):
                            token_idx = 0

            def kernel_func(rest_api_token: dict, stock_market_code_list: list) -> None:
                for stock_market_code in stock_market_code_list:
                    try:
                        min_micro = 1000000. / self.__rest.MAX_REST_API_COUNT_PER_KEY + self.__rest.REST_API_DELAY_MICRO
                        start_dt = DateTime.now()
                        stock_market = stock_market_code[0]
                        stock_type = stock_market_code[1]
                        stock_code = stock_market_code[2]
                        rest_api_token_header = rest_api_token["TOKEN_HEADER"]

                        if stock_market in ["KOSPI", "KOSDAQ", "KONEX"]:
                            min_micro *= 1
                            stock_info_dict = self.__rest.kr_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
                            tables.enqueue_update_stock_info(self.__sql, self.__sql_main_db, stock_info_dict)
                        elif stock_market in ["NASDAQ", "NYSE", "AMEX"]:
                            min_micro *= 2
                            stock_info_dict = self.__rest.ex_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
                            tables.enqueue_update_stock_info(self.__sql, self.__sql_main_db, stock_info_dict)
                        else:
                            continue

                        diff_micro = (DateTime.now() - start_dt).microseconds
                        if min_micro > diff_micro:
                            time.sleep((min_micro - diff_micro) / 1000000.)

                    except Exception as e:
                        util.InsertLog("ApiKoreaInvest", "E", f"Fail to update stock info [ {stock_market} | {stock_code} | {e.__str__()}]")


            temp_thread_list = []
            for idx in range(len(rest_api_token_list)):
                t_thread = Thread(
                    name= f"KoreaInvest_Update_Stock_Info_{idx}",
                    target= kernel_func,
                    args=(rest_api_token_list[idx], temp_market_code_list[idx])
                   )
                t_thread.daemon = True
                t_thread.start()
                temp_thread_list.append(t_thread)

            for temp_thread in temp_thread_list:
                temp_thread.join()


            util.InsertLog("ApiKoreaInvest", "N", "Success to update stock info")

        except Exception as e:
            util.InsertLog("ApiKoreaInvest", "E", "Fail to update stock info : " + e.__str__())

    def __sync_ws_query_list(self) -> None:
        try:
            if self.__ws_query_type == "KR":
                select_query = (
                    "SELECT "
                    + "L.stock_code, I.stock_market, L.stock_api_type, L.stock_api_stock_code "
                    + "FROM stock_last_ws_query AS L "
                    + "JOIN stock_info AS I "
                    + "ON L.stock_code = I.stock_code "
                    + "WHERE I.stock_market='KOSPI' "
                     + "OR I.stock_market='KOSDAQ' "
                     + "OR I.stock_market='KONEX'"
                )
            elif self.__ws_query_type == "EX":
                select_query = (
                    "SELECT "
                    + "L.stock_code, I.stock_market, L.stock_api_type, L.stock_api_stock_code "
                    + "FROM stock_last_ws_query AS L "
                    + "JOIN stock_info AS I "
                    + "ON L.stock_code = I.stock_code "
                    + "WHERE I.stock_market='NYSE' "
                     + "OR I.stock_market='NASDAQ' "
                     + "OR I.stock_market='AMEX'"
                )
            else:
                raise Exception(f"Invalid ws_query_type [ {self.__ws_query_type} ]")

            cursor = self.__sql.execute_sync(select_query)
            sql_query_list = cursor.fetchall()

            self.__ws.update_subscriptions(sql_query_list)

        except Exception as e: raise Exception(f"Fail to sync websocket query list | {e}")


    ##########################################################################


    def GetCurrentCollectingType(self) -> str:
        return self.__ws_query_type


    def SyncPartitions(self) -> None:
        try:
            for select_query in [
                "SELECT L.stock_code, L.stock_api_type FROM stock_last_ws_query AS L "
                "JOIN stock_info AS I ON L.stock_code = I.stock_code "
                "WHERE I.stock_market IN ('KOSPI','KOSDAQ','KONEX')",
                "SELECT L.stock_code, L.stock_api_type FROM stock_last_ws_query AS L "
                "JOIN stock_info AS I ON L.stock_code = I.stock_code "
                "WHERE I.stock_market IN ('NYSE','NASDAQ','AMEX')",
            ]:
                cursor = self.__sql.execute_sync(select_query)
                sql_query_list = cursor.fetchall()

                this_year = DateTime.now().year
                for sql_query in sql_query_list:
                    if sql_query[1] in ("H0STCNT0", "HDFSCNT0"):
                        tables.create_stock_execution_tables(self.__sql, sql_query[0], this_year)
                        tables.create_stock_execution_tables(self.__sql, sql_query[0], this_year + 1)
                    elif sql_query[1] in ("H0STASP0", "HDFSASP0"):
                        tables.create_stock_orderbook_tables(self.__sql, sql_query[0], this_year)
                        tables.create_stock_orderbook_tables(self.__sql, sql_query[0], this_year + 1)
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync partitions for korea invest api [ {ex.__str__()} ] ")

    def SyncDailyInfo(self, target_market: str) -> None:
        try:
            self.__ws_query_type = target_market
            self.__rest.sync_token_list()
            self.__sync_ws_query_list()
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync daily info for korea invest api [ {ex.__str__()} ] ")

    def SyncWeeklyInfo(self) -> None:
        try:
            self.__rest.sync_token_list()
            self.__sync_stock_info_table()
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync weekly info for korea invest api [ {ex.__str__()} ] ")
