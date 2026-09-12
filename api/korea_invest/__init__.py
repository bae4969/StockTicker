from core import config
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
    __QUOTE_POLL_INTERVAL_SEC: int = 60

    def __init__(self, sql_host: str, sql_port: int, sql_id: str, sql_pw: str, sql_db: str, sql_charset: str, api_key_list: list):
        self.__sql_main_db = sql_db
        self.__sql = KoreaInvestSqlClient(sql_host, sql_port, sql_id, sql_pw, sql_db, sql_charset)

        tables.create_stock_info_table(self.__sql)
        tables.create_last_ws_query_table(self.__sql)
        tables.create_quote_info_table(self.__sql)
        tables.create_last_rest_query_table(self.__sql)
        self.__sql.start()

        self.__rest = KoreaInvestRestClient(api_key_list)
        self.__ws_query_type: str = ""
        self.__ws = KoreaInvestWsClient(self.__sql, self.__rest, api_key_list)
        self.__ws.start()

        # 지수·환율은 실시간 WS 가 없어 REST 폴링으로 모은다.
        self.__quote_keep_polling = True
        self.__quote_query_list: list = []
        self.__quote_last_stored_dict: dict = {}
        self.__quote_thread = Thread(name="KoreaInvest_Quote_Polling", target=self.__run_quote_polling)
        self.__quote_thread.daemon = True
        self.__quote_thread.start()

    def StopCollecting(self) -> None:
        self.__quote_keep_polling = False
        self.__ws.stop()
        self.__sql.stop()


    ##########################################################################


    def __sync_stock_info_table(self, kr_index_list: list | None = None) -> None:
        try:
            if not os.path.exists("./temp"):
                os.makedirs("./temp")
            else:
                # temp/ 하위 폴더(input·output·scripts)는 건너뛴다.
                # os.remove 가 디렉토리에서 IsADirectoryError 를 던지면 종목 마스터 갱신 전체가 중단된다.
                files = glob.glob('./temp/*')
                for f in files:
                    if os.path.isfile(f):
                        os.remove(f)

            if not kr_index_list:
                kr_index_list = master.get_kr_index_list()
            kr_category_names = dict(kr_index_list)

            stock_code_list = {
                "KOSPI" : master.get_kospi_stock_list(kr_category_names),
                "KOSDAQ" : master.get_kosdaq_stock_list(kr_category_names),
                "KONEX" : master.get_konex_stock_list(kr_category_names),
                "NASDAQ" : master.get_nasdaq_stock_list(),
                "NYSE" : master.get_nyse_stock_list(),
                "AMEX" : master.get_amex_stock_list(),
            }

            rest_api_token_list = self.__rest.get_token_list()
            temp_market_code_list = [[] for _ in range(len(rest_api_token_list))]
            token_idx = 0
            for stock_market, stock_code_infos in stock_code_list.items():
                for stock_type, stock_code_list in stock_code_infos.items():
                    for stock_code, category_code, category_name in stock_code_list:
                        temp_market_code_list[token_idx].append([
                            stock_market, stock_type, stock_code, category_code, category_name
                        ])
                        token_idx += 1
                        if token_idx >= len(rest_api_token_list):
                            token_idx = 0

            def kernel_func(rest_api_token: dict, stock_market_code_list: list) -> None:
                for stock_market_code in stock_market_code_list:
                    try:
                        # 호출 간격은 _rest 의 키 단위 공유 리미터가 건다 (지수·환율 폴링과 같은 예산).
                        stock_market = stock_market_code[0]
                        stock_type = stock_market_code[1]
                        stock_code = stock_market_code[2]
                        category_code = stock_market_code[3]
                        category_name = stock_market_code[4]
                        rest_api_token_header = rest_api_token["TOKEN_HEADER"]

                        if stock_market in ["KOSPI", "KOSDAQ", "KONEX"]:
                            stock_info_dict = self.__rest.kr_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
                        elif stock_market in ["NASDAQ", "NYSE", "AMEX"]:
                            stock_info_dict = self.__rest.ex_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
                        else:
                            continue

                        stock_info_dict["stock_category_code"] = category_code
                        stock_info_dict["stock_category_name"] = category_name
                        tables.enqueue_update_stock_info(self.__sql, self.__sql_main_db, stock_info_dict)

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

    def __sync_quote_info_table(self) -> list:
        # 지수·환율 카탈로그 갱신. 마스터 파일 2개만 받으면 되고 REST 는 한 건도 쓰지 않는다.
        kr_index_list = []
        try:
            if not os.path.exists("./temp"):
                os.makedirs("./temp")

            kr_index_list = master.get_kr_index_list()
            for index_code, index_name in kr_index_list:
                tables.enqueue_update_quote_info(self.__sql, self.__sql_main_db, {
                    'quote_code': index_code,
                    'quote_api_code': index_code,
                    'quote_name_kr': index_name,
                    'quote_name_en': "",
                    'quote_category': "KR_INDEX",
                })

            ex_index_list, fx_list = master.get_overseas_index_fx_list()

            for symbol, name_kr, name_en in ex_index_list:
                tables.enqueue_update_quote_info(self.__sql, self.__sql_main_db, {
                    'quote_code': symbol,
                    'quote_api_code': symbol,
                    'quote_name_kr': name_kr,
                    'quote_name_en': name_en,
                    'quote_category': "EX_INDEX",
                })

            for symbol, pair_name, country in fx_list:
                tables.enqueue_update_quote_info(self.__sql, self.__sql_main_db, {
                    # 한투 코드에는 '@' 가 들어 있어 그대로는 테이블명·식별자로 쓸 수 없다.
                    'quote_code': symbol.replace("@", ""),
                    'quote_api_code': symbol,
                    'quote_name_kr': pair_name,
                    'quote_name_en': country,
                    'quote_category': "FX",
                })

            util.InsertLog(
                "ApiKoreaInvest", "N",
                f"Success to update quote info [ kr_index={len(kr_index_list)} | ex_index={len(ex_index_list)} | fx={len(fx_list)} ]"
            )

        except Exception as e:
            util.InsertLog("ApiKoreaInvest", "E", "Fail to update quote info : " + e.__str__())
        return kr_index_list

    def __get_quote_query_list(self) -> list:
        select_query = (
            "SELECT Q.quote_query, Q.query_type, Q.quote_operator, "
            "I.quote_api_code, B.quote_api_code "
            "FROM quote_last_rest_query AS Q "
            "JOIN quote_info AS I ON Q.quote_code = I.quote_code "
            "LEFT JOIN quote_info AS B ON Q.quote_base_code = B.quote_code"
        )
        cursor = self.__sql.execute_sync(select_query)
        return cursor.fetchall()

    def __is_quote_market_open(self, query_type: str, now: DateTime) -> bool:
        # 창을 넉넉히 잡는다. 서머타임으로 개장이 한 시간 밀려도 덮이고, 창 안이지만 값이
        # 안 변하는 구간은 '마지막 저장 시각 이후만 적재' 규칙이 걸러내므로 중복이 쌓이지 않는다.
        weekday = now.weekday()  # 월=0, 일=6
        hour_min = now.hour * 100 + now.minute

        if query_type == "INDEX_KR":
            return weekday <= 4 and 900 <= hour_min <= 1540

        if query_type == "INDEX_EX":
            # 미국 월~금 장이 한국시간으로는 '월~금 밤' 에 열려 '화~토 새벽' 에 닫힌다.
            # 월요일 새벽은 미국 일요일이라 휴장이다.
            if hour_min >= 2200:
                return weekday <= 4
            if hour_min <= 630:
                return 1 <= weekday <= 5
            return False

        if query_type == "FX":
            # 월 06:00 ~ 토 06:00 연속.
            if weekday == 5:
                return hour_min <= 600
            if weekday == 6:
                return False
            if weekday == 0:
                return hour_min >= 600
            return True

        return False

    def __poll_quote_once(self) -> None:
        # 대상 목록은 매번 DB 에서 읽지 않는다. WS 구독 목록과 마찬가지로 시장 전환 때
        # SyncPartitions 가 갈아끼운 것을 쓴다 (그 시점에 저장 테이블도 함께 만들어진다).
        now = DateTime.now()
        query_list = self.__quote_query_list
        if not query_list:
            return

        rest_api_token = self.__rest.get_token_list()[0]
        token_header = rest_api_token["TOKEN_HEADER"]
        if not token_header:
            return

        # 환산에 쓰는 기준 시세는 한 사이클 안에서 재사용한다 (시점이 섞이지 않게).
        base_rate_dict = {}

        for quote_query, query_type, operator, api_code, base_api_code in query_list:
            if not self.__is_quote_market_open(query_type, now):
                continue

            try:
                if query_type == "INDEX_KR":
                    quote_id = "i" + quote_query
                    self.__store_quote_rows(quote_id, self.__rest.kr_index_tick_list(token_header, api_code))

                elif query_type == "INDEX_EX":
                    quote_id = "i" + quote_query
                    candle_list = self.__rest.ex_index_candle_list(token_header, api_code)
                    self.__store_quote_rows(quote_id, [(dt, close, volume) for dt, close, _o, _h, _l, volume in candle_list])

                elif query_type == "FX":
                    quote_id = "f" + quote_query
                    rate = self.__rest.fx_rate(token_header, api_code)

                    if operator in ("MUL", "DIV"):
                        if not base_api_code:
                            raise Exception(f"Base code is missing for operator [ {operator} ]")
                        if base_api_code not in base_rate_dict:
                            base_rate_dict[base_api_code] = self.__rest.fx_rate(token_header, base_api_code)
                        base_rate = base_rate_dict[base_api_code]

                        if operator == "MUL":
                            rate = base_rate * rate
                        elif rate == 0:
                            raise Exception("Divide by zero on fx conversion")
                        else:
                            rate = base_rate / rate

                    # 환율은 시계열을 주지 않으므로 조회 시각을 초 단위로 끊어 쓴다.
                    self.__store_quote_rows(quote_id, [(now.replace(microsecond=0), rate, 0.0)])

                else:
                    continue

            except Exception as e:
                util.InsertLog("ApiKoreaInvest", "E", f"Fail to poll quote [ {quote_query} | {e.__str__()} ]")

    def __get_quote_last_stored(self, quote_id: str) -> DateTime:
        # 프로세스가 재시작되면 메모리 기준점이 사라진다. 그대로 두면 조회가 돌려주는 과거 구간을
        # 통째로 다시 넣어 같은 시각이 중복 행으로 쌓이므로, DB 에 남은 마지막 시각을 기준으로 삼는다.
        try:
            cursor = self.__sql.execute_sync(
                f"SELECT MAX(execution_datetime) FROM {config.SQL_TICK_DB}.{quote_id}"
            )
            row = cursor.fetchone()
            if row is not None and row[0] is not None:
                return row[0]
        except Exception as e:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to read last stored datetime [ {quote_id} | {e.__str__()} ]")
        return DateTime.min

    def __store_quote_rows(self, quote_id: str, row_list: list) -> None:
        # 조회는 최근 구간을 통째로 돌려주므로, 마지막으로 저장한 시각보다 새 행만 넣는다.
        if quote_id not in self.__quote_last_stored_dict:
            self.__quote_last_stored_dict[quote_id] = self.__get_quote_last_stored(quote_id)
        last_stored = self.__quote_last_stored_dict[quote_id]

        new_row_list = [row for row in row_list if row[0] > last_stored and row[1] > 0]
        if not new_row_list:
            return

        for dt, price, volume in sorted(new_row_list):
            tables.enqueue_update_quote_execution(self.__sql, quote_id, dt, price, volume)

        self.__quote_last_stored_dict[quote_id] = max(row[0] for row in new_row_list)

    def __run_quote_polling(self) -> None:
        while self.__quote_keep_polling:
            try:
                self.__poll_quote_once()
            except Exception as e:
                util.InsertLog("ApiKoreaInvest", "E", f"Fail to run quote polling [ {e.__str__()} ]")

            for _ in range(self.__QUOTE_POLL_INTERVAL_SEC):
                if not self.__quote_keep_polling:
                    break
                time.sleep(1)

    def __sync_ws_query_list(self, target_market: str) -> None:
        # 인스턴스 상태가 아니라 인자를 본다. 그래야 __ws_query_type 대입을
        # '전부 성공한 뒤'로 미룰 수 있다 (실패 시 재시도가 살아나도록).
        try:
            if target_market == "KR":
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
            elif target_market == "EX":
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
                raise Exception(f"Invalid ws_query_type [ {target_market} ]")

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

            # 지수·환율 수집 대상도 WS 구독 목록과 같은 시점(시장 전환)에 교체한다.
            # 테이블을 만든 뒤에 목록을 갈아끼우므로, 폴링이 저장 테이블 없는 대상을 잡는 일이 없다.
            quote_query_list = self.__get_quote_query_list()
            this_year = DateTime.now().year
            for quote_query, query_type, _operator, _api_code, _base_api_code in quote_query_list:
                quote_id = ("f" if query_type == "FX" else "i") + quote_query
                tables.create_quote_execution_tables(self.__sql, quote_id, this_year)
                tables.create_quote_execution_tables(self.__sql, quote_id, this_year + 1)
            self.__quote_query_list = quote_query_list

        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync partitions for korea invest api [ {ex.__str__()} ] ")

    def SyncDailyInfo(self, target_market: str) -> None:
        # __ws_query_type 은 구독 갱신까지 전부 끝난 뒤에 바꾼다.
        # 먼저 바꾸면, 뒤에서 예외가 나도 메인 루프의 전환 조건이 거짓이 되어
        # 구독은 옛 시장 그대로인 채 다음 전환(8시간 뒤)까지 방치된다.
        # 재시도 간격은 호출부(main.py)가 지킨다.
        try:
            self.__rest.sync_token_list()
            self.__sync_ws_query_list(target_market)
            self.__ws_query_type = target_market
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync daily info for korea invest api [ {ex.__str__()} ] ")

    def SyncWeeklyInfo(self) -> None:
        try:
            self.__rest.sync_token_list()
            # 카탈로그 갱신을 먼저 한다. 파일 2개만 받으면 끝나므로, 수 분 걸리는
            # 종목 마스터 갱신 뒤에 두면 그만큼 늦어지기만 한다.
            kr_index_list = self.__sync_quote_info_table()
            self.__sync_stock_info_table(kr_index_list)
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync weekly info for korea invest api [ {ex.__str__()} ] ")
