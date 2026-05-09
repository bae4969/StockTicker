from core import config
from core import korea_invest_token_info
from core import util
import requests
from websocket import WebSocketApp
import pymysql
import json
import asyncio
import aiomysql
from threading import Thread, Event, Lock
import os
import pandas as pd
import urllib.request
import ssl
import glob
import zipfile
import time
from datetime import datetime as DateTime


class ApiKoreaInvestType:
    __sql_main_db:str
    __sql_config:dict = {}
    __sql_common_connection:pymysql.Connection = None
    __sql_is_stop:bool = False
    __sql_query_threads:list = []

    __API_BASE_URL:str = "https://openapi.koreainvestment.com:9443"
    __api_key_list:list = []	# KEY, SECRET

    __rest_api_token_list:list = []
    __ws_app_info_list:list = []
 

    __ws_query_type:str = ""
    __ws_ex_excution_last_volume:dict = {}
    # KIS 가 MAX SUBSCRIBE OVER 응답한 (stock_api_stock_code, stock_api_type) 튜플의 set.
    # 한 번 거절된 종목은 이후 sync 분배에서 영구 제외 (프로세스 재시작 시 초기화).
    __ws_blocked_keys:set = set()

    __MAX_REST_API_COUNT_PER_KEY:int = 18
    __MAX_WS_QUERY_COUNT_PER_KEY:int = 40
    __REST_API_DELAY_MICRO:int = 50000
    __AUTH_ISSUE_MIN_INTERVAL_SEC:float = 1.1
    __WS_APPROVAL_TIMEOUT:tuple = (3.05, 10)
    __WS_RECONNECT_BACKOFF_MAX_SEC:int = 600
    __WS_STABLE_CONNECT_SEC:int = 60
    __WS_BAN_SUSPECT_THRESHOLD:int = 10
    __WS_PING_INTERVAL_SEC:int = 30
    __WS_PING_TIMEOUT_SEC:int = 10
    __WS_SEND_GAP_SEC:float = 0.15

    __token_issue_lock:Lock = Lock()
    __token_issue_last_at:DateTime = DateTime.min
    __approval_issue_lock:Lock = Lock()
    __approval_issue_last_at:DateTime = DateTime.min


    ##########################################################################


    def __init__(self, sql_host:str, sql_port:int, sql_id:str, sql_pw:str, sql_db:str, sql_charset:str, api_key_list:list):
        self.__sql_main_db = sql_db
        self.__sql_config = {
            'host' : sql_host,
            'port' : sql_port,
            'user' : sql_id,
            'password' : sql_pw,
            'db' : sql_db,
            'charset' : sql_charset,
            'autocommit' : True,
        }
        self.__sql_common_connection = pymysql.connect(**self.__sql_config)
        config.set_session_timeouts(self.__sql_common_connection)
        self.__api_key_list = api_key_list
  
        self.__create_stock_info_table()
        self.__create_last_ws_query_table()
        self.__start_dequeue_sql_query()
  
        self.__create_rest_api_token_list()
        self.__create_ws_app_info_list()

    def __del__(self):
        for ws_app_info in self.__ws_app_info_list:
            ws_app_info["WS_KEEP_CONNECT"] = False
            ws_app_info["WS_APP"].close()
        self.__stop_dequeue_sql_query()

    def __enqueue_sql(self, query:str, params=None) -> None:
        if params is not None:
            self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, (query, params))
        else:
            self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, query)

    def __reconnect(self) -> None:
        self.__sql_common_connection.ping(reconnect=True)
        config.set_session_timeouts(self.__sql_common_connection)

    # 한국투자증권 공지: /oauth2/tokenP, /oauth2/Approval 모두 1초당 1건 한도.
    # 다중 키 동시 만료/재연결 상황에서도 글로벌 1Hz 직렬화 보장.
    def __throttle_auth_issue(self, kind:str) -> None:
        if kind == "token":
            lock = ApiKoreaInvestType.__token_issue_lock
        else:
            lock = ApiKoreaInvestType.__approval_issue_lock
        lock.acquire()
        try:
            if kind == "token":
                last_at = ApiKoreaInvestType.__token_issue_last_at
            else:
                last_at = ApiKoreaInvestType.__approval_issue_last_at
            elapsed = (DateTime.now() - last_at).total_seconds()
            if elapsed < self.__AUTH_ISSUE_MIN_INTERVAL_SEC:
                time.sleep(self.__AUTH_ISSUE_MIN_INTERVAL_SEC - elapsed)
            if kind == "token":
                ApiKoreaInvestType.__token_issue_last_at = DateTime.now()
            else:
                ApiKoreaInvestType.__approval_issue_last_at = DateTime.now()
        finally:
            lock.release()

    def __execute_sync_query(self, query:str, params=None):
        for attempt in range(1, config.SQL_MAX_RETRY + 1):
            try:
                self.__reconnect()
                cursor = self.__sql_common_connection.cursor()
                cursor.execute(query, params)
                return cursor
            except Exception as ex:
                if config.is_retryable_error(ex) and attempt < config.SQL_MAX_RETRY:
                    util.InsertLog("ApiKoreaInvest", "E", f"Sync query retry #{attempt} [ {ex.args[0]} ]")
                    time.sleep(min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX))
                    continue
                raise


    def __create_stock_info_table(self) -> None:
        try:
            table_query_str = (
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

            self.__execute_sync_query(table_query_str)

        except: raise Exception("Fail to create stock info table")

    def __create_last_ws_query_table(self) -> None:
        try:
            create_table_query = (
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
            self.__execute_sync_query(create_table_query)

        except: raise Exception("Fail to create last websocket query table")

    def __create_rest_api_token_list(self) -> None:
        try:
            file_data = korea_invest_token_info.load_last_token_info()
        except:
            file_data = json.loads("{}")
            pass
   
        self.__rest_api_token_list = []
        for api_key in self.__api_key_list:
            if api_key["KEY"] in file_data:
                last_token_info = file_data[api_key["KEY"]]
                self.__rest_api_token_list.append({
                    "API_KEY" : api_key["KEY"],
                    "API_SECRET" : api_key["SECRET"],
                    "TOKEN_TYPE" : last_token_info["TOKEN_TYPE"],
                    "TOKEN_VAL" : last_token_info["TOKEN_VAL"],
                    "TOKEN_EXPIRED_DATETIME" : DateTime.strptime(last_token_info["TOKEN_EXPIRED_DATETIME"], "%Y-%m-%d %H:%M:%S"),
                    "TOKEN_HEADER" : {
                            "content-type" : "application/json; charset=utf-8",
                            "authorization" : last_token_info["TOKEN_TYPE"] + " " + last_token_info["TOKEN_VAL"],
                            "appkey" : api_key["KEY"],
                            "appsecret" : api_key["SECRET"],
                            "custtype" : "P"
                        },
                    "LAST_USE_DATETIME" : DateTime.min,
                })
            else:
                self.__rest_api_token_list.append({
                    "API_KEY" : api_key["KEY"],
                    "API_SECRET" : api_key["SECRET"],
                    "TOKEN_TYPE" : "",
                    "TOKEN_VAL" : "",
                    "TOKEN_EXPIRED_DATETIME" : DateTime.min,
                    "TOKEN_HEADER" : {},
                    "LAST_USE_DATETIME" : DateTime.min,
                })
            
    def __create_ws_app_info_list(self) -> None:
        self.__ws_app_info_list = []
        for api_key in self.__api_key_list:
            ws_name = f"KoreaInvest_WS_{len(self.__ws_app_info_list)}"
            # 매 연결마다 새 APPROVAL_KEY 를 발급한다 (재사용 금지).
            # 시작 시 미리 발급해 두지 않으면, ws 가 안정적으로 open 되어 reconnect 루프가 돌지 않을 때
            # APPROVAL_KEY 가 영원히 빈 채로 남아 sync 의 send 가드에 걸려 구독 메시지가 송신되지 않는다.
            approval_key = ""
            try:
                self.__throttle_auth_issue("approval")
                response = requests.post(
                    url = self.__API_BASE_URL + "/oauth2/Approval",
                    headers = {"content-type": "application/json; utf-8"},
                    data = json.dumps({
                        "grant_type" : "client_credentials",
                        "appkey" : api_key["KEY"],
                        "secretkey" : api_key["SECRET"],
                    }),
                    timeout = self.__WS_APPROVAL_TIMEOUT,
                )
                response.raise_for_status()
                rep_json = response.json()
                if "approval_key" in rep_json:
                    approval_key = rep_json["approval_key"]
                    self.__save_approval_key(api_key["KEY"], approval_key)
                    util.InsertLog(
                        "ApiKoreaInvest",
                        "N",
                        f"Issued approval key on startup [ {ws_name} | key={approval_key[:8]}.. | http={response.status_code} ]"
                    )
                else:
                    util.InsertLog(
                        "ApiKoreaInvest",
                        "E",
                        f"Approval key missing on startup [ {ws_name} | http={response.status_code} | {response.text[:200]} ]"
                    )
            except Exception as ex:
                util.InsertLog(
                    "ApiKoreaInvest",
                    "E",
                    f"Fail to issue approval key on startup [ {ws_name} | {ex.__str__()} ]"
                )

            ws_app = WebSocketApp(
                url= "ws://ops.koreainvestment.com:21000",
                on_message= self.__on_ws_recv_message,
                on_open= self.__on_ws_open,
                on_close= self.__on_ws_close,
                on_error= self.__on_ws_error,
            )
            ws_thread = Thread(
                name= ws_name,
                target= ws_app.run_forever,
                kwargs= {
                    "ping_interval": self.__WS_PING_INTERVAL_SEC,
                    "ping_timeout": self.__WS_PING_TIMEOUT_SEC,
                },
            )
            ws_thread.daemon = True
            ws_thread.start()
            self.__ws_app_info_list.append({
                "API_KEY" : api_key["KEY"],
                "API_SECRET" : api_key["SECRET"],
                "APPROVAL_KEY" : approval_key,
                "WS_NAME" : ws_name,
                "WS_APP" : ws_app,
                "WS_THREAD" : ws_thread,
                "WS_IS_OPENED" : False,
                "WS_KEEP_CONNECT" : True,
                "WS_QUERY_LIST" : [],
                "WS_LAST_CONNECT_ATTEMPT" : DateTime.min,
                "WS_OPENED_AT" : DateTime.min,
                "WS_RECONNECT_FAIL_COUNT" : 0,
            })


    def __get_kospi_stock_list(self) -> dict:
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip", "./temp/kospi_code.zip")

        kospi_zip = zipfile.ZipFile("./temp/kospi_code.zip")
        kospi_zip.extractall("./temp")
        kospi_zip.close()

        part1_columns = ["단축코드", "표준코드", "한글명"]
        part2_columns = ["그룹코드","시가총액규모","지수업종대분류","지수업종중분류","지수업종소분류","제조업","저유동성","지배구조지수종목","KOSPI200섹터업종","KOSPI100","KOSPI50","KRX","ETP","ELW발행","KRX100","KRX자동차","KRX반도체","KRX바이오","KRX은행","SPAC","KRX에너지화학","KRX철강","단기과열","KRX미디어통신","KRX건설","Non1","KRX증권","KRX선박","KRX섹터_보험","KRX섹터_운송","SRI","기준가","매매수량단위","시간외수량단위","거래정지","정리매매","관리종목","시장경고","경고예고","불성실공시","우회상장","락구분","액면변경","증자구분","증거금비율","신용가능","신용기간","전일거래량","액면가","상장일자","상장주수","자본금","결산월","공모가","우선주","공매도과열","이상급등","KRX300","KOSPI","매출액","영업이익","경상이익","당기순이익","ROE","기준년월","시가총액","그룹사코드","회사신용한도초과","담보대출가능","대주가능",]
        field_specs = [2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1,]
        offset = sum(field_specs) + 1

        tmp_fil1 = "./temp/kospi_code_part1.tmp"
        tmp_fil2 = "./temp/kospi_code_part2.tmp"
        
        wf1 = open(tmp_fil1, mode="w", encoding="cp949")
        wf2 = open(tmp_fil2, mode="w", encoding="cp949")

        with open("./temp/kospi_code.mst", mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0 : len(row) - offset]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
                rf2 = row[-offset:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
        df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
        df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
        df["단축코드"] = df["단축코드"].astype('str')

        def format_etn(symbol):
            if symbol[0].isalpha():
                return symbol[0] + symbol[1:].zfill(6)
            else:
                return symbol.zfill(6) 

        return {
            "STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
            "ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
            "ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
        }
        
    def __get_kosdaq_stock_list(self) -> dict:
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip", "./temp/kosdaq_code.zip")

        kospi_zip = zipfile.ZipFile("./temp/kosdaq_code.zip")
        kospi_zip.extractall("./temp")
        kospi_zip.close()

        part1_columns = ["단축코드", "표준코드", "한글명"]
        part2_columns = ["그룹코드","시가총액 규모 구분 코드 유가","지수업종 대분류 코드","지수 업종 중분류 코드","지수업종 소분류 코드","벤처기업 여부","저유동성종목 여부","KRX 종목 여부","ETP 상품구분코드","KRX100 종목 여부","KRX 자동차 여부","KRX 반도체 여부","KRX 바이오 여부","KRX 은행 여부","기업인수목적회사여부","KRX 에너지 화학 여부","KRX 철강 여부","단기과열종목구분코드","KRX 미디어 통신 여부","KRX 건설 여부","투자주의환기종목여부","KRX 증권 구분","KRX 선박 구분","KRX섹터지수 보험여부","KRX섹터지수 운송여부","KOSDAQ150지수여부","주식 기준가","정규 시장 매매 수량 단위","시간외 시장 매매 수량 단위","거래정지 여부","정리매매 여부","관리 종목 여부","시장 경고 구분 코드","시장 경고위험 예고 여부","불성실 공시 여부","우회 상장 여부","락구분 코드","액면가 변경 구분 코드","증자 구분 코드","증거금 비율","신용주문 가능 여부","신용기간","전일 거래량","주식 액면가","주식 상장 일자","상장 주수","자본금","결산 월","공모 가격","우선주 구분 코드","공매도과열종목여부","이상급등종목여부","KRX300 종목 여부","매출액","영업이익","경상이익","단기순이익","ROE","기준년월","전일기준 시가총액","그룹사 코드","회사신용한도초과여부","담보대출가능여부","대주가능여부",]
        field_specs = [2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1,]
        offset = sum(field_specs) + 1

        tmp_fil1 = "./temp/kosdaq_code_part1.tmp"
        tmp_fil2 = "./temp/kosdaq_code_part2.tmp"
        
        wf1 = open(tmp_fil1, mode="w", encoding="cp949")
        wf2 = open(tmp_fil2, mode="w", encoding="cp949")

        with open("./temp/kosdaq_code.mst", mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0 : len(row) - offset]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
                rf2 = row[-offset:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
        df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
        df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
        df["단축코드"] = df["단축코드"].astype('str')

        def format_etn(symbol):
            if symbol[0].isalpha():
                return symbol[0] + symbol[1:].zfill(6)
            else:
                return symbol.zfill(6) 

        return {
            "STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
            "ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
            "ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
        }
        
    def __get_konex_stock_list(self) -> dict:
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip", "./temp/konex_code.zip")

        kospi_zip = zipfile.ZipFile("./temp/konex_code.zip")
        kospi_zip.extractall("./temp")
        kospi_zip.close()

        part1_columns = ["단축코드", "표준코드", "한글명"]
        part2_columns = ["그룹코드","주식 기준가","정규 시장 매매 수량 단위","시간외 시장 매매 수량 단위","거래정지 여부","정리매매 여부","관리 종목 여부","시장 경고 구분 코드","시장 경고위험 예고 여부","불성실 공시 여부","우회 상장 여부","락구분 코드","액면가 변경 구분 코드","증자 구분 코드","증거금 비율","신용주문 가능 여부","신용기간","전일 거래량","주식 액면가","주식 상장 일자","상장 주수","자본금","결산 월","공모 가격","우선주 구분 코드","공매도과열종목여부","이상급등종목여부","KRX300 종목 여부","매출액","영업이익","경상이익","단기순이익","ROE","기준년월","전일기준 시가총액","회사신용한도초과여부","담보대출가능여부","대주가능여부",]
        field_specs = [2,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,1,1,1,]
        offset = sum(field_specs) + 1

        tmp_fil1 = "./temp/konex_code_part1.tmp"
        tmp_fil2 = "./temp/konex_code_part2.tmp"
        
        wf1 = open("./temp/konex_code_part1.tmp", mode="w", encoding="cp949")
        wf2 = open("./temp/konex_code_part2.tmp", mode="w", encoding="cp949")

        with open("./temp/konex_code.mst", mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0 : len(row) - offset]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
                rf2 = row[-offset:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
        df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
        df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
        df["단축코드"] = df["단축코드"].astype('str')

        def format_etn(symbol):
            if symbol[0].isalpha():
                return symbol[0] + symbol[1:].zfill(6)
            else:
                return symbol.zfill(6) 

        return {
            "STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
            "ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
            "ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
        }


    def __get_nyse_stock_list(self) -> dict:
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/nysmst.cod.zip", "./temp/nysmst.cod.zip")

        overseas_zip = zipfile.ZipFile(f'./temp/nysmst.cod.zip')
        overseas_zip.extractall("./temp")
        overseas_zip.close()

        # Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
        # 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
        df = pd.read_table("./temp/NYSMST.COD",sep='\t',encoding='cp949', na_values=[], keep_default_na=False)
        df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
        df["Symbol"] = df["Symbol"].astype('str').str.upper()

        return {
            "STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
            "ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
            "ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
        }
    
    def __get_nasdaq_stock_list(self) -> dict:
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/nasmst.cod.zip", "./temp/nasmst.cod.zip")

        overseas_zip = zipfile.ZipFile(f'./temp/nasmst.cod.zip')
        overseas_zip.extractall("./temp")
        overseas_zip.close()

        # Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
        # 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
        df = pd.read_table("./temp/NASMST.COD", sep='\t',encoding='cp949', na_values=[], keep_default_na=False)
        df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
        df["Symbol"] = df["Symbol"].astype('str').str.upper()

        files = glob.glob('./temp/*')
        for f in files:
            os.remove(f)

        return {
            "STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
            "ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
            "ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
        }
    
    def __get_amex_stock_list(self) -> dict:
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/amsmst.cod.zip", "./temp/amsmst.cod.zip")

        overseas_zip = zipfile.ZipFile('./temp/amsmst.cod.zip')
        overseas_zip.extractall("./temp")
        overseas_zip.close()

        # Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
        # 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
        df = pd.read_table("./temp/AMSMST.COD",sep='\t',encoding='cp949', na_values=['', 'NULL', 'NA'], keep_default_na=False)
        df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
        df["Symbol"] = df["Symbol"].astype('str').str.upper()

        files = glob.glob('./temp/*')
        for f in files:
            os.remove(f)

        return {
            "STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
            "ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
            "ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
        }
    

    ##########################################################################
 

    def __start_dequeue_sql_query(self) -> None:
        self.__ready_event = Event()
        self.__loop = asyncio.new_event_loop()
        self.__async_thread = Thread(name="KoreaInvest_Async", target=self.__run_async_loop, daemon=True)
        self.__async_thread.start()
        self.__ready_event.wait()
        
    def __stop_dequeue_sql_query(self) -> None:
        for _ in range(8):
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
                self.__sql_pool = await aiomysql.create_pool(**self.__sql_config, maxsize=8)
                break
            except Exception as ex:
                retry_count += 1
                util.InsertLog("ApiKoreaInvest", "E", f"Fail to create sql pool, retry #{retry_count} [ {ex.__str__()} ]")
                await asyncio.sleep(2)
        self.__ready_event.set()
        tasks = [asyncio.create_task(self.__async_dequeue()) for _ in range(8)]
        await asyncio.gather(*tasks)
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
                        util.InsertLog("ApiKoreaInvest", "E", f"DB query retry #{attempt} [ {ex.args[0]} ]")
                        await asyncio.sleep(min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX))
                        continue

                    util.InsertLog("ApiKoreaInvest", "E", f"Fail to execute sql query [ {ex.__str__()} ]")
                    break


    def __update_stock_info_table(self, stock_info_dict:dict) -> None:
        self.__enqueue_sql(
            f"INSERT INTO {self.__sql_main_db}.stock_info ("
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

    def __update_stock_execution_table(self, stock_code:str, dt:DateTime, price:float, non_volume:float, ask_volume:float, bid_volume:float) -> None:
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
 
        self.__enqueue_sql(
            f"INSERT INTO {raw_table_name} "
            "(execution_datetime, execution_price, execution_non_volume, execution_ask_volume, execution_bid_volume) "
            "VALUES (%s, %s, %s, %s, %s)",
            (datetime_00_min.strftime("%Y-%m-%d %H:%M:%S"), price_str, non_volume_str, ask_volume_str, bid_volume_str)
        )
        self.__enqueue_sql(
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

    def __update_stock_orderbook_table(self, stock_code:str, dt:DateTime, data) -> None:
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

        self.__execute_sync_query(create_orderbook_table_query_str)
        self.__execute_sync_query(insert_orderbook_table_query_str)

    
    def __create_stock_execution_table(self, stock_code:str, year:int):
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

        self.__execute_sync_query(create_tick_db_query)
        self.__execute_sync_query(create_candle_db_query)
        self.__execute_sync_query(create_tick_table_query)
        self.__execute_sync_query(create_candle_table_query)

        partition_name = f"p{year:04d}"
        tick_db, tick_tbl = tick_table_name.split(".")
        candle_db, candle_tbl = candle_table_name.split(".")

        check_query = (
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE PARTITION_NAME = %s AND ("
            "(TABLE_SCHEMA = %s AND TABLE_NAME = %s) OR "
            "(TABLE_SCHEMA = %s AND TABLE_NAME = %s))"
        )
        cursor = self.__execute_sync_query(
            check_query, (partition_name, tick_db, tick_tbl, candle_db, candle_tbl)
        )
        if cursor is None:
            existing = set()
        else:
            existing = {(r[0], r[1]) for r in cursor.fetchall()}

        if (tick_db, tick_tbl) not in existing:
            self.__execute_sync_query(add_tick_partition_query)
        if (candle_db, candle_tbl) not in existing:
            self.__execute_sync_query(add_candle_partition_query)
        
    def __create_stock_orderbook_table(self, stock_code:str, year:int):
        # TODO
        return


    ##########################################################################
        

    def __rest_api_kr_stock_info_dict(self, rest_api_token_header:dict, stock_code:str, stock_type:str, stock_market:str) -> dict:
        try:
            api_url = "/uapi/domestic-stock/v1/quotations/search-stock-info"
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "CTPF1002R"
            api_para = {
                "PRDT_TYPE_CD" : "300",
                "PDNO" : stock_code,
            }
  
            response = requests.get (
                url = self.__API_BASE_URL + api_url,
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0:
                raise Exception("Recv Code")

            rep_stock_info = rep_json["output"]
            stock_price = util.TryParseFloat(rep_stock_info["thdt_clpr"])
            stock_count = util.TryParseFloat(rep_stock_info["lstg_stqt"])

            return {
                "is_good_info" : True,
                "stock_code" : stock_code,
                "stock_name_kr" : rep_stock_info["prdt_abrv_name"],
                "stock_name_en" : rep_stock_info["prdt_eng_abrv_name"],
                "stock_market" : stock_market,
                "stock_type" : stock_type,
                "stock_price" : stock_price,
                "stock_count" : stock_count,
                "stock_cap" : str(stock_count * stock_price),
            }
   
        except Exception as e:
            raise Exception("[ kr stock info ][ %s ][ %s ]"%(stock_code, e.__str__()))

    def __rest_api_ex_stock_info_dict(self, rest_api_token_header:dict, stock_code:str, stock_type:str, stock_market:str) -> dict:
        try:
            api_url = "/uapi/overseas-price/v1/quotations/search-info"
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "CTPF1702R"
            if stock_market == "NASDAQ":
                api_para = {
                    "PRDT_TYPE_CD" : "512",
                    "PDNO" : stock_code,
                }
            elif stock_market == "NYSE":
                api_para = {
                    "PRDT_TYPE_CD" : "513",
                    "PDNO" : stock_code,
                }
            else:
                api_para = {
                    "PRDT_TYPE_CD" : "529",
                    "PDNO" : stock_code,
                }
        
            response = requests.get (
                url = self.__API_BASE_URL + api_url,
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0: raise Exception("Recv Code 1")

            rep_stock_info1 = rep_json["output"]


            api_url = "/uapi/overseas-price/v1/quotations/price-detail"
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "HHDFS76200200"
            if stock_market == "NASDAQ":
                api_para = {
                    "AUTH" : "",
                    "EXCD" : "NAS",
                    "SYMB" : stock_code,
                }
            elif stock_market == "NYSE":
                api_para = {
                    "AUTH" : "",
                    "EXCD" : "NYS",
                    "SYMB" : stock_code,
                }
            else:
                api_para = {
                    "AUTH" : "",
                    "EXCD" : "AMS",
                    "SYMB" : stock_code,
                }				
        
            response = requests.get (
                url = self.__API_BASE_URL + api_url,
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0: raise Exception("Recv Code 2")

            rep_stock_info2 = rep_json["output"]

            stock_price = util.TryParseFloat(rep_stock_info2["base"])
            stock_count = util.TryParseFloat(rep_stock_info1["lstg_stck_num"])

            return {
                "is_good_info" : True,
                "stock_code" : stock_code,
                "stock_name_kr" : rep_stock_info1["prdt_name"],
                "stock_name_en" : rep_stock_info1["prdt_eng_name"],
                "stock_market" : stock_market,
                "stock_type" : stock_type,
                "stock_price" : stock_price,
                "stock_count" : stock_count,
                "stock_cap" : str(stock_count * stock_price),
            }

        except Exception as e:
            raise Exception("[ ex stock info ][ %s ][ %s ]"%(stock_code, e.__str__()))


    ##########################################################################


    def __on_recv_kr_stock_execution(self, data_cnt:int, data:str) -> None:
        # "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
        pValue = data.split('^')
        for data_idx in range(data_cnt):
            try:
                data_offset = 46 * data_idx

                stock_execution_dt_str = DateTime.now().strftime("%Y%m%d") + pValue[data_offset + 1]

                stock_code = pValue[data_offset + 0]
                dt = DateTime.strptime(stock_execution_dt_str, "%Y%m%d%H%M%S")
                price = float(pValue[data_offset + 2])
                volume = float(pValue[data_offset + 12])
                
                if pValue[data_offset + 21] == "1":
                    self.__update_stock_execution_table(stock_code, dt, price, 0, 0, volume)
                elif pValue[data_offset + 21] == "5":
                    self.__update_stock_execution_table(stock_code, dt, price, 0, volume, 0)
                else:
                    self.__update_stock_execution_table(stock_code, dt, price, volume, 0, 0)
                    
            except Exception as e:
                raise Exception("[ kr stock execution ][ %s ][ %s ]"%(stock_code, e.__str__()))

    def __on_recv_ex_stock_execution(self, data_cnt:int, data:str) -> None:
        # "실시간종목코드|종목코드|수수점자리수|현지영업일자|현지일자|현지시간|한국일자|한국시간|시가|고가|저가|현재가|대비구분|전일대비|등락율|매수호가|매도호가|매수잔량|매도잔량|체결량|거래량|거래대금|매도체결량|매수체결량|체결강도|시장구분"
        pValue = data.split('^')
        for data_idx in range(data_cnt):
            try:
                data_offset = 26 * data_idx

                stock_execution_dt_str = pValue[data_offset + 4] + pValue[data_offset + 5]

                stock_code = pValue[data_offset + 1]
                dt = DateTime.strptime(stock_execution_dt_str, "%Y%m%d%H%M%S")
                price = float(pValue[data_offset + 11])
                tot_volume = float(pValue[data_offset + 19])
                bid_volume_amount = float(pValue[data_offset + 22])
                ask_volume_amount = float(pValue[data_offset + 23])

                ask_volume = 0.0
                bid_volume = 0.0
                if stock_code in self.__ws_ex_excution_last_volume:
                    last_volumes = self.__ws_ex_excution_last_volume[stock_code]
                    if bid_volume_amount >= last_volumes[0]:
                        bid_volume = bid_volume_amount - last_volumes[0]
                        self.__ws_ex_excution_last_volume[stock_code][0] = bid_volume_amount
                    else:
                        self.__ws_ex_excution_last_volume[stock_code][0] = 0.0
                    if ask_volume_amount >= last_volumes[1]:
                        ask_volume = ask_volume_amount - last_volumes[1]
                        self.__ws_ex_excution_last_volume[stock_code][1] = ask_volume_amount
                    else:
                        self.__ws_ex_excution_last_volume[stock_code][1] = 0.0
                else:
                    self.__ws_ex_excution_last_volume[stock_code] = [0.0, 0.0]

                if ask_volume + bid_volume == tot_volume:
                    self.__update_stock_execution_table(stock_code, dt, price, 0.0, ask_volume, bid_volume)
                else:
                    self.__update_stock_execution_table(stock_code, dt, price, tot_volume, 0.0, 0.0)

            except Exception as e:
                raise Exception("[ ex stock execution ][ %s ][ %s ]"%(stock_code, e.__str__()))

    def __on_recv_kr_stock_orderbook(self, data:str) -> None:
        return
        """ 넘겨받는데이터가 정상인지 확인
        print("stockhoka[%s]"%(data))
        """
        recvvalue = data.split('^')  # 수신데이터를 split '^'

        print("유가증권 단축 종목코드 [" + recvvalue[0] + "]")
        print("영업시간 [" + recvvalue[1] + "]" + "시간구분코드 [" + recvvalue[2] + "]")
        print("======================================")
        print("매도호가10 [%s]    잔량10 [%s]" % (recvvalue[12], recvvalue[32]))
        print("매도호가09 [%s]    잔량09 [%s]" % (recvvalue[11], recvvalue[31]))
        print("매도호가08 [%s]    잔량08 [%s]" % (recvvalue[10], recvvalue[30]))
        print("매도호가07 [%s]    잔량07 [%s]" % (recvvalue[9], recvvalue[29]))
        print("매도호가06 [%s]    잔량06 [%s]" % (recvvalue[8], recvvalue[28]))
        print("매도호가05 [%s]    잔량05 [%s]" % (recvvalue[7], recvvalue[27]))
        print("매도호가04 [%s]    잔량04 [%s]" % (recvvalue[6], recvvalue[26]))
        print("매도호가03 [%s]    잔량03 [%s]" % (recvvalue[5], recvvalue[25]))
        print("매도호가02 [%s]    잔량02 [%s]" % (recvvalue[4], recvvalue[24]))
        print("매도호가01 [%s]    잔량01 [%s]" % (recvvalue[3], recvvalue[23]))
        print("--------------------------------------")
        print("매수호가01 [%s]    잔량01 [%s]" % (recvvalue[13], recvvalue[33]))
        print("매수호가02 [%s]    잔량02 [%s]" % (recvvalue[14], recvvalue[34]))
        print("매수호가03 [%s]    잔량03 [%s]" % (recvvalue[15], recvvalue[35]))
        print("매수호가04 [%s]    잔량04 [%s]" % (recvvalue[16], recvvalue[36]))
        print("매수호가05 [%s]    잔량05 [%s]" % (recvvalue[17], recvvalue[37]))
        print("매수호가06 [%s]    잔량06 [%s]" % (recvvalue[18], recvvalue[38]))
        print("매수호가07 [%s]    잔량07 [%s]" % (recvvalue[19], recvvalue[39]))
        print("매수호가08 [%s]    잔량08 [%s]" % (recvvalue[20], recvvalue[40]))
        print("매수호가09 [%s]    잔량09 [%s]" % (recvvalue[21], recvvalue[41]))
        print("매수호가10 [%s]    잔량10 [%s]" % (recvvalue[22], recvvalue[42]))
        print("======================================")
        print("총매도호가 잔량        [%s]" % (recvvalue[43]))
        print("총매도호가 잔량 증감   [%s]" % (recvvalue[54]))
        print("총매수호가 잔량        [%s]" % (recvvalue[44]))
        print("총매수호가 잔량 증감   [%s]" % (recvvalue[55]))
        print("시간외 총매도호가 잔량 [%s]" % (recvvalue[45]))
        print("시간외 총매수호가 증감 [%s]" % (recvvalue[46]))
        print("시간외 총매도호가 잔량 [%s]" % (recvvalue[56]))
        print("시간외 총매수호가 증감 [%s]" % (recvvalue[57]))
        print("예상 체결가            [%s]" % (recvvalue[47]))
        print("예상 체결량            [%s]" % (recvvalue[48]))
        print("예상 거래량            [%s]" % (recvvalue[49]))
        print("예상체결 대비          [%s]" % (recvvalue[50]))
        print("부호                   [%s]" % (recvvalue[51]))
        print("예상체결 전일대비율    [%s]" % (recvvalue[52]))
        print("누적거래량             [%s]" % (recvvalue[53]))
        print("주식매매 구분코드      [%s]" % (recvvalue[58]))
        
    def __on_recv_ex_stock_orderbook(self, data:str) -> None:
        return
        """ 넘겨받는데이터가 정상인지 확인
        print("stockhoka[%s]"%(data))
        """
        
        recvvalue = data.split('^')  # 수신데이터를 split '^'

        print("실시간종목코드 [" + recvvalue[0] + "]" + ", 종목코드 [" + recvvalue[1] + "]")
        print("소숫점자리수 [" + recvvalue[2] + "]")
        print("현지일자 [" + recvvalue[3] + "]" + ", 현지시간 [" + recvvalue[4] + "]")
        print("한국일자 [" + recvvalue[5] + "]" + ", 한국시간 [" + recvvalue[6] + "]")
        print("======================================")    
        print("매도호가10 [%s]    잔량10 [%s]" % (recvvalue[66], recvvalue[68]))
        print("매도호가09 [%s]    잔량09 [%s]" % (recvvalue[60], recvvalue[62]))
        print("매도호가08 [%s]    잔량08 [%s]" % (recvvalue[54], recvvalue[56]))
        print("매도호가07 [%s]    잔량07 [%s]" % (recvvalue[48], recvvalue[50]))
        print("매도호가06 [%s]    잔량06 [%s]" % (recvvalue[42], recvvalue[44]))
        print("매도호가05 [%s]    잔량05 [%s]" % (recvvalue[36], recvvalue[38]))
        print("매도호가04 [%s]    잔량04 [%s]" % (recvvalue[30], recvvalue[32]))
        print("매도호가03 [%s]    잔량03 [%s]" % (recvvalue[24], recvvalue[26]))
        print("매도호가02 [%s]    잔량02 [%s]" % (recvvalue[18], recvvalue[20]))
        print("매도호가01 [%s]    잔량01 [%s]" % (recvvalue[12], recvvalue[14]))
        print("--------------------------------------")
        print("매수호가01 [%s]    잔량01 [%s]" % (recvvalue[11], recvvalue[13]))
        print("매수호가02 [%s]    잔량02 [%s]" % (recvvalue[17], recvvalue[19]))
        print("매수호가03 [%s]    잔량03 [%s]" % (recvvalue[23], recvvalue[25]))
        print("매수호가04 [%s]    잔량04 [%s]" % (recvvalue[29], recvvalue[31]))
        print("매수호가05 [%s]    잔량05 [%s]" % (recvvalue[35], recvvalue[37]))
        print("매수호가06 [%s]    잔량06 [%s]" % (recvvalue[41], recvvalue[43]))
        print("매수호가07 [%s]    잔량07 [%s]" % (recvvalue[47], recvvalue[49]))
        print("매수호가08 [%s]    잔량08 [%s]" % (recvvalue[53], recvvalue[55]))
        print("매수호가09 [%s]    잔량09 [%s]" % (recvvalue[59], recvvalue[61]))
        print("매수호가10 [%s]    잔량10 [%s]" % (recvvalue[65], recvvalue[67]))
        print("======================================")
        print("매수총 잔량        [%s]" % (recvvalue[7]))
        print("매수총잔량대비      [%s]" % (recvvalue[9]))
        print("매도총 잔량        [%s]" % (recvvalue[8]))
        print("매도총잔략대비      [%s]" % (recvvalue[10]))


    def __on_ws_recv_message(self, ws:WebSocketApp, msg:str) -> None:
        try:
            if msg[0] == '0':
                # 수신데이터가 실데이터 이전은 '|'로 나눠 있음
                recvstr = msg.split('|') 
                trid0 = recvstr[1]

                # H0STCNT0 : 국내 주식 체결
                # HDFSCNT0 : 해외 주식 체결
                # H0STASP0 : 국내 주식 호가
                # HDFSASP0 : 해외 주식 호가
                if trid0 == "H0STCNT0":
                    Thread(name="KoreaInvest_KR_Execution", target=self.__on_recv_kr_stock_execution, args=(int(recvstr[2]), recvstr[3])).start()
                elif trid0 == "HDFSCNT0":
                    Thread(name="KoreaInvest_EX_Execution", target=self.__on_recv_ex_stock_execution, args=(int(recvstr[2]), recvstr[3])).start()
                elif trid0 == "H0STASP0":
                    Thread(name="KoreaInvest_KR_Orderbook", target=self.__on_recv_kr_stock_orderbook, args=(recvstr[3],)).start()
                elif trid0 == "HDFSASP0":
                    Thread(name="KoreaInvest_EX_Orderbook", target=self.__on_recv_ex_stock_orderbook, args=(recvstr[3],)).start()

            else:
                msg_json = json.loads(msg)

                if msg_json["header"]["tr_id"] == "PINGPONG":
                    ws.send(msg)

                else:
                    rt_cd = msg_json["body"]["rt_cd"]
                    msg1 = msg_json["body"].get("msg1", "")
                    tr_id = msg_json["header"].get("tr_id", "")
                    tr_key = msg_json["header"].get("tr_key", "")
                    ws_name = next(
                        (info["WS_NAME"] for info in self.__ws_app_info_list if info["WS_APP"] == ws),
                        "?"
                    )

                    if rt_cd != '0':
                        util.InsertLog(
                            "ApiKoreaInvest",
                            "E",
                            f"Server response error [ {ws_name} | {tr_key}:{tr_id} | rt_cd={rt_cd} | {msg1} ]"
                        )
                        if "MAX SUBSCRIBE OVER" in msg1.upper():
                            self.__ws_blocked_keys.add((tr_key, tr_id))
                            util.InsertLog(
                                "ApiKoreaInvest",
                                "W",
                                f"Blocked subscription added [ {tr_key}:{tr_id} | total_blocked={len(self.__ws_blocked_keys)} ]"
                            )
                    else:
                        util.InsertLog(
                            "ApiKoreaInvest",
                            "N",
                            f"Server response ok [ {ws_name} | {tr_key}:{tr_id} | {msg1} ]"
                        )

        except Exception as e:
            util.InsertLog("ApiKoreaInvest", "E", "Fail to process ws recv msg : " + e.__str__())
    
    # tr_type "1": 등록(subscribe), "2": 해제(unsubscribe)
    def __send_ws_subscription(self, ws_app_info:dict, query_info:dict, tr_type:str) -> bool:
        try:
            msg = {
                "header" : {
                    "approval_key": ws_app_info["APPROVAL_KEY"],
                    "content-type": "utf-8",
                    "custtype": "P",
                    "tr_type": tr_type
                },
                "body" : {
                    "input": {
                        "tr_id": query_info["stock_api_type"],
                        "tr_key": query_info["stock_api_stock_code"]
                    }
                }
            }
            ws_app_info["WS_APP"].send(json.dumps(msg))
            action = "register" if tr_type == "1" else "unregister"
            util.InsertLog(
                "ApiKoreaInvest",
                "N",
                f"Sent ws subscription [ {ws_app_info['WS_NAME']} | {action} | {query_info.get('stock_api_stock_code')}:{query_info.get('stock_api_type')} ]"
            )
            return True
        except Exception as e:
            action = "register" if tr_type == "1" else "unregister"
            util.InsertLog(
                "ApiKoreaInvest",
                "E",
                f"Fail to {action} ws subscription [ {ws_app_info['WS_NAME']} | {query_info.get('stock_api_stock_code')}:{query_info.get('stock_api_type')} | {e.__str__()} ]"
            )
            return False

    def __on_ws_open(self, ws:WebSocketApp) -> None:
        for ws_app_info in self.__ws_app_info_list:
            if ws_app_info["WS_APP"] != ws: continue
            ws_app_info["WS_IS_OPENED"] = True
            ws_app_info["WS_OPENED_AT"] = DateTime.now()

            sub_count = len(ws_app_info["WS_QUERY_LIST"])
            util.InsertLog(
                "ApiKoreaInvest",
                "N",
                f"Opened korea invest websocket [ {ws_app_info['WS_NAME']} | initial_subs={sub_count} ]"
            )

            for query_info in ws_app_info["WS_QUERY_LIST"]:
                self.__send_ws_subscription(ws_app_info, query_info, "1")
                time.sleep(self.__WS_SEND_GAP_SEC)

            util.InsertLog(
                "ApiKoreaInvest",
                "N",
                f"Initial subscriptions sent [ {ws_app_info['WS_NAME']} | count={sub_count} ]"
            )
            break

    def __on_ws_error(self, ws, error) -> None:
        ws_name = next(
            (info["WS_NAME"] for info in self.__ws_app_info_list if info["WS_APP"] == ws),
            "?"
        )
        util.InsertLog(
            "ApiKoreaInvest",
            "E",
            f"Websocket error [ {ws_name} | type={type(error).__name__} | {str(error)[:300]} ]"
        )

    def __on_ws_close(self, ws:WebSocketApp, close_code, close_msg) -> None:
        for ws_app_info in self.__ws_app_info_list:
            if ws_app_info["WS_APP"] != ws: continue
            ws_app_info["WS_IS_OPENED"] = False
            ws_name = ws_app_info["WS_NAME"]

            # 직전 연결이 충분히 오래 유지되었는지 판정 (단명 연결은 실패로 간주)
            opened_at = ws_app_info["WS_OPENED_AT"]
            if opened_at != DateTime.min:
                connection_lifetime = (DateTime.now() - opened_at).total_seconds()
            else:
                connection_lifetime = 0
            ws_app_info["WS_OPENED_AT"] = DateTime.min

            util.InsertLog(
                "ApiKoreaInvest",
                "W",
                f"Websocket closed [ {ws_name} | code={close_code}, msg={str(close_msg)} | lifetime={connection_lifetime:.1f}s | fail_count={ws_app_info['WS_RECONNECT_FAIL_COUNT']} ]"
            )

            if connection_lifetime >= self.__WS_STABLE_CONNECT_SEC:
                ws_app_info["WS_RECONNECT_FAIL_COUNT"] = 0
            else:
                # 단명 연결(핸드셰이크 거부 포함) 은 실패로 간주하여 fail_count 증가 → 백오프 활성화
                ws_app_info["WS_RECONNECT_FAIL_COUNT"] += 1

            fail_count = ws_app_info["WS_RECONNECT_FAIL_COUNT"]
            elapsed = (DateTime.now() - ws_app_info["WS_LAST_CONNECT_ATTEMPT"]).total_seconds()
            already_waited = max(0, elapsed)
            reconnect_attempt = 0
            # 연속 실패가 임계치에 도달한 시점에 한 번만 의심 로그 (자동 ban 처리는 하지 않음)
            if fail_count == self.__WS_BAN_SUSPECT_THRESHOLD:
                util.InsertLog(
                    "ApiKoreaInvest",
                    "E",
                    f"Suspected ban — fail_count reached threshold [ {ws_name} | fail_count={fail_count} ] (manual intervention recommended)"
                )
            while ws_app_info["WS_KEEP_CONNECT"] == True:
                wait_sec = max(0, min(2 ** fail_count, self.__WS_RECONNECT_BACKOFF_MAX_SEC) - already_waited)
                already_waited = 0
                if wait_sec > 0:
                    util.InsertLog("ApiKoreaInvest", "W", f"Reconnect waiting {wait_sec:.0f}s [ {ws_name} | fail_count={fail_count} ]")
                # 인터럽트 가능한 분할 sleep
                slept = 0
                while ws_app_info["WS_KEEP_CONNECT"] == True and slept < wait_sec:
                    chunk = min(5, wait_sec - slept)
                    time.sleep(chunk)
                    slept += chunk
                if ws_app_info["WS_KEEP_CONNECT"] != True:
                    break

                ws_app_info["WS_LAST_CONNECT_ATTEMPT"] = DateTime.now()
                try:
                    # 매 연결 시도마다 APPROVAL_KEY 를 새로 발급한다 (재사용 금지).
                    api_url = "/oauth2/Approval"
                    api_header = {
                        "content-type" : "application/json; utf-8"
                    }
                    api_body = {
                        "grant_type" : "client_credentials",
                        "appkey" : ws_app_info["API_KEY"],
                        "secretkey" : ws_app_info["API_SECRET"],
                    }
                    self.__throttle_auth_issue("approval")
                    response = requests.post(
                        url = self.__API_BASE_URL + api_url,
                        headers = api_header,
                        data = json.dumps(api_body),
                        timeout = self.__WS_APPROVAL_TIMEOUT,
                    )
                    response.raise_for_status()

                    rep_json = response.json()
                    if "approval_key" not in rep_json:
                        raise Exception(f"Approval key is missing [ {response.text[:200]} ]")

                    ws_app_info["APPROVAL_KEY"] = rep_json["approval_key"]
                    self.__save_approval_key(ws_app_info["API_KEY"], ws_app_info["APPROVAL_KEY"])
                    util.InsertLog(
                        "ApiKoreaInvest",
                        "N",
                        f"Issued approval key [ {ws_name} | key={ws_app_info['APPROVAL_KEY'][:8]}.. | http={response.status_code} ]"
                    )
                    ws_app_info["WS_APP"] = WebSocketApp(
                        url = "ws://ops.koreainvestment.com:21000",
                        on_message= self.__on_ws_recv_message,
                        on_open= self.__on_ws_open,
                        on_close= self.__on_ws_close,
                        on_error= self.__on_ws_error,
                    )
                    ws_app_info["WS_THREAD"] = Thread(
                        name= ws_app_info["WS_NAME"],
                        target= ws_app_info["WS_APP"].run_forever,
                        kwargs= {
                            "ping_interval": self.__WS_PING_INTERVAL_SEC,
                            "ping_timeout": self.__WS_PING_TIMEOUT_SEC,
                        },
                    )
                    ws_app_info["WS_THREAD"].daemon = True
                    ws_app_info["WS_THREAD"].start()

                    # 주의: 여기서 fail_count를 즉시 0으로 리셋하지 않는다.
                    # 서버가 핸드셰이크 직후 끊는 경우를 단명 연결로 감지하기 위해
                    # 다음 __on_ws_close 진입 시 WS_OPENED_AT 기준으로 리셋한다.
                    util.InsertLog("ApiKoreaInvest", "N", f"Reconnect started [ {ws_name} | attempt={reconnect_attempt + 1} | fail_count={fail_count} ]")
                    break

                except Exception as ex:
                    fail_count += 1
                    ws_app_info["WS_RECONNECT_FAIL_COUNT"] = fail_count
                    util.InsertLog(
                        "ApiKoreaInvest",
                        "E",
                        f"Fail to reconnect korea invest websocket [ {ws_name} | attempt={reconnect_attempt + 1} | wait={wait_sec:.0f}s | fail_count={fail_count} | {ex.__str__()} ]"
                    )
                    # 3회 실패 시 APPROVAL_KEY 초기화하여 다음 시도에서 재발급
                    if fail_count >= 3:
                        ws_app_info["APPROVAL_KEY"] = ""
                        self.__save_approval_key(ws_app_info["API_KEY"], "")
                    reconnect_attempt += 1

            util.InsertLog("ApiKoreaInvest", "N", f"Closed korea invest websocket [ {ws_name} ]")
            break


    def __save_approval_key(self, api_key: str, approval_key: str) -> None:
        try:
            file_data = korea_invest_token_info.load_last_token_info()
            entry = file_data.get(api_key, {})
            entry["APPROVAL_KEY"] = approval_key
            if approval_key:
                entry["APPROVAL_KEY_ISSUED_AT"] = DateTime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                entry.pop("APPROVAL_KEY_ISSUED_AT", None)
            file_data[api_key] = entry
            korea_invest_token_info.save_last_token_info(file_data)
        except:
            pass


    ##########################################################################


    def __sync_rest_api_token_list(self) -> None:
        for rest_api_token in self.__rest_api_token_list:
            remain_sec = (rest_api_token["TOKEN_EXPIRED_DATETIME"] - DateTime.now()).total_seconds()

            # 토큰 만료까지 20시간 이상 남아있으면 재활용
            if remain_sec >= 72000:
                util.InsertLog("ApiKoreaInvest", "N", f"Token reused ({remain_sec:.0f}s remaining) | {rest_api_token['API_KEY'][:8]}...")
                continue

            # 유효하지만 곧 만료되는 토큰은 revoke 후 재발급
            try:
                if remain_sec > 0 and rest_api_token["TOKEN_VAL"]:
                    api_url = "/oauth2/revokeP"
                    api_body = {
                        "grant_type" : "client_credentials",
                        "appkey" : rest_api_token["API_KEY"],
                        "appsecret" : rest_api_token["API_SECRET"],
                        "token" : rest_api_token["TOKEN_VAL"]
                    }
                    response = requests.post (
                        url = self.__API_BASE_URL + api_url,
                        data = json.dumps(api_body),
                    )
    
                    rest_api_token["TOKEN_TYPE"] = ""
                    rest_api_token["TOKEN_VAL"] = ""
                    rest_api_token["TOKEN_EXPIRED_DATETIME"] = DateTime.min
                    rest_api_token["TOKEN_HEADER"] = {}
    
            except: pass

            # 새 토큰 발급
            try:
                api_url = "/oauth2/tokenP"
                api_body = {
                    "grant_type" : "client_credentials",
                    "appkey" : rest_api_token["API_KEY"],
                    "appsecret" : rest_api_token["API_SECRET"],
                }
                self.__throttle_auth_issue("token")
                response = requests.post (
                    url = self.__API_BASE_URL + api_url,
                    data = json.dumps(api_body),
                )

                rep_json = json.loads(response.text)

                rest_api_token["TOKEN_TYPE"] = rep_json["token_type"]
                rest_api_token["TOKEN_VAL"] = rep_json["access_token"]
                rest_api_token["TOKEN_EXPIRED_DATETIME"] = DateTime.strptime(rep_json["access_token_token_expired"], "%Y-%m-%d %H:%M:%S")
                rest_api_token["TOKEN_HEADER"] = {
                    "content-type" : "application/json; charset=utf-8",
                    "authorization" : rest_api_token["TOKEN_TYPE"] + " " + rest_api_token["TOKEN_VAL"],
                    "appkey" : rest_api_token["API_KEY"],
                    "appsecret" : rest_api_token["API_SECRET"],
                    "custtype" : "P"
                }
    
            except: raise Exception(f"Get New Token | {rest_api_token['API_KEY']}")

        try:
            file_data = korea_invest_token_info.load_last_token_info()
        except:
            file_data = {}

        try:
            for rest_api_token in self.__rest_api_token_list:
                key = rest_api_token["API_KEY"]
                entry = file_data.get(key, {})
                entry.update({
                    "TOKEN_TYPE" : rest_api_token["TOKEN_TYPE"],
                    "TOKEN_VAL" : rest_api_token["TOKEN_VAL"],
                    "TOKEN_EXPIRED_DATETIME" : rest_api_token["TOKEN_EXPIRED_DATETIME"].strftime("%Y-%m-%d %H:%M:%S"),
                })
                file_data[key] = entry

            korea_invest_token_info.save_last_token_info(file_data)

        except: util.InsertLog("ApiKoreaInvest", "E", "Fail to create access token for KoreaInvest Api")
    
    def __sync_stock_info_table(self) -> None:
        try:
            if not os.path.exists("./temp"):
                os.makedirs("./temp")
            else:
                files = glob.glob('./temp/*')
                for f in files:
                    os.remove(f)

            stock_code_list = {
                "KOSPI" : self.__get_kospi_stock_list(),
                "KOSDAQ" : self.__get_kosdaq_stock_list(),
                "KONEX" : self.__get_konex_stock_list(),
                "NASDAQ" : self.__get_nasdaq_stock_list(),
                "NYSE" : self.__get_nyse_stock_list(),
                "AMEX" : self.__get_amex_stock_list(),
            }
   
            temp_market_code_list = [[] for _ in range(len(self.__rest_api_token_list))]
            token_idx = 0
            for stock_market, stock_code_infos in stock_code_list.items():
                for stock_type, stock_code_list in stock_code_infos.items():
                    for stock_code in stock_code_list:
                        temp_market_code_list[token_idx].append([stock_market, stock_type, stock_code])
                        token_idx += 1
                        if token_idx >= len(self.__rest_api_token_list):
                            token_idx = 0
   
            def kernel_func(rest_api_token:dict, stock_market_code_list:list) -> None:
                for stock_market_code in stock_market_code_list:
                    try:
                        min_micro = 1000000. / self.__MAX_REST_API_COUNT_PER_KEY + self.__REST_API_DELAY_MICRO
                        start_dt = DateTime.now()
                        stock_market = stock_market_code[0]
                        stock_type = stock_market_code[1]
                        stock_code = stock_market_code[2]
                        rest_api_token_header = rest_api_token["TOKEN_HEADER"]
      
                        if stock_market in ["KOSPI", "KOSDAQ", "KONEX"]:
                            min_micro *= 1
                            stock_info_dict = self.__rest_api_kr_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
                            self.__update_stock_info_table(stock_info_dict)
                        elif stock_market in ["NASDAQ", "NYSE", "AMEX"]:
                            min_micro *= 2
                            stock_info_dict = self.__rest_api_ex_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
                            self.__update_stock_info_table(stock_info_dict)
                        else:
                            continue

                        diff_micro = (DateTime.now() - start_dt).microseconds
                        if min_micro > diff_micro:
                            time.sleep((min_micro - diff_micro) / 1000000.)
       
                    except Exception as e:
                        util.InsertLog("ApiKoreaInvest", "E", f"Fail to update stock info [ {stock_market} | {stock_code} | {e.__str__()}]")

      
            temp_thread_list = []
            for idx in range(len(self.__rest_api_token_list)):
                t_thread = Thread(
                    name= f"KoreaInvest_Update_Stock_Info_{idx}",
                    target= kernel_func,
                    args=(self.__rest_api_token_list[idx], temp_market_code_list[idx])
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
                raise

            cursor = self.__execute_sync_query(select_query)
            sql_query_list = cursor.fetchall()
   
            temp_list = [[] for _ in range(len(self.__ws_app_info_list))]
            app_idx = 0
            for sql_query in sql_query_list:
                # KIS 가 한 번이라도 MAX SUBSCRIBE OVER 응답한 종목은 분배 자체에서 제외
                if (sql_query[3], sql_query[2]) in self.__ws_blocked_keys:
                    continue
                if len(temp_list[app_idx]) > self.__MAX_WS_QUERY_COUNT_PER_KEY:
                    util.InsertLog("ApiKoreaInvest", "E", f"WS query was overflowed ( {sql_query[0]} : {sql_query[2]} )")
                else:
                    temp_list[app_idx].append({
                        "stock_code" : sql_query[0],
                        "stock_market" : sql_query[1],
                        "stock_api_type" : sql_query[2],
                        "stock_api_stock_code" : sql_query[3]
                    })

                app_idx += 1
                if app_idx >= len(self.__ws_app_info_list):
                    app_idx = 0
     
            # 종목 변경분만 unsub/sub delta 메시지로 전송하여 WS 연결을 유지한다.
            # (기존 구현은 close() 후 reconnect로 전체 재구독했으나, 이는 한국투자증권의
            #  '무한 연결시도' 차단 정책에 가까워질 수 있어 delta 방식으로 전환.)
            def _key(q:dict) -> tuple:
                return (q["stock_api_type"], q["stock_api_stock_code"])

            for idx in range(len(self.__ws_app_info_list)):
                ws_app_info = self.__ws_app_info_list[idx]
                new_list = temp_list[idx]
                old_list = ws_app_info["WS_QUERY_LIST"]
                old_keys = {_key(q) for q in old_list}
                new_keys = {_key(q) for q in new_list}

                removed = [q for q in old_list if _key(q) not in new_keys]
                added = [q for q in new_list if _key(q) not in old_keys]

                # 새 리스트로 먼저 갱신 (재연결 시 on_open이 정확한 상태로 등록되도록)
                ws_app_info["WS_QUERY_LIST"] = new_list

                util.InsertLog(
                    "ApiKoreaInvest",
                    "N",
                    f"Sync ws query list [ {ws_app_info['WS_NAME']} | total={len(new_list)} | removed={len(removed)} | added={len(added)} | opened={ws_app_info['WS_IS_OPENED']} ]"
                )

                # WS가 열려있고 승인키가 있을 때만 delta 메시지 송신.
                # 닫혀 있으면 다음 on_open에서 새 리스트가 일괄 등록되므로 별도 처리 불필요.
                if not (ws_app_info["WS_IS_OPENED"] and ws_app_info["APPROVAL_KEY"]):
                    continue

                # 41건 한도 보호: 해제 먼저, 그 다음 등록.
                for q in removed:
                    self.__send_ws_subscription(ws_app_info, q, "2")
                    time.sleep(self.__WS_SEND_GAP_SEC)
                for q in added:
                    self.__send_ws_subscription(ws_app_info, q, "1")
                    time.sleep(self.__WS_SEND_GAP_SEC)

        except: raise Exception("Fail to sync websocket query list")
    

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
                cursor = self.__execute_sync_query(select_query)
                sql_query_list = cursor.fetchall()

                this_year = DateTime.now().year
                for sql_query in sql_query_list:
                    if sql_query[1] in ("H0STCNT0", "HDFSCNT0"):
                        self.__create_stock_execution_table(sql_query[0], this_year)
                        self.__create_stock_execution_table(sql_query[0], this_year + 1)
                    elif sql_query[1] in ("H0STASP0", "HDFSASP0"):
                        self.__create_stock_orderbook_table(sql_query[0], this_year)
                        self.__create_stock_orderbook_table(sql_query[0], this_year + 1)
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync partitions for korea invest api [ {ex.__str__()} ] ")

    def SyncDailyInfo(self, target_market:str) -> None:
        try:
            self.__ws_query_type = target_market
            self.__sync_rest_api_token_list()
            self.__sync_ws_query_list()
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync daily info for korea invest api [ {ex.__str__()} ] ")
   
    def SyncWeeklyInfo(self) -> None:
        try:
            self.__sync_rest_api_token_list()
            self.__sync_stock_info_table()
        except Exception as ex:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync weekly info for korea invest api [ {ex.__str__()} ] ")
       
        





