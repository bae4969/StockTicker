from core import config
from core import util
from . import _token_storage
from threading import Lock
from datetime import datetime as DateTime
import json
import requests
import time


class _AuthThrottle:
    # 한국투자증권 공지: /oauth2/tokenP, /oauth2/Approval 모두 1초당 1건 한도.
    # 다중 키 동시 만료/재연결 상황에서도 글로벌 1Hz 직렬화 보장.
    __AUTH_ISSUE_MIN_INTERVAL_SEC: float = 1.1

    __token_issue_lock: Lock = Lock()
    __token_issue_last_at: DateTime = DateTime.min
    __approval_issue_lock: Lock = Lock()
    __approval_issue_last_at: DateTime = DateTime.min

    @classmethod
    def throttle(cls, kind: str) -> None:
        if kind == "token":
            lock = _AuthThrottle.__token_issue_lock
        else:
            lock = _AuthThrottle.__approval_issue_lock
        lock.acquire()
        try:
            if kind == "token":
                last_at = _AuthThrottle.__token_issue_last_at
            else:
                last_at = _AuthThrottle.__approval_issue_last_at
            elapsed = (DateTime.now() - last_at).total_seconds()
            if elapsed < cls.__AUTH_ISSUE_MIN_INTERVAL_SEC:
                time.sleep(cls.__AUTH_ISSUE_MIN_INTERVAL_SEC - elapsed)
            if kind == "token":
                _AuthThrottle.__token_issue_last_at = DateTime.now()
            else:
                _AuthThrottle.__approval_issue_last_at = DateTime.now()
        finally:
            lock.release()


class _RestThrottle:
    # 한국투자증권 시세조회 REST 는 API 키당 1초 20건 한도 (초과 시 EGW00201).
    #
    # 종목마스터 주간 싱크와 지수·환율 폴링이 같은 키를 공유하므로, 호출부마다 sleep 을 두면
    # 서로의 소비량을 모른 채 합계가 한도를 넘는다. 그래서 키 단위로 여기 한 곳에서 직렬화한다.
    # 간격은 기존 주간 싱크가 쓰던 값과 동일하게 맞춰, 기존 부하 프로파일을 바꾸지 않는다.
    __key_lock_dict: dict = {}
    __key_last_call_at_dict: dict = {}
    __dict_lock: Lock = Lock()

    @classmethod
    def throttle(cls, api_key: str, min_interval_sec: float) -> None:
        with _RestThrottle.__dict_lock:
            if api_key not in _RestThrottle.__key_lock_dict:
                _RestThrottle.__key_lock_dict[api_key] = Lock()
                _RestThrottle.__key_last_call_at_dict[api_key] = DateTime.min
            key_lock = _RestThrottle.__key_lock_dict[api_key]

        with key_lock:
            elapsed = (DateTime.now() - _RestThrottle.__key_last_call_at_dict[api_key]).total_seconds()
            if elapsed < min_interval_sec:
                time.sleep(min_interval_sec - elapsed)
            _RestThrottle.__key_last_call_at_dict[api_key] = DateTime.now()


class KoreaInvestRestClient:
    API_BASE_URL: str = "https://openapi.koreainvestment.com:9443"

    MAX_REST_API_COUNT_PER_KEY: int = 18
    REST_API_DELAY_MICRO: int = 50000
    REST_API_TIMEOUT: tuple = (5, 15)
    REST_API_RETRY: int = 2
    WS_APPROVAL_TIMEOUT: tuple = (3.05, 10)

    def __init__(self, api_key_list: list):
        self.__api_key_list = api_key_list
        self.__rest_api_token_list: list = []
        self.__create_rest_api_token_list()

    def get_token_list(self) -> list:
        return self.__rest_api_token_list

    def issue_approval_key(self, api_key: str, api_secret: str, ws_name: str) -> str:
        approval_key = ""
        try:
            _AuthThrottle.throttle("approval")
            response = requests.post(
                url=self.API_BASE_URL + "/oauth2/Approval",
                headers={"content-type": "application/json; utf-8"},
                data=json.dumps({
                    "grant_type": "client_credentials",
                    "appkey": api_key,
                    "secretkey": api_secret,
                }),
                timeout=self.WS_APPROVAL_TIMEOUT,
            )
            response.raise_for_status()
            rep_json = response.json()
            if "approval_key" in rep_json:
                approval_key = rep_json["approval_key"]
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
        return approval_key

    def reissue_approval_key_for_reconnect(self, ws_app_info: dict) -> None:
        # 매 연결 시도마다 APPROVAL_KEY 를 새로 발급한다 (재사용 금지).
        ws_name = ws_app_info["WS_NAME"]
        api_url = "/oauth2/Approval"
        api_header = {
            "content-type" : "application/json; utf-8"
        }
        api_body = {
            "grant_type" : "client_credentials",
            "appkey" : ws_app_info["API_KEY"],
            "secretkey" : ws_app_info["API_SECRET"],
        }
        _AuthThrottle.throttle("approval")
        response = requests.post(
            url = self.API_BASE_URL + api_url,
            headers = api_header,
            data = json.dumps(api_body),
            timeout = self.WS_APPROVAL_TIMEOUT,
        )
        response.raise_for_status()

        rep_json = response.json()
        if "approval_key" not in rep_json:
            raise Exception(f"Approval key is missing [ {response.text[:200]} ]")

        ws_app_info["APPROVAL_KEY"] = rep_json["approval_key"]
        util.InsertLog(
            "ApiKoreaInvest",
            "N",
            f"Issued approval key [ {ws_name} | key={ws_app_info['APPROVAL_KEY'][:8]}.. | http={response.status_code} ]"
        )

    def sync_token_list(self) -> None:
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
                        url = self.API_BASE_URL + api_url,
                        data = json.dumps(api_body),
                        timeout = self.REST_API_TIMEOUT,
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
                _AuthThrottle.throttle("token")
                response = requests.post (
                    url = self.API_BASE_URL + api_url,
                    data = json.dumps(api_body),
                    timeout = self.REST_API_TIMEOUT,
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

            except Exception as e: raise Exception(f"Get New Token | {rest_api_token['API_KEY']} | {e}")

        try:
            file_data = _token_storage.load_last_token_info()
        except Exception as e:
            util.InsertLog("ApiKoreaInvest", "E", f"Fail to reload token file before save [ {e} ]")
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

            _token_storage.save_last_token_info(file_data)

        except Exception as e: util.InsertLog("ApiKoreaInvest", "E", f"Fail to create access token for KoreaInvest Api | {e}")

    def kr_stock_info_dict(self, rest_api_token_header: dict, stock_code: str, stock_type: str, stock_market: str) -> dict:
        try:
            api_url = "/uapi/domestic-stock/v1/quotations/search-stock-info"
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "CTPF1002R"
            api_para = {
                "PRDT_TYPE_CD" : "300",
                "PDNO" : stock_code,
            }

            response = self.__safe_get(
                url = self.API_BASE_URL + api_url,
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0:
                raise Exception(f"Recv Code [ {rep_json.get('msg1', '')} ]")

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

    def ex_stock_info_dict(self, rest_api_token_header: dict, stock_code: str, stock_type: str, stock_market: str) -> dict:
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

            response = self.__safe_get(
                url = self.API_BASE_URL + api_url,
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0: raise Exception(f"Recv Code 1 [ {rep_json.get('msg1', '')} ]")

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

            response = self.__safe_get(
                url = self.API_BASE_URL + api_url,
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0: raise Exception(f"Recv Code 2 [ {rep_json.get('msg1', '')} ]")

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

    def kr_index_tick_list(self, rest_api_token_header: dict, index_code: str) -> list:
        # 국내업종 시간별지수(분). 최신순 [(DateTime, 지수값, 거래량), ...]
        try:
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "FHPUP02110200"
            api_para = {
                "FID_COND_MRKT_DIV_CODE" : "U",
                "FID_INPUT_ISCD" : index_code,
                "FID_INPUT_HOUR_1" : "60",
            }

            response = self.__safe_get(
                url = self.API_BASE_URL + "/uapi/domestic-stock/v1/quotations/inquire-index-timeprice",
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0:
                raise Exception(f"Recv Code [ {rep_json.get('msg1', '')} ]")

            today_str = DateTime.now().strftime("%Y%m%d")
            result_list = []
            for row in rep_json.get("output", []):
                hour_str = row.get("bsop_hour", "")
                # 999999(현재값)·888888(장마감 집계)은 시각이 아닌 특수행이라 버린다.
                if len(hour_str) != 6 or hour_str in ("999999", "888888"):
                    continue
                result_list.append((
                    DateTime.strptime(today_str + hour_str, "%Y%m%d%H%M%S"),
                    util.TryParseFloat(row.get("bstp_nmix_prpr")),
                    util.TryParseFloat(row.get("cntg_vol")),
                ))

            return result_list

        except Exception as e:
            raise Exception("[ kr index ][ %s ][ %s ]"%(index_code, e.__str__()))

    def ex_index_candle_list(self, rest_api_token_header: dict, index_code: str) -> list:
        # 해외지수 분봉. 최신순 [(DateTime(현지시각), 종가, 시가, 고가, 저가, 거래량), ...]
        try:
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "FHKST03030200"
            api_para = {
                "FID_COND_MRKT_DIV_CODE" : "N",
                "FID_INPUT_ISCD" : index_code,
                "FID_HOUR_CLS_CODE" : "0",
                "FID_PW_DATA_INCU_YN" : "Y",
            }

            response = self.__safe_get(
                url = self.API_BASE_URL + "/uapi/overseas-price/v1/quotations/inquire-time-indexchartprice",
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0:
                raise Exception(f"Recv Code [ {rep_json.get('msg1', '')} ]")

            if not self.__is_valid_quote(rep_json.get("output1")):
                raise Exception("Invalid index code (rt_cd=0 but empty quote)")

            result_list = []
            for row in rep_json.get("output2", []):
                date_str = row.get("stck_bsop_date", "")
                hour_str = row.get("stck_cntg_hour", "")
                if len(date_str) != 8 or len(hour_str) != 6:
                    continue
                result_list.append((
                    DateTime.strptime(date_str + hour_str, "%Y%m%d%H%M%S"),
                    util.TryParseFloat(row.get("optn_prpr")),
                    util.TryParseFloat(row.get("optn_oprc")),
                    util.TryParseFloat(row.get("optn_hgpr")),
                    util.TryParseFloat(row.get("optn_lwpr")),
                    util.TryParseFloat(row.get("cntg_vol")),
                ))

            return result_list

        except Exception as e:
            raise Exception("[ ex index ][ %s ][ %s ]"%(index_code, e.__str__()))

    def fx_rate(self, rest_api_token_header: dict, fx_code: str) -> float:
        # 환율 현재값. 시계열(output2)을 주지 않으므로 조회 시점 스냅샷만 얻는다.
        try:
            api_header = rest_api_token_header.copy()
            api_header["tr_id"] = "FHKST03030200"
            api_para = {
                "FID_COND_MRKT_DIV_CODE" : "X",
                "FID_INPUT_ISCD" : fx_code,
                "FID_HOUR_CLS_CODE" : "0",
                "FID_PW_DATA_INCU_YN" : "Y",
            }

            response = self.__safe_get(
                url = self.API_BASE_URL + "/uapi/overseas-price/v1/quotations/inquire-time-indexchartprice",
                headers= api_header,
                params= api_para,
            )

            rep_json = json.loads(response.text)
            if int(rep_json["rt_cd"]) != 0:
                raise Exception(f"Recv Code [ {rep_json.get('msg1', '')} ]")

            rep_output = rep_json.get("output1")
            if not self.__is_valid_quote(rep_output):
                raise Exception("Invalid fx code (rt_cd=0 but empty quote)")

            return util.TryParseFloat(rep_output.get("ovrs_nmix_prpr"))

        except Exception as e:
            raise Exception("[ fx rate ][ %s ][ %s ]"%(fx_code, e.__str__()))

    @staticmethod
    def __is_valid_quote(output1) -> bool:
        # 존재하지 않는 종목코드에도 KIS 는 rt_cd=0 을 주고 값만 0 으로 채워 보낸다.
        # 이름이 비었거나 현재값이 0이면 무효 코드로 판단한다.
        if not isinstance(output1, dict):
            return False
        return bool(output1.get("hts_kor_isnm")) and util.TryParseFloat(output1.get("ovrs_nmix_prpr")) != 0.0

    def rest_min_interval_sec(self) -> float:
        # 키당 허용 호출 간격. 기존 주간 싱크가 쓰던 계산식을 그대로 쓴다.
        return 1.0 / self.MAX_REST_API_COUNT_PER_KEY + self.REST_API_DELAY_MICRO / 1000000.0

    def __safe_get(self, url: str, headers: dict, params: dict):
        # 유량 제어는 호출부가 아니라 여기서 키 단위로 건다 (주간 싱크·폴링이 같은 예산을 공유).
        api_key = headers.get("appkey", "")

        for attempt in range(self.REST_API_RETRY + 1):
            try:
                _RestThrottle.throttle(api_key, self.rest_min_interval_sec())
                return requests.get(url=url, headers=headers, params=params, timeout=self.REST_API_TIMEOUT)
            except requests.exceptions.ConnectionError:
                if attempt == self.REST_API_RETRY: raise
                time.sleep(0.5)

    def __create_rest_api_token_list(self) -> None:
        try:
            file_data = _token_storage.load_last_token_info()
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
