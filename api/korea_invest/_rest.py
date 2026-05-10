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

            response = self.__safe_get(
                url = self.API_BASE_URL + api_url,
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

    def __safe_get(self, url: str, headers: dict, params: dict):
        for attempt in range(self.REST_API_RETRY + 1):
            try:
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
