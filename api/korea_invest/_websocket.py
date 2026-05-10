from core import util
from . import _tables as tables
from websocket import WebSocketApp
from threading import Thread
from datetime import datetime as DateTime
import json
import time


class KoreaInvestWsClient:
    __WS_BASE_URL: str = "ws://ops.koreainvestment.com:21000"

    __MAX_WS_QUERY_COUNT_PER_KEY: int = 40
    __WS_RECONNECT_BACKOFF_MAX_SEC: int = 600
    __WS_STABLE_CONNECT_SEC: int = 60
    __WS_BAN_SUSPECT_THRESHOLD: int = 10
    __WS_PING_INTERVAL_SEC: int = 30
    __WS_PING_TIMEOUT_SEC: int = 10
    __WS_SEND_GAP_SEC: float = 0.15

    # KIS 가 MAX SUBSCRIBE OVER 응답한 (stock_api_stock_code, stock_api_type) 튜플의 set.
    # 한 번 거절된 종목은 이후 sync 분배에서 영구 제외 (프로세스 재시작 시 초기화).
    __ws_blocked_keys: set = set()

    def __init__(self, sql_client, rest_client, api_key_list: list):
        self.__sql_client = sql_client
        self.__rest_client = rest_client
        self.__api_key_list = api_key_list
        self.__ws_app_info_list: list = []
        self.__ws_ex_excution_last_volume: dict = {}

    def start(self) -> None:
        self.__ws_app_info_list = []
        for api_key in self.__api_key_list:
            ws_name = f"KoreaInvest_WS_{len(self.__ws_app_info_list)}"
            # 매 연결마다 새 APPROVAL_KEY 를 발급한다 (재사용 금지).
            # 시작 시 미리 발급해 두지 않으면, ws 가 안정적으로 open 되어 reconnect 루프가 돌지 않을 때
            # APPROVAL_KEY 가 영원히 빈 채로 남아 sync 의 send 가드에 걸려 구독 메시지가 송신되지 않는다.
            approval_key = self.__rest_client.issue_approval_key(api_key["KEY"], api_key["SECRET"], ws_name)

            ws_app = WebSocketApp(
                url= self.__WS_BASE_URL,
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

    def stop(self) -> None:
        for ws_app_info in self.__ws_app_info_list:
            ws_app_info["WS_KEEP_CONNECT"] = False
            ws_app_info["WS_APP"].close()

    def update_subscriptions(self, sql_query_list) -> None:
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
        def _key(q: dict) -> tuple:
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


    ##########################################################################


    # tr_type "1": 등록(subscribe), "2": 해제(unsubscribe)
    def __send_ws_subscription(self, ws_app_info: dict, query_info: dict, tr_type: str) -> bool:
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

    def __on_ws_recv_message(self, ws: WebSocketApp, msg: str) -> None:
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

    def __on_ws_open(self, ws: WebSocketApp) -> None:
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

    def __on_ws_close(self, ws: WebSocketApp, close_code, close_msg) -> None:
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
                    self.__rest_client.reissue_approval_key_for_reconnect(ws_app_info)
                    ws_app_info["WS_APP"] = WebSocketApp(
                        url = self.__WS_BASE_URL,
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
                    reconnect_attempt += 1

            util.InsertLog("ApiKoreaInvest", "N", f"Closed korea invest websocket [ {ws_name} ]")
            break

    def __on_recv_kr_stock_execution(self, data_cnt: int, data: str) -> None:
        # "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
        pValue = data.split('^')
        for data_idx in range(data_cnt):
            stock_code = "?"
            try:
                data_offset = 46 * data_idx
                stock_code = pValue[data_offset + 0]

                stock_execution_dt_str = DateTime.now().strftime("%Y%m%d") + pValue[data_offset + 1]

                dt = DateTime.strptime(stock_execution_dt_str, "%Y%m%d%H%M%S")
                price = float(pValue[data_offset + 2])
                volume = float(pValue[data_offset + 12])

                if pValue[data_offset + 21] == "1":
                    tables.enqueue_update_stock_execution(self.__sql_client, stock_code, dt, price, 0, 0, volume)
                elif pValue[data_offset + 21] == "5":
                    tables.enqueue_update_stock_execution(self.__sql_client, stock_code, dt, price, 0, volume, 0)
                else:
                    tables.enqueue_update_stock_execution(self.__sql_client, stock_code, dt, price, volume, 0, 0)

            except Exception as e:
                util.InsertLog("ApiKoreaInvest", "E", "[ kr stock execution ][ %s ][ %s ]"%(stock_code, e.__str__()))

    def __on_recv_ex_stock_execution(self, data_cnt: int, data: str) -> None:
        # "실시간종목코드|종목코드|수수점자리수|현지영업일자|현지일자|현지시간|한국일자|한국시간|시가|고가|저가|현재가|대비구분|전일대비|등락율|매수호가|매도호가|매수잔량|매도잔량|체결량|거래량|거래대금|매도체결량|매수체결량|체결강도|시장구분"
        pValue = data.split('^')
        for data_idx in range(data_cnt):
            stock_code = "?"
            try:
                data_offset = 26 * data_idx
                stock_code = pValue[data_offset + 1]

                stock_execution_dt_str = pValue[data_offset + 4] + pValue[data_offset + 5]

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
                    tables.enqueue_update_stock_execution(self.__sql_client, stock_code, dt, price, 0.0, ask_volume, bid_volume)
                else:
                    tables.enqueue_update_stock_execution(self.__sql_client, stock_code, dt, price, tot_volume, 0.0, 0.0)

            except Exception as e:
                util.InsertLog("ApiKoreaInvest", "E", "[ ex stock execution ][ %s ][ %s ]"%(stock_code, e.__str__()))

    def __on_recv_kr_stock_orderbook(self, data: str) -> None:
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

    def __on_recv_ex_stock_orderbook(self, data: str) -> None:
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
