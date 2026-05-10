from core import util
from . import _tables as tables
from websocket import WebSocketApp
from threading import Thread
import json
import time
from datetime import datetime as DateTime


class BithumbWsClient:
    __WS_BASE_URL: str = "wss://pubwss.bithumb.com/pub/ws"

    def __init__(self, sql_client):
        self.__sql_client = sql_client
        self.__ws_app: WebSocketApp = None
        self.__ws_thread: Thread = None
        self.__ws_is_opened = False
        self.__ws_keep_connect = True
        self.__ws_query_list = []

    def start(self) -> None:
        self.__ws_app = WebSocketApp(
            url=self.__WS_BASE_URL,
            on_message=self.__on_ws_recv_message,
            on_open=self.__on_ws_open,
            on_close=self.__on_ws_close,
        )
        self.__ws_thread = Thread(
            name="Bithumb_WS",
            target=self.__ws_app.run_forever,
        )
        self.__ws_is_opened = False
        self.__ws_thread.daemon = True
        self.__ws_thread.start()
        self.__ws_keep_connect = True
        self.__ws_query_list = []

    def stop(self) -> None:
        self.__ws_keep_connect = False
        if self.__ws_app is not None:
            self.__ws_app.close()

    def update_subscriptions(self, ws_query_list: list) -> None:
        self.__ws_query_list = ws_query_list
        if self.__ws_app is not None:
            self.__ws_app.close()

    def __on_ws_recv_message(self, ws: WebSocketApp, msg: str) -> None:
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

    def __on_ws_open(self, ws: WebSocketApp) -> None:
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
                    "type": key,
                    "symbols": val,
                }
                self.__ws_app.send(json.dumps(msg))

            except Exception as e:
                util.InsertLog("ApiBithumb", "E", f"Fail to process ws send msg [ Bithumb_WS | {e.__str__()} ]")

            time.sleep(0.25)

        util.InsertLog("ApiBithumb", "N", "Opened bithumb websocket [ Bithumb_WS ]")

    def __on_ws_close(self, ws: WebSocketApp, close_code, close_msg) -> None:
        self.__ws_is_opened = False

        while self.__ws_keep_connect == True:
            time.sleep(1)
            try:
                self.__ws_app = WebSocketApp(
                    url=self.__WS_BASE_URL,
                    on_message=self.__on_ws_recv_message,
                    on_open=self.__on_ws_open,
                    on_close=self.__on_ws_close,
                )
                self.__ws_thread = Thread(
                    name="Bithumb_WS",
                    target=self.__ws_app.run_forever,
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

    def __on_recv_coin_execution(self, msg_json) -> None:
        for data in msg_json["content"]["list"]:
            coin_code = "?"
            try:
                from_to = data["symbol"].split("_")
                coin_code = from_to[0]
                dt_str = data["contDtm"].split(".")

                dt = DateTime.strptime(dt_str[0], "%Y-%m-%d %H:%M:%S")
                price = float(data["contPrice"])
                volume = float(data["contQty"])

                if data["buySellGb"] == "1":
                    tables.enqueue_update_coin_execution(self.__sql_client, coin_code, dt, price, 0, volume, 0)
                elif data["buySellGb"] == "2":
                    tables.enqueue_update_coin_execution(self.__sql_client, coin_code, dt, price, 0, 0, volume)
                else:
                    tables.enqueue_update_coin_execution(self.__sql_client, coin_code, dt, price, volume, 0, 0)

            except Exception as e:
                util.InsertLog("ApiBithumb", "E", "[ coin execution ][ %s ][ %s ]"%(coin_code, e.__str__()))

    def __on_recv_coin_orderbook(self, msg_json) -> None:
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
