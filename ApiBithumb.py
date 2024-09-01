import Util
import requests
from websocket import WebSocketApp
import pymysql
import json
import queue
from threading import Thread
import time
from datetime import datetime as DateTime


class ApiBithumbType:
	__sql_main_db:str
	__sql_common_connection:pymysql.Connection = None
	__sql_query_connection:pymysql.Connection = None
	__sql_is_stop:bool = False
	__sql_thread:Thread = None
	__sql_query_queue:queue.Queue = queue.Queue()

	__API_BASE_URL:str = "https://api.bithumb.com/public"

	__WS_BASE_URL:str = "wss://pubwss.bithumb.com/pub/ws"
	__ws_app:WebSocketApp = None
	__ws_thread:Thread = None
	__ws_is_opened = False
	__ws_keep_connect = True
	__ws_query_list = []

	__ws_query_datetime = DateTime.min
	__ws_query_list_buf = {}


	##########################################################################


	def __init__(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str):
		self.__sql_main_db = sql_db
		self.__sql_common_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			db = sql_db,
			charset = 'utf8',
			autocommit=True
		)
		self.__sql_query_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			charset = 'utf8',
			autocommit=True,
		)

		self.__create_coin_info_table()
		self.__create_last_ws_query_table()
		self.__load_last_ws_query_table()
		self.__start_dequeue_sql_query()
  
		self.__create_ws_app_info()

	def __del__(self):
		self.__ws_keep_connect = False
		self.__ws_app.close()
		self.__stop_dequeue_sql_query()


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

			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(table_query_str)

		except: raise Exception("Fail to create coin info table")

	def __create_last_ws_query_table(self) -> None:
		try:
			create_table_query = (
				"CREATE TABLE IF NOT EXISTS coin_last_ws_query ("
				+ "coin_query VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "coin_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "coin_api_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "coin_api_coin_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "PRIMARY KEY (coin_query) USING BTREE,"
				+ "INDEX coin_code (coin_code) USING BTREE,"
				+ "CONSTRAINT FK_coin_list_last_query_coin_info FOREIGN KEY (coin_code) REFERENCES coin_info (coin_code) ON UPDATE CASCADE ON DELETE CASCADE"
				+ ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
				)
			
			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(create_table_query)

		except: raise Exception("Fail to create last websocket query table")

	def __load_last_ws_query_table(self) -> None:
		try:
			select_query = "SELECT * FROM coin_last_ws_query"
			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(select_query)

			last_query_list = cursor.fetchall()
			self.__ws_query_list_buf = {}
			for info in last_query_list:
				self.__ws_query_list_buf[info[0]] = {
					"coin_code" : info[1],
					"coin_api_type" : info[2],
					"coin_api_coin_code" : info[3]
				}

		except: raise Exception("Fail to load last websocket query table")
	
 
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
			raise Exception("UPBIT REQUEST ERROR")
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
		self.__sql_is_stop = False
		self.__sql_thread = Thread(name= "Bithumb_Dequeue", target=self.__func_dequeue_sql_query)
		self.__sql_thread.daemon = True
		self.__sql_thread.start()
		
	def __stop_dequeue_sql_query(self) -> None:
		self.__sql_is_stop = True
		self.__sql_thread.join()

	def __func_dequeue_sql_query(self) -> None:
		while self.__sql_is_stop == False:
			self.__sql_query_connection.ping(reconnect=True)
			cursor = self.__sql_query_connection.cursor()
			while self.__sql_query_queue.empty() == False:
				try: cursor.execute(self.__sql_query_queue.get())
				except: pass

			time.sleep(0.2)
		
		self.__sql_query_connection.ping(reconnect=True)
		cursor = self.__sql_query_connection.cursor()
		while self.__sql_query_queue.empty() == False:
			try: cursor.execute(self.__sql_query_queue.get())
			except: pass


	def __update_coin_info_table(self, coin_info_dict:dict) -> None:
		self.__sql_query_queue.put(
			f"INSERT INTO {self.__sql_main_db}.coin_info ("
			+ "coin_code, coin_name_kr, coin_name_en, coin_price, coin_amount"
			+ ") VALUES ("
			+ f"'{coin_info_dict['coin_code']}',"
			+ f"'{coin_info_dict['coin_name_kr']}',"
			+ f"'{coin_info_dict['coin_name_en']}',"
			+ f"'{coin_info_dict['coin_price']}',"
			+ f"'{coin_info_dict['coin_amount']}' "
			+ ") ON DUPLICATE KEY UPDATE "
			+ f"coin_name_kr='{coin_info_dict['coin_name_kr']}',"
			+ f"coin_name_en='{coin_info_dict['coin_name_en']}',"
			+ f"coin_price='{coin_info_dict['coin_price']}',"
			+ f"coin_amount='{coin_info_dict['coin_amount']}'"
		)
  
	def __update_coin_execution_table(self, coin_code:str, dt:DateTime, price:float, non_volume:float, ask_volume:float, bid_volume:float) -> None:
		database_name = "Z_Coin" + coin_code.replace("/", "_")
		raw_table_name = database_name + ".Raw" + dt.strftime("%Y%V")
		candle_table_name = database_name + ".Candle" + dt.strftime("%Y%V")

		datetime_00_min = dt
		datetime_10_min = dt.replace(minute=dt.minute // 10 * 10, second=0)
		price_str = str(price)
		non_volume_str = str(non_volume)
		ask_volume_str = str(ask_volume)
		bid_volume_str = str(bid_volume)
		non_amount_str = str(price * non_volume)
		ask_amount_str = str(price * ask_volume)
		bid_amount_str = str(price * bid_volume)
		
		self.__sql_query_queue.put(
			"CREATE DATABASE IF NOT EXISTS " + database_name + " "
			+ "CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_general_ci'"
		)
		self.__sql_query_queue.put(
			"CREATE TABLE IF NOT EXISTS " + raw_table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0' "
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		self.__sql_query_queue.put(
			"CREATE TABLE IF NOT EXISTS " + candle_table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_open DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_close DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_min DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_max DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "PRIMARY KEY (execution_datetime) USING BTREE"
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
		)
		self.__sql_query_queue.put(
			"INSERT INTO " + raw_table_name + " VALUES ("
			+ "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + non_volume_str + "',"
			+ "'" + ask_volume_str + "',"
			+ "'" + bid_volume_str + "' "
			+ ")"
		)
		self.__sql_query_queue.put(
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
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)

		self.__sql_common_connection.ping(reconnect=True)
		cursor = self.__sql_common_connection.cursor()
		cursor.execute(orderbook_table_query_str)

		table_name = coin_code.replace("/", "_")
		orderbook_table_query_str = (
			"INSERT INTO coin_orderbook_" + table_name + " VALUES ("
			+ "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + data + "'"
			+ ")"
		)

		self.__sql_common_connection.ping(reconnect=True)
		cursor = self.__sql_common_connection.cursor()
		cursor.execute(orderbook_table_query_str)


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
					Util.InsertLog("ApiBithumb", "E", f"Fail msg : [ {status}:{msg} ]")

			elif "type" in msg_json:
				if msg_json["type"] == "transaction":
					Thread(name="Bithumb_Execution", target=self.__on_recv_coin_execution(msg_json)).start()
				elif msg_json["type"] == "orderbooksnapshot":
					Thread(name="Bithumb_Orderbook", target=self.__on_recv_coin_orderbook(msg_json)).start()

		except Exception as e:
			Util.InsertLog("ApiBithumb", "E", "Fail to process ws recv msg : " + e.__str__())
	
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
				Util.InsertLog("ApiBithumb", "E", f"Fail to process ws send msg [ Bithumb_WS | {e.__str__()} ]")

			time.sleep(0.25)

		Util.InsertLog("ApiBithumb", "N", "Opened bithumb websocket [ Bithumb_WS ]")

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
	
				Util.InsertLog("ApiBithumb", "N", f"Reconnected bithumb websocket [ Bithumb_WS ]")
				break
	
			except Exception as ex:
				Util.InsertLog("ApiBithumb", "E", f"Fail to reconnect korea bithumb websocket [ Bithumb_WS | {ex.__str__()} ]")
	
		Util.InsertLog("ApiBithumb", "N", "Closed bithumb websocket [ Bithumb_WS ]")


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

				
			Util.InsertLog("ApiBithumb", "N", "Success to update coin info")

		except Exception as e: 
			Util.InsertLog("ApiBithumb", "E", "Fail to update coin info : " + e.__str__())
 
	def __sync_ws_query_list(self) -> None:
		try:
			select_query = "SELECT * FROM coin_last_ws_query"
			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(select_query)

			last_query_list = cursor.fetchall()

			temp_list = []
			for info in last_query_list:
				temp_list.append({
					"coin_code" : info[1],
					"coin_api_type" : info[2],
					"coin_api_coin_code" : info[3]
				})
	
			self.__ws_query_list = temp_list
			self.__ws_app.close()

		except: raise Exception("Fail to load last websocket query table")
 

	##########################################################################


	def GetCurrentCollectingDateTime(self) -> DateTime:
		return self.__ws_query_datetime


	def SyncDailyInfo(self) -> None:
		try:
			self.__ws_query_datetime = DateTime.now().replace(hour=4, minute=0, second=0)
			self.__sync_ws_query_list()
		except Exception as ex:
			Util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync daily info for bithumb api [ {ex.__str__()} ] ")
   
	def SyncWeeklyInfo(self) -> None:
		try:
			self.__sync_coin_info_table()
		except Exception as ex:
			Util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync weekly info for bithumb api [ {ex.__str__()} ] ")




