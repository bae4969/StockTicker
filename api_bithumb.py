import requests
import websocket
import pymysql
import json
import threading
from datetime import datetime
from datetime import timedelta


class ApiBithumbType:
	__sql_connection:pymysql.Connection = None

	__API_BASE_URL:str = "https://api.bithumb.com/public"

	__WS_BASE_URL:str = "wss://pubwss.bithumb.com/pub/ws"
	__ws_app:websocket.WebSocketApp = None
	__ws_thread:threading.Thread = None
	__ws_send_lock:threading.Lock = threading.Lock()
	__ws_is_started = False

	__ws_sub_list = {
		"coin_execution" : [],
		"coin_orderbook" : [],
		}

	##########################################################################


	def __create_sql_connection(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str):
		try:
			self.__sql_connection = pymysql.connect(
				host = sql_host,
				port = 3306,
				user = sql_id,
				passwd = sql_pw,
				db = sql_db,
				charset = 'utf8',
				autocommit=True
			)

			return True
		except:
			self.__sql_connection = None
			return False
		
	def __update_coin_infomation(self):
		try:
			if True:
				table_query_str = (
					"CREATE TABLE IF NOT EXISTS coin_info ("
					+ "coin_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
					+ "coin_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
					+ "coin_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
					+ "coin_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
					+ "coin_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
					+ "PRIMARY KEY (coin_code) USING BTREE,"
					+ "UNIQUE INDEX coin_code (coin_code) USING BTREE,"
					+ "INDEX coin_name_kr (coin_name_kr) USING BTREE,"
					+ "INDEX coin_name_en (coin_name_en) USING BTREE"
					+ ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
				)

				cursor = self.__sql_connection.cursor()
				cursor.execute(table_query_str)

			if True:
				# 항목은 빗썸에서 한글 이름은 업비트에서 가져옴
				upbit_rep = requests.get(url = "https://api.upbit.com/v1/market/all?isDetails=true")
				upbit_rep_json = json.loads(upbit_rep.text)
				code_kr_name_dict = {}
				for data in upbit_rep_json:
					try:
						from_to = data["market"].split("-")
						if from_to[0] != "KRW": continue
						code_kr_name_dict[from_to[1]] = [data["korean_name"], data["english_name"]]
					except:
						continue

				bithumb_rep = requests.get(url = "https://api.bithumb.com/public/ticker/ALL_KRW")
				bithumb_rep_json = json.loads(bithumb_rep.text)
				if bithumb_rep_json["status"] != "0000":
					return False

				data_list = bithumb_rep_json["data"]
				for key in data_list:
					if key == "date": continue
					
					if key in code_kr_name_dict:
						name_kr = code_kr_name_dict[key][0]
						name_en = code_kr_name_dict[key][1]
					else:
						name_kr = key
						name_en = key

					val = data_list[key]
					coin_query_str = (
						"INSERT INTO coin_info VALUES ("
						+ "'" + key + "',"
						+ "'" + name_kr + "',"
						+ "'" + name_en + "',"
						+ "'" + val["closing_price"] + "',"
						+ "'" + val["acc_trade_value_24H"] + "' "
						+ ") ON DUPLICATE KEY UPDATE "
						+ "coin_name_kr='" + name_kr + "',"
						+ "coin_name_en='" + name_en + "',"
						+ "coin_price='" + val["closing_price"] + "',"
						+ "coin_amount='" + val["acc_trade_value_24H"] + "'"
					)
					cursor = self.__sql_connection.cursor()
					cursor.execute(coin_query_str)
					
			return True
		except:
			return False
		
	def __create_websocket_app(self):
		try:
			self.__ws_send_lock = threading.Lock()
			self.__ws_app = websocket.WebSocketApp(
				url = self.__WS_BASE_URL,
				on_message= self.__on_recv_message_ws,
				)
			self.__ws_thread = threading.Thread(target=self.__ws_app.run_forever)
			self.__ws_thread.start()

			return True
		
		except:
			return False

	def Initialize(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str):
		if self.__create_sql_connection(sql_host, sql_id, sql_pw, sql_db) == False:
			print("Fail to create local SQL connection")
			return False
		if self.__update_coin_infomation() == False:
			print("Fail to update coin information table")
			return False
		if self.__create_websocket_app() == False:
			print("Fail to create websocket app for API server")
			return False
		
		return True


	##########################################################################
 

	def __create_coin_execution_table(self, coin_code:str):
		table_name = coin_code.replace("/", "_")
		raw_table_query_str = (
			"CREATE TABLE IF NOT EXISTS coin_execution_raw_" + table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0' "
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		candle_table_query_str = (
			"CREATE TABLE IF NOT EXISTS coin_execution_candle_" + table_name + " ("
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
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=MEMORY"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(raw_table_query_str)
		cursor.execute(candle_table_query_str)

	def __update_coin_execution_table(self, coin_code:str, dt:datetime, price:float, non_volume:float, ask_volume:float, bid_volume:float):
		table_name = coin_code.replace("/", "_")
		datetime_00_min = dt
		datetime_10_min = dt.replace(minute=dt.minute // 10 * 10, second=0)
		price_str = str(price)
		non_volume_str = str(non_volume)
		ask_volume_str = str(ask_volume)
		bid_volume_str = str(bid_volume)
		non_amount_str = str(price * non_volume)
		ask_amount_str = str(price * ask_volume)
		bid_amount_str = str(price * bid_volume)
		
		raw_table_query_str = (
			"INSERT INTO coin_execution_raw_" + table_name + " VALUES ("
			+ "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + non_volume_str + "',"
			+ "'" + ask_volume_str + "',"
			+ "'" + bid_volume_str + "' "
			+ ")"
		)
		candle_table_query_str = (
			"INSERT INTO coin_execution_candle_" + table_name + " VALUES ("
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

		cursor = self.__sql_connection.cursor()
		cursor.execute(raw_table_query_str)
		cursor.execute(candle_table_query_str)


	def __create_coin_orderbook_table(self, coin_code:str):
		# TODO
		#무엇을 저장할지, 어떤 방식으로 저장할지 안 정해짐
		return

		table_name = coin_code.replace("/", "_")
		orderbook_table_query_str = (
			"CREATE TABLE IF NOT EXISTS coin_orderbook_" + table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_volume BIGINT(20) UNSIGNED NOT NULL DEFAULT '0'"
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(orderbook_table_query_str)

	def __update_coin_orderbook_table(self, coin_code:str, dt:datetime, data):
		# TODO
		#무엇을 저장할지, 어떤 방식으로 저장할지 안 정해짐
		return

		table_name = coin_code.replace("/", "_")
		orderbook_table_query_str = (
			"INSERT INTO coin_orderbook_" + table_name + " VALUES ("
			+ "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + data + "'"
			+ ")"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(orderbook_table_query_str)


	##########################################################################
  

	def __on_recv_coin_execution(self, msg_json:json):
		for data in msg_json["content"]["list"]:
			from_to = data["symbol"].split("_")
			dt_str = data["contDtm"].split(".")

			coin_code = from_to[0]
			dt = datetime.strptime(dt_str[0], "%Y-%m-%d %H:%M:%S")
			price = float(data["contPrice"])
			volume = float(data["contQty"])

			if data["buySellGb"] == "1":
				self.__update_coin_execution_table(coin_code, dt, price, 0, volume, 0)
			elif data["buySellGb"] == "2":
				self.__update_coin_execution_table(coin_code, dt, price, 0, 0, volume)
			else:
				self.__update_coin_execution_table(coin_code, dt, price, volume, 0, 0)

	def __on_recv_coin_orderbook(self, msg_json:json):
		print(json.dumps(msg_json["content"]))
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
	

	def __on_recv_message_ws(self, ws:websocket.WebSocketApp, msg:str):
			self.__ws_send_lock.acquire()
			self.__ws_is_started = True
			try:
				msg_json = json.loads(msg)

				if "status" in msg_json:
					if msg_json["status"] == "0000":
						print("\r Success : " + msg_json["resmsg"] + "\n> ", end="")
					else:
						print("\r Error" + msg_json["resmsg"] + "\n> ", end="")

				elif "type" in msg_json:
					# if msg_json["type"] == "ticker": self.__on_recv_coin_execution(msg_json)
					if msg_json["type"] == "transaction": self.__on_recv_coin_execution(msg_json)
					elif msg_json["type"] == "orderbooksnapshot": self.__on_recv_coin_orderbook(msg_json)

			except:
				print("\rError message\n> ", end="")
			
			self.__ws_send_lock.release()

	def __on_send_message_ws(self, api_code:str, coin_code_list:list):
		self.__ws_send_lock.acquire()
		try:
			msg = {
				"type" : api_code,
				"symbols" : coin_code_list,
			}
			self.__ws_app.send(json.dumps(msg))
		except:
			print("\rError message\n> ", end="")
	
		self.__ws_send_lock.release()


	##########################################################################
	

	def FindCoin(self, name:str):
		query_str = (
			"SELECT coin_code, coin_name_kr, coin_name_en "
			+ "FROM coin_info WHERE "
			+ "coin_name_kr LIKE '%" + name + "%' or "
			+ "coin_name_en LIKE '%" + name + "%' "
			+ "ORDER BY coin_capitalization DESC"
		)
		cursor = self.__sql_connection.cursor()
		cursor.execute(query_str)

		return cursor.fetchall()

	def GetCoinList(self, cnt:int, offset:int = 0):
		query_str = (
			"SELECT coin_code, coin_name_kr, coin_name_en "
			+ "FROM coin_info "
			+ "ORDER BY coin_capitalization DESC "
			+ "LIMIT " + str(offset) + " " + str(cnt)
		)
		cursor = self.__sql_connection.cursor()
		cursor.execute(query_str)

		return cursor.fetchall()
	

	def AddCoinExecution(self, coin_code:str):
		try:
			self.__create_coin_execution_table(coin_code)
			self.__ws_sub_list["coin_execution"].append(coin_code)

			coin_code_list = []
			for coin_code in self.__ws_sub_list["coin_execution"]:
				coin_code_list.append(coin_code + "_KRW")

			self.__on_send_message_ws("transaction", coin_code_list)

			return True
		
		except:
			return False

	def DelCoinExecution(self, coin_code:str):
		try:
			self.__ws_sub_list["coin_execution"].remove(coin_code)

			coin_code_list = []
			for coin_code in self.__ws_sub_list["coin_execution"]:
				coin_code_list.append(coin_code + "_KRW")

			self.__on_send_message_ws("transaction", coin_code_list)

			return True
		
		except:
			return False


	def AddCoinOrderbook(self, coin_code:str):
		try:
			self.__create_coin_orderbook_table(coin_code)
			self.__ws_sub_list["coin_orderbook"].append(coin_code)

			coin_code_list = []
			for coin_code in self.__ws_sub_list["coin_orderbook"]:
				coin_code_list.append(coin_code + "_KRW")

			self.__on_send_message_ws("orderbooksnapshot", coin_code_list)

			return True
		
		except:
			return False
		
	def DelCoinOrderbook(self, coin_code:str):
		try:
			self.__ws_sub_list["coin_orderbook"].remove(coin_code)

			coin_code_list = []
			for coin_code in self.__ws_sub_list["coin_orderbook"]:
				coin_code_list.append(coin_code + "_KRW")

			self.__on_send_message_ws("orderbooksnapshot", coin_code_list)

			return True
		
		except:
			return False

	


