import Util
import requests
import websocket
import pymysql
import json
import threading
import os
import time
from datetime import datetime as DateTime
from datetime import timedelta


class ApiBithumbType:
	__sql_connection:pymysql.Connection = None

	__API_BASE_URL:str = "https://api.bithumb.com/public"

	__WS_BASE_URL:str = "wss://pubwss.bithumb.com/pub/ws"
	__ws_app:websocket.WebSocketApp = None
	__ws_thread:threading.Thread = None
	__ws_is_opened = False

	__ws_query_list_buf = {}
	__ws_query_list_cur = {}


	##########################################################################


	def __init__(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str):
		self.__sql_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			db = sql_db,
			charset = 'utf8',
			autocommit=True
		)

		self.__create_coin_info_table()
		self.__update_coin_info_table()
		self.__create_last_ws_query_table()
		self.__load_last_ws_query_table()

	def __del__(self):
		if self.__ws_app != None:
			self.__ws_app.close()


	def __create_coin_info_table(self) -> None:
		try:
			table_query_str = (
				"CREATE TABLE IF NOT EXISTS coin_info ("
				+ "coin_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "coin_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "coin_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "coin_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
				+ "coin_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
				+ "`coin_order` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
				+ "PRIMARY KEY (coin_code) USING BTREE,"
				+ "UNIQUE INDEX coin_code (coin_code) USING BTREE,"
				+ "INDEX coin_name (coin_name_kr, coin_name_en) USING BTREE,"
				+ "INDEX coin_order (coin_order) USING BTREE"
				+ ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
			)

			cursor = self.__sql_connection.cursor()
			cursor.execute(table_query_str)

		except: raise Exception("Fail to create coin info table")

	def __update_coin_info_table(self) -> None:
		try:
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
					"INSERT INTO coin_info ("
					+ "coin_code, coin_name_kr, coin_name_en, coin_price, coin_amount"
					+ ") VALUES ("
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

		except: raise Exception("Fail to update coin info table")

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
			
			cursor = self.__sql_connection.cursor()
			cursor.execute(create_table_query)

		except: raise Exception("Fail to create last websocket query table")

	def __load_last_ws_query_table(self) -> None:
		try:
			select_query = "SELECT * FROM coin_last_ws_query"
			cursor = self.__sql_connection.cursor()
			cursor.execute(select_query)

			last_query_list = cursor.fetchall()
			self.__ws_query_list_buf.clear()
			for info in last_query_list:
				self.__ws_query_list_buf[info[0]] = [info[1], info[2], info[3]]

		except: raise Exception("Fail to load last websocket query table")


	def __sync_last_ws_query_table(self) -> None:
		try:
			select_query = "SELECT * FROM coin_last_ws_query"
			cursor = self.__sql_connection.cursor()
			cursor.execute(select_query)

			last_query_list = cursor.fetchall()
			self.__ws_query_list_cur.clear()
			for info in last_query_list:
				self.__ws_query_list_cur[info[0]] = [info[1], info[2], info[3]]

		except: raise Exception("Fail to load last websocket query table")

	def __create_websocket_app(self) -> None:
		try:
			if self.__ws_app != None:
				self.__ws_app.close()
			self.__ws_app = websocket.WebSocketApp(
				url = self.__WS_BASE_URL,
				on_message= self.__on_ws_recv_message,
				on_open= self.__on_ws_open,
				on_close= self.__on_ws_close,
				)
			self.__ws_thread = threading.Thread(target=self.__ws_app.run_forever)
			self.__ws_thread.start()
			
		except: raise Exception("Fail to create web socket")
		

	##########################################################################
 

	def __update_coin_execution_table(self, coin_code:str, dt:DateTime, price:float, non_volume:float, ask_volume:float, bid_volume:float) -> None:
		table_name = (
			coin_code.replace("/", "_")
			+ "_"
			+ dt.strftime("%Y%V")
		)

		datetime_00_min = dt
		datetime_10_min = dt.replace(minute=dt.minute // 10 * 10, second=0)
		price_str = str(price)
		non_volume_str = str(non_volume)
		ask_volume_str = str(ask_volume)
		bid_volume_str = str(bid_volume)
		non_amount_str = str(price * non_volume)
		ask_amount_str = str(price * ask_volume)
		bid_amount_str = str(price * bid_volume)
		
		create_raw_table_query_str = (
			"CREATE TABLE IF NOT EXISTS coin_execution_raw_" + table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0' "
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		create_candle_table_query_str = (
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
		insert_raw_table_query_str = (
			"INSERT INTO coin_execution_raw_" + table_name + " VALUES ("
			+ "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + non_volume_str + "',"
			+ "'" + ask_volume_str + "',"
			+ "'" + bid_volume_str + "' "
			+ ")"
		)
		insert_candle_table_query_str = (
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
		cursor.execute(create_raw_table_query_str)
		cursor.execute(create_candle_table_query_str)
		cursor.execute(insert_raw_table_query_str)
		cursor.execute(insert_candle_table_query_str)

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

		cursor = self.__sql_connection.cursor()
		cursor.execute(orderbook_table_query_str)

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
  

	def __on_recv_coin_execution(self, msg_json:json) -> None:
		for data in msg_json["content"]["list"]:
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

	def __on_recv_coin_orderbook(self, msg_json:json) -> None:
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
	

	def __on_ws_recv_message(self, ws:websocket.WebSocketApp, msg:str) -> None:
		try:
			msg_json = json.loads(msg)

			if "status" in msg_json:
				if msg_json["status"] == "0000":
					Util.PrintNormalLog("Success msg : " + "[ %s ]"%(msg_json["resmsg"]))
				else:
					Util.PrintErrorLog("Success msg : " + "[ %s ]"%(msg_json["resmsg"]))

			elif "type" in msg_json:
				if msg_json["type"] == "transaction": self.__on_recv_coin_execution(msg_json)
				elif msg_json["type"] == "orderbooksnapshot": self.__on_recv_coin_orderbook(msg_json)

		except Exception as e:
			Util.PrintErrorLog("Fail to process ws recv msg : " + e.__str__())

	def __on_ws_send_message(self, api_code:str, coin_code_list:list) -> None:
		try:
			msg = {
				"type" : api_code,
				"symbols" : coin_code_list,
			}
			self.__ws_app.send(json.dumps(msg))
		except Exception as e:
			Util.PrintErrorLog("Fail to process ws send msg : " + e.__str__())
			
	def __on_ws_open(self, ws:websocket.WebSocketApp) -> None:
		self.__ws_is_opened = True

		buf_dict = {}
		for key, val in self.__ws_query_list_cur.items():
			if val[1] in buf_dict:
				buf_dict[val[1]].append(val[2])
			else:
				buf_dict[val[1]] = [val[2]]

		for key, val in buf_dict.items():
			self.__on_ws_send_message(key, val)
			time.sleep(0.5)

		Util.PrintNormalLog("Opened bithumb websocket")

	def __on_ws_close(self, ws:websocket.WebSocketApp) -> None:
		self.__ws_is_opened = False
		Util.PrintNormalLog("Closed bithumb websocket")


	##########################################################################
	

	def FindCoin(self, name:str) -> list:
		try:
			query_str = (
				"SELECT coin_code, coin_name_kr, coin_name_en "
				+ "FROM coin_info WHERE "
				+ "coin_name_kr LIKE '%" + name + "%' or "
				+ "coin_name_en LIKE '%" + name + "%' "
				+ "ORDER BY coin_order ASC"
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(query_str)

			return cursor.fetchall()
		
		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return []

	def GetCoinList(self, cnt:int, offset:int = 0) -> list:
		try:
			query_str = (
				"SELECT coin_code, coin_name_kr, coin_name_en "
				+ "FROM coin_info "
				+ "ORDER BY coin_order ASC "
				+ "LIMIT " + str(offset) + ", " + str(cnt)
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(query_str)

			return cursor.fetchall()
		
		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return []
	

	def ClearAllQuery(self) -> None:
		self.__ws_query_list_buf.clear()

	def GetInsertedQueryList(self) -> list:
		try:
			ret = []
			for key, val in self.__ws_query_list_buf.items():
				query_str = (
					"SELECT coin_code, coin_name_kr, coin_name_en "
					+ "FROM coin_info WHERE "
					+ "coin_code='" + val[0] + "'"
				)
				cursor = self.__sql_connection.cursor()
				cursor.execute(query_str)
				ret.append(cursor.fetchall()[0])

			return ret

		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return []

	def UpdateAllQuery(self) -> bool:
		try:
			delete_list_query = "DELETE FROM coin_last_ws_query"
			cursor = self.__sql_connection.cursor()
			cursor.execute(delete_list_query)

			varified_list = {}
			for key, val in self.__ws_query_list_buf.items():
				try:
					insert_list_query = "INSERT INTO coin_last_ws_query VALUES ('%s','%s','%s','%s')"%(key, val[0], val[1], val[2])
					cursor.execute(insert_list_query)
					varified_list[key] = val
				except:
					continue

			self.__ws_query_list_buf = varified_list

			return True

		except:
			return False


	def AddCoinExecutionQuery(self, coin_code:str) -> int:
		try:
			if self.__ws_query_list_buf.__contains__("EX_" + coin_code):
				return len(self.__ws_query_list_buf)
			
			query_str = (
				"SELECT COUNT(*) "
				+ "FROM coin_info WHERE "
				+ "coin_code='" + coin_code +"'"
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(query_str)
			sql_ret = cursor.fetchall()
			if int(sql_ret[0][0]) == 0: return -500
		
			api_stock_code = coin_code + "_KRW"

			self.__ws_query_list_buf["EX_" + coin_code] = [coin_code, "transaction", api_stock_code]

			return len(self.__ws_query_list_buf)
		
		except:
			return -400

	def AddCoinOrderbookQuery(self, coin_code:str) -> int:
		try:
			if self.__ws_query_list_buf.__contains__("EX_" + coin_code):
				return len(self.__ws_query_list_buf)
			
			query_str = (
				"SELECT COUNT(*) "
				+ "FROM coin_info WHERE "
				+ "coin_code='" + coin_code +"'"
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(query_str)
			sql_ret = cursor.fetchall()
			if int(sql_ret[0][0]) == 0: return -501
		
			api_stock_code = coin_code + "_KRW"

			self.__ws_query_list_buf["OB_" + coin_code] = [coin_code, "orderbooksnapshot", api_stock_code]

			return len(self.__ws_query_list_buf)
		
		except:
			return -400

	def DelStockExecutionQuery(self, coin_code:str) -> None:
		try:
			del self.__ws_query_list_buf["EX_" + coin_code]
		except:
			pass

	def DelStockOrderbookQuery(self, coin_code:str) -> None:
		try:
			del self.__ws_query_list_buf["OB_" + coin_code]
		except:
			pass



	def IsCollecting(self) -> bool:
		return self.__ws_is_opened

	def StartCollecting(self) -> bool:
		try:
			self.__sync_last_ws_query_table()
			self.__create_websocket_app()
			
			return True

		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return False

	def StopCollecting(self) -> None:
		if self.__ws_app != None:
			self.__ws_app.close()
	



