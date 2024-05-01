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


def GetKrQueryCount(query_list:dict) -> int:
	cnt = 0
	for key, val in query_list.items():
		if (val[1].find("KOSPI") != -1 or
			val[1].find("KOSDAQ") != -1 or
			val[1].find("KONEX") != -1):
			cnt += 1

	return cnt
def GetUsQueryCount(query_list:dict) -> int:
	cnt = 0
	for key, val in query_list.items():
		if (val[1].find("NYSE") != -1 or
			val[1].find("NASDAQ") != -1):
			cnt += 1

	return cnt

class ApiKoreaInvestType:
	__sql_connection:pymysql.Connection = None
	__app_key:str = ""
	__app_secret:str = ""

	__API_BASE_URL:str = "https://openapi.koreainvestment.com:9443"
	__api_token_type:str = ""
	__api_token_val:str = ""
	__api_token_valid_datetime:DateTime = DateTime.min
	__api_header:dict = {}

	__WS_BASE_URL:str = "ws://ops.koreainvestment.com:21000"
	__ws_approval_key:str = ""
	__ws_app:websocket.WebSocketApp = None
	__ws_thread:threading.Thread = None
	__ws_is_opened:bool = False

	__ws_query_type:str = "KR"
	__ws_query_list_buf:dict = {}
	__ws_query_list_cur:dict = {}
	__ws_ex_excution_last_volume:dict = {}


	##########################################################################


	def __init__(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str, app_key:str, app_secret:str):
		self.__sql_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			db = sql_db,
			charset = 'utf8',
			autocommit=True
		)
		self.__app_key = app_key
		self.__app_secret = app_secret

		self.__create_stock_info_table()
		self.__create_last_ws_query_table()
		self.__load_last_ws_query_table()

	def __del__(self):
		if self.__ws_app != None:
			self.__ws_app.close()


	def __create_stock_info_table(self) -> None:
		try:
			table_query_str = (
				"CREATE TABLE IF NOT EXISTS stock_info ("
				+ "stock_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_market VARCHAR(32) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_country VARCHAR(64) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_sector VARCHAR(32) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_industry VARCHAR(128) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_count BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',"
				+ "stock_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
				+ "stock_capitalization DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
				+ "stock_volume BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',"
				+ "PRIMARY KEY (stock_code) USING BTREE,"
				+ "UNIQUE INDEX stock_code (stock_code) USING BTREE,"
				+ "INDEX stock_name (stock_name_kr, stock_name_en) USING BTREE"
				+ ")COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
			)

			cursor = self.__sql_connection.cursor()
			cursor.execute(table_query_str)

		except: raise Exception("Fail to create stock info table")

	def __create_last_ws_query_table(self) -> None:
		try:
			create_table_query = (
				"CREATE TABLE IF NOT EXISTS stock_last_ws_query ("
				+ "stock_query VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_market VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_api_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_api_stock_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "PRIMARY KEY (stock_query) USING BTREE,"
				+ "INDEX stock_code (stock_code) USING BTREE,"
				+ "CONSTRAINT FK_stock_list_last_query_stock_info FOREIGN KEY (stock_code) REFERENCES stock_info (stock_code) ON UPDATE CASCADE ON DELETE CASCADE"
				+ ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
				)
			cursor = self.__sql_connection.cursor()
			cursor.execute(create_table_query)

		except: raise Exception("Fail to create last websocket query table")

	def __load_last_ws_query_table(self) -> None:
		try:
			select_query = "SELECT * FROM stock_last_ws_query"
			cursor = self.__sql_connection.cursor()
			cursor.execute(select_query)

			last_query_list = cursor.fetchall()
			self.__ws_query_list_buf.clear()
			for info in last_query_list:
				self.__ws_query_list_buf[info[0]] = [info[1], info[2], info[3], info[4]]

		except: raise Exception("Fail to load last websocket query table")
		

	def __sync_last_ws_query_table(self) -> None:
		try:
			select_query = "SELECT * FROM stock_last_ws_query"
			cursor = self.__sql_connection.cursor()
			cursor.execute(select_query)

			last_query_list = cursor.fetchall()
			self.__ws_query_list_cur.clear()
			for info in last_query_list:
				self.__ws_query_list_cur[info[0]] = [info[1], info[2], info[3], info[4]]

		except: raise Exception("Fail to load last websocket query table")

	def __create_token(self) -> None:
		try:
			file = open("./doc/last_token_info.dat", 'r')
			strs = file.read().split("\n")
			file.close()

			last_api_token_type = strs[0]
			last_api_token_val = strs[1]
			last_api_token_valid_datetime = DateTime.strptime(strs[2], "%Y-%m-%d %H:%M:%S")

			remain_sec = (last_api_token_valid_datetime - DateTime.now()).seconds

			# 토큰이 생긴지 3분도 안 되었다면 그냥 사용
			if remain_sec > 86220:
				self.__api_token_type = last_api_token_type
				self.__api_token_val = last_api_token_val
				self.__api_token_valid_datetime = last_api_token_valid_datetime
				self.__api_header = {
					"content-type" : "application/json; charset=utf-8",
					"authorization" : self.__api_token_type + " " + self.__api_token_val,
					"appkey" : self.__app_key,
					"appsecret" : self.__app_secret,
					"custtype" : "P"
				}
				return

			# 마지막 토큰이 생성된지 대략 12시간도 안 되었다면 토큰 패기 요청 보냄
			# 안 그러면 같은 유효기간의 토큰이 생성됨
			if remain_sec < 43200:
				api_url = "/oauth2/revokeP"
				api_body = {
					"grant_type" : "client_credentials",
					"appkey" : self.__app_key,
					"appsecret" : self.__app_secret,
					"token" : last_api_token_val
				}
				response = requests.post (
					url = self.__API_BASE_URL + api_url,
					data = json.dumps(api_body),
				)
				os.remove("./doc/last_token_info.dat")

		except: pass

		try:
			api_url = "/oauth2/tokenP"
			api_body = {
				"grant_type" : "client_credentials",
				"appkey" : self.__app_key,
				"appsecret" : self.__app_secret,
			}
			response = requests.post (
				url = self.__API_BASE_URL + api_url,
				data = json.dumps(api_body),
			)

			rep_json = json.loads(response.text)

			self.__api_token_type = rep_json["token_type"]
			self.__api_token_val = rep_json["access_token"]
			self.__api_token_valid_datetime = DateTime.strptime(rep_json["access_token_token_expired"], "%Y-%m-%d %H:%M:%S")
			self.__api_header = {
				"content-type" : "application/json; charset=utf-8",
				"authorization" : self.__api_token_type + " " + self.__api_token_val,
				"appkey" : self.__app_key,
				"appsecret" : self.__app_secret,
				"custtype" : "P"
			}
			
			file_str_list = [
				self.__api_token_type,
				self.__api_token_val,
				self.__api_token_valid_datetime.strftime("%Y-%m-%d %H:%M:%S")
			]
				
			file = open("./doc/last_token_info.dat", 'w')
			file.write("\n".join(file_str_list))
			file.close()

		except: raise Exception("Fail to create access token for KoreaInvest Api")

	def __create_approval_key(self) -> None:
		try:
			api_url = "/oauth2/Approval"
			api_header = {
				"content-type" : "application/json; utf-8"
			}
			api_body = {
				"grant_type" : "client_credentials",
				"appkey" : self.__app_key,
				"secretkey" : self.__app_secret,
			}
			response = requests.post (
				url = self.__API_BASE_URL + api_url,
				headers = api_header,
				data = json.dumps(api_body),
			)

			rep_json = json.loads(response.text)

			self.__ws_approval_key = rep_json["approval_key"]
			
		except: raise Exception("Fail to create web socket approval key")
 
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
			
		except: raise Exception("Fail to create web socket approval key")


	##########################################################################
 

	def __update_stock_execution_table(self, stock_code:str, dt:DateTime, price:float, non_volume:float, ask_volume:float, bid_volume:float) -> None:
		table_name = (
			stock_code.replace("/", "_")
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
			"CREATE TABLE IF NOT EXISTS stock_execution_raw_" + table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0' "
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		create_candle_table_query_str = (
			"CREATE TABLE IF NOT EXISTS stock_execution_candle_" + table_name + " ("
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
		insert_raw_table_query_str = (
			"INSERT INTO stock_execution_raw_" + table_name + " VALUES ("
			+ "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + non_volume_str + "',"
			+ "'" + ask_volume_str + "',"
			+ "'" + bid_volume_str + "' "
			+ ")"
		)
		insert_candle_table_query_str = (
			"INSERT INTO stock_execution_candle_" + table_name + " VALUES ("
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
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		insert_orderbook_table_query_str = (
			"INSERT INTO stock_orderbook_" + table_name + " VALUES ("
			+ "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + data + "'"
			+ ")"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(create_orderbook_table_query_str)
		cursor.execute(insert_orderbook_table_query_str)


	##########################################################################
  

	def __on_recv_kr_stock_execution(self, data_cnt:int, data:str) -> None:
		try:
			# "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
			pValue = data.split('^')
			for data_idx in range(data_cnt):
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
			raise Exception("kr stock execution %s [%s]"%(stock_code, e.__str__()))

	def __on_recv_ex_stock_execution(self, data_cnt:int, data:str) -> None:
		try:
    	# "실시간종목코드|종목코드|수수점자리수|현지영업일자|현지일자|현지시간|한국일자|한국시간|시가|고가|저가|현재가|대비구분|전일대비|등락율|매수호가|매도호가|매수잔량|매도잔량|체결량|거래량|거래대금|매도체결량|매수체결량|체결강도|시장구분"
			pValue = data.split('^')
			for data_idx in range(data_cnt):
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
			raise Exception("kr stock execution %s [%s]"%(stock_code, e.__str__()))

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
		print("매수총 잔량        [%s]" % (recvvalue[7]))
		print("매수총잔량대비      [%s]" % (recvvalue[9]))
		print("매도총 잔량        [%s]" % (recvvalue[8]))
		print("매도총잔략대비      [%s]" % (recvvalue[10]))
		print("매수호가           [%s]" % (recvvalue[11]))
		print("매도호가           [%s]" % (recvvalue[12]))
		print("매수잔량           [%s]" % (recvvalue[13]))
		print("매도잔량           [%s]" % (recvvalue[14]))
		print("매수잔량대비        [%s]" % (recvvalue[15]))
		print("매도잔량대비        [%s]" % (recvvalue[16]))


	def __on_ws_recv_message(self, ws:websocket.WebSocketApp, msg:str) -> None:
		try:
			if msg[0] == '0':
				# 수신데이터가 실데이터 이전은 '|'로 나눠 있음
				recvstr = msg.split('|') 
				trid0 = recvstr[1]

				# H0STCNT0 : 국내 주식 체결
				# HDFSCNT0 : 해외 주식 체결
				# H0STASP0 : 국내 주식 호가
				# HDFSASP0 : 해외 주식 호가
				if trid0 == "H0STCNT0": self.__on_recv_kr_stock_execution(int(recvstr[2]), recvstr[3])
				elif trid0 == "HDFSCNT0": self.__on_recv_ex_stock_execution(int(recvstr[2]), recvstr[3])
				elif trid0 == "H0STASP0": self.__on_recv_kr_stock_orderbook(recvstr[3])
				elif trid0 == "HDFSASP0": self.__on_recv_ex_stock_orderbook(recvstr[3])

			else:
				msg_json = json.loads(msg)

				if msg_json["header"]["tr_id"] == "PINGPONG":
					ws.send(msg)
					Util.PrintNormalLog("Executed PINGPONG msg")

				else:
					rt_cd = msg_json["body"]["rt_cd"]

					if rt_cd == '0':
						Util.PrintNormalLog("Success msg : " + "[ %s ][ %s ][ %s ]"%(msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]))
					else:
						Util.PrintErrorLog("Error msg : " + "[ %s ][ %s ][ %s ]"%(msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]))

		except Exception as e:
			Util.PrintErrorLog("Fail to process ws recv msg : " + e.__str__())
	
	def __on_ws_send_message(self, api_code:str, stock_code:str) -> None:
		try:
			msg = {
				"header" : {
					"approval_key": self.__ws_approval_key,
					"content-type": "utf-8",
					"custtype": "P",
					"tr_type": "1"
				},
				"body" : {
					"input": {
						"tr_id": api_code,
						"tr_key": stock_code
					}
				}
			}
			self.__ws_app.send(json.dumps(msg))
		except Exception as e:
			Util.PrintErrorLog("Fail to process ws send msg : " + e.__str__())

	def __on_ws_open(self, ws:websocket.WebSocketApp) -> None:
		self.__ws_is_opened = True
		for key, val in self.__ws_query_list_cur.items():
			if (self.__ws_query_type == "KR" and 
				val[1].find("KOSPI") == -1 and
				val[1].find("KOSDAQ") == -1 and
				val[1].find("KONEX") == -1
				) or (
				self.__ws_query_type == "EX" and 
				val[1].find("NYSE") == -1 and
				val[1].find("NASDAQ") == -1
				): continue
			
			self.__on_ws_send_message(val[2], val[3])
			time.sleep(0.5)

		Util.PrintNormalLog("Opened korea invest websocket")

	def __on_ws_close(self, ws:websocket.WebSocketApp) -> None:
		self.__ws_is_opened = False
		Util.PrintNormalLog("Closed korea invest websocket")


	##########################################################################
	

	def FindStock(self, name:str) -> list:
		try:
			query_str = (
				"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
				+ "FROM stock_info WHERE "
				+ "stock_name_kr LIKE '%" + name + "%' or "
				+ "stock_name_en LIKE '%" + name + "%' "
				+ "ORDER BY stock_capitalization DESC"
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(query_str)

			return cursor.fetchall()
		
		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return []

	def FindKrStock(self, name:str) -> list:
			try:
				query_str = (
					"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
					+ "FROM stock_info WHERE ("
					+ "stock_market LIKE 'KOSPI%' or "
					+ "stock_market LIKE 'KOSDAQ%' or "
					+ "stock_market LIKE 'KONEX%') "
					+ "AND ("
					+ "stock_name_kr LIKE '%" + name + "%' or "
					+ "stock_name_en LIKE '%" + name + "%')"
					+ "ORDER BY stock_capitalization DESC"
				)
				cursor = self.__sql_connection.cursor()
				cursor.execute(query_str)

				return cursor.fetchall()
			
			except Exception as e:
				Util.PrintErrorLog(e.__str__())
				return []

	def FindUsStock(self, name:str) -> list:
			try:
				query_str = (
					"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
					+ "FROM stock_info WHERE ("
					+ "stock_market LIKE 'NYSE%' or "
					+ "stock_market LIKE 'NASDAQ%') "
					+ "AND ("
					+ "stock_name_kr LIKE '%" + name + "%' or "
					+ "stock_name_en LIKE '%" + name + "%')"
					+ "ORDER BY stock_capitalization DESC"
				)
				cursor = self.__sql_connection.cursor()
				cursor.execute(query_str)

				return cursor.fetchall()
			
			except Exception as e:
				Util.PrintErrorLog(e.__str__())
				return []

	def GetKrStockList(self, cnt:int, offset:int = 0) -> list:
		try:
			query_str = (
				"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
				+ "FROM stock_info WHERE "
				+ "stock_market LIKE 'KOSPI%' or "
				+ "stock_market LIKE 'KOSDAQ%' or "
				+ "stock_market LIKE 'KONEX%' "
				+ "ORDER BY stock_capitalization DESC "
				+ "LIMIT " + str(offset) + ", " + str(cnt)
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(query_str)

			return cursor.fetchall()
		
		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return []
	
	def GetUsStockList(self, cnt:int, offset:int = 0) -> list:
		try:
			query_str = (
				"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
				+ "FROM stock_info WHERE "
				+ "stock_market LIKE 'NYSE%' or "
				+ "stock_market LIKE 'NASDAQ%' "
				+ "ORDER BY stock_capitalization DESC "
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

	def GetInsertedKrQueryList(self) -> list:
		try:
			ret = []
			for key, val in self.__ws_query_list_buf.items():
				if (val[1].find("KOSPI") == -1 and
					val[1].find("KOSDAQ") == -1 and
					val[1].find("KONEX") == -1):
					continue

				query_str = (
					"SELECT stock_code, stock_name_kr, stock_name_en "
					+ "FROM stock_info WHERE "
					+ "stock_code='" + val[0] + "'"
				)
				cursor = self.__sql_connection.cursor()
				cursor.execute(query_str)
				ret.append(cursor.fetchall()[0])

			return ret

		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return []
		
	def GetInsertedUsQueryList(self) -> list:
		try:
			ret = []
			for key, val in self.__ws_query_list_buf.items():
				if (val[1].find("NYSE") == -1 and
					val[1].find("NASDAQ") == -1):
					continue

				query_str = (
					"SELECT stock_code, stock_name_kr, stock_name_en "
					+ "FROM stock_info WHERE "
					+ "stock_code='" + val[0] + "'"
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
			delete_list_query = "DELETE FROM stock_last_ws_query"
			cursor = self.__sql_connection.cursor()
			cursor.execute(delete_list_query)
			
			varified_list = {}
			for key, val in self.__ws_query_list_buf.items():
				try:
					insert_list_query = "INSERT INTO stock_last_ws_query VALUES ('%s','%s','%s','%s','%s')"%(key, val[0], val[1], val[2], val[3])
					cursor.execute(insert_list_query)
					varified_list[key] = val
				except:
					continue

			self.__ws_query_list_buf = varified_list

			return True

		except:
			return False


	def AddStockExecutionQuery(self, stock_code:str) -> int:
		try:
			stock_code = stock_code.upper()
			if self.__ws_query_list_buf.__contains__("EX_" + stock_code):
				stock_market = self.__ws_query_list_buf["EX_" + stock_code][1]
				if (stock_market == "NYSE" or
					stock_market == "NASDAQ"):
					return GetUsQueryCount(self.__ws_query_list_buf)
				else:
					return GetKrQueryCount(self.__ws_query_list_buf)

			exist_query_str = (
				"SELECT stock_market "
				+ "FROM stock_info WHERE "
				+ "stock_code='" + stock_code +"'"
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(exist_query_str)
			exist_ret = cursor.fetchall()
			if len(exist_ret) == 0: return -500
			
			stock_market = exist_ret[0][0]
			if stock_market == "NYSE":
				if GetUsQueryCount(self.__ws_query_list_buf) >= 40: return -501
				api_code = "HDFSCNT0"
				api_stock_code = "DNYS" + stock_code
			elif stock_market == "NASDAQ":
				if GetUsQueryCount(self.__ws_query_list_buf) >= 40: return -501
				api_code = "HDFSCNT0"
				api_stock_code = "DNAS" + stock_code
			else:
				if GetKrQueryCount(self.__ws_query_list_buf) >= 40: return -501
				api_code = "H0STCNT0"
				api_stock_code = stock_code

			self.__ws_query_list_buf["EX_" + stock_code] = [stock_code, stock_market, api_code, api_stock_code]

			if (stock_market == "NYSE" or
				stock_market == "NASDAQ"):
				return GetUsQueryCount(self.__ws_query_list_buf)
			else:
				return GetKrQueryCount(self.__ws_query_list_buf)
		
		except:
			return -400

	def AddStockOrderbookQuery(self, stock_code:str) -> int:
		try:
			stock_code = stock_code.upper()
			if self.__ws_query_list_buf.__contains__("OB_" + stock_code):
				stock_market = self.__ws_query_list_buf["OB_" + stock_code][1]
				if (stock_market == "NYSE" or
					stock_market == "NASDAQ"):
					return GetUsQueryCount(self.__ws_query_list_buf)
				else:
					return GetKrQueryCount(self.__ws_query_list_buf)
			
			exist_query_str = (
				"SELECT stock_market "
				+ "FROM stock_info WHERE "
				+ "stock_code='" + stock_code +"'"
			)
			cursor = self.__sql_connection.cursor()
			cursor.execute(exist_query_str)
			exist_ret = cursor.fetchall()
			if len(exist_ret) == 0: return -500

			stock_market = exist_ret[0][0]
			if stock_market == "NYSE":
				if GetUsQueryCount(self.__ws_query_list_buf) >= 40: return -501
				api_code = "HDFSASP0"
				api_stock_code = "DNYS" + stock_code
			elif stock_market == "NASDAQ":
				if GetUsQueryCount(self.__ws_query_list_buf) >= 40: return -501
				api_code = "HDFSASP0"
				api_stock_code = "DNAS" + stock_code
			else:
				if GetKrQueryCount(self.__ws_query_list_buf) >= 40: return -501
				api_code = "H0STASP0"
				api_stock_code = stock_code

			self.__ws_query_list_buf["OB_" + stock_code] = [stock_code, stock_market, api_code, api_stock_code]

			if (stock_market == "NYSE" or
				stock_market == "NASDAQ"):
				return GetUsQueryCount(self.__ws_query_list_buf)
			else:
				return GetKrQueryCount(self.__ws_query_list_buf)
		
		except:
			return -400
		
	def DelStockExecutionQuery(self, stock_code:str) -> None:
		try:
			stock_code = stock_code.upper()
			del self.__ws_query_list_buf["EX_" + stock_code]
		except:
			pass

	def DelStockOrderbookQuery(self, stock_code:str) -> None:
		try:
			stock_code = stock_code.upper()
			del self.__ws_query_list_buf["OB_" + stock_code]
		except:
			pass


	def IsCollecting(self) -> bool:
		return self.__ws_is_opened

	def GetCurrentCollectingType(self) -> str:
		return self.__ws_query_type

	def StartCollecting(self, query_type:str) -> bool:
		try:
			self.__ws_query_type = query_type
			self.__sync_last_ws_query_table()
			self.__create_token()
			self.__create_approval_key()
			self.__create_websocket_app()
			
			return True

		except Exception as e:
			Util.PrintErrorLog(e.__str__())
			return False

	def StopCollecting(self) -> None:
		if self.__ws_app != None:
			self.__ws_app.close()
	


