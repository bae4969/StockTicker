import requests
import websocket
import pymysql
import json
import threading
import asyncio
from datetime import datetime
from datetime import timedelta

import websockets.connection


class ApiKoreaInvestType:
	__sql_connection = None
	__app_key = ""
	__app_secret = ""

	__API_BASE_URL = "https://openapi.koreainvestment.com:9443"
	__api_token_type = ""
	__api_token_val = ""
	__api_token_valid_datetime = datetime.min
	__api_header = {}

	__WS_BASE_URL = "ws://ops.koreainvestment.com:21000"
	__ws_approval_key = ""
	__ws_app = None
	__ws_thread = None
	__ws_send_lock = None


	##########################################################################

	def __create_sql_connection(self, sql_host, sql_id, sql_pw, sql_db):
		try:
			self.__sql_connection = pymysql.connect(
				host = sql_host,
				port = 3306,
				user = sql_id,
				passwd = sql_pw,
				db = sql_db,
				charset = 'utf8'
			)

			return True
		except:
			self.__sql_connection = None
			return False
		
	def __create_access_Token(self, app_key, app_secret):
		try:
			self.__app_key = app_key
			self.__app_secret = app_secret
			self.__api_token_type = 'Bearer'
			self.__api_token_val = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjllODAwMjhiLWIyMDItNDVlMi05MzczLTAxM2Q1NmYzMDA1MyIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3OTU3MywiaWF0IjoxNzEzNTkzMTczLCJqdGkiOiJQU25aZmpVM1MydnFFaVlOZGpFQkowWm9zWkNxOXhMYlJEaTkifQ.oYmkBnGa8nLlHK5ZGmxWBEHxdbuyGvW9qYhuYqM-u3_5nYtJFWeoVa3XqvlijA8Ki0ERVWHPI0mqS800elE82w'
			self.__api_token_valid_datetime = datetime.now()
			self.__api_header = {
				"content-type" : "application/json; charset=utf-8",
				"authorization" : self.__api_token_type + " " + self.__api_token_val,
				"appkey" : self.__app_key,
				"appsecret" : self.__app_secret,
				"custtype" : "P"
			}

			return True


			api_url = "/oauth2/tokenP"
			api_params = {

			}
			api_header = {

			}
			api_body = {
				"grant_type" : "client_credentials",
				"appkey" : app_key,
				"appsecret" : app_secret,
			}
			response = requests.post (
				url = self.__API_BASE_URL + api_url,
				# params = api_params,
				# headers = api_header
				data = json.dumps(api_body),
			)

			rep_json = json.loads(response.text)

			self.__app_key = app_key
			self.__app_secret = app_secret
			self.__api_token_type = rep_json["token_type"]
			self.__api_token_val = rep_json["access_token"]
			self.__api_token_valid_datetime = datetime.strptime(rep_json["access_token_token_expired"], "%Y-%m-%d %H:%M:%S")
			self.__api_header = {
				"content-type" : "application/json; charset=utf-8",
				"authorization" : self.__api_token_type + " " + self.__api_token_val,
				"appkey" : self.__app_key,
				"appsecret" : self.__app_secret,
				"custtype" : "P"
			}

			return True

		except:
			return False

	def __create_websocket_app(self):
		try:
			# approval키 받기
			if True:
				api_url = "/oauth2/Approval"
				api_params = {

				}
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
					# params = api_params,
					headers = api_header,
					data = json.dumps(api_body),
				)

				rep_json = json.loads(response.text)

				self.__ws_approval_key = rep_json["approval_key"]

			# ws_app 생성
			if True:
				self.__ws_send_lock = threading.Lock()
				self.__ws_app = websocket.WebSocketApp(
					url = self.__WS_BASE_URL,
					on_message= self.__on_message_ws,
					)
				self.__ws_thread = threading.Thread(target=self.__ws_app.run_forever)
				self.__ws_thread.start()

			return True
		
		except:
			return False

	def Initialize(self, sql_host, sql_id, sql_pw, sql_db, app_key, app_secret):
		if self.__create_sql_connection(sql_host, sql_id, sql_pw, sql_db) == False:
			print("Fail to create local SQL connection")
			return False
		if self.__create_access_Token(app_key, app_secret) == False:
			print("Fail to create access token for API server")
			return False
		if self.__create_websocket_app() == False:
			print("Fail to create websocket app for API server")
			return False

	##########################################################################
 
	def __create_stock_execution_table(self, stock_code: str):
		table_name = stock_code.replace("/", "_")
		raw_table_query_str = (
			"CREATE TABLE IF NOT EXISTS 'stock_execution_raw_" + table_name +"' ("
			+ "`execution_datetime` DATETIME NOT NULL,"
			+ "`execution_price` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_volume` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0'"
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		candle_table_query_str = (
			"CREATE TABLE IF NOT EXISTS `stock_execution_candle_" + table_name +"` ("
			+ "`execution_datetime` DATETIME NOT NULL,"
			+ "`execution_open` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_close` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_min` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_max` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_volume` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_amount` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "PRIMARY KEY (`execution_datetime`) USING HASH"
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(raw_table_query_str)
		cursor.execute(candle_table_query_str)
		self.__sql_connection.commit()

	def __update_stock_execution(self, stock_code: str, datetime: datetime, price: float, volume: int):
		table_name = stock_code.replace("/", "_")
		datetime_00_min = datetime
		datetime_10_min = datetime.replace(minute=datetime.minute // 10 * 10, second=0)
		price_str = str(price)
		volume_str = str(volume)
		amount_str = str(price * volume)
		
		raw_table_query_str = (
			"INSERT INTO " + table_name + " VALUES ("
			+ "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + volume_str + "'"
			+ ")"
		)
		candle_table_query_str = (
			"INSERT INTO " + table_name + " VALUES ("
			+ "'" + datetime_10_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + price_str + "',"
			+ "'" + price_str + "',"
			+ "'" + price_str + "',"
			+ "'" + volume_str + "',"
			+ "'" + amount_str + "'"
			+ ") ON DUPLICATE KEY UPDATE "
			+ "execution_close='" + price_str + "',"
			+ "execution_min=LEAST(execution_min,'" + price_str + "'),"
			+ "execution_max=GREATEST(execution_max,'" + price_str + "'),"
			+ "execution_amount=execution_amount+'" + amount_str + "'"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(raw_table_query_str)
		cursor.execute(candle_table_query_str)
		self.__sql_connection.commit()

	def __create_stock_orderbook_table(self, stock_code: str):
		table_name = stock_code.replace("/", "_")
		orderbook_table_query_str = (
			"CREATE TABLE IF NOT EXISTS 'stock_orderbook_" + table_name +"' ("
			+ "`execution_datetime` DATETIME NOT NULL,"
			+ "`execution_price` DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "`execution_volume` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0'"
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(orderbook_table_query_str)
		self.__sql_connection.commit()


	##########################################################################
  
	def __on_recv_kr_stock_execution(self, data_cnt, data):
    	# "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
		pValue = data.split('^')
		for data_idx in range(data_cnt):
			data_offset = 46 * data_idx

			stock_execution_datetime_str = datetime.now().strftime("%Y%m%d") + pValue[data_offset + 1]

			stock_code = pValue[data_offset + 0]
			datetime = datetime.strptime(stock_execution_datetime_str, "%Y%m%d%H%M%S")
			price = float(pValue[data_offset + 2])
			volume = int(pValue[data_offset + 12])

			self.__update_stock_execution(stock_code, datetime, price, volume)

	def __on_recv_ex_stock_execution(self, data_cnt, data):
    	# "실시간종목코드|종목코드|수수점자리수|현지영업일자|현지일자|현지시간|한국일자|한국시간|시가|고가|저가|현재가|대비구분|전일대비|등락율|매수호가|매도호가|매수잔량|매도잔량|체결량|거래량|거래대금|매도체결량|매수체결량|체결강도|시장구분"
		pValue = data.split('^')
		for data_idx in range(data_cnt):
			data_offset = 26 * data_idx

			stock_execution_datetime_str = pValue[data_offset + 6] + " " + pValue[data_offset + 7]

			stock_code = pValue[data_offset + 1]
			datetime = datetime.strptime(stock_execution_datetime_str, "%Y%m%d%H%M%S")
			price = float(pValue[data_offset + 11])
			volume = int(pValue[data_offset + 19])

			self.__update_stock_execution(stock_code, datetime, price, volume)

	def __on_recv_kr_stock_orderbook(self, data):
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
		
	def __on_recv_ex_stock_orderbook(self, data):
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

	def __on_message_ws(self, ws, msg):
			self.__ws_send_lock.acquire()
			try:
				if msg[0] == '0':
					recvstr = msg.split('|')  # 수신데이터가 실데이터 이전은 '|'로 나뉘어져있어 split
					trid0 = recvstr[1]

					if trid0 == "H0STCNT0":  # 국내 주식 체결
						self.__on_recv_kr_stock_execution(int(recvstr[2]), recvstr[3])
					
					elif trid0 == "HDFSCNT0":  # 해외 주식 체결
						self.__on_recv_ex_stock_execution(int(recvstr[2]), recvstr[3])
	
					elif trid0 == "H0STASP0":  # 국내 주식호가
						self.__on_recv_kr_stock_orderbook(recvstr[3])

					elif trid0 == "HDFSASP0":  # 해외 주식 호가
						self.__on_recv_ex_stock_orderbook(recvstr[3])

				else:
					msg_json = json.loads(msg)

					if msg_json["header"]["tr_id"] == "PINGPONG":
						ws.send(msg)

					else:
						rt_cd = msg_json["body"]["rt_cd"]

						if rt_cd == '0':  # 정상일 경우 처리
							print("### RETURN CODE [ %s ][ %s ] MSG [ %s ]" % (msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]))
						else:
							print("### ERROR RETURN CODE [ %s ][ %s ] MSG [ %s ]" % (msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]))

			except:
				print("Error message")
			
			self.__ws_send_lock.release()

	##########################################################################
 
	def test_(self):
		msg = {
			"header" : {
				"approval_key": self.__ws_approval_key,
				"content-type": "utf-8",
				"custtype": "P",
				"tr_type": "1"
			},
			"body" : {
				"input": {
            		"tr_id": "H0STASP0",
            		"tr_key": "005930"
        		}
			}
		}
		
		# print(json.dumps(msg))
		self.__ws_app.send(json.dumps(msg))
 
	# def __add_websocket_query(self, api_add, api_code, api_stock_code):
	# 	'{"header":{"approval_key": "%s","custtype":"P","tr_type":"%s","content-type":"utf-8"},"body":{"input":{"tr_id":"%s","tr_key":"%s"}}}'%(self.__api_approval_key,api_add,api_code,api_stock_code)
 
	# def AddStockExecution(self, stock_code, stock_market):
	# 	api_code = ""
	# 	api_stock_code = ""

	# 	if stock_market == "NYSE":
	# 		api_code = "HDFSCNT0"
	# 		api_stock_code = "DNYS" + stock_code
	# 	elif stock_market == "NASDAQ":
	# 		api_code = "HDFSCNT0"
	# 		api_stock_code = "DNAS" + stock_code
	# 	else:
	# 		api_code = "H0STCNT0"
	# 		api_stock_code = stock_code

	# 	api_query = self.__add_websocket_query("1", api_code, api_stock_code)

	# 	self.__ws_query_list[] = ["H0STCNT0", stock_code]
 
	# def AddStockOrderbook(self, stock_code, stock_market):
        
	# 	api_code = ""
        
	# 	if stock_market == "NYSE":
	# 		self.__stock_execution_query_list.append(["HDFSASP0", "DNYS" + stock_code])
	# 	elif stock_market == "NASDAQ":
	# 		self.__stock_execution_query_list.append(["HDFSASP0", "DNAS" + stock_code])
	# 	else:
	# 		self.__stock_execution_query_list.append(["H0STASP0", stock_code])

	


