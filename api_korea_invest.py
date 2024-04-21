import requests
import websocket
import pymysql
import json
import threading
from datetime import datetime
from datetime import timedelta


class ApiKoreaInvestType:
	__sql_connection:pymysql.Connection = None
	__app_key:str = ""
	__app_secret:str = ""

	__API_BASE_URL:str = "https://openapi.koreainvestment.com:9443"
	__api_token_type:str = ""
	__api_token_val:str = ""
	__api_token_valid_datetime:datetime = datetime.min
	__api_header:dict = {}

	__WS_BASE_URL:str = "ws://ops.koreainvestment.com:21000"
	__ws_approval_key:str = ""
	__ws_app:websocket.WebSocketApp = None
	__ws_thread:threading.Thread = None
	__ws_send_lock:threading.Lock = threading.Lock()

	__ws_sub_list = {
		"stock_execution" : [],
		"stock_orderbook" : [],
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
		
	def __create_access_Token(self, app_key:str, app_secret:str):
		try:
			# 임시
			self.__app_key = app_key
			self.__app_secret = app_secret
			self.__api_token_type = "Bearer"
			self.__api_token_val = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6ImI3ZThhNjhiLTU3NjUtNDk0Yy04ZjhiLTJjZmQxNWRkZjQ0MSIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzcyMzEzOSwiaWF0IjoxNzEzNjM2NzM5LCJqdGkiOiJQU25aZmpVM1MydnFFaVlOZGpFQkowWm9zWkNxOXhMYlJEaTkifQ.x96SHlPgzU8lyITdmMdeIbyCl7BlD5ynVcBuWs3fU-hc-o5lgNtbvkgfGYClZAqg4_6x86O9R1A0W85BPJTYSg"
			self.__api_token_valid_datetime = datetime.strptime("2024-04-22 03:12:19", "%Y-%m-%d %H:%M:%S")
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
			
			file_str_list = [
				self.__api_token_type,
				self.__api_token_val,
				self.__api_token_valid_datetime.strftime("%Y-%m-%d %H:%M:%S")
			]
				
			file = open("./doc/last_token_info.dat", 'w')
			file.write("\n".join(file_str_list))
			file.close()
			

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
					on_message= self.__on_recv_message_ws,
					)
				self.__ws_thread = threading.Thread(target=self.__ws_app.run_forever)
				self.__ws_thread.start()

			return True
		
		except:
			return False

	def Initialize(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str, app_key:str, app_secret:str):
		if self.__create_sql_connection(sql_host, sql_id, sql_pw, sql_db) == False:
			print("Fail to create local SQL connection")
			return False
		if self.__create_access_Token(app_key, app_secret) == False:
			print("Fail to create access token for API server")
			return False
		if self.__create_websocket_app() == False:
			print("Fail to create websocket app for API server")
			return False

		return True

	##########################################################################
 

	def __create_stock_execution_table(self, stock_code:str):
		table_name = stock_code.replace("/", "_")
		raw_table_query_str = (
			"CREATE TABLE IF NOT EXISTS stock_execution_raw_" + table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0' "
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)
		candle_table_query_str = (
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
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=MEMORY"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(raw_table_query_str)
		cursor.execute(candle_table_query_str)

	def __update_stock_execution_table(self, stock_code:str, dt:datetime, price:float, non_volume:float, ask_volume:float, bid_volume:float):
		table_name = stock_code.replace("/", "_")
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
			"INSERT INTO stock_execution_raw_" + table_name + " VALUES ("
			+ "'" + datetime_00_min.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + price_str + "',"
			+ "'" + non_volume_str + "',"
			+ "'" + ask_volume_str + "',"
			+ "'" + bid_volume_str + "' "
			+ ")"
		)
		candle_table_query_str = (
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
		cursor.execute(raw_table_query_str)
		cursor.execute(candle_table_query_str)


	def __create_stock_orderbook_table(self, stock_code:str):
		# TODO
		#무엇을 저장할지, 어떤 방식으로 저장할지 안 정해짐
		return
	
		table_name = stock_code.replace("/", "_")
		orderbook_table_query_str = (
			"CREATE TABLE IF NOT EXISTS stock_orderbook_" + table_name + " ("
			+ "execution_datetime DATETIME NOT NULL,"
			+ "execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
			+ "execution_volume BIGINT(20) UNSIGNED NOT NULL DEFAULT '0'"
			+ ") COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(orderbook_table_query_str)

	def __update_stock_orderbook_table(self, stock_code:str, dt:datetime, data):
		# TODO
		#무엇을 저장할지, 어떤 방식으로 저장할지 안 정해짐
		return

		table_name = stock_code.replace("/", "_")
		orderbook_table_query_str = (
			"INSERT INTO stock_orderbook_" + table_name + " VALUES ("
			+ "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "',"
			+ "'" + data + "'"
			+ ")"
		)

		cursor = self.__sql_connection.cursor()
		cursor.execute(orderbook_table_query_str)


	##########################################################################
  

	def __on_recv_kr_stock_execution(self, data_cnt:int, data:str):
    	# "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
		pValue = data.split('^')
		for data_idx in range(data_cnt):
			data_offset = 46 * data_idx

			stock_execution_dt_str = datetime.now().strftime("%Y%m%d") + pValue[data_offset + 1]

			stock_code = pValue[data_offset + 0]
			dt = datetime.strptime(stock_execution_dt_str, "%Y%m%d%H%M%S")
			price = float(pValue[data_offset + 2])
			volume = float(pValue[data_offset + 12])
			
			if pValue[data_offset + 21] == "1":
				self.__update_stock_execution_table(stock_code, dt, price, 0, 0, volume)
			elif pValue[data_offset + 21] == "5":
				self.__update_stock_execution_table(stock_code, dt, price, 0, volume, 0)
			else:
				self.__update_stock_execution_table(stock_code, dt, price, volume, 0, 0)

	def __on_recv_ex_stock_execution(self, data_cnt:int, data:str):
    	# "실시간종목코드|종목코드|수수점자리수|현지영업일자|현지일자|현지시간|한국일자|한국시간|시가|고가|저가|현재가|대비구분|전일대비|등락율|매수호가|매도호가|매수잔량|매도잔량|체결량|거래량|거래대금|매도체결량|매수체결량|체결강도|시장구분"
		pValue = data.split('^')
		for data_idx in range(data_cnt):
			data_offset = 26 * data_idx

			stock_execution_dt_str = pValue[data_offset + 6] + " " + pValue[data_offset + 7]

			stock_code = pValue[data_offset + 1]
			dt = datetime.strptime(stock_execution_dt_str, "%Y%m%d%H%M%S")
			price = float(pValue[data_offset + 11])
			# volume = float(pValue[data_offset + 19])
			bid_volume = float(pValue[data_offset + 22])
			ask_volume = float(pValue[data_offset + 23])

			self.__update_stock_execution_table(stock_code, dt, price, 0, ask_volume, bid_volume)


	def __on_recv_kr_stock_orderbook(self, data:str):
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
		
	def __on_recv_ex_stock_orderbook(self, data:str):
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


	def __on_recv_message_ws(self, ws:websocket.WebSocketApp, msg:str):
			self.__ws_send_lock.acquire()
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

					else:
						rt_cd = msg_json["body"]["rt_cd"]

						if rt_cd == '0':  # 정상일 경우 처리
							print("\r### RETURN CODE [ %s ][ %s ] MSG [ %s ]\n> " % (msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]), end="")
						else:
							print("\r### ERROR RETURN CODE [ %s ][ %s ] MSG [ %s ]\n> " % (msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]), end="")

			except:
				print("\rError message\n> ", end="")
			
			self.__ws_send_lock.release()

	def __on_send_message_ws(self, is_reg:bool, api_code:str, stock_code:str):
		if is_reg == True:
			tr_type = "1"
		else:
			tr_type = "2"

		msg = {
			"header" : {
				"approval_key": self.__ws_approval_key,
				"content-type": "utf-8",
				"custtype": "P",
				"tr_type": tr_type
			},
			"body" : {
				"input": {
            		"tr_id": api_code,
            		"tr_key": stock_code
        		}
			}
		}
		self.__ws_app.send(json.dumps(msg))


	##########################################################################
	

	def FindStock(self, name:str):
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

	def GetKrStockList(self, cnt:int, offset:int = 0):
		query_str = (
			"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
			+ "FROM stock_info WHERE "
			+ "stock_market LIKE 'KOSPI%' or "
			+ "stock_market LIKE 'KOSDAQ%' or "
			+ "stock_market LIKE 'KONEX%' "
			+ "ORDER BY stock_capitalization DESC "
			+ "LIMIT " + str(offset) + " " + str(cnt)
		)
		cursor = self.__sql_connection.cursor()
		cursor.execute(query_str)

		return cursor.fetchall()
	
	def GetUsStockList(self, cnt:int, offset:int = 0):
		query_str = (
			"SELECT stock_code, stock_name_kr, stock_name_en, stock_market "
			+ "FROM stock_info WHERE "
			+ "stock_market LIKE 'NYSE%' or "
			+ "stock_market LIKE 'NASDAQ%' "
			+ "ORDER BY stock_capitalization DESC "
			+ "LIMIT " + str(offset) + " " + str(cnt)
		)
		cursor = self.__sql_connection.cursor()
		cursor.execute(query_str)
		
		return cursor.fetchall()


	def AddStockExecution(self, stock_code:str, stock_market:str):
		try:
			if stock_market == "NYSE":
				api_code = "HDFSCNT0"
				api_stock_code = "DNYS" + stock_code
			elif stock_market == "NASDAQ":
				api_code = "HDFSCNT0"
				api_stock_code = "DNAS" + stock_code
			else:
				api_code = "H0STCNT0"
				api_stock_code = stock_code

			self.__create_stock_execution_table(stock_code)
			self.__on_send_message_ws(True, api_code, api_stock_code)
			self.__ws_sub_list["stock_execution"].append(stock_code)

			return True
		
		except:
			return False

	def DelStockExecution(self, stock_code:str, stock_market:str):
		try:
			if stock_market == "NYSE":
				api_code = "HDFSCNT0"
				api_stock_code = "DNYS" + stock_code
			elif stock_market == "NASDAQ":
				api_code = "HDFSCNT0"
				api_stock_code = "DNAS" + stock_code
			else:
				api_code = "H0STCNT0"
				api_stock_code = stock_code

			self.__on_send_message_ws(False, api_code, api_stock_code)
			self.__ws_sub_list["stock_execution"].remove(stock_code)

			return True
		
		except:
			return False


	def AddStockOrderbook(self, stock_code:str, stock_market:str):
		try:
			if stock_market == "NYSE":
				api_code = "HDFSASP0"
				api_stock_code = "DNYS" + stock_code
			elif stock_market == "NASDAQ":
				api_code = "HDFSASP0"
				api_stock_code = "DNAS" + stock_code
			else:
				api_code = "H0STASP0"
				api_stock_code = stock_code

			self.__create_stock_orderbook_table(stock_code)
			self.__on_send_message_ws(True, api_code, api_stock_code)
			self.__ws_sub_list["stock_orderbook"].remove(stock_code)

			return True
		
		except:
			return False
		
	def DelStockOrderbook(self, stock_code:str, stock_market:str):
		try:
			if stock_market == "NYSE":
				api_code = "HDFSASP0"
				api_stock_code = "DNYS" + stock_code
			elif stock_market == "NASDAQ":
				api_code = "HDFSASP0"
				api_stock_code = "DNAS" + stock_code
			else:
				api_code = "H0STASP0"
				api_stock_code = stock_code

			self.__on_send_message_ws(False, api_code, api_stock_code)
			self.__ws_sub_list["stock_orderbook"].append(stock_code)

			return True
		
		except:
			return False

	


