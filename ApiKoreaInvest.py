import Util
import requests
from websocket import WebSocketApp
import pymysql
import json
import queue
from threading import Thread
import os
import pandas as pd
import urllib.request
import ssl
import glob
import zipfile
import time
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta


class ApiKoreaInvestType:
	__sql_main_db:str
	__sql_common_connection:pymysql.Connection = None
	__sql_query_connection:pymysql.Connection = None
	__sql_is_stop:bool = False
	__sql_thread:Thread = None
	__sql_query_queue:queue.Queue = queue.Queue()

	__API_BASE_URL:str = "https://openapi.koreainvestment.com:9443"
	__api_key_list:list = []	# KEY, SECRET

	__rest_api_token_list:list = []
	__ws_app_info_list:list = []
 

	__ws_query_type:str = ""
	__ws_ex_excution_last_volume:dict = {}

	__MAX_REST_API_COUNT_PER_KEY:int = 20
	__MAX_WS_QUERY_COUNT_PER_KEY:int = 40


	##########################################################################


	def __init__(self, sql_host:str, sql_id:str, sql_pw:str, sql_db:str, api_key_list:list):
		self.__sql_main_db = sql_db
		self.__sql_common_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			db = sql_db,
			charset = 'utf8',
			autocommit=True,
		)
		self.__sql_query_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			charset = 'utf8',
			autocommit=True,
		)
		self.__api_key_list = api_key_list
  
		self.__create_stock_info_table()
		self.__create_last_ws_query_table()
		self.__start_dequeue_sql_query()
  
		self.__create_rest_api_token_list()
		self.__create_ws_app_info_list()

	def __del__(self):
		for ws_app_info in self.__ws_app_info_list:
			ws_app_info["WS_KEEP_CONNECT"] = False
			ws_app_info["WS_APP"].close()
		self.__stop_dequeue_sql_query()


	def __create_stock_info_table(self) -> None:
		try:
			table_query_str = (
				"CREATE TABLE IF NOT EXISTS stock_info ("
				+ "stock_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_name_kr VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_name_en VARCHAR(256) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_market VARCHAR(32) NOT NULL DEFAULT '' COLLATE 'utf8mb4_general_ci',"
				+ "stock_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_count BIGINT(20) UNSIGNED NOT NULL DEFAULT '0',"
				+ "stock_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
				+ "stock_capitalization DOUBLE UNSIGNED NOT NULL DEFAULT '0',"
				+ "stock_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
				+ "PRIMARY KEY (stock_code) USING BTREE,"
				+ "UNIQUE INDEX stock_code (stock_code) USING BTREE,"
				+ "INDEX stock_name (stock_name_kr, stock_name_en) USING BTREE"
				+ ")COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
			)

			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(table_query_str)

		except: raise Exception("Fail to create stock info table")

	def __create_last_ws_query_table(self) -> None:
		try:
			create_table_query = (
				"CREATE TABLE IF NOT EXISTS stock_last_ws_query ("
				+ "stock_query VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_code VARCHAR(16) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_api_type VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "stock_api_stock_code VARCHAR(32) NOT NULL COLLATE 'utf8mb4_general_ci',"
				+ "PRIMARY KEY (stock_query) USING BTREE,"
				+ "INDEX stock_code (stock_code) USING BTREE,"
				+ "CONSTRAINT FK_stock_list_last_query_stock_info FOREIGN KEY (stock_code) REFERENCES stock_info (stock_code) ON UPDATE CASCADE ON DELETE CASCADE"
				+ ") COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"
				)
			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(create_table_query)

		except: raise Exception("Fail to create last websocket query table")

	def __create_rest_api_token_list(self) -> None:
		try:
			file = open("./doc/last_token_info.dat", 'r')
			file_all_string = file.read()
			file.close()
			file_data = json.loads(file_all_string)
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
			
	def __create_ws_app_info_list(self) -> None:
		self.__ws_app_info_list = []
		for api_key in self.__api_key_list:
			ws_name = f"KoreaInvest_WS_{len(self.__ws_app_info_list)}"
			ws_app = WebSocketApp(
				url= "ws://ops.koreainvestment.com:21000",
				on_message= self.__on_ws_recv_message,
				on_open= self.__on_ws_open,
				on_close= self.__on_ws_close,
			)
			ws_thread = Thread(
				name= ws_name,
				target= ws_app.run_forever
			)
			ws_thread.daemon = True
			ws_thread.start()
			self.__ws_app_info_list.append({
				"API_KEY" : api_key["KEY"],
				"API_SECRET" : api_key["SECRET"],
				"APPROVAL_KEY" : "",
				"WS_NAME" : ws_name,
				"WS_APP" : ws_app,
				"WS_THREAD" : ws_thread,
				"WS_IS_OPENED" : False,
				"WS_KEEP_CONNECT" : True,
				"WS_QUERY_LIST" : [],
			})


	def __get_kospi_stock_list(self) -> dict:
		ssl._create_default_https_context = ssl._create_unverified_context
		urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip", "./temp/kospi_code.zip")

		kospi_zip = zipfile.ZipFile("./temp/kospi_code.zip")
		kospi_zip.extractall("./temp")
		kospi_zip.close()

		part1_columns = ["단축코드", "표준코드", "한글명"]
		part2_columns = ["그룹코드","시가총액규모","지수업종대분류","지수업종중분류","지수업종소분류","제조업","저유동성","지배구조지수종목","KOSPI200섹터업종","KOSPI100","KOSPI50","KRX","ETP","ELW발행","KRX100","KRX자동차","KRX반도체","KRX바이오","KRX은행","SPAC","KRX에너지화학","KRX철강","단기과열","KRX미디어통신","KRX건설","Non1","KRX증권","KRX선박","KRX섹터_보험","KRX섹터_운송","SRI","기준가","매매수량단위","시간외수량단위","거래정지","정리매매","관리종목","시장경고","경고예고","불성실공시","우회상장","락구분","액면변경","증자구분","증거금비율","신용가능","신용기간","전일거래량","액면가","상장일자","상장주수","자본금","결산월","공모가","우선주","공매도과열","이상급등","KRX300","KOSPI","매출액","영업이익","경상이익","당기순이익","ROE","기준년월","시가총액","그룹사코드","회사신용한도초과","담보대출가능","대주가능",]
		field_specs = [2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1,]
		offset = sum(field_specs) + 1

		tmp_fil1 = "./temp/kospi_code_part1.tmp"
		tmp_fil2 = "./temp/kospi_code_part2.tmp"
		
		wf1 = open(tmp_fil1, mode="w", encoding="cp949")
		wf2 = open(tmp_fil2, mode="w", encoding="cp949")

		with open("./temp/kospi_code.mst", mode="r", encoding="cp949") as f:
			for row in f:
				rf1 = row[0 : len(row) - offset]
				rf1_1 = rf1[0:9].rstrip()
				rf1_2 = rf1[9:21].rstrip()
				rf1_3 = rf1[21:].strip()
				wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
				rf2 = row[-offset:]
				wf2.write(rf2)

		wf1.close()
		wf2.close()

		df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
		df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
		df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
		df["단축코드"] = df["단축코드"].astype('str')

		def format_etn(symbol):
			if symbol[0].isalpha():
				return symbol[0] + symbol[1:].zfill(6)
			else:
				return symbol.zfill(6) 

		return {
			"STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
			"ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
			"ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
		}
		
	def __get_kosdaq_stock_list(self) -> dict:
		ssl._create_default_https_context = ssl._create_unverified_context
		urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip", "./temp/kosdaq_code.zip")

		kospi_zip = zipfile.ZipFile("./temp/kosdaq_code.zip")
		kospi_zip.extractall("./temp")
		kospi_zip.close()

		part1_columns = ["단축코드", "표준코드", "한글명"]
		part2_columns = ["그룹코드","시가총액 규모 구분 코드 유가","지수업종 대분류 코드","지수 업종 중분류 코드","지수업종 소분류 코드","벤처기업 여부","저유동성종목 여부","KRX 종목 여부","ETP 상품구분코드","KRX100 종목 여부","KRX 자동차 여부","KRX 반도체 여부","KRX 바이오 여부","KRX 은행 여부","기업인수목적회사여부","KRX 에너지 화학 여부","KRX 철강 여부","단기과열종목구분코드","KRX 미디어 통신 여부","KRX 건설 여부","투자주의환기종목여부","KRX 증권 구분","KRX 선박 구분","KRX섹터지수 보험여부","KRX섹터지수 운송여부","KOSDAQ150지수여부","주식 기준가","정규 시장 매매 수량 단위","시간외 시장 매매 수량 단위","거래정지 여부","정리매매 여부","관리 종목 여부","시장 경고 구분 코드","시장 경고위험 예고 여부","불성실 공시 여부","우회 상장 여부","락구분 코드","액면가 변경 구분 코드","증자 구분 코드","증거금 비율","신용주문 가능 여부","신용기간","전일 거래량","주식 액면가","주식 상장 일자","상장 주수","자본금","결산 월","공모 가격","우선주 구분 코드","공매도과열종목여부","이상급등종목여부","KRX300 종목 여부","매출액","영업이익","경상이익","단기순이익","ROE","기준년월","전일기준 시가총액","그룹사 코드","회사신용한도초과여부","담보대출가능여부","대주가능여부",]
		field_specs = [2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1,]
		offset = sum(field_specs) + 1

		tmp_fil1 = "./temp/kosdaq_code_part1.tmp"
		tmp_fil2 = "./temp/kosdaq_code_part2.tmp"
		
		wf1 = open(tmp_fil1, mode="w", encoding="cp949")
		wf2 = open(tmp_fil2, mode="w", encoding="cp949")

		with open("./temp/kosdaq_code.mst", mode="r", encoding="cp949") as f:
			for row in f:
				rf1 = row[0 : len(row) - offset]
				rf1_1 = rf1[0:9].rstrip()
				rf1_2 = rf1[9:21].rstrip()
				rf1_3 = rf1[21:].strip()
				wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
				rf2 = row[-offset:]
				wf2.write(rf2)

		wf1.close()
		wf2.close()

		df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
		df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
		df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
		df["단축코드"] = df["단축코드"].astype('str')

		def format_etn(symbol):
			if symbol[0].isalpha():
				return symbol[0] + symbol[1:].zfill(6)
			else:
				return symbol.zfill(6) 

		return {
			"STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
			"ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
			"ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
		}
		
	def __get_konex_stock_list(self) -> dict:
		ssl._create_default_https_context = ssl._create_unverified_context
		urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip", "./temp/konex_code.zip")

		kospi_zip = zipfile.ZipFile("./temp/konex_code.zip")
		kospi_zip.extractall("./temp")
		kospi_zip.close()

		part1_columns = ["단축코드", "표준코드", "한글명"]
		part2_columns = ["그룹코드","주식 기준가","정규 시장 매매 수량 단위","시간외 시장 매매 수량 단위","거래정지 여부","정리매매 여부","관리 종목 여부","시장 경고 구분 코드","시장 경고위험 예고 여부","불성실 공시 여부","우회 상장 여부","락구분 코드","액면가 변경 구분 코드","증자 구분 코드","증거금 비율","신용주문 가능 여부","신용기간","전일 거래량","주식 액면가","주식 상장 일자","상장 주수","자본금","결산 월","공모 가격","우선주 구분 코드","공매도과열종목여부","이상급등종목여부","KRX300 종목 여부","매출액","영업이익","경상이익","단기순이익","ROE","기준년월","전일기준 시가총액","회사신용한도초과여부","담보대출가능여부","대주가능여부",]
		field_specs = [2,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,1,1,1,]
		offset = sum(field_specs) + 1

		tmp_fil1 = "./temp/konex_code_part1.tmp"
		tmp_fil2 = "./temp/konex_code_part2.tmp"
		
		wf1 = open("./temp/konex_code_part1.tmp", mode="w", encoding="cp949")
		wf2 = open("./temp/konex_code_part2.tmp", mode="w", encoding="cp949")

		with open("./temp/konex_code.mst", mode="r", encoding="cp949") as f:
			for row in f:
				rf1 = row[0 : len(row) - offset]
				rf1_1 = rf1[0:9].rstrip()
				rf1_2 = rf1[9:21].rstrip()
				rf1_3 = rf1[21:].strip()
				wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
				rf2 = row[-offset:]
				wf2.write(rf2)

		wf1.close()
		wf2.close()

		df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
		df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
		df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
		df["단축코드"] = df["단축코드"].astype('str')

		def format_etn(symbol):
			if symbol[0].isalpha():
				return symbol[0] + symbol[1:].zfill(6)
			else:
				return symbol.zfill(6) 

		return {
			"STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
			"ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
			"ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
		}


	def __get_nyse_stock_list(self) -> dict:
		ssl._create_default_https_context = ssl._create_unverified_context
		urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/nysmst.cod.zip", "./temp/nysmst.cod.zip")

		overseas_zip = zipfile.ZipFile(f'./temp/nysmst.cod.zip')
		overseas_zip.extractall("./temp")
		overseas_zip.close()

		# Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
		# 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
		df = pd.read_table("./temp/NYSMST.COD",sep='\t',encoding='cp949')
		df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
		df["Symbol"] = df["Symbol"].astype('str').str.upper()

		return {
			"STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
			"ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
			"ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
		}
	
	def __get_nasdaq_stock_list(self) -> dict:
		ssl._create_default_https_context = ssl._create_unverified_context
		urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/nasmst.cod.zip", "./temp/nasmst.cod.zip")

		overseas_zip = zipfile.ZipFile(f'./temp/nasmst.cod.zip')
		overseas_zip.extractall("./temp")
		overseas_zip.close()

		# Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
		# 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
		df = pd.read_table("./temp/NASMST.COD",sep='\t',encoding='cp949')
		df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
		df["Symbol"] = df["Symbol"].astype('str').str.upper()

		files = glob.glob('./temp/*')
		for f in files:
			os.remove(f)

		return {
			"STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
			"ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
			"ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
		}
	
	def __get_amex_stock_list(self) -> dict:
		ssl._create_default_https_context = ssl._create_unverified_context
		urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/amsmst.cod.zip", "./temp/amsmst.cod.zip")

		overseas_zip = zipfile.ZipFile('./temp/amsmst.cod.zip')
		overseas_zip.extractall("./temp")
		overseas_zip.close()

		# Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
		# 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
		df = pd.read_table("./temp/AMSMST.COD",sep='\t',encoding='cp949')
		df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
		df["Symbol"] = df["Symbol"].astype('str').str.upper()

		files = glob.glob('./temp/*')
		for f in files:
			os.remove(f)

		return {
			"STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
			"ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
			"ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
		}
	

	##########################################################################
 

	def __start_dequeue_sql_query(self) -> None:
		self.__sql_is_stop = False
		self.__sql_thread = Thread(name="KoreaInvest_Dequeue",target=self.__func_dequeue_sql_query)
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


	def __update_stock_info_table(self, stock_info_dict:dict) -> None:
		self.__sql_query_queue.put(
			f"INSERT INTO {self.__sql_main_db}.stock_info ("
			+ "stock_code, stock_name_kr, stock_name_en, stock_market, stock_type, stock_count, stock_price, stock_capitalization"
			+ ") VALUES ("
			+ f"'{stock_info_dict['stock_code']}',"
			+ f"'{stock_info_dict['stock_name_kr']}',"
			+ f"'{stock_info_dict['stock_name_en']}',"
			+ f"'{stock_info_dict['stock_market']}',"
			+ f"'{stock_info_dict['stock_type']}',"
			+ f"'{stock_info_dict['stock_count']}',"
			+ f"'{stock_info_dict['stock_price']}',"
			+ f"'{stock_info_dict['stock_cap']}' "
			+ ") ON DUPLICATE KEY UPDATE "
			+ f"stock_name_kr='{stock_info_dict['stock_name_kr']}',"
			+ f"stock_name_en='{stock_info_dict['stock_name_en']}',"
			+ f"stock_market='{stock_info_dict['stock_market']}',"
			+ f"stock_type='{stock_info_dict['stock_type']}',"
			+ f"stock_count='{stock_info_dict['stock_count']}',"
			+ f"stock_price='{stock_info_dict['stock_price']}',"
			+ f"stock_capitalization='{stock_info_dict['stock_cap']}'"
		)

	def __update_stock_execution_table(self, stock_code:str, dt:DateTime, price:float, non_volume:float, ask_volume:float, bid_volume:float) -> None:
		database_name = "Z_Stock" + stock_code.replace("/", "_")
		raw_table_name = database_name + ".Raw" + dt.strftime("%Y")
		candle_table_name = database_name + ".Candle" + dt.strftime("%Y")

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

		self.__sql_common_connection.ping(reconnect=True)
		cursor = self.__sql_common_connection.cursor()
		cursor.execute(create_orderbook_table_query_str)
		cursor.execute(insert_orderbook_table_query_str)

	
	def __create_stock_execution_table(self, stock_code:str, year:int):
		database_name = "Z_Stock" + stock_code.replace("/", "_")
		raw_table_name = f"{database_name}.Raw{year:04d}"
		candle_table_name = f"{database_name}.Candle{year:04d}"
	 
		partition_str = f" PARTITION BY RANGE (YEARWEEK(execution_datetime)) ("
		for i in range(1, 53):
			partition_str += f"PARTITION p{year:04d}{i:02d} VALUES LESS THAN ({year:04d}{i+1:02d}),"
		partition_str += f"PARTITION p{year:04d}{53:02d} VALUES LESS THAN MAXVALUE)"

		create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET='utf8mb4' COLLATE='utf8mb4_general_ci'" 
		create_ex_raw_table_query = (
			f"""CREATE TABLE IF NOT EXISTS {raw_table_name} (
			execution_datetime DATETIME NOT NULL,
			execution_price DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0' 
			) COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE"""
		)
		create_ex_candle_table_query = (
			f"""CREATE TABLE IF NOT EXISTS {candle_table_name} (
			execution_datetime DATETIME NOT NULL,
			execution_open DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_close DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_min DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_max DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_non_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_ask_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_bid_volume DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_non_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_ask_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			execution_bid_amount DOUBLE UNSIGNED NOT NULL DEFAULT '0',
			PRIMARY KEY (execution_datetime) USING BTREE
			) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB"""
		)
  
		create_ex_raw_table_query += partition_str
		create_ex_candle_table_query += partition_str
  
		self.__sql_query_queue.put(create_database_query)
		self.__sql_query_queue.put(create_ex_raw_table_query)
		self.__sql_query_queue.put(create_ex_candle_table_query)
		
	def __create_stock_orderbook_table(self, stock_code:str, year:int):
		# TODO
		return


	##########################################################################
		

	def __rest_api_kr_stock_info_dict(self, rest_api_token_header:dict, stock_code:str, stock_type:str, stock_market:str) -> dict:
		try:
			api_url = "/uapi/domestic-stock/v1/quotations/search-stock-info"
			api_header = rest_api_token_header.copy()
			api_header["tr_id"] = "CTPF1002R"
			api_para = {
				"PRDT_TYPE_CD" : "300",
				"PDNO" : stock_code,
			}
  
			response = requests.get (
				url = self.__API_BASE_URL + api_url,
				headers= api_header,
				params= api_para,
			)

			rep_json = json.loads(response.text)
			if int(rep_json["rt_cd"]) != 0:
				raise Exception("Recv Code")

			rep_stock_info = rep_json["output"]
			stock_price = float(rep_stock_info["thdt_clpr"])
			stock_count = float(rep_stock_info["lstg_stqt"])

			return {
				"is_good_info" : True,
				"stock_code" : stock_code,
				"stock_name_kr" : rep_stock_info["prdt_abrv_name"].replace("'", " "),
				"stock_name_en" : rep_stock_info["prdt_eng_abrv_name"].replace("'", " "),
				"stock_market" : stock_market,
				"stock_type" : stock_type,
				"stock_price" : stock_price,
				"stock_count" : stock_count,
				"stock_cap" : str(float(stock_count) * float(stock_price)),
			}
   
		except Exception as e:
			raise Exception("[ kr stock info ][ %s ][ %s ]"%(stock_code, e.__str__()))

	def __rest_api_ex_stock_info_dict(self, rest_api_token_header:dict, stock_code:str, stock_type:str, stock_market:str) -> dict:
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
		
			response = requests.get (
				url = self.__API_BASE_URL + api_url,
				headers= api_header,
				params= api_para,
			)

			rep_json = json.loads(response.text)
			if int(rep_json["rt_cd"]) != 0: raise Exception("Recv Code")

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
		
			response = requests.get (
				url = self.__API_BASE_URL + api_url,
				headers= api_header,
				params= api_para,
			)

			rep_json = json.loads(response.text)
			if int(rep_json["rt_cd"]) != 0: raise Exception("Recv Code")

			rep_stock_info2 = rep_json["output"]

			stock_price = float(rep_stock_info2["base"])
			stock_count = float(rep_stock_info1["lstg_stck_num"])

			return {
				"is_good_info" : True,
				"stock_code" : stock_code,
				"stock_name_kr" : rep_stock_info1["prdt_name"].replace("'", " "),
				"stock_name_en" : rep_stock_info1["prdt_eng_name"].replace("'", " "),
				"stock_market" : stock_market,
				"stock_type" : stock_type,
				"stock_price" : stock_price,
				"stock_count" : stock_count,
				"stock_cap" : str(float(stock_count) * float(stock_price)),
			}

		except Exception as e:
			raise Exception("[ ex stock info ][ %s ][ %s ]"%(stock_code, e.__str__()))


	##########################################################################


	def __on_recv_kr_stock_execution(self, data_cnt:int, data:str) -> None:
		# "유가증권단축종목코드|주식체결시간|주식현재가|전일대비부호|전일대비|전일대비율|가중평균주식가격|주식시가|주식최고가|주식최저가|매도호가1|매수호가1|체결거래량|누적거래량|누적거래대금|매도체결건수|매수체결건수|순매수체결건수|체결강도|총매도수량|총매수수량|체결구분|매수비율|전일거래량대비등락율|시가시간|시가대비구분|시가대비|최고가시간|고가대비구분|고가대비|최저가시간|저가대비구분|저가대비|영업일자|신장운영구분코드|거래정지여부|매도호가잔량|매수호가잔량|총매도호가잔량|총매수호가잔량|거래량회전율|전일동시간누적거래량|전일동시간누적거래량비율|시간구분코드|임의종료구분코드|정적VI발동기준가"
		pValue = data.split('^')
		for data_idx in range(data_cnt):
			try:
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
				raise Exception("[ kr stock execution ][ %s ][ %s ]"%(stock_code, e.__str__()))

	def __on_recv_ex_stock_execution(self, data_cnt:int, data:str) -> None:
		# "실시간종목코드|종목코드|수수점자리수|현지영업일자|현지일자|현지시간|한국일자|한국시간|시가|고가|저가|현재가|대비구분|전일대비|등락율|매수호가|매도호가|매수잔량|매도잔량|체결량|거래량|거래대금|매도체결량|매수체결량|체결강도|시장구분"
		pValue = data.split('^')
		for data_idx in range(data_cnt):
			try:
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
				raise Exception("[ ex stock execution ][ %s ][ %s ]"%(stock_code, e.__str__()))

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


	def __on_ws_recv_message(self, ws:WebSocketApp, msg:str) -> None:
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
					Thread(name="KoreaInvest_KR_Execution", target=self.__on_recv_kr_stock_execution(int(recvstr[2]), recvstr[3])).start()
				elif trid0 == "HDFSCNT0":
					Thread(name="KoreaInvest_EX_Execution", target=self.__on_recv_ex_stock_execution(int(recvstr[2]), recvstr[3])).start()
				elif trid0 == "H0STASP0":
					Thread(name="KoreaInvest_KR_Orderbook", target=self.__on_recv_kr_stock_orderbook(recvstr[3])).start()
				elif trid0 == "HDFSASP0":
					Thread(name="KoreaInvest_EX_Orderbook", target=self.__on_recv_ex_stock_orderbook(recvstr[3])).start()

			else:
				msg_json = json.loads(msg)

				if msg_json["header"]["tr_id"] == "PINGPONG":
					ws.send(msg)

				else:
					rt_cd = msg_json["body"]["rt_cd"]

					if rt_cd != '0':
						Util.InsertLog("ApiKoreaInvest", "E", "Error msg : [ %s ][ %s ][ %s ]"%(msg_json["header"]["tr_key"], rt_cd, msg_json["body"]["msg1"]))

		except Exception as e:
			Util.InsertLog("ApiKoreaInvest", "E", "Fail to process ws recv msg : " + e.__str__())
	
	def __on_ws_open(self, ws:WebSocketApp) -> None:
		for ws_app_info in self.__ws_app_info_list:
			if ws_app_info["WS_APP"] != ws: continue
			ws_app_info["WS_IS_OPENED"] = True

			for query_info in ws_app_info["WS_QUERY_LIST"]:
				try:
					msg = {
						"header" : {
							"approval_key": ws_app_info["APPROVAL_KEY"],
							"content-type": "utf-8",
							"custtype": "P",
							"tr_type": "1"
						},
						"body" : {
							"input": {
								"tr_id": query_info["stock_api_type"],
								"tr_key": query_info["stock_api_stock_code"]
							}
						}
					}
					ws.send(json.dumps(msg))

				except Exception as e:
					Util.InsertLog("ApiKoreaInvest", "E", f"Fail to process ws send msg [ {ws_app_info["WS_NAME"]} | {e.__str__()} ] ")

				time.sleep(0.5)

			Util.InsertLog("ApiKoreaInvest", "N", f"Opened korea invest websocket [ {ws_app_info["WS_NAME"]} ]")
			break

	def __on_ws_close(self, ws:WebSocketApp, close_code, close_msg) -> None:
		for ws_app_info in self.__ws_app_info_list:
			if ws_app_info["WS_APP"] != ws: continue
			ws_app_info["WS_IS_OPENED"] = False

			while ws_app_info["WS_KEEP_CONNECT"] == True:
				time.sleep(1)
				try:
					api_url = "/oauth2/Approval"
					api_header = {
						"content-type" : "application/json; utf-8"
					}
					api_body = {
						"grant_type" : "client_credentials",
						"appkey" : ws_app_info["API_KEY"],
						"secretkey" : ws_app_info["API_SECRET"],
					}
					response = requests.post (
						url = self.__API_BASE_URL + api_url,
						headers = api_header,
						data = json.dumps(api_body),
					)

					rep_json = json.loads(response.text)
					ws_app_info["APPROVAL_KEY"] = rep_json["approval_key"]
					ws_app_info["WS_APP"] = WebSocketApp(
						url = "ws://ops.koreainvestment.com:21000",
						on_message= self.__on_ws_recv_message,
						on_open= self.__on_ws_open,
						on_close= self.__on_ws_close,
					)
					ws_app_info["WS_THREAD"] = Thread(
						name= ws_app_info["WS_NAME"],
						target= ws_app_info["WS_APP"].run_forever
					)
					ws_app_info["WS_THREAD"].daemon = True
					ws_app_info["WS_THREAD"].start()
	 
					Util.InsertLog("ApiKoreaInvest", "N", f"Reconnected korea invest websocket [ {ws_app_info["WS_NAME"]} ]")
					break
	   
				except Exception as ex:
					Util.InsertLog("ApiKoreaInvest", "E", f"Fail to reconnect korea invest websocket [ {ws_app_info["WS_NAME"]} | {ex.__str__()} ]")
   
			Util.InsertLog("ApiKoreaInvest", "N", f"Closed korea invest websocket [ {ws_app_info["WS_NAME"]} ]")
			break


	##########################################################################


	def __sync_rest_api_token_list(self) -> None:
		for rest_api_token in self.__rest_api_token_list:
			try:
				remain_sec = (rest_api_token["TOKEN_EXPIRED_DATETIME"] - DateTime.now()).seconds
				if 43200 < remain_sec and remain_sec < 86220 :
					api_url = "/oauth2/revokeP"
					api_body = {
						"grant_type" : "client_credentials",
						"appkey" : rest_api_token["API_KEY"],
						"appsecret" : rest_api_token["API_SECRET"],
						"token" : rest_api_token["TOKEN_VAL"]
					}
					response = requests.post (
						url = self.__API_BASE_URL + api_url,
						data = json.dumps(api_body),
					)
	
					rest_api_token["TOKEN_TYPE"] = ""
					rest_api_token["TOKEN_VAL"] = ""
					rest_api_token["TOKEN_EXPIRED_DATETIME"] = DateTime.min
					rest_api_token["TOKEN_HEADER"] = {}
	
			except: pass

			try:
				api_url = "/oauth2/tokenP"
				api_body = {
					"grant_type" : "client_credentials",
					"appkey" : rest_api_token["API_KEY"],
					"appsecret" : rest_api_token["API_SECRET"],
				}
				response = requests.post (
					url = self.__API_BASE_URL + api_url,
					data = json.dumps(api_body),
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
	
			except: raise Exception("Fail to sync rest api token list")

		try:
			file_data = {}
			for rest_api_token in self.__rest_api_token_list:
				file_data[rest_api_token["API_KEY"]] = {
					"TOKEN_TYPE" : rest_api_token["TOKEN_TYPE"],
					"TOKEN_VAL" : rest_api_token["TOKEN_VAL"],
					"TOKEN_EXPIRED_DATETIME" : rest_api_token["TOKEN_EXPIRED_DATETIME"].strftime("%Y-%m-%d %H:%M:%S"),
				}

			file = open("./doc/last_token_info.dat", 'w')
			file.write(file_data.__str__().replace("'", "\""))
			file.close()

		except: Util.InsertLog("ApiKoreaInvest", "E", "Fail to create access token for KoreaInvest Api")
	
	def __sync_stock_info_table(self) -> None:
		try:
			if not os.path.exists("./temp"):
				os.makedirs("./temp")
			else:
				files = glob.glob('./temp/*')
				for f in files:
					os.remove(f)

			stock_code_list = {
				"KOSPI" : self.__get_kospi_stock_list(),
				"KOSDAQ" : self.__get_kosdaq_stock_list(),
				"KONEX" : self.__get_konex_stock_list(),
				"NASDAQ" : self.__get_nasdaq_stock_list(),
				"NYSE" : self.__get_nyse_stock_list(),
				"AMEX" : self.__get_amex_stock_list(),
			}
   
			temp_market_code_list = [[] for _ in range(len(self.__rest_api_token_list))]
			token_idx = 0
			for stock_market, stock_code_infos in stock_code_list.items():
				for stock_type, stock_code_list in stock_code_infos.items():
					for stock_code in stock_code_list:
						temp_market_code_list[token_idx].append([stock_market, stock_type, stock_code])
						token_idx += 1
						if token_idx >= len(self.__rest_api_token_list):
							token_idx = 0
   
			def kernel_func(rest_api_token:dict, stock_market_code_list:list) -> None:
				for stock_market_code in stock_market_code_list:
					try:
						min_micro = 1000000. / self.__MAX_REST_API_COUNT_PER_KEY
						start_dt = DateTime.now()
						stock_market = stock_market_code[0]
						stock_type = stock_market_code[1]
						stock_code = stock_market_code[2]
						rest_api_token_header = rest_api_token["TOKEN_HEADER"]
	  
						if stock_market in ["KOSPI", "KOSDAQ", "KONEX"]:
							min_micro *= 1
							stock_info_dict = self.__rest_api_kr_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
							self.__update_stock_info_table(stock_info_dict)
						elif stock_market in ["NASDAQ", "NYSE", "AMEX"]:
							min_micro *= 2
							stock_info_dict = self.__rest_api_ex_stock_info_dict(rest_api_token_header, stock_code, stock_type, stock_market)
							self.__update_stock_info_table(stock_info_dict)
						else:
							continue

						diff_micro = (DateTime.now() - start_dt).microseconds
						if min_micro > diff_micro:
							time.sleep((min_micro - diff_micro) / 1000000.)
	   
					except Exception as e:
						Util.InsertLog("ApiKoreaInvest", "E", f"Fail to update stock info [ {stock_market} | {stock_code} | {e.__str__()}]")

	  
			temp_thread_list = []
			for idx in range(len(self.__rest_api_token_list)):
				t_thread = Thread(
					name= f"KoreaInvest_Update_Stock_Info_{idx}",
					target= kernel_func,
					args=(self.__rest_api_token_list[idx], temp_market_code_list[idx])
		   		)
				t_thread.daemon = True
				t_thread.start()
				temp_thread_list.append(t_thread)

			for temp_thread in temp_thread_list:
				temp_thread.join()
						

			Util.InsertLog("ApiKoreaInvest", "N", "Success to update stock info")

		except Exception as e:
			Util.InsertLog("ApiKoreaInvest", "E", "Fail to update stock info : " + e.__str__())

	def __sync_ws_data_table(self) -> None:
		try:
			if self.__ws_query_type == "KR":
				select_query = (
					"SELECT "
					+ "L.stock_query, L.stock_code, I.stock_market, L.stock_api_type, L.stock_api_stock_code "
					+ "FROM stock_last_ws_query AS L "
					+ "JOIN stock_info AS I "
					+ "ON L.stock_code = I.stock_code "
					+ "WHERE I.stock_market='KOSPI' "
	 				+ "OR I.stock_market='KOSDAQ' "
		 			+ "OR I.stock_market='KONEX'"
				)
			elif self.__ws_query_type == "EX":
				select_query = (
					"SELECT "
					+ "L.stock_query, L.stock_code, I.stock_market, L.stock_api_type, L.stock_api_stock_code "
					+ "FROM stock_last_ws_query AS L "
					+ "JOIN stock_info AS I "
					+ "ON L.stock_code = I.stock_code "
					+ "WHERE I.stock_market='NYSE' "
	 				+ "OR I.stock_market='NASDAQ' "
		 			+ "OR I.stock_market='AMEX'"
				)
			else:
				raise

			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(select_query)
			sql_query_list = cursor.fetchall()
   
			this_year = DateTime.now().year
   
			for sql_query in sql_query_list:
				if sql_query[3] == "H0STCNT0" or sql_query[3] == "HDFSCNT0":
					self.__create_stock_execution_table(sql_query[1], this_year)
					self.__create_stock_execution_table(sql_query[1], this_year + 1)
     
				elif sql_query[3] == "H0STASP0" or sql_query[3] == "HDFSASP0":
					self.__create_stock_orderbook_table(sql_query[1], this_year)
					self.__create_stock_orderbook_table(sql_query[1], this_year + 1)
	 
				else:
					continue
	  
		except: raise Exception("Fail to sync websocket data table")

	def __sync_ws_query_list(self) -> None:
		try:
			if self.__ws_query_type == "KR":
				select_query = (
					"SELECT "
					+ "L.stock_query, L.stock_code, I.stock_market, L.stock_api_type, L.stock_api_stock_code "
					+ "FROM stock_last_ws_query AS L "
					+ "JOIN stock_info AS I "
					+ "ON L.stock_code = I.stock_code "
					+ "WHERE I.stock_market='KOSPI' "
	 				+ "OR I.stock_market='KOSDAQ' "
		 			+ "OR I.stock_market='KONEX'"
				)
			elif self.__ws_query_type == "EX":
				select_query = (
					"SELECT "
					+ "L.stock_query, L.stock_code, I.stock_market, L.stock_api_type, L.stock_api_stock_code "
					+ "FROM stock_last_ws_query AS L "
					+ "JOIN stock_info AS I "
					+ "ON L.stock_code = I.stock_code "
					+ "WHERE I.stock_market='NYSE' "
	 				+ "OR I.stock_market='NASDAQ' "
		 			+ "OR I.stock_market='AMEX'"
				)
			else:
				raise

			self.__sql_common_connection.ping(reconnect=True)
			cursor = self.__sql_common_connection.cursor()
			cursor.execute(select_query)
			sql_query_list = cursor.fetchall()
   
			temp_list = [[] for _ in range(len(self.__ws_app_info_list))]
   
			app_idx = 0
			for sql_query in sql_query_list:
				if temp_list[app_idx].count() > self.__MAX_WS_QUERY_COUNT_PER_KEY:
					Util.InsertLog("ApiKoreaInvest", "E", f"WS query was overflowed ( {sql_query[1]} : {sql_query[3]} )")
				else:
					temp_list[app_idx].append({
						"stock_code" : sql_query[1],
						"stock_market" : sql_query[2],
						"stock_api_type" : sql_query[3],
						"stock_api_stock_code" : sql_query[4]
					})
				
				app_idx += 1
				if app_idx >= len(self.__ws_app_info_list):
					app_idx = 0
	 
			for idx in range(len(self.__ws_app_info_list)):
				self.__ws_app_info_list[idx]["WS_QUERY_LIST"] = temp_list[idx]
				self.__ws_app_info_list[idx]["WS_APP"].close()
   
		except: raise Exception("Fail to sync websocket query list")
	

	##########################################################################
	

	def GetCurrentCollectingType(self) -> str:
		return self.__ws_query_type


	def SyncDailyInfo(self, target_market:str) -> None:
		try:
			self.__ws_query_type = target_market
			self.__sync_rest_api_token_list()
			self.__sync_ws_data_table()
			self.__sync_ws_query_list()
		except Exception as ex:
			Util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync daily info for korea invest api [ {ex.__str__()} ] ")
   
	def SyncWeeklyInfo(self) -> None:
		try:
			self.__sync_stock_info_table()
		except Exception as ex:
			Util.InsertLog("ApiKoreaInvest", "E", f"Fail to sync weekly info for korea invest api [ {ex.__str__()} ] ")
	   
		





