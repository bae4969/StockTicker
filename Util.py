import doc.Define as Define
from datetime import datetime as DateTime
import inspect
import pymysql
import queue
import time
from threading import Thread


class MySqlLogger:
	__sql_query_connection:pymysql.Connection
	__sql_is_stop:bool
	__sql_query_queue:queue.Queue = queue.Queue()

	def __init__(self, sql_host:str, sql_id:str, sql_pw:str) -> None:
		self.__sql_query_connection = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			db = "Log",
			charset = 'utf8',
			autocommit=True,
		)
  
		self.__sql_is_stop = False
		self.__sql_thread = Thread(name= "Log_Dequeue", target=self.__func_dequeue_sql_query)
		self.__sql_thread.daemon = True
		self.__sql_thread.start()
  
	def __del__(self) -> None:
		self.__sql_is_stop = True
		

	def __func_dequeue_sql_query(self) -> None:
		last_year = DateTime.min.year
		while self.__sql_is_stop == False:
			self.__sql_query_connection.ping(reconnect=True)
			cursor = self.__sql_query_connection.cursor()

			while self.__sql_query_queue.empty() == False:
				try:
					t_log = self.__sql_query_queue.get()

					this_year = t_log["DATETIME"].year
					table_name = f"Log{this_year:04d}"
					if last_year != this_year:
						last_year = this_year

						create_table_query = f"""
						CREATE TABLE IF NOT EXISTS {table_name} (
							log_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
							log_name VARCHAR(255),
							log_type CHAR(1),
							log_message TEXT,
							log_function VARCHAR(255),
							log_file VARCHAR(255),
							log_line INT
						) COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE
						"""
					
						create_table_query += f" PARTITION BY RANGE (YEARWEEK(log_datetime)) ("
						for i in range(1, 53):
							create_table_query += f"PARTITION p{last_year:04d}{i:02d} VALUES LESS THAN ({last_year:04d}{i+1:02d}),"
						create_table_query += f"PARTITION p{last_year:04d}{53:02d} VALUES LESS THAN MAXVALUE)"
					
						cursor.execute(create_table_query)

					name = t_log["NAME"].replace("'", '"')
					type = t_log["TYPE"].replace("'", '"')
					msg = t_log["MSG"].replace("'", '"')
					func = t_log["FUNC"].replace("'", '"')
					file = t_log["FILE"].replace("'", '"')
					line = t_log["LINE"]
     
					insert_query = f"""
					INSERT INTO {table_name} (log_name, log_type, log_message, log_function, log_file, log_line)
					VALUES ('{name}', '{type}', '{msg}', '{func}', '{file}', '{line}');
					"""
     
					cursor.execute(insert_query)
     
					print(f"[{name}] |{type}| {msg} ({func}|{file}:{line})")
     
				except Exception as ex:
					print(ex.__str__())
					pass
 
			time.sleep(0.2)

	def InsertLog(self, name:str, type:str, msg:str, func:str, file:str, line:int) -> None:
		self.__sql_query_queue.put({
			"DATETIME" : DateTime.now(),
			"NAME" : name,
			"TYPE" : type,
			"MSG" : msg,
			"FUNC" : func,
			"FILE" : file,
			"LINE" : line,
		})


logger_obj:MySqlLogger = MySqlLogger(
	Define.SQL_HOST,
	Define.SQL_ID,
	Define.SQL_PW
)
def InsertLog(name:str, type:str, msg:str) -> None:
	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]

	logger_obj.InsertLog(
		name=name,
		type=type,
		msg=msg,
		func=inspect.stack()[1][3],
		file=filename,
		line=inspect.stack()[1][2],
	)



def TryGetDictStr(dict, key, default_str="") -> str:
    try:
        return dict[key]
    except:
        return default_str
    

def TryGetDictInt(dict, key, default_str=0) -> int:
    try:
        return int(dict[key])
    except:
        return default_str
    

def TryGetDictFloat(dict, key, default_str=0.0) -> float:
    try:
        return float(dict[key])
    except:
        return default_str
    


def TryParseInt(value, default_value=0) -> int:
    try:
        return int(value)
    except:
        return default_value
    

def TryParseFloat(value, default_value=0.0) -> float:
    try:
        return float(value)
    except:
        return default_value

