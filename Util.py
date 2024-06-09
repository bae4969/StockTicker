from datetime import datetime as DateTime
import inspect
import pymysql


class MySqlLogger:
	__sql_log_conn:pymysql.Connection = None
	__log_name:str = "None"

	def __init__(self, sql_host:str, sql_id:str, sql_pw:str, log_name:str) -> None:
		self.__sql_log_conn = pymysql.connect(
			host = sql_host,
			port = 3306,
			user = sql_id,
			passwd = sql_pw,
			db = "Log",
			charset = 'utf8',
			autocommit=True,
		)
		self.__log_name = log_name

	def __insertLog(self, type:str, msg:str, func:str, file:str, line:int) -> None:
		table_name = DateTime.now().strftime("Log%Y%V")

		create_table_query = f"""
		CREATE TABLE IF NOT EXISTS `{table_name}` (
			`log_datetime` DATETIME DEFAULT CURRENT_TIMESTAMP,
			`log_name` VARCHAR(255),
			`log_type` CHAR(1),
			`log_message` TEXT,
			`log_function` VARCHAR(255),
			`log_file` VARCHAR(255),
			`log_line` INT
		) COLLATE='utf8mb4_general_ci' ENGINE=ARCHIVE;
		"""

		insert_query = f"""
		INSERT INTO `{table_name}` (log_name, log_type, log_message, log_function, log_file, log_line)
		VALUES ('{self.__log_name}', '{type}', '{msg}', '{func}', '{file}', '{str(line)}');
		"""

		self.__sql_log_conn.ping(reconnect=True)
		cursor = self.__sql_log_conn.cursor()
		cursor.execute(create_table_query)
		cursor.execute(insert_query)

	def InsertNormalLog(self, msg:str) -> None:
		filepath = inspect.stack()[1][1]
		filename = filepath[filepath.rfind("/") + 1:]

		self.__insertLog("N", msg, inspect.stack()[1][3], filename, inspect.stack()[1][2])

	def InsertErrorLog(self, msg:str) -> None:
		filepath = inspect.stack()[1][1]
		filename = filepath[filepath.rfind("/") + 1:]

		self.__insertLog("E", msg, inspect.stack()[1][3], filename, inspect.stack()[1][2])

