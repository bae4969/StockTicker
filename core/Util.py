from core import config
import doc.Define as Define
from datetime import datetime as DateTime
import inspect
import asyncio
import aiomysql
from threading import Thread, Event


class MySqlLogger:
    def __init__(self, sql_host:str, sql_id:str, sql_pw:str) -> None:
        self.__sql_config = {
            'host': sql_host,
            'port': config.SQL_PORT,
            'user': sql_id,
            'password': sql_pw,
            'db': 'Log',
            'charset': config.SQL_CHARSET,
            'autocommit': True,
        }
        self.__ready_event = Event()
        self.__loop = asyncio.new_event_loop()
        self.__async_thread = Thread(name="Log_Async", target=self.__run_async_loop, daemon=True)
        self.__async_thread.start()
        self.__ready_event.wait()
  
    def __del__(self) -> None:
        self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, None)

    def __run_async_loop(self) -> None:
        asyncio.set_event_loop(self.__loop)
        self.__loop.run_until_complete(self.__async_main())

    async def __async_main(self) -> None:
        self.__sql_query_queue = asyncio.Queue()
        self.__sql_pool = await aiomysql.create_pool(**self.__sql_config)
        self.__ready_event.set()
        await self.__async_dequeue()
        self.__sql_pool.close()
        await self.__sql_pool.wait_closed()

    async def __async_dequeue(self) -> None:
        last_year = DateTime.min.year
        while True:
            t_log = await self.__sql_query_queue.get()
            if t_log is None:
                break

            for attempt in range(1, config.SQL_MAX_RETRY + 1):
                try:
                    async with self.__sql_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            this_year = t_log["DATETIME"].year
                            table_name = f"stock_ticker_log"
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
                                ) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB
                                """

                                create_table_query += f" PARTITION BY RANGE (YEAR(log_datetime)) ("
                                create_table_query += f"PARTITION p{last_year:04d} VALUES LESS THAN ({last_year + 1}),"
                                create_table_query += f"PARTITION pmax VALUES LESS THAN MAXVALUE)"

                                await cursor.execute(create_table_query)

                            name = t_log["NAME"]
                            type = t_log["TYPE"]
                            msg = t_log["MSG"]
                            func = t_log["FUNC"]
                            file = t_log["FILE"]
                            line = t_log["LINE"]

                            insert_query = f"""
                            INSERT INTO {table_name} (log_name, log_type, log_message, log_function, log_file, log_line)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """

                            await cursor.execute(insert_query, (name, type, msg, func, file, line))

                            print(f"[{name}] |{type}| {msg} ({func}|{file}:{line})")
                    break
                except Exception as ex:
                    if config.is_retryable_error(ex) and attempt < config.SQL_MAX_RETRY:
                        await asyncio.sleep(min(2 ** attempt, 10))
                        continue
                    print(f"Log query failed: {ex.__str__()}")
                    break

    def InsertLog(self, name:str, type:str, msg:str, func:str, file:str, line:int) -> None:
        self.__loop.call_soon_threadsafe(
            self.__sql_query_queue.put_nowait,
            {
                "DATETIME" : DateTime.now(),
                "NAME" : name,
                "TYPE" : type,
                "MSG" : msg,
                "FUNC" : func,
                "FILE" : file,
                "LINE" : line,
            }
        )


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
    

def TryGetDictInt(dict, key, default_value=0) -> int:
    try:
        return int(dict[key])
    except:
        return default_value
    

def TryGetDictFloat(dict, key, default_value=0.0) -> float:
    try:
        return float(dict[key])
    except:
        return default_value
    


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

