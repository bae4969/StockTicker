from core import config
from core import util
import pymysql
import asyncio
import aiomysql
from threading import Thread, Event
import time


class BithumbSqlClient:
    def __init__(self, sql_host: str, sql_port: int, sql_id: str, sql_pw: str, sql_db: str, sql_charset: str):
        self.__sql_query_config = {
            'host': sql_host,
            'port': sql_port,
            'user': sql_id,
            'password': sql_pw,
            'charset': sql_charset,
            'autocommit': True,
        }
        self.__sql_common_connection = pymysql.connect(
            host=sql_host,
            port=sql_port,
            user=sql_id,
            passwd=sql_pw,
            db=sql_db,
            charset=sql_charset,
            autocommit=True,
        )
        config.set_session_timeouts(self.__sql_common_connection)

    def start(self) -> None:
        self.__ready_event = Event()
        self.__loop = asyncio.new_event_loop()
        self.__async_thread = Thread(name="Bithumb_Async", target=self.__run_async_loop, daemon=True)
        self.__async_thread.start()
        self.__ready_event.wait()

    def stop(self) -> None:
        self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, None)
        self.__async_thread.join()

    def execute_sync(self, query: str, params=None):
        for attempt in range(1, config.SQL_MAX_RETRY + 1):
            try:
                self.__reconnect()
                cursor = self.__sql_common_connection.cursor()
                cursor.execute(query, params)
                return cursor
            except Exception as ex:
                if config.is_retryable_error(ex) and attempt < config.SQL_MAX_RETRY:
                    util.InsertLog("ApiBithumb", "E", f"Sync query retry #{attempt} [ {ex.args[0]} ]")
                    time.sleep(min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX))
                    continue
                raise

    def enqueue(self, query: str, params=None) -> None:
        if params is not None:
            self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, (query, params))
        else:
            self.__loop.call_soon_threadsafe(self.__sql_query_queue.put_nowait, query)

    def __reconnect(self) -> None:
        self.__sql_common_connection.ping(reconnect=True)
        config.set_session_timeouts(self.__sql_common_connection)

    def __run_async_loop(self) -> None:
        asyncio.set_event_loop(self.__loop)
        self.__loop.run_until_complete(self.__async_main())

    async def __async_main(self) -> None:
        self.__sql_query_queue = asyncio.Queue()
        retry_count = 0
        while True:
            try:
                self.__sql_pool = await aiomysql.create_pool(**self.__sql_query_config)
                break
            except Exception as ex:
                retry_count += 1
                util.InsertLog("ApiBithumb", "E", f"Fail to create sql pool, retry #{retry_count} [ {ex.__str__()} ]")
                await asyncio.sleep(2)
        self.__ready_event.set()
        await self.__async_dequeue()
        self.__sql_pool.close()
        await self.__sql_pool.wait_closed()

    async def __async_dequeue(self) -> None:
        while True:
            item = await self.__sql_query_queue.get()
            if item is None:
                break

            if isinstance(item, tuple):
                query, params = item
            else:
                query, params = item, None

            for attempt in range(1, config.SQL_MAX_RETRY + 1):
                try:
                    async with self.__sql_pool.acquire() as conn:
                        async with conn.cursor() as cursor:
                            await cursor.execute(query, params)
                    break
                except Exception as ex:
                    if config.is_retryable_error(ex) and attempt < config.SQL_MAX_RETRY:
                        util.InsertLog("ApiBithumb", "E", f"DB query retry #{attempt} [ {ex.args[0]} ]")
                        await asyncio.sleep(min(2 ** attempt, config.SQL_RETRY_BACKOFF_MAX))
                        continue

                    util.InsertLog("ApiBithumb", "E", f"Fail to execute sql query [ {ex.__str__()} ]")
                    break
