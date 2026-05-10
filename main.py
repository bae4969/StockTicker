from core import config
from core import util
from api.korea_invest import ApiKoreaInvestType as API_KI
from api.bithumb import ApiBithumbType as API_BH
import signal
import time
from threading import Thread
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta


util.Init(
    config.SQL_HOST,
    config.SQL_PORT,
    config.SQL_ID,
    config.SQL_PW,
    config.SQL_LOG_DB,
    config.SQL_CHARSET,
)
bh = API_BH(
    config.SQL_HOST,
    config.SQL_PORT,
    config.SQL_ID,
    config.SQL_PW,
    config.SQL_BH_DB,
    config.SQL_CHARSET,
)
ki = API_KI(
    config.SQL_HOST,
    config.SQL_PORT,
    config.SQL_ID,
    config.SQL_PW,
    config.SQL_KI_DB,
    config.SQL_CHARSET,
    config.KI_API_KEY_LIST,
)

next_update_info_datetime = DateTime.now().replace(hour=8, minute=0, second=0, microsecond=0)
days_until_sunday = (6 - next_update_info_datetime.weekday()) % 7
next_update_info_datetime += TimeDelta(days=days_until_sunday)
if next_update_info_datetime <= DateTime.now():
    next_update_info_datetime += TimeDelta(days=7)

stop_requested = False

def _on_shutdown_signal(signum, _frame):
    global stop_requested
    stop_requested = True
    util.InsertLog("Main", "N", f"Shutdown signal received [ {signal.Signals(signum).name} ]")

signal.signal(signal.SIGTERM, _on_shutdown_signal)
signal.signal(signal.SIGINT, _on_shutdown_signal)

while not stop_requested:
    time.sleep(2)
    try:
        kr_min_datetime = DateTime.now().replace(hour=8, minute=0, second=0)
        kr_max_datetime = DateTime.now().replace(hour=16, minute=0, second=0)
        if kr_min_datetime < DateTime.now() < kr_max_datetime:
            target_market = "KR"
        else:
            target_market = "EX"

        if next_update_info_datetime < DateTime.now():
            next_update_info_datetime += TimeDelta(days=7)
            Thread(name="Bithumb_Update_Coin_Info", target=bh.SyncWeeklyInfo).start()
            Thread(name="KoreaInvest_Update_Stock_Info", target=ki.SyncWeeklyInfo).start()

        if DateTime.now() - bh.GetCurrentCollectingDateTime() > TimeDelta(days=1):
            bh.SyncPartitions()
            bh.SyncDailyInfo()

        if target_market != ki.GetCurrentCollectingType():
            ki.SyncPartitions()
            ki.SyncDailyInfo(target_market)

    except Exception as ex:
        util.InsertLog("Main", "E", f"Main loop error [ {ex.__str__()} ]")

bh.StopCollecting()
ki.StopCollecting()
