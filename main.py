from core import config
from core import util
from api.korea_invest import ApiKoreaInvestType as API_KI
from api.bithumb import ApiBithumbType as API_BH
import signal
import time
from threading import Thread
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta


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

DAILY_SYNC_RETRY_INTERVAL = TimeDelta(minutes=5)
last_ki_daily_sync_try = DateTime.min
last_bh_daily_sync_try = DateTime.min

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

        if config.ENABLE_WEEKLY_SYNC and next_update_info_datetime < DateTime.now():
            next_update_info_datetime += TimeDelta(days=7)
            Thread(name="Bithumb_Update_Coin_Info", target=bh.SyncWeeklyInfo).start()
            Thread(name="KoreaInvest_Update_Stock_Info", target=ki.SyncWeeklyInfo).start()

        # KIS 와 같은 이유로 재시도 간격을 둔다. 실패 지점이 DB 조회라 재연결을
        # 유발하지는 않지만, 그대로 두면 2초마다 같은 SELECT 를 반복한다.
        if DateTime.now() - bh.GetCurrentCollectingDateTime() > TimeDelta(days=1):
            if DateTime.now() - last_bh_daily_sync_try >= DAILY_SYNC_RETRY_INTERVAL:
                last_bh_daily_sync_try = DateTime.now()
                bh.SyncPartitions()
                bh.SyncDailyInfo()

        # SyncDailyInfo 는 전부 성공했을 때만 시장을 바꾼다. 실패하면 이 조건이 계속 참이라
        # 재시도되는데, 루프가 2초짜리라 그대로 두면 토큰 발급을 연타하게 된다.
        # 실패 원인이 대개 발급 한도라 간격을 넉넉히 둔다.
        if target_market != ki.GetCurrentCollectingType():
            if DateTime.now() - last_ki_daily_sync_try >= DAILY_SYNC_RETRY_INTERVAL:
                last_ki_daily_sync_try = DateTime.now()
                ki.SyncPartitions()
                ki.SyncDailyInfo(target_market)

    except Exception as ex:
        util.InsertLog("Main", "E", f"Main loop error [ {ex.__str__()} ]")

bh.StopCollecting()
ki.StopCollecting()
