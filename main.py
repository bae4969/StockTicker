import doc.settings as Settings
from core import util
from api.korea_invest import ApiKoreaInvestType as API_KI
from api.bithumb import ApiBithumbType as API_BH
import time
from threading import Thread
import tabulate
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta


util.Init(
    Settings.SQL_HOST,
    Settings.SQL_PORT,
    Settings.SQL_ID,
    Settings.SQL_PW,
    Settings.SQL_LOG_DB,
    Settings.SQL_CHARSET,
)
bh = API_BH(
    Settings.SQL_HOST,
    Settings.SQL_PORT,
    Settings.SQL_ID,
    Settings.SQL_PW,
    Settings.SQL_BH_DB,
    Settings.SQL_CHARSET,
)
ki = API_KI(
    Settings.SQL_HOST,
    Settings.SQL_PORT,
    Settings.SQL_ID,
    Settings.SQL_PW,
    Settings.SQL_KI_DB,
    Settings.SQL_CHARSET,
    Settings.KI_API_KEY_LIST,
)

next_update_info_datetime = DateTime.now().replace(hour=4, minute=0, second=0)
if next_update_info_datetime.weekday() < 6:
    next_update_info_datetime += TimeDelta(days= 6 - next_update_info_datetime.weekday())
else:
    next_update_info_datetime += TimeDelta(days= 7)

while True:
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
   
    except:
        pass

bh.StopCollecting()
ki.StopCollecting()
