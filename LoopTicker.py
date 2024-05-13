import doc.Define as Define
from ApiKoreaInvest import ApiKoreaInvestType as API_KI
from ApiBithumb import ApiBithumbType as API_BH
import time
from threading import Thread
import tabulate
from datetime import datetime as DateTime
from datetime import timedelta as TimeDelta


bh = API_BH(
    Define.SQL_HOST,
    Define.SQL_ID,
    Define.SQL_PW,
    Define.SQL_BH_DB,
)
ki = API_KI(
    Define.SQL_HOST,
    Define.SQL_ID,
    Define.SQL_PW,
    Define.SQL_KI_DB,
    Define.KI_API_KEY_LIST,
)

next_coin_reboot_datetime = DateTime.now().replace(hour=3, minute=0, second=0) + TimeDelta(days=1)
next_update_info_datetime = DateTime.now().replace(hour=4, minute=0, second=0)
if next_update_info_datetime.weekday() < 6:
    next_update_info_datetime += TimeDelta(days= 6 - next_update_info_datetime.weekday())
else:
	next_update_info_datetime += TimeDelta(days= 7)

while True:
    try:
        kr_min_datetime = DateTime.now().replace(hour=8, minute=0, second=0)
        kr_max_datetime = DateTime.now().replace(hour=15, minute=30, second=0)
        if kr_min_datetime < DateTime.now() < kr_max_datetime:
            target_market = "KR"
        else:
            target_market = "EX"

        if next_update_info_datetime < DateTime.now():
            next_update_info_datetime += TimeDelta(days=7)
            Thread(name="Bithumb_Update_Coin_Info", target=bh.UpdateCoinInfo).start()
            Thread(name="KoreaInvest_Update_Stock_Info", target=ki.UpdateStockInfo).start()

        if bh.IsCollecting() == False or next_coin_reboot_datetime < DateTime.now():
            if next_coin_reboot_datetime < DateTime.now():
                next_coin_reboot_datetime += TimeDelta(days=1)
            bh.StopCollecting()
            Thread(name="Bithumb_Start_Collector", target=bh.StartCollecting).start()

        if ki.IsCollecting() == False or target_market != ki.GetCurrentCollectingType():
            ki.StopCollecting()
            Thread(name="KoreaInvest_Start_Collector", target=ki.StartCollecting(target_market)).start()

        time.sleep(10)
    except:
        pass

bh.StopCollecting()
ki.StopCollecting()
