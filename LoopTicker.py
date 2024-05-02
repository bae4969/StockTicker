import Util
import doc.Define as Define
from ApiKoreaInvest import ApiKoreaInvestType as API_KI
from ApiBithumb import ApiBithumbType as API_BH
import time
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
    Define.APP_KEY,
    Define.APP_SECRET,
    )

last_datetime = DateTime.now().replace(hour=3, minute=0, second=0) + TimeDelta(days=1)
while True:
	try:
		current_market = ki.GetCurrentCollectingType()
		target_market = Util.GetCurrnetMarketOpened()

		if bh.IsCollecting() == False or last_datetime < DateTime.now():
			if last_datetime < DateTime.now():
				last_datetime += TimeDelta(days=1)
			bh.StopCollecting()
			while bh.StartCollecting() == False:
				time.sleep(3)
		
		if ki.IsCollecting() == False or current_market != target_market:
			ki.StopCollecting()
			while ki.StartCollecting(target_market) == False:
				time.sleep(3)              

		time.sleep(3)
	except: pass

bh.StopCollecting()
ki.StopCollecting()