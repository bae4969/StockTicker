import Util
import doc.Define as Define
from ApiKoreaInvest import ApiKoreaInvestType as API_KI
from ApiBithumb import ApiBithumbType as API_BH
import time
import tabulate


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

while True:
	try:
		current_market = ki.GetCurrentCollectingType()
		target_market = Util.GetCurrnetMarketOpened()

		if bh.IsCollecting() == False:
			bh.StopCollecting()
			while bh.StartCollecting() == False: time.sleep(3)
		
		if ki.IsCollecting() == False or current_market != target_market:
			ki.StopCollecting()
			while ki.StartCollecting(target_market) == False: time.sleep(3)              

		time.sleep(3)
	except: pass

bh.StopCollecting()
ki.StopCollecting()