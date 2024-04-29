import Util
import doc.Define as Define
from ApiKoreaInvest import ApiKoreaInvestType as API_KI
from ApiBithumb import ApiBithumbType as API_BH
import time
from datetime import datetime as DateTime
from datetime import timedelta

def main():
    bh = API_BH(
        Define.SQL_HOST,
        Define.SQL_ID,
        Define.SQL_PW,
        Define.SQL_BH_DB,
        )
    
    info_list = bh.GetCoinList(2)
    for info in info_list:
        bh.AddCoinExecutionQuery(info[0])
    while bh.StartCollecting() == False:
        time.sleep(10)
    
    user_str = input()

    bh.StopCollecting()
    
    #########################################################

    ki = API_KI(
        Define.SQL_HOST,
        Define.SQL_ID,
        Define.SQL_PW,
        Define.SQL_KI_DB,
        Define.APP_KEY,
        Define.APP_SECRET,
        )
    
    stock_list = ki.GetKrStockList(2)
    for info in stock_list:
        ki.AddStockExecutionQuery(info[0])
    while ki.StartCollecting() == False:
        time.sleep(10)

    user_str = input()

    ki.StopCollecting()


if __name__ == "__main__": main()
