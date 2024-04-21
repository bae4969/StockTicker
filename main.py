from api_korea_invest import ApiKoreaInvestType as API_KI
from api_bithumb import ApiBithumbType as API_BH
import doc.define as define
import asyncio
from datetime import datetime
from datetime import timedelta


async def main():
    apiBH = API_BH()
    if apiBH.Initialize(
        define.SQL_HOST,
        define.SQL_ID,
        define.SQL_PW,
        define.SQL_BH_DB,
        ) == False:
        return False
    
    print(">", end=" ")
    user_str = input()
    apiBH.AddCoinExecution("BTC")

    print(">", end=" ")
    user_str = input()
    apiBH.DelCoinExecution("BTC")


    # apiKI = API_KI()
    # if apiKI.Initialize(
    #     define.SQL_HOST,
    #     define.SQL_ID,
    #     define.SQL_PW,
    #     define.SQL_KI_DB,
    #     define.APP_KEY,
    #     define.APP_SECRET
    #     ) == False:
    #     return False
    
    # print("Success to initialize 'Korea Invest API'\n")
    
    # while True:
    #     print(">", end=" ")
    #     user_str = input()
    #     try:
    #         # TODO
    #         # 입력 받아 명령 처리하는 것 구현 필요
    #         args = user_str.split(" ")
    #         if args[0].upper() == "ADD":

    #             if args[1].upper() == "EX":
    #                 stockInfoList = apiKI.FindStock(args[2])
    #                 if len(stockInfoList) == 0: raise
    #                 apiKI.AddStockExecution(stockInfoList[0][0], stockInfoList[0][3])

    #             elif args[1].upper() == "OB":
    #                 stockInfoList = apiKI.FindStock(args[2])
    #                 if len(stockInfoList) == 0: raise
    #                 apiKI.AddStockOrderbook(stockInfoList[0][0], stockInfoList[0][3])

    #         elif args[0].upper() == "DEL":
                
    #             if args[1].upper() == "EX":
    #                 stockInfoList = apiKI.FindStock(args[2])
    #                 if len(stockInfoList) == 0: raise
    #                 apiKI.DelStockExecution(stockInfoList[0][0], stockInfoList[0][3])

    #             elif args[1].upper() == "OB":
    #                 stockInfoList = apiKI.FindStock(args[2])
    #                 if len(stockInfoList) == 0: raise
    #                 apiKI.DelStockOrderbook(stockInfoList[0][0], stockInfoList[0][3])

    #     except:
    #         print("Invalid input, if need ")


if __name__ == "__main__":
    asyncio.run(main())