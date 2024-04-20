from api_korea_invest import ApiKoreaInvestType as API_KI
import define
import asyncio
from datetime import datetime
from datetime import timedelta


async def main():
    apiKI = API_KI()
    if apiKI.Initialize(
        define.SQL_HOST,
        define.SQL_ID,
        define.SQL_PW,
        define.SQL_KI_DB,
        define.APP_KEY,
        define.APP_SECRET
        ) == False:
        return False
    
    print("Success to initialize 'Korea Invest API'")
    
    while True:
        user_str = input()
        print("input string : " + user_str)
        apiKI.test_()
    
    await apiKI.WaitThread(None)
    print("END")


if __name__ == "__main__":
    asyncio.run(main())