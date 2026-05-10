import requests
import json


class BithumbRestClient:
    def get_coin_name_dict(self) -> dict:
        # 항목은 빗썸에서 한글 이름은 업비트에서 가져옴
        upbit_rep = requests.get(url="https://api.upbit.com/v1/market/all?isDetails=true")
        upbit_rep_json = json.loads(upbit_rep.text)

        coin_name_dict = {}
        for data in upbit_rep_json:
            try:
                from_to = data["market"].split("-")
                if from_to[0] != "KRW":
                    continue
                coin_name_dict[from_to[1]] = [
                    data["korean_name"],
                    data["english_name"],
                ]
            except:
                continue

        return coin_name_dict

    def get_coin_price_dict(self) -> dict:
        bithumb_rep = requests.get(url="https://api.bithumb.com/public/ticker/ALL_KRW")
        bithumb_rep_json = json.loads(bithumb_rep.text)
        if bithumb_rep_json["status"] != "0000":
            raise Exception("BITHUMB REQUEST ERROR")
        data_list = bithumb_rep_json["data"]

        coin_price_dict = {}
        for key, val in data_list.items():
            if key == "date":
                continue

            coin_price_dict[key] = [
                val["closing_price"],
                val["acc_trade_value_24H"],
            ]

        return coin_price_dict
