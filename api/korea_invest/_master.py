import os
import pandas as pd
import urllib.request
import ssl
import glob
import zipfile


def get_kospi_stock_list() -> dict:
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip", "./temp/kospi_code.zip")

    kospi_zip = zipfile.ZipFile("./temp/kospi_code.zip")
    kospi_zip.extractall("./temp")
    kospi_zip.close()

    part1_columns = ["단축코드", "표준코드", "한글명"]
    part2_columns = ["그룹코드","시가총액규모","지수업종대분류","지수업종중분류","지수업종소분류","제조업","저유동성","지배구조지수종목","KOSPI200섹터업종","KOSPI100","KOSPI50","KRX","ETP","ELW발행","KRX100","KRX자동차","KRX반도체","KRX바이오","KRX은행","SPAC","KRX에너지화학","KRX철강","단기과열","KRX미디어통신","KRX건설","Non1","KRX증권","KRX선박","KRX섹터_보험","KRX섹터_운송","SRI","기준가","매매수량단위","시간외수량단위","거래정지","정리매매","관리종목","시장경고","경고예고","불성실공시","우회상장","락구분","액면변경","증자구분","증거금비율","신용가능","신용기간","전일거래량","액면가","상장일자","상장주수","자본금","결산월","공모가","우선주","공매도과열","이상급등","KRX300","KOSPI","매출액","영업이익","경상이익","당기순이익","ROE","기준년월","시가총액","그룹사코드","회사신용한도초과","담보대출가능","대주가능",]
    field_specs = [2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1,]
    offset = sum(field_specs) + 1

    tmp_fil1 = "./temp/kospi_code_part1.tmp"
    tmp_fil2 = "./temp/kospi_code_part2.tmp"

    wf1 = open(tmp_fil1, mode="w", encoding="cp949")
    wf2 = open(tmp_fil2, mode="w", encoding="cp949")

    with open("./temp/kospi_code.mst", mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0 : len(row) - offset]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
            rf2 = row[-offset:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
    df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
    df["단축코드"] = df["단축코드"].astype('str')

    def format_etn(symbol):
        if symbol[0].isalpha():
            return symbol[0] + symbol[1:].zfill(6)
        else:
            return symbol.zfill(6)

    return {
        "STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
        "ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
        "ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
    }


def get_kosdaq_stock_list() -> dict:
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip", "./temp/kosdaq_code.zip")

    kospi_zip = zipfile.ZipFile("./temp/kosdaq_code.zip")
    kospi_zip.extractall("./temp")
    kospi_zip.close()

    part1_columns = ["단축코드", "표준코드", "한글명"]
    part2_columns = ["그룹코드","시가총액 규모 구분 코드 유가","지수업종 대분류 코드","지수 업종 중분류 코드","지수업종 소분류 코드","벤처기업 여부","저유동성종목 여부","KRX 종목 여부","ETP 상품구분코드","KRX100 종목 여부","KRX 자동차 여부","KRX 반도체 여부","KRX 바이오 여부","KRX 은행 여부","기업인수목적회사여부","KRX 에너지 화학 여부","KRX 철강 여부","단기과열종목구분코드","KRX 미디어 통신 여부","KRX 건설 여부","투자주의환기종목여부","KRX 증권 구분","KRX 선박 구분","KRX섹터지수 보험여부","KRX섹터지수 운송여부","KOSDAQ150지수여부","주식 기준가","정규 시장 매매 수량 단위","시간외 시장 매매 수량 단위","거래정지 여부","정리매매 여부","관리 종목 여부","시장 경고 구분 코드","시장 경고위험 예고 여부","불성실 공시 여부","우회 상장 여부","락구분 코드","액면가 변경 구분 코드","증자 구분 코드","증거금 비율","신용주문 가능 여부","신용기간","전일 거래량","주식 액면가","주식 상장 일자","상장 주수","자본금","결산 월","공모 가격","우선주 구분 코드","공매도과열종목여부","이상급등종목여부","KRX300 종목 여부","매출액","영업이익","경상이익","단기순이익","ROE","기준년월","전일기준 시가총액","그룹사 코드","회사신용한도초과여부","담보대출가능여부","대주가능여부",]
    field_specs = [2,1,4,4,4,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,3,1,1,1,]
    offset = sum(field_specs) + 1

    tmp_fil1 = "./temp/kosdaq_code_part1.tmp"
    tmp_fil2 = "./temp/kosdaq_code_part2.tmp"

    wf1 = open(tmp_fil1, mode="w", encoding="cp949")
    wf2 = open(tmp_fil2, mode="w", encoding="cp949")

    with open("./temp/kosdaq_code.mst", mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0 : len(row) - offset]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
            rf2 = row[-offset:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
    df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
    df["단축코드"] = df["단축코드"].astype('str')

    def format_etn(symbol):
        if symbol[0].isalpha():
            return symbol[0] + symbol[1:].zfill(6)
        else:
            return symbol.zfill(6)

    return {
        "STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
        "ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
        "ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
    }


def get_konex_stock_list() -> dict:
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip", "./temp/konex_code.zip")

    kospi_zip = zipfile.ZipFile("./temp/konex_code.zip")
    kospi_zip.extractall("./temp")
    kospi_zip.close()

    part1_columns = ["단축코드", "표준코드", "한글명"]
    part2_columns = ["그룹코드","주식 기준가","정규 시장 매매 수량 단위","시간외 시장 매매 수량 단위","거래정지 여부","정리매매 여부","관리 종목 여부","시장 경고 구분 코드","시장 경고위험 예고 여부","불성실 공시 여부","우회 상장 여부","락구분 코드","액면가 변경 구분 코드","증자 구분 코드","증거금 비율","신용주문 가능 여부","신용기간","전일 거래량","주식 액면가","주식 상장 일자","상장 주수","자본금","결산 월","공모 가격","우선주 구분 코드","공매도과열종목여부","이상급등종목여부","KRX300 종목 여부","매출액","영업이익","경상이익","단기순이익","ROE","기준년월","전일기준 시가총액","회사신용한도초과여부","담보대출가능여부","대주가능여부",]
    field_specs = [2,9,5,5,1,1,1,2,1,1,1,2,2,2,3,1,3,12,12,8,15,21,2,7,1,1,1,1,9,9,9,5,9,8,9,1,1,1,]
    offset = sum(field_specs) + 1

    tmp_fil1 = "./temp/konex_code_part1.tmp"
    tmp_fil2 = "./temp/konex_code_part2.tmp"

    wf1 = open("./temp/konex_code_part1.tmp", mode="w", encoding="cp949")
    wf2 = open("./temp/konex_code_part2.tmp", mode="w", encoding="cp949")

    with open("./temp/konex_code.mst", mode="r", encoding="cp949") as f:
        for row in f:
            rf1 = row[0 : len(row) - offset]
            rf1_1 = rf1[0:9].rstrip()
            rf1_2 = rf1[9:21].rstrip()
            rf1_3 = rf1[21:].strip()
            wf1.write(rf1_1 + "," + rf1_2 + "," + rf1_3 + "\n")
            rf2 = row[-offset:]
            wf2.write(rf2)

    wf1.close()
    wf2.close()

    df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, sep=',', encoding="cp949")
    df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns, encoding="cp949")
    df = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)
    df["단축코드"] = df["단축코드"].astype('str')

    def format_etn(symbol):
        if symbol[0].isalpha():
            return symbol[0] + symbol[1:].zfill(6)
        else:
            return symbol.zfill(6)

    return {
        "STOCK" : df[(df["그룹코드"] == "ST")|(df["그룹코드"] == "RT")]["단축코드"].apply(format_etn).tolist(),
        "ETF" : df[df["그룹코드"] == "EF"]["단축코드"].apply(format_etn).tolist(),
        "ETN" : df[df["그룹코드"] == "EN"]["단축코드"].apply(format_etn).tolist(),
    }


def get_nyse_stock_list() -> dict:
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/nysmst.cod.zip", "./temp/nysmst.cod.zip")

    overseas_zip = zipfile.ZipFile(f'./temp/nysmst.cod.zip')
    overseas_zip.extractall("./temp")
    overseas_zip.close()

    # Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
    # 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
    df = pd.read_table("./temp/NYSMST.COD",sep='\t',encoding='cp949', na_values=[], keep_default_na=False)
    df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
    df["Symbol"] = df["Symbol"].astype('str').str.upper()

    return {
        "STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
        "ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
        "ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
    }


def get_nasdaq_stock_list() -> dict:
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/nasmst.cod.zip", "./temp/nasmst.cod.zip")

    overseas_zip = zipfile.ZipFile(f'./temp/nasmst.cod.zip')
    overseas_zip.extractall("./temp")
    overseas_zip.close()

    # Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
    # 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
    df = pd.read_table("./temp/NASMST.COD", sep='\t',encoding='cp949', na_values=[], keep_default_na=False)
    df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
    df["Symbol"] = df["Symbol"].astype('str').str.upper()

    files = glob.glob('./temp/*')
    for f in files:
        os.remove(f)

    return {
        "STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
        "ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
        "ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
    }


def get_amex_stock_list() -> dict:
    ssl._create_default_https_context = ssl._create_unverified_context
    urllib.request.urlretrieve("https://new.real.download.dws.co.kr/common/master/amsmst.cod.zip", "./temp/amsmst.cod.zip")

    overseas_zip = zipfile.ZipFile('./temp/amsmst.cod.zip')
    overseas_zip.extractall("./temp")
    overseas_zip.close()

    # Security type(1:Index,2:Stock,3:ETP(ETF),4:Warrant)
    # 구분코드(001:ETF,002:ETN,003:ETC,004:Others,005:VIX Underlying ETF,006:VIX Underlying ETN)
    df = pd.read_table("./temp/AMSMST.COD",sep='\t',encoding='cp949', na_values=['', 'NULL', 'NA'], keep_default_na=False)
    df.columns = ['National code', 'Exchange id', 'Exchange code', 'Exchange name', 'Symbol', 'realtime symbol', 'Korea name', 'English name', 'Security type', 'currency', 'float position', 'data type', 'base price', 'Bid order size', 'Ask order size', 'market start time', 'market end time', 'DR 여부', 'DR 국가코드', '업종분류코드', '지수구성종목 존재 여부', 'Tick size Type', '구분코드','Tick size type 상세']
    df["Symbol"] = df["Symbol"].astype('str').str.upper()

    files = glob.glob('./temp/*')
    for f in files:
        os.remove(f)

    return {
        "STOCK" : df[df["Security type"] == 2]["Symbol"].tolist(),
        "ETF" : df[df["구분코드"] == '001']["Symbol"].tolist(),
        "ETN" : df[df["구분코드"] == '002']["Symbol"].tolist(),
    }
