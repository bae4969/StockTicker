# StockTicker

**StockTicker** 프로젝트는 가상화폐(빗썸) 및 국내/해외 주식(한국투자증권)의 실시간 체결 및 호가 데이터를 수집하여 MariaDB에 저장하는 자동화된 데이터 수집기(Data Collector)입니다. WebSocket을 이용하여 실시간으로 데이터를 스트리밍받아 파티셔닝된 데이터베이스 테이블에 저장합니다.

## 🚀 주요 기능

- **가상화폐 데이터 수집**: 빗썸 API를 통해 전체 상장 코인의 정보를 가져오고 실시간 체결 내역 및 호가 데이터를 수집합니다.
- **국내/미국 주식 데이터 수집**: 한국투자증권 Open API를 사용하여 KOSPI, KOSDAQ, KONEX를 비롯한 국내 주식과 NYSE, NASDAQ, AMEX 등 미국 주식의 종목 정보를 갱신하고 실시간 체결 및 호가를 수집합니다.
- **주기적 데이터 갱신 (Sync)**:
  - 매일: 일일 시세 및 변동 정보 갱신
  - 매주 일요일/월요일 새벽: 상장 종목 리스트 갱신 등의 대규모 메타 정보 싱크 처리
- **안정적인 DB 저장**: 별도의 Thread와 Queue를 구조화하여 병목 현상 없이 비동기로 SQL 쿼리를 실행 (Data Dequeue). 연/주 단위로 파티셔닝(Partitioning)된 테이블을 동적으로 생성하여 대용량 데이터를 최적화하여 보관합니다.
- **커스텀 데이터베이스 로거**: 작업 단위별로 로그를 남기며, 이 또한 로그 전용 DB 및 파티셔닝된 테이블에 적재됩니다.

## 📁 파일 구조

- `LoopTicker.py`
  - 프로그램의 메인 진입점(Main Entry) 모듈입니다.
  - 가상화폐(`ApiBithumbType`)와 주식(`ApiKoreaInvestType`) 인스턴스를 생성하고, 무한 루프 상태로 시간에 맞춰 Daily, Weekly 정보 업데이트 스레드를 실행시킵니다.
  - 한국/해외 주식장 오픈 시간(08:00 ~ 16:00) 여부에 따라 수집 시장("KR" / "EX")을 자동으로 스위칭합니다.
- `ApiBithumb.py`
  - 빗썸 거래소용 API 래퍼 클래스입니다.
  - REST API로 코인 목록을 조회하며, WebSocket으로 가상화폐 체결 정보 및 호가를 실시간으로 수신해 DB(MariaDB의 `Bithumb` DB)에 적재합니다.
- `ApiKoreaInvest.py`
  - 한국투자증권 API용 래퍼 클래스입니다. 여러 개의 API KEY를 로드해서 사용합니다.
  - 국내(KR)와 해외(EX) 증시의 종목 마스터 데이터를 주기적으로 zip 파일 형태로 받아서 파싱하여 업데이트합니다.
  - 주식 체결과 호가 데이터 역시 WebSocket으로 수신하며 병렬 쿼리 큐에 넣어 저장합니다.
- `Util.py`
  - 프로젝트 전반에서 사용하는 유틸리티 함수들을 포함합니다.
  - `MySqlLogger`: 에러 및 각종 시스템 상태 메세지를 MariaDB의 `Log` 데이터베이스에 비동기로 넣는 로깅 클래스입니다.
  - 안전한 형변환과 딕셔너리 파싱을 위한 `TryGetDictStr`, `TryParseInt` 등의 함수를 지원합니다.
- `doc/Define.py`
  - 프로젝트에서 사용되는 전역 상수, 데이터베이스 연결 정보, API Key List (한국투자증권), 기타 환경 설정값 뷰어입니다.

## 🗄 데이터베이스 구조

Data를 MariaDB를 통해 관리합니다. 각 API 연결 파일에서 데이터가 Insert 되기 전 필요 시 `CREATE TABLE IF NOT EXISTS`를 통해 자동으로 데이터베이스 테이블이 생성되도록 구성되어 있습니다.

- **Bithumb DB / KoreaInvest DB**:
  - `_StockInfo` / `_CoinInfo`: 종목 및 코인의 기본 정보를 담고 있는 테이블. 주기적으로 갱신됨.
  - `{Symbol}_Execution{YYYY}`: 특정 종목(Symbol)의 년도별 실제 거래 체결 데이터가 누적되는 테이블 (Week 기준으로 Partitioning 적용).
  - `{Symbol}_Orderbook{YYYY}`: 호가창 내역.
- **Log DB**: 
  - `Log{YYYY}`: 발생 시간, 파일명, 함수명, 라인 수까지 모두 포함하여 로깅하는 DB입니다.

## ⚙️ 실행 및 설정 방법

1. `doc/Define.py` 내부의 **SQL_HOST, SQL_ID, SQL_PW, KI_API_KEY_LIST**를 본인의 환경과 계정에 맞게 수정하십시오.
2. MariaDB(MySQL) 서버가 켜져 있어야 하며, Python 환경에서 `pymysql`, `requests`, `websocket-client`, `pandas`, `tabulate` 등의 패키지가 설치되어 있어야 합니다.
    ```bash
    pip install pymysql requests websocket-client pandas tabulate
    ```
3. 메인 스크립트를 실행합니다.
    ```bash
    python LoopTicker.py
    ```
