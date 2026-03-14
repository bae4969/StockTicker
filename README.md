# StockTicker

**StockTicker** 프로젝트는 가상화폐(빗썸) 및 국내/해외 주식(한국투자증권)의 실시간 체결 및 호가 데이터를 수집하여 MariaDB에 저장하는 자동화된 데이터 수집기(Data Collector)입니다. WebSocket을 이용하여 실시간으로 데이터를 스트리밍받아 파티셔닝된 데이터베이스 테이블에 저장합니다.

## 🚀 주요 기능

- **가상화폐 데이터 수집**: 빗썸 API를 통해 전체 상장 코인의 정보를 가져오고 실시간 체결 내역 및 호가 데이터를 수집합니다.
- **국내/미국 주식 데이터 수집**: 한국투자증권 Open API를 사용하여 KOSPI, KOSDAQ, KONEX를 비롯한 국내 주식과 NYSE, NASDAQ, AMEX 등 미국 주식의 종목 정보를 갱신하고 실시간 체결 및 호가를 수집합니다.
- **주기적 데이터 갱신 (Sync)**:
  - 매일: 일일 시세 및 변동 정보 갱신
  - 매주 일요일/월요일 새벽: 상장 종목 리스트 갱신 등의 대규모 메타 정보 싱크 처리
- **안정적인 DB 저장**: 별도의 Thread와 Queue를 구조화하여 병목 현상 없이 비동기로 SQL 쿼리를 실행 (Data Dequeue). 연 단위로 파티셔닝(Partitioning)된 테이블을 동적으로 생성하여 대용량 데이터를 최적화하여 보관합니다.
- **커스텀 데이터베이스 로거**: 작업 단위별로 로그를 남기며, 이 또한 로그 전용 DB 및 파티셔닝된 테이블에 적재됩니다.

## 📁 파일 구조

```
/workspace/
├── main.py                    # 메인 진입점 — 무한 루프 스케줄러
├── api/                       # API 래퍼 모듈
│   ├── bithumb.py             #   빗썸 REST/WebSocket (가상화폐)
│   └── korea_invest.py        #   한국투자증권 REST/WebSocket (국내·해외 주식)
├── core/                      # 공통 유틸/설정
│   ├── util.py                #   MySqlLogger, safe-parse 유틸
│   └── config.py              #   DB 재시도 설정, 타임아웃 상수
├── scripts/                   # 독립 실행 스크립트
│   └── migrate_db.py          #   레거시 DB → 신규 스키마 마이그레이션
├── docker/                    # Docker 빌드 및 환경 설정
│   ├── Dockerfile             #   컨테이너 이미지 빌드 파일
│   ├── requirements.txt       #   Python 의존성 목록
├── doc/                       # 비공개 설정 (.gitignore 대상)
│   └── settings.py            #   SQL 접속정보, API 키, DB명 상수
├── temp/                      # 런타임 임시 파일 (.gitignore 대상)
└── README.md
```

- **`main.py`**: 가상화폐(`ApiBithumbType`)와 주식(`ApiKoreaInvestType`) 인스턴스를 생성하고, 무한 루프 상태로 시간에 맞춰 Daily, Weekly 정보 업데이트 스레드를 실행. 한국/해외 주식장 오픈 시간(08:00 ~ 16:00) 여부에 따라 수집 시장을 자동 스위칭.
- **`api/bithumb.py`**: 빗썸 REST API로 코인 목록 조회, WebSocket으로 실시간 체결·호가 수신 → MariaDB 적재.
- **`api/korea_invest.py`**: 한국투자증권 API로 국내/해외 종목 마스터 데이터 갱신, WebSocket으로 실시간 체결·호가 수신 → 병렬 큐 저장.
- **`core/util.py`**: `MySqlLogger` (비동기 DB 로거), `TryGetDictStr`, `TryParseInt` 등 안전한 형변환 유틸.
- **`core/config.py`**: SQL 재시도 설정, 타임아웃 상수.
- **`scripts/migrate_db.py`**: 레거시 DB → 신규 스키마 마이그레이션 도구.
- **`doc/settings.py`**: 전역 상수, DB 연결 정보, API Key List.

## 🗄 데이터베이스 구조

Data를 MariaDB를 통해 관리합니다. 각 API 연결 파일에서 데이터가 Insert 되기 전 필요 시 `CREATE TABLE IF NOT EXISTS`를 통해 자동으로 데이터베이스 테이블이 생성되도록 구성되어 있습니다.

- **Bithumb DB / KoreaInvest DB**:
  - `_StockInfo` / `_CoinInfo`: 종목 및 코인의 기본 정보를 담고 있는 테이블. 주기적으로 갱신됨.
  - `{Symbol}_Execution{YYYY}`: 특정 종목(Symbol)의 년도별 실제 거래 체결 데이터가 누적되는 테이블 (Year 기준으로 Partitioning 적용).
  - `{Symbol}_Orderbook{YYYY}`: 호가창 내역.
- **Log DB**: 
  - `Log{YYYY}`: 발생 시간, 파일명, 함수명, 라인 수까지 모두 포함하여 로깅하는 DB입니다.

## ⚙️ 실행 및 설정 방법

1. `doc/settings.py` 내부의 **SQL_HOST, SQL_ID, SQL_PW, KI_API_KEY_LIST**를 본인의 환경과 계정에 맞게 수정하십시오.
2. MariaDB(MySQL) 서버가 켜져 있어야 하며, Python 환경에서 `pymysql`, `requests`, `websocket-client`, `pandas`, `tabulate` 등의 패키지가 설치되어 있어야 합니다.
    ```bash
    pip install pymysql requests websocket-client pandas tabulate
    ```
3. 메인 스크립트를 실행합니다.
    ```bash
    python main.py
    ```
4. Docker로 실행하는 경우:
    ```bash
    docker build -f docker/Dockerfile .
    docker run --env-file docker/env.txt stock-ticker
    ```
5. DB 마이그레이션이 필요한 경우:
    ```bash
    python scripts/migrate_db.py [--dry-run] [--workers N] [--only-coin|--only-stock]
    ```
