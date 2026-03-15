# Project Guidelines — StockTicker

## Overview

실시간 가상화폐(빗썸) 및 국내/해외 주식(한국투자증권) 체결·호가 데이터를 WebSocket으로 수집하여 MariaDB에 저장하는 Python 데이터 수집기입니다.

## Architecture

```
/workspace/
├── main.py                    # 메인 진입점 — 무한 루프 스케줄러
├── api/                       # API 래퍼 모듈
│   ├── bithumb.py             #   빗썸 REST/WebSocket (가상화폐)
│   └── korea_invest.py        #   한국투자증권 REST/WebSocket (국내·해외 주식)
├── core/                      # 공통 유틸/설정
│   ├── util.py                #   MySqlLogger, safe-parse 유틸
│   └── config.py              #   DB 재시도 설정, 타임아웃 상수
│   └── settings.py            #   SQL 접속정보, API 키, DB명 상수
├── scripts/                   # 독립 실행 스크립트
│   └── migrate_db.py          #   레거시 DB → 신규 스키마 마이그레이션
├── docker/                    # Docker 빌드 및 환경 설정
│   ├── Dockerfile             #   컨테이너 이미지 빌드 파일
│   ├── requirements.txt       #   Python 의존성 목록
├── config/                       # 비공개 설정 (.gitignore 대상)
├── temp/                      # 런타임 임시 파일 (.gitignore 대상)
└── README.md
```

- **Threading model**: 각 API 클래스는 WebSocket 수신 스레드 + DB 큐 디큐어 스레드(KI는 8개 병렬)를 운영
- **DB 쓰기**: `queue.Queue` → 별도 스레드에서 dequeue하여 INSERT 실행 (비동기)
- **파티셔닝**: `YEAR(execution_datetime)` 기준 연 단위 파티션, 연도 변경 시 동적 생성
- **스케줄링**: 08:00~16:00 한국장("KR"), 그 외 해외장("EX") 자동 전환; 매일 Daily sync, 일요일/월요일 새벽 Weekly sync

## Code Style

- **Python 3.11** (Dockerfile 기준)
- **네이밍**: 클래스 `PascalCase`, public 메서드 `PascalCase`, private 멤버/메서드 `__snake_case` (name-mangling)
- **import 순서**: 표준 라이브러리 → 외부 패키지 → 프로젝트 내부 (`from core import config`, `from core import util`, `from core import settings`)
- **DateTime alias**: `from datetime import datetime as DateTime, timedelta as TimeDelta`
- **로깅**: `util.InsertLog("모듈명", "E"/"N"/"W", "메시지")` — DB 기반 비동기 로그 (print 대신 사용)
- **한국어 주석/문서** 사용

## Build and Test

```bash
# 의존성 설치
pip install -r docker/requirements.txt

# 실행 (config/settings.json 설정 필요 — SQL_HOST, SQL_ID, SQL_PW, KI_API_KEY_LIST)
python main.py

# DB 마이그레이션 (레거시 → 신규 스키마)
python scripts/migrate_db.py [--dry-run] [--workers N] [--only-coin|--only-stock]

# tick PK 순서 보정 스크립트
python scripts/alter_tick_pk_order.py [--dry-run] [--table cBTC]

# YEARWEEK 파티션 → YEAR 파티션 재구성
python scripts/repartition_yearweek_to_year.py [--dry-run] [--only-tick|--only-candle]

# Docker
docker build -t stock-ticker -f docker/Dockerfile .
docker run --env-file docker/env.txt stock-ticker
```

테스트 스위트는 아직 없습니다.

## Conventions

### DB 패턴
- 테이블은 `CREATE TABLE IF NOT EXISTS`로 자동 생성
- tick 데이터: `tick.c{CODE}` (코인), `tick.s{CODE}` (주식) — YEARWEEK 파티션
- candle 데이터: `candle.c{CODE}`, `candle.s{CODE}` — 10분봉, ON DUPLICATE KEY UPDATE
- info 테이블: `coin_info`, `stock_info` — 마스터 데이터
- SQL 파라미터: 데이터 값은 반드시 `cursor.execute(query, params)` 사용

### 에러 처리
- DB 에러 재시도: `config.is_retryable_error(ex)` 사용 (2003, 2006, 2013, 1213, 1205)
- 지수 백오프: `2^attempt` 초, 최대 30초, 최대 5회

### 로깅/출력 규칙
- 런타임 수집 모듈(`api/*`, `main.py`)에서는 `util.InsertLog("모듈명", "E"/"N"/"W", "메시지")` 사용
- 운영/마이그레이션 스크립트(`scripts/*`)는 콘솔 `print` 출력 허용

### 주의사항
- `config/settings.json`은 `.gitignore` 대상이므로 별도 생성 필요 (SQL 접속정보, API 키)
- 동적 테이블명은 f-string으로 구성되므로, 외부 입력이 테이블명에 들어가지 않도록 주의
- WebSocket 연결 끊김 시 자동 재연결 루프 존재 — 로그 폭증 가능성 확인
- `main.py`는 루프 내부에서 `except: pass`를 사용하므로, 신규 코드 추가 시 예외가 묵살되지 않도록 `util.InsertLog`로 실패 원인을 남길 것
