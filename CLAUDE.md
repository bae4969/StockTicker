# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

# 프로젝트: StockTicker

빗썸(가상화폐)·한국투자증권(국내·미국 주식)의 실시간 체결·호가 데이터를 WebSocket으로 수집해 MariaDB에 적재하는 Python 데이터 수집기. 단일 장기 실행 프로세스(`main.py`)가 두 API 클라이언트를 무한 루프로 스케줄링한다.

## 자주 쓰는 명령

```bash
# 의존성 설치
pip install -r docker/requirements.txt

# 실행 (config/settings.json 필요 — 없으면 core/config.py에서 FileNotFoundError)
python main.py

# DB 마이그레이션 / 보정 스크립트 (모두 --dry-run 지원)
python scripts/migrate_db.py [--workers N] [--only-coin|--only-stock]
python scripts/alter_tick_pk_order.py [--table cBTC]
python scripts/repartition_yearweek_to_year.py [--only-tick|--only-candle]

# Docker
docker build -t stock-ticker -f docker/Dockerfile .
docker run --env-file docker/env.txt stock-ticker
```

테스트 스위트는 없다. 수동 확인은 MariaDB 로그 테이블(`stock_ticker_log`)과 `print` 출력으로 한다.

## 아키텍처 개요

전체 흐름은 `main.py` → 두 API 인스턴스(`ApiBithumbType`, `ApiKoreaInvestType`) → 각각의 WebSocket·SQL 클라이언트로 분기되며, **단일 진입점 / 두 도메인 / 동일 내부 패턴**이 핵심이다.

```
main.py (무한 루프 스케줄러, 8~16시는 "KR", 그 외는 "EX")
├─ ApiBithumbType   (api/bithumb/__init__.py)
│   ├─ BithumbRestClient   (_rest.py)        : 빗썸 REST — 코인 목록·시세
│   ├─ BithumbSqlClient    (_sql.py)         : 큐 + dequeue 스레드 → MariaDB
│   ├─ BithumbWsClient     (_websocket.py)   : 실시간 체결·호가 WebSocket
│   └─ _tables.py                            : CREATE TABLE IF NOT EXISTS + enqueue 헬퍼
└─ ApiKoreaInvestType (api/korea_invest/__init__.py)
    ├─ KoreaInvestRestClient (_rest.py)      : 한투 REST — 토큰·종목·시세
    ├─ KoreaInvestSqlClient  (_sql.py)       : 큐 + dequeue (병렬 워커)
    ├─ KoreaInvestWsClient   (_websocket.py) : 실시간 체결·호가 (구독 한도 처리)
    ├─ _master.py                            : KOSPI/KOSDAQ/KONEX/NASDAQ/NYSE/AMEX 마스터 다운로드
    ├─ _token_storage.py                     : 발급 토큰 영속화 (config/last_token_info.json)
    └─ _tables.py
```

**핵심 설계 패턴**:

- **각 API는 자체 SQL 클라이언트를 소유**한다 — 별도 DB 큐와 dequeue 스레드 분리. `util.MySqlLogger`(로그 DB 전용)와 다른 인스턴스. 셋(`bh.__sql`, `ki.__sql`, `util.logger_obj`) 모두 서로 다른 DB 접속 풀.
- **모든 DB INSERT는 비동기** — REST/WS 핸들러는 `enqueue_*` 헬퍼로 큐에 넣기만 하고 반환. dequeue 스레드가 실제 INSERT를 수행.
- **테이블은 런타임에 생성** — `_tables.py`가 `CREATE TABLE IF NOT EXISTS ... PARTITION BY RANGE (YEAR(execution_datetime))`로 자동 생성. 매년 `SyncPartitions()`에서 차년도 파티션을 미리 만들어 둠.
- **이름 매핑이 도메인마다 다름** — 빗썸: `coin_execution_*`, `coin_orderbook_*`, `coin_info`. 한투: `stock_execution_*`, `stock_orderbook_*`, `stock_info`. 한 클래스가 두 도메인을 다루지 않는다.
- **한투 WebSocket은 API 키 한 개당 구독 한도(=40)** 가 있어 `KI_API_KEY_LIST`를 여러 개 받아 라운드로빈 분배한다. REST 호출도 키당 호출 한도가 있어 `_rest.py`의 `MAX_REST_API_COUNT_PER_KEY` 기반 delay 계산이 들어간다.
- **스케줄링은 `main.py`의 시계 비교**가 전부다. 별도 스케줄러 라이브러리 없음. 매일 `SyncDailyInfo`(시장 전환 시), 매주 일요일 새벽 `SyncWeeklyInfo`(종목 마스터 재다운로드).

## 설정 로딩

`core/config.py`가 `config/settings.json`(레포 외부 — `.gitignore` 대상)을 import 시점에 읽고 모듈 상수로 노출한다. **파일이 없으면 import 자체가 `FileNotFoundError`로 실패**한다. 신규 환경 셋업 시 `config.example/settings.json`을 복사해 채울 것. JSON 키 누락도 `KeyError`로 즉시 실패한다(의도된 동작 — fail-fast).

## 코드 컨벤션 (이 레포 고유)

- **Python 3.11** (Dockerfile 기준). 외부 패키지는 `docker/requirements.txt`로 고정.
- **네이밍**: 클래스 `PascalCase`(접미사 `Type` — `ApiBithumbType`, `ApiKoreaInvestType`), public 메서드 `PascalCase`(`SyncDailyInfo`, `StopCollecting`), private 메서드/속성 `__snake_case`(name-mangling 사용). 이 혼합 스타일은 의도된 것 — 변경하지 말 것.
- **DateTime alias**: 모든 파일이 `from datetime import datetime as DateTime, timedelta as TimeDelta` 형태로 import. 새 코드도 동일 alias 사용.
- **로깅 분기**:
  - `api/*`, `main.py` (런타임 수집 경로) → **`util.InsertLog("모듈명", "E"|"N"|"W", "메시지")`** 사용. `print` 금지. 이 로거는 호출 위치(file/line/function)를 `inspect.stack()`으로 자동 캡처.
  - `scripts/*` (운영·마이그레이션) → `print` 허용. `util.InsertLog` 호출하지 않음 (로그 DB 의존성 없이 단독 실행 가능해야).
- **SQL 파라미터화**: 값 바인딩은 반드시 `cursor.execute(query, params)` 형태. 단, **테이블명·파티션 이름은 f-string으로 직접 조립**한다 (외부 입력이 절대 들어가지 않는 코드 경로). 새 INSERT 추가 시 같은 규약 유지.
- **DB 재시도**: `config.is_retryable_error(ex)`로 분기. 재시도 대상 에러 코드는 `(2003, 2006, 2013, 1213, 1205)`. 지수 백오프 `min(2**attempt, 30)`초, 최대 5회.
- **세션 타임아웃**: 새 커넥션을 만들 때 `config.set_session_timeouts(conn)` 호출 — `net_read_timeout` / `net_write_timeout` / `wait_timeout`을 3600초로 설정.

## 함정 / 운영 메모

- `main.py` 메인 루프는 `except Exception as ex: util.InsertLog("Main", "E", ...)`로 감싸 있다. 신규 코드가 예외를 던지면 **루프는 계속 돌고, 원인은 로그 DB에만 남는다**. 로컬 콘솔만 보면 조용히 실패한 것처럼 보이므로 `stock_ticker_log` 테이블 확인 필수.
- WebSocket 끊김 자동 재연결 루프가 있다. 잘못 만지면 로그 폭증 가능. 재연결 backoff 변경 시 `_websocket.py`의 fail_count 증가 로직 확인 — 과거 KIS WS IP ban 사건(memory: project_kis_ws_incident)이 이 카운터 누락에서 시작됨.
- `temp/` 디렉토리는 **두 용도가 겹친다**: (1) 멀티에이전트 워크플로우 산출물(`plan.md`/`progress.md`/`report.md`/`input/`/`output/`), (2) `_master.py`가 stock master 파일을 다운로드하는 작업 디렉토리(`__sync_stock_info_table` 시작 시 `glob('./temp/*')`로 비움 — 워크플로우 파일을 지울 위험 있음). 새 코드가 `temp/` 하위에 쓰는 경우 충돌 가능성 점검.
- `config/last_token_info.json`은 한투 토큰 캐시. 24시간 토큰을 재발급 횟수 줄이려고 디스크에 보관. 삭제해도 자동 재발급되지만 잠시 동안 호출 한도 소모.
- 마이그레이션 스크립트는 모두 `--dry-run` 우선 검증을 가정하고 만들어졌다. PR/실행 전 `--dry-run`으로 영향 테이블 수·SQL 출력 확인.

## README와의 차이

`README.md`는 일부 오래된 정보를 가짐 — 예: `core/settings.py`는 더 이상 존재하지 않고 `core/config.py`가 `settings.json`을 직접 로드한다. 의심스러우면 코드를 신뢰. 최근 변경은 `git log` 참조 (예: "api 모듈을 패키지로 분리하고 설정 로딩을 JSON 단일 경로로 통합").

---

# 멀티에이전트 베이스 — lite/full 듀얼 모드 (Claude only)

이 작업 디렉토리는 **기본 lite 모드(메인이 직접 Edit/Write → critic 검증)**로 동작하며, `/full` 슬래시 커맨드 사용 시에만 **4단계(서칭→기획→구현→검증)**로 진입하는 멀티에이전트 베이스다. lite에서는 메인이 외과적으로 직접 수정한 뒤 `critic`만 호출해 결과를 검증한다. `/full` 시에는 메인이 [`.claude/rules/main_full_procedure.md`](.claude/rules/main_full_procedure.md)를 따라 researcher/planner/coder/critic을 차례로 호출하며, 절차 본문은 [`.claude/agents/orchestrator.md`](.claude/agents/orchestrator.md)에 보존된다 (서브에이전트가 아닌 참조 절차서). 외부 CLI·다른 모델에 의존하지 않는 Claude only 구성이다.

## 모든 세션 시작 시 — 절대 진입 절차

1. **`.claude/state/current.json` 먼저 확인** (워크플로우 재개 여부 판단).
   - `status`가 `searching` / `planning` / `awaiting_approval` / `approved` / `implementing` / `verifying` → 사용자에게 재개·재계획·취소 옵션 질의(AskUserQuestion) 후 지시 대기.
   - `status`가 `blocked` → 차단 사유 표시 후 AskUserQuestion으로 "계획 수정 / 단계 건너뛰기 / 취소" 옵션 질의.
   - `status`가 `done` / `aborted` / 비어 있음 → 신규 요청으로 처리.
2. 신규 요청 처리 시 **모드 결정**:
   - 요청이 `/full`로 시작하면 → `mode=full`, 메인이 [`main_full_procedure`](.claude/rules/main_full_procedure.md)를 따라 4단계(researcher→planner→coder→critic) 실행.
   - `/full` prefix 없으면 → `mode=lite`, 메인이 직접 Edit/Write로 외과적 수정 후 `critic` 서브에이전트만 호출.
   - lite vs full 모드 결정은 `/full` slash prefix 매칭으로만 한다(다른 slash command — `/draft`, `/init` 등 — 은 자체 동작을 가지며 lite/full 모드 자체를 바꾸지 않는다). LLM이 "이 요청은 간단해 보인다" 같은 휴리스틱으로 모드를 바꾸는 것 금지.
3. lite 작업도 비단순 요청이면 본 사이클을 따른다. 단순 조회·typo는 메인이 즉시 처리(workflow_4stage.md "음성 예시" 참조).

## 모드별 사이클

### Lite 모드 (기본, `/full` prefix 없을 때)

```
유저 요청
  ↓
[수정]    메인이 직접 Edit/Write로 외과적 수정
  ↓
[검증]    critic 서브에이전트
   ├─ Pass + recommend_full:false → 메인이 한 줄 보고 → 종료
   ├─ Pass + recommend_full:true  → 메인이 보고 + 사용자에게 /full 재실행 안내
   └─ Fail → 메인이 critic 사유 받아 **자동 재수정 1회** → critic 재호출
        ├─ 재호출도 Fail → AskUser(자동 재구현 한번 더 / /full로 승격 / 중단)
        └─ 재호출 Pass → 종료
```

- 재시도 카운터·`blocked` 상태는 lite에서 쓰지 않는다 (full만 사용).
- `state.json`은 lite에서 생성·갱신하지 않는다. lite 종료 시에만 메인이 `history/<run_id>/run.json`을 작성한다 (스키마 동일).
- `temp/plan.md`는 lite에서 만들지 않는다. `temp/progress.md`·`temp/report.md`는 메인이 직접 작성.

### Full 모드 (`/full` prefix 사용 시)

```
# 절차서: main_full_procedure.md, 본문: orchestrator.md (비활성 참조)
/full 유저 요청
  ↓ (메인이 main_full_procedure 진입, mode=full)
[1 서칭]   메인 ─Agent→ researcher    → AskUser(범위 확정/추가 서칭)
  ↓
[2 기획]   메인 ─Agent→ planner       → AskUser(승인 / 수정 / 취소)  ← 승인 없이는 구현 진입 금지
  ↓
[3 구현]   메인 ─Agent→ coder         → Edit/Write로 파일 직접 변경, Bash로 자체 검증
  ↓
[4 검증]   메인 ─Agent→ critic
   ├─ Pass → REPORT → state.status=done, 종료
   └─ Fail → state.retry_count += 1
        ├─ retry_count < 3 : AskUser(자동 재구현 / 방향 변경 / 중단) → [3]로 복귀
        └─ retry_count == 3: 루프 중단, state.status=blocked
                              AskUser(계획 수정 / 단계 건너뛰기 / 취소)
```

매 단계 전이마다 `.claude/state/current.json`을 갱신해 세션이 끊겨도 재개 가능.

## 구성 — 5개 서브에이전트 (full 전용 4개 + lite/full 공용 1개)

| 에이전트 | 도구 | 역할 | 사용 모드 | 권한 |
|---|---|---|---|---|
| [orchestrator](.claude/agents/orchestrator.md) | Read/Grep/Glob/Agent/TodoWrite/Edit/Write/AskUserQuestion | 4단계 순서·분배·상태 관리 | **참조 절차서**(비활성 — 메인이 호출하지 않음, 절차 본문 SoT 보존용) | 읽기 + Agent 호출 + state·메모리 **메타데이터만 Edit/Write** |
| [researcher](.claude/agents/researcher.md) | Read/Grep/Glob/WebSearch/WebFetch | 코드·웹·MCP 조사 | **full 전용** | 읽기 전용 + WebSearch/WebFetch/MCP |
| [planner](.claude/agents/planner.md) | Read/Grep/Glob | 설계·작업 분해·승인 카드 작성 | **full 전용** | 읽기 전용 |
| [coder](.claude/agents/coder.md) | Read/Bash/Glob/Grep/**Edit/Write/NotebookEdit** | 코드·파일 변경(처리), 자체 검증 | **full 전용** | full 모드에서 Edit/Write를 가진 유일한 서브에이전트 |
| [critic](.claude/agents/critic.md) | Read/Grep/Glob/Bash | 산출물 검증·테스트·요구사항 일치 판정 | **lite·full 공용** | 읽기 + Bash(테스트 실행) |

격리 메커니즘:
- **full 모드**: Edit/Write는 `coder` 서브에이전트에만 부여. researcher/planner/critic은 도구 자체가 없어 구조적으로 코드 편집 불가. 메인은 단계 전이용 메타데이터(`.claude/state/**`, `temp/**` 등) 화이트리스트만 쓰고, 코드 파일은 손대지 않는다.
- **lite 모드**: 메인 에이전트가 자기 Edit/Write로 직접 외과적 수정. 자연어 규약상 lite 외 상황에서는 메인이 Edit/Write를 코드에 직접 쓰지 않는다(검증·재현 등으로 필요할 때 예외). critic이 사후에 "범위 외 변경" 사유로 잡아내는 게 안전망.

자세한 규약은 [.claude/rules/coding_principles.md](.claude/rules/coding_principles.md)와 각 에이전트 정의 파일 참조.

## temp/ 폴더 규약

`temp/` 디렉토리는 워크플로우 산출물과 사용자 자료의 임시 작업 공간이다.

- `temp/plan.md`는 Phase 2 기획서 본문이다. 매 run 덮어쓴다.
- `temp/progress.md`는 Phase 3/4 진행 상황이다. run 시작 시 초기화 후 append한다.
- `temp/report.md`는 Phase 5 완료 보고 본문이다. 매 run 덮어쓰며, 영속본은 `.claude/state/history/<run_id>/run.json`의 `report_body` 필드에 흡수한다 (도구 가드가 `report.md` Write를 차단하므로 별도 파일을 만들지 않음).
- `temp/input/`은 사용자 자료실이다. 사용자가 `input/foo.md`처럼 언급하면 Claude는 `temp/input/foo.md`를 참조한다.
- `temp/output/`은 산출물 위치다. 사용자가 보고서/문서를 요청하면 Claude가 이곳에 작성한다.
- 채팅에는 마크다운 링크와 AskUserQuestion 카드만 띄운다. 본문은 항상 파일에 작성한다.
- `.gitignore`는 수정하지 않는다.

## 코딩 행동 규칙 (모든 단계 공통)

모든 코드 변경 — 특히 coder가 Edit/Write로 만드는 모든 변경 — 에는 [.claude/rules/coding_principles.md](.claude/rules/coding_principles.md)의 4원칙이 적용된다:

1. **생각하고 코딩하기** — 가정 명시, 해석 갈리면 옵션 제시, 모호하면 멈추고 질문.
2. **단순함 우선** — 요청되지 않은 기능·추상화·유연성 금지. 200줄→50줄 가능하면 다시 쓰기.
3. **외과적 변경** — 인접 코드 손대지 않기, 무관한 dead code는 언급만 (삭제 X), 변경된 모든 줄이 요청에 직접 트레이스되어야.
4. **목표 주도 실행** — 검증 가능한 성공 기준 정의, 다단계는 "단계 → 검증" 형태로 분해.

coder는 매 변경 보고 말미에 "행동 4원칙 자기 점검" 한두 줄을 적는다. critic은 위반을 결함(Defect) 사유로 명시한다.

## 호출 흐름 (DAG, 순환 금지)

### Lite 모드 (기본)

```
메인 ─Edit/Write→ (워크스페이스 변경)
  │
  └→ critic  (검증)
       ├─ Pass → 메인이 한 줄 보고 (+ recommend_full true면 /full 재실행 안내)
       └─ Fail → 메인이 critic 사유로 자동 재수정 1회 → critic 재호출
                  ├─ Pass → 종료
                  └─ Fail → AskUser(자동 재구현 한번 더 / /full로 승격 / 중단)
```

### Full 모드 (`/full` prefix)

```
# 절차서: main_full_procedure.md, 본문: orchestrator.md (비활성 참조)
메인 ─Agent→ researcher / planner / coder / critic  (차례로 호출, 단계 사이마다 AskUser·state 갱신)
              │           │         │       │
              │           │         │       └─ Pass/Fail verdict 반환
              │           │         └─ Edit/Write로 워크스페이스 변경 + Bash 자체 검증
              │           └─ 변경 명세 + 단계 분해 반환
              └─ 발견 사항 구조화 반환

검증 Fail 시: 메인 → AskUser → (재구현이면) coder → critic ...
```

- 서브에이전트끼리 직접 통신 금지. full에서는 모든 핸드오프가 메인을 거치고, lite에서는 메인이 critic만 직접 호출한다.
- full에서 메인은 main_full_procedure 절차에 따라 researcher/planner/coder/critic 4개를 차례로 호출한다. lite에서는 메인이 critic만 호출한다.

## AskUserQuestion 사용 원칙

사용자 응답이 필요한 **모든 분기**에서 AskUserQuestion으로 카드를 띄운다. 텍스트로 "Q1: ..., Q2: ..." 나열 금지. 한 호출에 최대 4개 질문. 카드를 띄우는 대표 시점:

- (full) 서칭 종료 후: 발견 사항 요약 + "기획 진입 / 추가 서칭 / 범위 변경"
- (full) 기획 종료 후: 계획 브리핑 + "승인 / 수정 / 취소" — **승인 없이는 구현 진입 금지**
- (full) 검증 1·2차 실패 후: 실패 사유 + "자동 재구현 / 방향 변경 / 중단"
- (full) 검증 3회째 실패 후: blocked 상태 + "계획 수정 / 단계 건너뛰기 / 취소"
- (lite) 자동 재수정 후에도 critic Fail: "자동 재구현 한번 더 / /full로 승격 / 중단"
- 모호한 요구사항 발견 시: 옵션 카드로 결정 받기

## 사용법

1. 작업할 디렉토리에서 세션 시작 (subagent 디스커버리는 cwd의 `.claude/agents/`만 본다).
2. 베이스를 다른 작업 디렉토리에 가져갈 때:
   - `.claude/` 전체 + `CLAUDE.md` 복사
   - Claude Code만 있으면 동작 (외부 CLI·인증 불필요)
3. **기본 = lite 모드**: 요청을 그냥 입력하면 메인이 직접 외과적으로 수정한 뒤 critic 1회 검증.
4. **full 모드**: 요청 앞에 `/full`을 붙이면 researcher→planner→coder→critic 4단계로 처리. (자세한 내용: [.claude/commands/full.md](.claude/commands/full.md))
5. 단순 조회·typo 수정 같은 1줄 작업은 메인이 critic 호출 없이 직접 처리 (workflow_4stage.md "음성 예시" 참조).
6. **슬래시 커맨드 인덱스**:
   - [`/full`](.claude/commands/full.md) — lite → full 4단계 사이클 전환
   - [`/draft`](.claude/commands/draft.md) — 모호한 요청을 라운드 대화로 다듬어 한 줄 요청으로 조립
   - [`/init`](.claude/commands/init.md) — 새 프로젝트 진입 시 의도 파악 + memory/rules 기록
   - [`/export-team`](.claude/commands/export-team.md) — 베이스 초기상태를 타임스탬프 폴더로 export
   - [`/import-team`](.claude/commands/import-team.md) — export-team 패키지를 현 베이스에 흡수

## 변경 시 원칙

- 서브에이전트는 4개(researcher/planner/coder/critic)로 고정. orchestrator.md는 참조 절차서로 보존(서브에이전트로 호출하지 않음). 새 역할이 필요해도 같은 권한의 에이전트를 이름만 바꿔 늘리지 않는다.
- 읽기 전용과 쓰기 가능 권한을 항상 분리한다. **full 모드에서 Edit/Write는 `coder`에만 부여**한다 (메인의 full 모드 메타데이터 화이트리스트 예외 + lite 모드의 메인 직접 코드 편집 예외). 다른 서브에이전트(researcher/planner/critic)에 Edit/Write를 추가하지 않는다 — IMPLEMENT 격리의 핵심.
- 재시도 카운터 임계값은 **full만 사용**(3회). lite는 자동 재수정 1회 + 사용자 카드 1회로 고정. full 임계값을 바꾸려면 `workflow_4stage.md`, 본 파일, `.claude/agents/orchestrator.md`, `.claude/rules/main_full_procedure.md` 네 곳을 함께 갱신.
- 모드 분기 로직(슬래시 prefix 매칭)은 main_full_procedure.md와 CLAUDE.md 두 곳에 명시. 둘 다 함께 갱신.
- lite·full 모두 history write 책임자는 **메인** (full에서는 main_full_procedure 절차에 따라, 동일 스키마). 변경 시 `state/README.md` "쓰기 책임" 절도 함께 갱신.
