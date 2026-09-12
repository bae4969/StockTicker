# 이미지 기반 운영 배포

모든 CI/CD 계산은 self-hosted GitHub Actions Runner(UbuntuVM)에서 한다. GitHub 호스팅
Runner 와 Actions artifact 저장소는 쓰지 않는다.

## ⚠️ 이 서비스는 "머지=배포"가 아니다

재시작 한 번이 **2~4분치 체결 데이터 영구 손실**이다(재수집 경로가 없다). 그래서
파이프라인을 두 토막으로 나눴다.

| 언제 | 무엇이 도나 | 수집 영향 |
|---|---|---|
| 토픽 브랜치(`release/*`·`fix/*`·`chore/*`) push | `quality-check` (문법·compose·이미지 빌드·이미지 내용 검사) | 없음 |
| `main` push (=PR 머지) | 릴리스 생성 + 이미지 빌드·레지스트리 push | **없음** — 운영은 옛 이미지로 계속 돈다 |
| Actions → Run workflow (수동) | `apply` — TrueNAS 앱에 새 digest 적용 | **재시작 1회 (2~4분 공백)** |
| `dev` push | 아무것도 안 함 | 없음 |

즉 **언제 반영할지는 사람이 고른다.** 국내장·미국장이 모두 한산한 창에서 누른다.

## 흐름

1. VM 이 `docker/Dockerfile` 로 `bae-stock-ticker:vX.Y.Z` 이미지를 만든다(베이스는 digest 고정).
2. 이미지를 NAS 의 `bae-registry` 로 push 하고 SHA-256 digest 를 확정한다.
3. (수동 트리거) `truenas/bae-stock-ticker.yml` 을 제한 SSH 연결의 stdin 으로 보낸다.
4. NAS 의 강제 명령 스크립트가 이미지·마운트·네트워크·로그 설정을 allowlist 로 검사한다.
5. 버전과 digest 로 고정한 YAML 을 TrueNAS `bae-stock-ticker` 앱에 적용하고,
   새 컨테이너가 뜬 뒤 죽지 않는지(`DEPLOY_START_GRACE` 초)와 `SUBSCRIBE SUCCESS` 건수가
   더 늘지 않을 때까지를 확인한다(전 종목이면 288건 안팎).

## 영속 데이터

코드는 이미지 안에만 있고, 운영 컨테이너는 코드 디렉터리를 마운트하지 않는다.

- 설정·토큰 캐시: `/mnt/nvme/90.service/stockticker_data/config/`
  (`settings.json`, `last_token_info.json`)
- 적용된 compose: `/mnt/nvme/90.service/stockticker_data/compose.yml`
- 공유 데이터: `/mnt/nvme/90.service/share_data` → `/mnt/share_data`

최초 전환 때 강제 명령 스크립트가 옛 바인드 마운트 경로의 `settings.json`·
`last_token_info.json` 을 데이터 디렉터리로 복사하고 `.image-cutover-complete` 마커를
남긴다. **토큰 캐시를 그대로 가져가는 것이 중요하다** — 새로 발급하면 키 10개의 발급이
한꺼번에 몰려 과거 IP ban 경로와 겹친다.

## NAS 에만 있는 보호 설정

저장소에는 주소·계정을 넣지 않는다(공개 저장소). 러너 → NAS 주소는 저장소 변수
`DEPLOY_SSH_TARGET` 로 받는다. `nas/deploy-stockticker-image.sh` 는 **검토용 원본**이며,
실제 forced-command 경로(`$HOME/bin/`)에는 운영자가 직접 복사한다 — `10.project` 는
러너에 rw NFS 라 거기 두면 `command=` 제한이 무의미해진다.

## 롤백

각 적용 직전 TrueNAS 앱 설정이 `compose.previous.yml` 에 보존된다. 다만 **롤백 조건은
일부러 좁다** — 롤백도 재시작이라 공백이 한 번 더 생기기 때문이다.

| 상태 | 스크립트 동작 |
|---|---|
| 이미지 pull·앱 갱신 실패, 새 컨테이너가 안 뜸, 뜬 뒤 크래시 | **자동 롤백** |
| 컨테이너는 떠 있는데 재구독 로그가 안 옴 | **경고만** — 판단은 사람이 한다 |

수동 롤백은 이전 버전 태그로 `apply` 를 다시 돌린다(재시작 1회 추가).
