#!/usr/bin/env bash
# GitHub Actions 의 제한 SSH 키가 호출하는 운영 배포 진입점이다.
# 저장소의 compose 템플릿을 stdin 으로 받아 고정된 bae-stock-ticker 앱에만 적용한다.
#
# ⚠️ 이 저장소의 파일은 **검토용 원본**이다. 실제 forced-command 경로
#    (`$HOME/bin/deploy-stockticker-image.sh`)에는 운영자가 직접 복사한다.
#    `/mnt/nvme/10.project` 는 러너에 rw NFS 라 거기 두면 `command=` 제한이 무의미해진다.
#
# ⚠️ 이 서비스는 재시작 = 체결 데이터 영구 손실이다. 그래서 롤백 조건을 일부러 좁게 잡았다.
#    "컨테이너가 뜬 채로 조용하다"는 롤백하지 않는다 — 롤백은 재시작을 한 번 더 부르기 때문이다.
#    기동 자체가 실패(크래시 루프)할 때만 되돌린다.

set -euo pipefail

APP_NAME="bae-stock-ticker"
CONTAINER_NAME="bae-stock-ticker"
DATA_DIR="/mnt/nvme/90.service/stockticker_data"
LEGACY_DIR="/mnt/nvme/10.project/23.stock_ticker"
COMPOSE_FILE="$DATA_DIR/compose.yml"
PREVIOUS_COMPOSE="$DATA_DIR/compose.previous.yml"
CUTOVER_MARKER="$DATA_DIR/.image-cutover-complete"
REGISTRY="127.0.0.1:5000"
# 기동 판정: running 이 된 뒤 이 시간 동안 죽지 않으면 성공으로 본다.
START_GRACE="${DEPLOY_START_GRACE:-60}"
# 재구독 로그를 기다리는 시간. 안 와도 실패로 보지 않는다(위 주석 참조).
SUBSCRIBE_TIMEOUT="${DEPLOY_SUBSCRIBE_TIMEOUT:-300}"

app_state() {
    sudo -n midclt call app.query "[[\"id\",\"=\",\"$APP_NAME\"]]" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["state"])'
}

ensure_app_started() {
    local state
    state=$(app_state)
    if [[ "$state" == "STOPPED" ]]; then
        echo "TrueNAS 앱 시작: $APP_NAME"
        sudo -n midclt call -j app.start "$APP_NAME" >/dev/null
    fi
}

original_command="${SSH_ORIGINAL_COMMAND:-}"
if [[ -z "$original_command" && "$#" -gt 0 ]]; then
    original_command="$*"
fi
read -r action version digest extra <<< "$original_command"

if [[ "$action" != "deploy" || -n "${extra:-}" ]]; then
    echo "사용법: deploy v<major>.<minor>.<patch> sha256:<digest>" >&2
    exit 2
fi
if [[ ! "${version:-}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "허용되지 않은 버전: ${version:-없음}" >&2
    exit 2
fi
if [[ ! "${digest:-}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "허용되지 않은 이미지 digest: ${digest:-없음}" >&2
    exit 2
fi

template_file=$(mktemp /tmp/bae-stock-ticker-compose.incoming.XXXXXX)
rendered_file=$(mktemp /tmp/bae-stock-ticker-compose.rendered.XXXXXX)
previous_file=$(mktemp /tmp/bae-stock-ticker-compose.previous.XXXXXX)
# trap 에서 간접 호출한다.
# shellcheck disable=SC2329
cleanup() {
    rm -f "$template_file" "$rendered_file" "$previous_file"
}
trap cleanup EXIT

# 비정상적으로 큰 입력은 compose 가 아니라 잘못된 전송으로 본다.
head -c 65537 > "$template_file"
if [[ $(wc -c < "$template_file") -gt 65536 ]]; then
    echo "compose 입력이 64KiB를 넘는다" >&2
    exit 4
fi

VERSION="$version" DIGEST="$digest" \
    python3 - "$template_file" "$rendered_file" <<'PY'
import os
import pathlib
import sys

import yaml

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
version = os.environ["VERSION"]
digest = os.environ["DIGEST"]

text = source.read_text(encoding="utf-8")
if text.count("__VERSION__") != 1 or text.count("__DIGEST__") != 1:
    raise SystemExit("compose 템플릿 자리표시자가 올바르지 않다")
text = text.replace("__VERSION__", version).replace("__DIGEST__", digest)
config = yaml.safe_load(text)

if set(config or {}) - {"networks", "services"}:
    raise SystemExit("허용되지 않은 compose 최상위 항목")
services = config.get("services") or {}
if set(services) != {"bae-stock-ticker"}:
    raise SystemExit("bae-stock-ticker 단일 서비스만 허용한다")
service = services["bae-stock-ticker"]

allowed_service_keys = {
    "image", "container_name", "command", "working_dir", "user", "environment",
    "volumes", "networks", "restart", "deploy", "logging",
}
if set(service) != allowed_service_keys:
    raise SystemExit("허용되지 않았거나 빠진 서비스 설정")

expected = {
    "image": f"127.0.0.1:5000/bae-stock-ticker:{version}@{digest}",
    "container_name": "bae-stock-ticker",
    "command": "python3 main.py",
    "working_dir": "/workspace",
    "user": "1000:3000",
    "environment": {"TZ": "Asia/Seoul"},
    "volumes": [
        "/mnt/nvme/90.service/stockticker_data/config:/workspace/config",
        "/mnt/nvme/90.service/share_data:/mnt/share_data",
    ],
    "networks": ["db_bridge"],
    "restart": "unless-stopped",
}
for key, value in expected.items():
    if service.get(key) != value:
        raise SystemExit(f"허용되지 않은 {key} 설정")

expected_deploy = {"resources": {"limits": {"cpus": "4", "memory": "4G"}}}
if service.get("deploy") != expected_deploy:
    raise SystemExit("허용되지 않은 자원 제한")

# mode: non-blocking 이 빠지면 logsink 지연이 수집 스레드를 멈춘다.
expected_logging = {
    "driver": "syslog",
    "options": {
        "max-buffer-size": "4m",
        "mode": "non-blocking",
        "syslog-address": "udp://127.0.0.1:5514",
        "syslog-format": "rfc5424micro",
        "tag": "{{.Name}}",
    },
}
if service.get("logging") != expected_logging:
    raise SystemExit("허용되지 않은 로그 설정")
if config.get("networks") != {"db_bridge": {"external": True}}:
    raise SystemExit("db_bridge 외 네트워크는 허용하지 않는다")

target.write_text(text, encoding="utf-8")
PY

image="$REGISTRY/bae-stock-ticker:$version@$digest"
echo "이미지 받기: $image"
docker pull "$image"

current_config=$(sudo -n midclt call app.config "$APP_NAME")
CURRENT_CONFIG="$current_config" python3 - "$previous_file" <<'PY'
import json
import os
import pathlib
import sys

import yaml

config = json.loads(os.environ["CURRENT_CONFIG"])
pathlib.Path(sys.argv[1]).write_text(
    yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
    encoding="utf-8",
)
PY
sudo -n install -o 1000 -g 3000 -m 640 "$previous_file" "$PREVIOUS_COMPOSE"

# 최초 전환: 바인드 마운트로 쓰던 설정·토큰 캐시를 데이터 디렉토리로 옮긴다.
# 토큰 캐시를 그대로 가져가야 재기동 때 키 10개를 새로 발급하지 않는다(IP ban 경로).
first_cutover=false
if ! sudo -n test -f "$CUTOVER_MARKER"; then
    if [[ ! -r "$LEGACY_DIR/config/settings.json" ]]; then
        echo "기존 설정을 읽을 수 없다: $LEGACY_DIR/config/settings.json" >&2
        exit 5
    fi
    echo "최초 전환: 설정·토큰 캐시를 $DATA_DIR/config 로 복사한다"
    sudo -n install -d -o 1000 -g 3000 -m 750 "$DATA_DIR/config"
    sudo -n install -o 1000 -g 3000 -m 640 \
        "$LEGACY_DIR/config/settings.json" "$DATA_DIR/config/settings.json"
    if [[ -r "$LEGACY_DIR/config/last_token_info.json" ]]; then
        sudo -n install -o 1000 -g 3000 -m 640 \
            "$LEGACY_DIR/config/last_token_info.json" "$DATA_DIR/config/last_token_info.json"
    else
        echo "경고: 토큰 캐시가 없다 — 기동 때 새로 발급한다" >&2
    fi
    first_cutover=true
fi

sudo -n install -o 1000 -g 3000 -m 640 "$rendered_file" "$COMPOSE_FILE"

compose_payload() {
    python3 - "$1" <<'PY'
import json
import pathlib
import sys
print(json.dumps({"custom_compose_config_string": pathlib.Path(sys.argv[1]).read_text()}))
PY
}

rollback() {
    echo "직전 compose 로 되돌리는 중" >&2
    previous_payload=$(compose_payload "$previous_file")
    if sudo -n midclt call -j app.update "$APP_NAME" "$previous_payload"; then
        sudo -n install -o 1000 -g 3000 -m 640 "$previous_file" "$COMPOSE_FILE"
        if ! ensure_app_started; then
            echo "롤백 설정은 적용했지만 앱 시작에 실패했다" >&2
            return 1
        fi
        echo "롤백 적용 완료" >&2
    else
        echo "롤백도 실패했다" >&2
    fi
}

payload=$(compose_payload "$rendered_file")

echo "TrueNAS 앱 갱신: $APP_NAME"
if ! sudo -n midclt call -j app.update "$APP_NAME" "$payload"; then
    echo "앱 갱신 실패" >&2
    rollback
    exit 5
fi
if ! ensure_app_started; then
    echo "갱신한 앱을 시작하지 못했다" >&2
    rollback
    exit 5
fi

# 1) 새 컨테이너가 뜨고 실행 이미지가 맞는지
deadline=$((SECONDS + 120))
started=false
while (( SECONDS < deadline )); do
    status=$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || true)
    if [[ "$status" == "running" ]]; then
        running_image=$(docker inspect -f '{{.Config.Image}}' "$CONTAINER_NAME")
        if [[ "$running_image" != "$image" ]]; then
            # app.update 경계에서 옛 컨테이너가 잠깐 보일 수 있다.
            sleep 3
            continue
        fi
        started=true
        break
    fi
    sleep 3
done
if [[ "$started" != true ]]; then
    echo "새 컨테이너가 뜨지 않았다 (status=${status:-없음})" >&2
    docker logs --tail 60 "$CONTAINER_NAME" >&2 2>&1 || true
    rollback
    exit 6
fi

# 2) 크래시 루프가 아닌지 — 설정 누락·문법 오류는 여기서 드러난다
hold_deadline=$((SECONDS + START_GRACE))
while (( SECONDS < hold_deadline )); do
    status=$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || true)
    restarts=$(docker inspect -f '{{.RestartCount}}' "$CONTAINER_NAME" 2>/dev/null || echo 0)
    if [[ "$status" != "running" || "$restarts" -gt 0 ]]; then
        echo "기동 직후 죽었다 (status=$status restarts=$restarts)" >&2
        docker logs --tail 60 "$CONTAINER_NAME" >&2 2>&1 || true
        rollback
        exit 7
    fi
    sleep 5
done

if [[ "$first_cutover" == true ]]; then
    sudo -n touch "$CUTOVER_MARKER"
    sudo -n chown 1000:3000 "$CUTOVER_MARKER"
fi

echo "기동 확인: image=$image (${START_GRACE}s 동안 재시작 없음)"

# 3) 재구독까지 확인한다. "Up" 은 "수집 중"이 아니다 — 다만 여기서 실패시키면
#    롤백이 재시작을 한 번 더 부르므로 경고만 남긴다.
# ⚠️ `docker logs | grep -q` 로 쓰지 말 것. `grep -q` 가 먼저 끝나 `docker logs` 가 SIGPIPE(141)
#    로 죽고, `set -o pipefail` 때문에 **매치해도 파이프라인이 실패로 잡힌다**(2026-09-12 실측).
#    그래서 로그를 변수로 받아 셸 패턴으로 본다.
# ⚠️ `Initial subscriptions sent` 는 증거가 아니다 — WS 가 열리는 순간 `count=0` 으로 찍힌다
#    (2026-09-12 컷오버에서 10건 전부 count=0). 실제로 붙은 것은 `SUBSCRIBE SUCCESS` 다.
#    건수가 더 늘지 않으면 재구독이 끝난 것으로 본다(2026-09-02·09-12 모두 288건).
sub_deadline=$((SECONDS + SUBSCRIBE_TIMEOUT))
prev_count=-1
while (( SECONDS < sub_deadline )); do
    recent_logs=$(docker logs --tail 4000 "$CONTAINER_NAME" 2>&1 || true)
    sub_count=$(printf '%s\n' "$recent_logs" | grep -c "SUBSCRIBE SUCCESS" || true)
    if (( sub_count > 0 && sub_count == prev_count )); then
        echo "수집 재개 확인 — 구독 성공 ${sub_count}건"
        exit 0
    fi
    prev_count=$sub_count
    sleep 15
done

echo "경고: ${SUBSCRIBE_TIMEOUT}s 안에 재구독이 끝나지 않았다 (구독 성공 ${prev_count}건) — 컨테이너는 떠 있다." >&2
echo "      로그를 확인할 것. 되돌리려면 이전 버전 태그로 다시 배포한다(재시작 1회 추가)." >&2
docker logs --tail 40 "$CONTAINER_NAME" >&2 2>&1 || true
exit 0
