# 운영 이미지 — 코드를 이미지 안에 넣는다(빌드 컨텍스트는 저장소 루트).
# 설정(config/)만 호스트에서 마운트한다.
ARG BASE_IMAGE=python:3.11
FROM ${BASE_IMAGE}

ARG APP_VERSION=dev
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="bae-stock-ticker" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"

WORKDIR /workspace

# 컨테이너에 들어가 상태를 볼 때 쓰는 도구들 — 기존 이미지와 같은 구성.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        btop \
        vim \
        nano \
        procps \
        iproute2 && \
    rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt /workspace/docker/requirements.txt
RUN pip install --no-cache-dir -r /workspace/docker/requirements.txt

RUN groupadd --gid 3000 bae4969 && \
    useradd --uid 1000 --gid bae4969 --shell /bin/bash --create-home bae4969

COPY --chown=1000:3000 main.py /workspace/main.py
COPY --chown=1000:3000 api/ /workspace/api/
COPY --chown=1000:3000 core/ /workspace/core/
COPY --chown=1000:3000 scripts/ /workspace/scripts/

# `_master.py` 가 마스터 파일을 `./temp` 로 받는다 — 쓸 수 있는 자리를 미리 만든다.
# `config/` 는 호스트에서 마운트한다(settings.json·last_token_info.json).
RUN mkdir -p /workspace/temp /workspace/config && \
    chown -R 1000:3000 /workspace

USER 1000:3000
ENV PYTHONUNBUFFERED=1

CMD ["python3", "main.py"]
