import json
from pathlib import Path


def _get_token_info_path() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "config" / "last_token_info.json"


def load_last_token_info() -> dict:
    token_info_path = _get_token_info_path()

    if not token_info_path.exists():
        return {}

    with token_info_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def save_last_token_info(token_info: dict) -> None:
    token_info_path = _get_token_info_path()
    token_info_path.parent.mkdir(parents=True, exist_ok=True)

    # 임시 파일에 먼저 기록 후 교체하여 저장 중 파일 손상을 줄인다.
    tmp_path = token_info_path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as fp:
        json.dump(token_info, fp, ensure_ascii=False, indent=2)

    tmp_path.replace(token_info_path)
