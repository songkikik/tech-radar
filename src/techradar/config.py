"""타깃(dev/ci/prod) 설정.

전신 프로젝트의 LakehouseLayout 은 산출물 경로를 프로퍼티로 일일이 들고 있었다
(documents_parquet, inverted_index_path, ...). 여기서는 그 책임이 DuckLake 카탈로그로
넘어갔으므로, 남는 건 "카탈로그 어디에 붙고 데이터를 어디에 쓰느냐" 두 개뿐이다.

타깃이 바뀌어도 ATTACH 문 한 줄만 달라지고 이후 SQL·Python 은 전부 동일하다.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# DuckLake 카탈로그를 attach 할 때 쓰는 이름. SQL 전반에서 이 이름으로 참조한다.
CATALOG = "lake"


@dataclass(frozen=True)
class Target:
    name: str
    #: ATTACH 문에 들어가는 카탈로그 지정자. 로컬은 파일 경로, prod 는 postgres DSN.
    catalog_uri: str
    #: parquet 이 실제로 쌓이는 위치. 로컬 디렉토리 또는 s3:// (R2).
    data_path: str

    @property
    def is_remote(self) -> bool:
        return self.data_path.startswith("s3://")


def _dev_target() -> Target:
    """로컬 개발 타깃.

    경로는 env 우선, 없으면 REPO_ROOT 기준 폴백. env 를 보는 이유는 dbt 가
    파이썬 설정을 읽을 수 없어 profiles.yml 이 같은 값을 env_var 로 받기 때문이고,
    폴백을 두는 이유는 direnv 없이 `uv run techradar` 만으로도 돌아야 하기 때문이다.
    """
    root = REPO_ROOT / "data"
    catalog = os.environ.get("TECHRADAR_DEV_CATALOG") or str(root / "catalog.ducklake")
    data_path = os.environ.get("TECHRADAR_DEV_DATA_PATH") or f"{root / 'lakehouse'}/"
    Path(data_path).mkdir(parents=True, exist_ok=True)
    return Target(name="dev", catalog_uri=f"ducklake:{catalog}", data_path=data_path)


def _remote_target(name: str) -> Target:
    """ci/prod: 카탈로그=Neon Postgres, 데이터=R2.

    ⚠️ ci 는 카탈로그만 브랜치로 분리해서는 안 된다. Neon 브랜칭은 메타데이터만
    분기하고 R2 의 parquet 은 분기하지 않으므로, DATA_PATH 프리픽스도 반드시
    따로 줘야 prod 데이터를 오염시키지 않는다.
    """
    dsn = os.environ.get("TECHRADAR_CATALOG_DSN")
    data_path = os.environ.get("TECHRADAR_DATA_PATH")
    missing = [
        k
        for k, v in (
            ("TECHRADAR_CATALOG_DSN", dsn),
            ("TECHRADAR_DATA_PATH", data_path),
        )
        if not v
    ]
    if missing:
        raise RuntimeError(
            f"타깃 '{name}' 에 필요한 환경변수가 없습니다: {', '.join(missing)}. "
            f".envrc.local 을 설정하거나 TECHRADAR_TARGET=dev 로 실행하세요."
        )
    if not data_path.endswith("/"):
        data_path += "/"
    return Target(name=name, catalog_uri=f"ducklake:postgres:{dsn}", data_path=data_path)


def load_target(name: str | None = None) -> Target:
    name = name or os.environ.get("TECHRADAR_TARGET", "dev")
    if name == "dev":
        return _dev_target()
    if name in ("ci", "prod"):
        return _remote_target(name)
    raise ValueError(f"알 수 없는 타깃: {name!r} (dev|ci|prod)")


def new_run_id() -> str:
    """실행 식별자.

    GitHub Actions 에서는 run_id-run_attempt 를 쓴다. 재시도(attempt)를 구분해야
    ops__ingest_run_log 에서 "같은 잡의 2회차"를 추적할 수 있다.
    """
    gh_id = os.environ.get("GITHUB_RUN_ID")
    if gh_id:
        return f"gha-{gh_id}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return f"local-{uuid.uuid4().hex[:12]}"
