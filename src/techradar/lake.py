"""DuckLake 연결·스키마 부트스트랩·트랜잭션 헬퍼.

이 파이프라인의 재시도 안전성은 전부 `transaction()` 하나에 걸려 있다.
bronze INSERT 와 워터마크 UPDATE 가 같은 트랜잭션 안에서 커밋되지 않으면,
커밋 사이에 러너가 죽었을 때 데이터는 들어갔는데 커서는 안 움직였거나(중복)
커서만 움직이고 데이터는 없는(유실) 상태가 된다.

Phase 0 스파이크(scripts/spike_ducklake.py S2/S3)에서 다중 테이블 변경이
단일 스냅샷으로 커밋되고 롤백 시 함께 원복됨을 확인했다.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import duckdb

from techradar.config import CATALOG, Target

# ---------------------------------------------------------------- 스키마
#
# bronze 는 원본 payload 를 파싱 없이 그대로 보존한다. 파서에 버그가 있어도
# 재수집 없이 silver 만 다시 만들면 되도록.
#
# 멱등키 = (source_name, native_id, payload_hash).
# payload_hash 를 키에 넣는 이유: HN 처럼 점수가 계속 변하는 소스는 같은 아이템이라도
# 내용이 바뀌면 새 행이어야 이력이 남는다. 안 변했으면 재수집해도 행이 안 늘어난다.
_DDL = [
    """
    CREATE TABLE IF NOT EXISTS bronze__raw_item (
        source_name  VARCHAR      NOT NULL,
        native_id    VARCHAR      NOT NULL,
        payload_hash VARCHAR      NOT NULL,
        payload      VARCHAR      NOT NULL,
        event_ts     TIMESTAMPTZ,
        fetched_at   TIMESTAMPTZ  NOT NULL,
        run_id       VARCHAR      NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops__source_watermark (
        source_name      VARCHAR NOT NULL,
        cursor_ts        TIMESTAMPTZ,
        cursor_int       BIGINT,
        last_run_id      VARCHAR,
        last_success_at  TIMESTAMPTZ,
        consecutive_fail INTEGER NOT NULL DEFAULT 0,
        updated_at       TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops__ingest_run_log (
        run_id           VARCHAR NOT NULL,
        source_name      VARCHAR NOT NULL,
        started_at       TIMESTAMPTZ NOT NULL,
        ended_at         TIMESTAMPTZ,
        status           VARCHAR NOT NULL,
        rows_fetched     BIGINT,
        rows_new         BIGINT,
        watermark_before TIMESTAMPTZ,
        watermark_after  TIMESTAMPTZ,
        error_msg        VARCHAR
    )
    """,
]


def connect(target: Target) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")

    if target.is_remote:
        _configure_r2(con)

    con.execute(
        f"ATTACH '{target.catalog_uri}' AS {CATALOG} "
        f"(DATA_PATH '{target.data_path}')"
    )
    con.execute(f"USE {CATALOG}")
    return con


def _configure_r2(con: duckdb.DuckDBPyConnection) -> None:
    """R2 자격증명을 DuckDB SECRET 으로 등록한다 (S3 호환 엔드포인트).

    ⚠️ Phase 0 S5 미검증 구간. R2 계정 발급 후 스파이크로 왕복을 확인할 것.
    """
    account = os.environ["R2_ACCOUNT_ID"]
    con.execute(
        f"""
        CREATE OR REPLACE SECRET r2 (
            TYPE s3,
            KEY_ID '{os.environ["R2_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["R2_SECRET_ACCESS_KEY"]}',
            ENDPOINT '{account}.r2.cloudflarestorage.com',
            REGION 'auto',
            URL_STYLE 'path'
        )
        """
    )


def bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    for ddl in _DDL:
        con.execute(ddl)


@contextmanager
def transaction(con: duckdb.DuckDBPyConnection) -> Iterator[duckdb.DuckDBPyConnection]:
    """예외가 나면 롤백한다.

    bronze 적재와 워터마크 전진을 반드시 이 안에서 함께 처리할 것.
    커밋 전에 죽으면 워터마크가 안 움직이므로 다음 실행이 같은 구간을 재수집하고,
    bronze 의 멱등키가 중복을 흡수한다 → at-least-once + 멱등 = effectively-once.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        yield con
    except Exception:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")
