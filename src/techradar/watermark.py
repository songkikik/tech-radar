"""소스별 수집 커서 읽기/전진 + 실행 로그 기록.

전진 규칙 3가지 — 셋 다 지키지 않으면 조용히 데이터가 새거나 무한 재처리된다.

1. **같은 트랜잭션 안에서만 전진**한다. lake.transaction() 밖에서 부르지 말 것.
2. **실제로 처리한 지점까지만** 전진한다(요청한 지점이 아니라). 3만 건 중 8천 건
   처리 후 타임아웃이면 8천 건째까지만 → 재개형 백필이 공짜로 성립한다.
3. **후퇴 금지** (greatest). 소스가 이상한 값을 주거나 재시도 순서가 꼬여도
   커서가 뒤로 가면 이미 처리한 구간을 영원히 다시 돈다.
"""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure(con: duckdb.DuckDBPyConnection, source_name: str) -> None:
    """워터마크 행이 없으면 만든다(커서는 NULL = 최초 수집)."""
    con.execute(
        """
        INSERT INTO ops__source_watermark
            (source_name, cursor_ts, cursor_int, consecutive_fail, updated_at)
        SELECT ?, NULL, NULL, 0, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM ops__source_watermark WHERE source_name = ?
        )
        """,
        [source_name, now_utc(), source_name],
    )


def read_cursor(con: duckdb.DuckDBPyConnection, source_name: str) -> datetime | None:
    row = con.execute(
        "SELECT cursor_ts FROM ops__source_watermark WHERE source_name = ?",
        [source_name],
    ).fetchone()
    return row[0] if row else None


def advance(
    con: duckdb.DuckDBPyConnection,
    source_name: str,
    new_cursor: datetime | None,
    run_id: str,
) -> None:
    """커서를 전진시킨다. new_cursor 가 None 이면 성공 시각만 갱신한다.

    greatest() 로 후퇴를 막는다. 수동 백필이 필요하면 이 함수를 쓰지 말고
    ops__source_watermark 를 직접 UPDATE 할 것(의도적 후퇴는 예외 경로여야 한다).
    """
    con.execute(
        """
        UPDATE ops__source_watermark
        SET cursor_ts        = CASE
                                 WHEN ? IS NULL THEN cursor_ts
                                 WHEN cursor_ts IS NULL THEN ?
                                 ELSE greatest(cursor_ts, ?)
                               END,
            last_run_id      = ?,
            last_success_at  = ?,
            consecutive_fail = 0,
            updated_at       = ?
        WHERE source_name = ?
        """,
        [new_cursor, new_cursor, new_cursor, run_id, now_utc(), now_utc(), source_name],
    )


def record_failure(
    con: duckdb.DuckDBPyConnection, source_name: str, run_id: str
) -> None:
    """실패를 카운트한다. 커서는 건드리지 않는다 — 전진하면 그 구간이 유실된다.

    consecutive_fail 은 Phase 6 의 DQ 체크(워터마크 정체 탐지)가 읽는다.
    """
    con.execute(
        """
        UPDATE ops__source_watermark
        SET consecutive_fail = consecutive_fail + 1,
            last_run_id      = ?,
            updated_at       = ?
        WHERE source_name = ?
        """,
        [run_id, now_utc(), source_name],
    )


def record_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    source_name: str,
    started_at: datetime,
    status: str,
    rows_fetched: int = 0,
    rows_new: int = 0,
    watermark_before: datetime | None = None,
    watermark_after: datetime | None = None,
    error_msg: str | None = None,
) -> None:
    """append-only 실행 기록.

    Phase 6 의 볼륨 급감 탐지가 이 테이블의 rows_new 를 28일 트레일링으로 읽는다.
    성격상 price-integrity 의 notification_dispatch 와 같다(비멱등 사실 기록).
    """
    con.execute(
        """
        INSERT INTO ops__ingest_run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            source_name,
            started_at,
            now_utc(),
            status,
            rows_fetched,
            rows_new,
            watermark_before,
            watermark_after,
            error_msg,
        ],
    )
