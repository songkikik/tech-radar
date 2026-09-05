"""커넥터 공통 — 원본 아이템 표현과 bronze 멱등 적재.

전신 프로젝트는 커넥터가 raw 존에 파일을 떨구고 별도 파이프라인이 그걸 스캔했다.
그 분리는 좋았지만 "파일이 이미 있는지" 판정이 없어서 매번 전량 재다운로드했다.
여기서는 파일 단계를 없애고 bronze 테이블에 직행하되, 멱등키 안티조인으로
같은 내용의 재수집이 행을 늘리지 않게 한다.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

import duckdb


@dataclass(frozen=True)
class RawItem:
    """소스에서 받은 아이템 하나. payload 는 파싱하지 않고 그대로 보존한다."""

    native_id: str
    payload: dict[str, Any]
    #: 이 아이템의 이벤트 시각(발행일 등). 워터마크 전진의 근거가 된다.
    event_ts: datetime | None

    @property
    def payload_json(self) -> str:
        # sort_keys: 같은 내용이 키 순서 때문에 다른 해시가 되는 것을 막는다.
        return json.dumps(self.payload, ensure_ascii=False, sort_keys=True)

    @property
    def payload_hash(self) -> str:
        return hashlib.sha256(self.payload_json.encode("utf-8")).hexdigest()[:32]


def insert_items(
    con: duckdb.DuckDBPyConnection,
    *,
    source_name: str,
    items: Sequence[RawItem],
    run_id: str,
    fetched_at: datetime,
) -> int:
    """bronze 에 멱등 적재하고 '실제로 새로 들어간' 행 수를 돌려준다.

    멱등키 = (source_name, native_id, payload_hash).
    같은 아이템이라도 내용이 바뀌면 새 행 → HN 점수 변동 같은 이력이 보존된다.
    내용이 그대로면 재수집해도 0건 → 재시도가 안전해진다.

    ⚠️ 반드시 lake.transaction() 안에서 호출할 것.
    """
    if not items:
        return 0

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _staged (
            source_name  VARCHAR,
            native_id    VARCHAR,
            payload_hash VARCHAR,
            payload      VARCHAR,
            event_ts     TIMESTAMPTZ,
            fetched_at   TIMESTAMPTZ,
            run_id       VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO _staged VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                source_name,
                it.native_id,
                it.payload_hash,
                it.payload_json,
                it.event_ts,
                fetched_at,
                run_id,
            )
            for it in items
        ],
    )

    # 같은 배치 안의 중복(같은 native_id 가 두 번 온 경우)도 제거해야 하므로
    # DISTINCT ON 대신 QUALIFY 로 하나만 남긴다.
    before = con.execute("SELECT count(*) FROM bronze__raw_item").fetchone()[0]
    con.execute("""
        INSERT INTO bronze__raw_item
        SELECT s.source_name, s.native_id, s.payload_hash, s.payload,
               s.event_ts, s.fetched_at, s.run_id
        FROM (
            SELECT * FROM _staged
            QUALIFY row_number() OVER (
                PARTITION BY source_name, native_id, payload_hash
                ORDER BY event_ts DESC NULLS LAST
            ) = 1
        ) s
        WHERE NOT EXISTS (
            SELECT 1 FROM bronze__raw_item b
            WHERE b.source_name  = s.source_name
              AND b.native_id    = s.native_id
              AND b.payload_hash = s.payload_hash
        )
    """)
    after = con.execute("SELECT count(*) FROM bronze__raw_item").fetchone()[0]
    return after - before
