"""Phase 0 아키텍처 스파이크.

코드를 본격적으로 쓰기 전에 DuckLake 가 이 설계의 전제를 실제로 만족하는지 검증한다.
DuckLake 1.0 은 2026-04 릴리스로 신생이라, 문서만 믿고 Phase 1~8 을 짜면
전제가 깨졌을 때 되돌릴 비용이 크다.

가장 중요한 건 S2 다. "워터마크를 데이터 적재와 같은 트랜잭션에 묶는다"가
이 파이프라인의 재시도 안전성 전부이고, 그게 불가능하면 설계를 바꿔야 한다.

실행:  uv run python scripts/spike_ducklake.py
"""

from __future__ import annotations

import os
import shutil
import tempfile
import traceback
from pathlib import Path

import duckdb

CATALOG = "lake"


def _connect(tmp: Path) -> duckdb.DuckDBPyConnection:
    """로컬 DuckDB 카탈로그 + 로컬 디렉토리 DATA_PATH 로 DuckLake 를 attach 한다.

    prod 에서는 카탈로그가 Neon Postgres, DATA_PATH 가 R2 로 바뀌지만
    ATTACH 문 한 줄만 달라지고 이후 SQL 은 동일하다. 그게 이 저장 계층을 고른 이유.
    """
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    catalog_file = tmp / "catalog.ducklake"
    data_path = tmp / "lakehouse"
    data_path.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"ATTACH 'ducklake:{catalog_file}' AS {CATALOG} "
        f"(DATA_PATH '{data_path}/')"
    )
    con.execute(f"USE {CATALOG}")
    return con


def _seed(con: duckdb.DuckDBPyConnection) -> None:
    """bronze(append 대상) + watermark(갱신 대상) 최소 스키마."""
    con.execute("""
        CREATE TABLE bronze__raw_item (
            source_name  VARCHAR,
            native_id    VARCHAR,
            payload_hash VARCHAR,
            fetched_at   TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE ops__source_watermark (
            source_name VARCHAR,
            cursor_ts   TIMESTAMPTZ,
            updated_at  TIMESTAMPTZ
        )
    """)
    con.execute("""
        INSERT INTO ops__source_watermark
        VALUES ('arxiv:cs.AI', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00+00')
    """)


def _snapshot_count(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        f"SELECT count(*) FROM ducklake_snapshots('{CATALOG}')"
    ).fetchone()[0]


# ---------------------------------------------------------------- checks


def s1_attach(con, tmp):
    """S1: DuckLake attach + 테이블 생성 + parquet 실제 기록."""
    con.execute("""
        INSERT INTO bronze__raw_item
        VALUES ('arxiv:cs.AI', '2601.00001', 'h1', now())
    """)
    n = con.execute("SELECT count(*) FROM bronze__raw_item").fetchone()[0]
    assert n == 1, f"행이 안 들어감: {n}"
    # 인라이닝(기본 10행 한도) 때문에 소량 insert 는 parquet 파일이 안 생길 수 있다.
    con.execute(f"CALL ducklake_flush_inlined_data('{CATALOG}')")
    files = list((tmp / "lakehouse").rglob("*.parquet"))
    return f"행 {n}건, parquet {len(files)}개 (flush 후)"


def s2_multi_table_txn(con, _tmp):
    """S2: bronze INSERT + watermark UPDATE 가 '한 스냅샷'으로 커밋되는가.

    ★ 이 설계의 핵심 전제. 실패하면 워터마크를 max(bronze.fetched_at) 으로
      유도(derive)하는 방식으로 폴백해야 한다.
    """
    before = _snapshot_count(con)
    con.execute("BEGIN TRANSACTION")
    con.execute("""
        INSERT INTO bronze__raw_item
        VALUES ('arxiv:cs.AI', '2601.00002', 'h2', now())
    """)
    con.execute("""
        UPDATE ops__source_watermark
        SET cursor_ts = '2026-02-01 00:00:00+00', updated_at = now()
        WHERE source_name = 'arxiv:cs.AI'
    """)
    con.execute("COMMIT")
    after = _snapshot_count(con)

    delta = after - before
    cur = con.execute(
        "SELECT cursor_ts FROM ops__source_watermark WHERE source_name='arxiv:cs.AI'"
    ).fetchone()[0]
    assert str(cur).startswith("2026-02-01"), f"워터마크 미전진: {cur}"
    assert delta == 1, f"스냅샷이 {delta}개 생성됨 (기대: 1) — 원자성 전제 불성립"
    return f"스냅샷 +{delta}개, 워터마크 전진 확인 → 원자성 성립"


def s3_rollback(con, _tmp):
    """S3: 트랜잭션 중단 시 bronze/watermark 가 함께 원복되는가.

    러너가 커밋 전에 죽는 시나리오. 워터마크가 전진하지 않아야
    다음 실행이 같은 구간을 재수집하고, 멱등키가 중복을 흡수한다.
    """
    rows_before = con.execute("SELECT count(*) FROM bronze__raw_item").fetchone()[0]
    wm_before = con.execute(
        "SELECT cursor_ts FROM ops__source_watermark WHERE source_name='arxiv:cs.AI'"
    ).fetchone()[0]

    con.execute("BEGIN TRANSACTION")
    con.execute("""
        INSERT INTO bronze__raw_item
        VALUES ('arxiv:cs.AI', '2601.00003', 'h3', now())
    """)
    con.execute("""
        UPDATE ops__source_watermark
        SET cursor_ts = '2026-12-31 00:00:00+00'
        WHERE source_name = 'arxiv:cs.AI'
    """)
    con.execute("ROLLBACK")

    rows_after = con.execute("SELECT count(*) FROM bronze__raw_item").fetchone()[0]
    wm_after = con.execute(
        "SELECT cursor_ts FROM ops__source_watermark WHERE source_name='arxiv:cs.AI'"
    ).fetchone()[0]

    assert rows_after == rows_before, f"bronze 원복 실패: {rows_before}→{rows_after}"
    assert wm_after == wm_before, f"워터마크 원복 실패: {wm_before}→{wm_after}"
    return f"bronze {rows_after}건 유지, 워터마크 {wm_after} 유지 → 원복 성립"


def s4_merge(con, _tmp):
    """S4: DuckLake 테이블에 네이티브 MERGE (when_matched 단일 UPDATE 분기).

    dbt-duckdb 의 incremental_strategy='merge' 가 생성하는 SQL 형태.
    DuckLake 는 when_matched 에 액션 1개만 허용하므로 다분기 merge 는 불가.
    실패 시 delete+insert 전략으로 폴백한다.
    """
    con.execute("""
        CREATE TABLE silver__document (
            doc_id             VARCHAR,
            title              VARCHAR,
            published_at       TIMESTAMPTZ,
            _source_fetched_at TIMESTAMPTZ
        )
    """)
    con.execute("""
        INSERT INTO silver__document VALUES
            ('arxiv:2601.00001', '원본 제목', '2026-01-05 00:00:00+00', '2026-01-06 00:00:00+00')
    """)
    con.execute("""
        CREATE TEMP TABLE staged AS SELECT * FROM (VALUES
            ('arxiv:2601.00001', '개정된 제목', TIMESTAMPTZ '2026-01-05 00:00:00+00', TIMESTAMPTZ '2026-01-07 00:00:00+00'),
            ('arxiv:2601.00099', '신규 문서',   TIMESTAMPTZ '2026-01-07 00:00:00+00', TIMESTAMPTZ '2026-01-07 00:00:00+00')
        ) t(doc_id, title, published_at, _source_fetched_at)
    """)
    con.execute("""
        MERGE INTO silver__document AS tgt
        USING staged AS src
          ON tgt.doc_id = src.doc_id
        WHEN MATCHED THEN UPDATE SET
            title = src.title,
            published_at = src.published_at,
            _source_fetched_at = src._source_fetched_at
        WHEN NOT MATCHED THEN INSERT VALUES
            (src.doc_id, src.title, src.published_at, src._source_fetched_at)
    """)
    rows = con.execute(
        "SELECT doc_id, title FROM silver__document ORDER BY doc_id"
    ).fetchall()
    assert len(rows) == 2, f"행 수 이상: {rows}"
    assert rows[0][1] == "개정된 제목", f"UPDATE 분기 미동작: {rows[0]}"
    return f"upsert 성립 (갱신 1 + 신규 1) → merge 전략 사용 가능"


def s5_r2(con, _tmp):
    """S5: R2 왕복. 자격증명이 없으면 스킵."""
    required = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SkipCheck(f"env 미설정: {', '.join(missing)}")
    raise SkipCheck("TODO: R2 계정 발급 후 구현")


def s6_neon(con, _tmp):
    """S6: Neon Postgres 카탈로그 attach. DSN 이 없으면 스킵."""
    if not os.environ.get("TECHRADAR_CATALOG_DSN"):
        raise SkipCheck("env 미설정: TECHRADAR_CATALOG_DSN")
    raise SkipCheck("TODO: Neon 프로젝트 생성 후 구현")


class SkipCheck(Exception):
    """검증 불가(외부 자격증명 부재) — 실패와 구분한다."""


CHECKS = [
    ("S1", "DuckLake attach + parquet 기록", s1_attach),
    ("S2", "다중 테이블 단일 트랜잭션 (★핵심 전제)", s2_multi_table_txn),
    ("S3", "트랜잭션 롤백 시 워터마크 원복", s3_rollback),
    ("S4", "네이티브 MERGE 단일 UPDATE 분기", s4_merge),
    ("S5", "R2 DATA_PATH 읽기/쓰기 왕복", s5_r2),
    ("S6", "Neon Postgres 카탈로그 attach", s6_neon),
]


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="techradar-spike-"))
    results = []
    try:
        con = _connect(tmp)
        _seed(con)
        for code, name, fn in CHECKS:
            try:
                detail = fn(con, tmp)
                results.append((code, name, "PASS", detail))
            except SkipCheck as e:
                results.append((code, name, "SKIP", str(e)))
            except Exception as e:
                results.append((code, name, "FAIL", f"{type(e).__name__}: {e}"))
                traceback.print_exc()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'=' * 78}")
    print(f"Phase 0 스파이크 결과  (duckdb {duckdb.__version__})")
    print("=" * 78)
    for code, name, status, detail in results:
        mark = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️ "}[status]
        print(f"{mark} {code}  {name}")
        print(f"      {detail}")
    print("=" * 78)

    failed = [r for r in results if r[2] == "FAIL"]
    skipped = [r for r in results if r[2] == "SKIP"]
    print(
        f"PASS {len(results) - len(failed) - len(skipped)} / "
        f"FAIL {len(failed)} / SKIP {len(skipped)}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
