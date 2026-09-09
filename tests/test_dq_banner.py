"""DQ 경고 → 다이제스트 배너 문장 변환.

여기서 검증할 것은 SQL 필터(오늘자만 · cooldown 결과를 그대로 신뢰)와 렌더링뿐이다.
판정 로직 자체는 dbt 모델의 책임이라 여기서 다시 테스트하지 않는다.
"""

from __future__ import annotations

import pytest

from techradar import dq


@pytest.fixture
def dq_tables(con):
    """dq 모델 2개의 최소 스키마를 흉내낸다 (dbt 없이 파이썬만 검증하기 위해)."""
    con.execute("""
        CREATE TABLE dq__alert_dispatch (
            severity VARCHAR, check_name VARCHAR, source_name VARCHAR,
            detail VARCHAR, alerted_at TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE dq__check_result (
            check_name VARCHAR, check_date DATE, source_name VARCHAR, status VARCHAR
        )
    """)
    return con


def _alert(con, severity, check, source, detail, *, days_ago=0):
    con.execute(
        "INSERT INTO dq__alert_dispatch VALUES (?, ?, ?, ?, now() - INTERVAL (?) DAY)",
        [severity, check, source, detail, days_ago],
    )


def test_어제_경고는_배너에_안_나온다(dq_tables):
    """alert_dispatch 는 append-only 누적 테이블이라 날짜로 자르지 않으면
    지난 경고가 영원히 배너에 남는다."""
    _alert(dq_tables, "critical", "freshness", "arxiv:cs.AI", "63시간 전", days_ago=1)
    _alert(dq_tables, "warn", "volume_drop", "hackernews:top", "40% 감소", days_ago=0)

    lines = dq.pending_warnings(dq_tables)

    assert lines == ["[경고] volume_drop · hackernews:top — 40% 감소"]


def test_critical_이_warn_보다_먼저_온다(dq_tables):
    _alert(dq_tables, "warn", "a_check", "src", "덜 급함")
    _alert(dq_tables, "critical", "z_check", "src", "급함")

    lines = dq.pending_warnings(dq_tables)

    assert lines[0].startswith("[심각]")
    assert lines[1].startswith("[경고]")


def test_상한을_넘으면_접는다(dq_tables):
    for i in range(dq.MAX_LISTED + 3):
        _alert(dq_tables, "warn", f"check_{i}", "src", "무언가")

    lines = dq.pending_warnings(dq_tables)

    assert len(lines) == dq.MAX_LISTED + 1
    assert "외 3건" in lines[-1]


def test_판정불가는_건수만_알린다(dq_tables):
    """insufficient_data 는 통보 대상이 아니라 나열하지 않는다 — 건수만 붙는다."""
    for i in range(15):
        dq_tables.execute(
            "INSERT INTO dq__check_result VALUES (?, current_date, 'src', 'insufficient_data')",
            [f"check_{i}"],
        )
    dq_tables.execute("INSERT INTO dq__check_result VALUES ('ok_check', current_date, 'src', 'ok')")

    lines = dq.pending_warnings(dq_tables)

    assert lines == ["판정 불가 15건 (기준선 미확보 — 이력이 쌓이면 자동 해소)"]


def test_아무_문제_없으면_배너가_비어있다(dq_tables):
    assert dq.pending_warnings(dq_tables) == []
