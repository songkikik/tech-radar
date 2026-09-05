"""증분 수집의 불변식 검증.

여기서 지키는 것은 두 가지뿐이고, 둘 다 깨지면 조용히 깨진다.

  진행성  커서는 매 실행 전진한다 (전방 구간에 데이터가 있는 한).
  멱등성  같은 내용을 다시 받아도 bronze 행이 늘지 않는다.

test_cursor_never_stalls 는 실제로 발생시킨 버그의 회귀 테스트다.
전방/소급 조회를 한 쿼리로 합쳤을 때 커서가 3회 실행 만에 정체했다.
"""

from __future__ import annotations

from datetime import timedelta

from techradar.collect import arxiv
from tests.conftest import FakeArxiv


def _run(con, run_id="t", page_size=10, max_pages=1):
    return arxiv.collect(
        con,
        category="cs.AI",
        run_id=run_id,
        page_size=page_size,
        max_pages=max_pages,
    )


def _bronze_count(con):
    return con.execute("SELECT count(*) FROM bronze__raw_item").fetchone()[0]


def test_cursor_never_stalls(con, base_time, monkeypatch):
    """회귀 테스트: 런 용량보다 소급 구간이 커도 커서는 계속 전진해야 한다.

    OVERLAP(2일) 구간에 용량(10건)의 수십 배를 깔아둔다. 전방/소급을 한 쿼리로
    합치면 오름차순 페이징이 소급 구간을 벗어나지 못해 커서가 멈춘다.
    """
    # 최근 3일에 걸쳐 300건 — 어느 2일 구간을 잘라도 용량 10건을 크게 넘는다.
    items = [
        (f"26{i:04d}", base_time - timedelta(days=3) + timedelta(minutes=i * 14))
        for i in range(300)
    ]
    fake = FakeArxiv(items)
    monkeypatch.setattr(arxiv, "_fetch_window", fake.fetch)

    cursors = []
    for n in range(6):
        r = _run(con, run_id=f"run-{n}")
        cursors.append(r["cursor_after"])
        assert not r["stalled"], f"{n}회차에서 커서 정체"

    for prev, cur in zip(cursors, cursors[1:]):
        assert cur > prev, f"커서가 전진하지 않음: {prev} → {cur}"


def test_idempotent_rerun(con, base_time, monkeypatch):
    """전방 구간을 소진한 뒤 재실행하면 신규 0 이어야 한다."""
    items = [
        (f"26{i:04d}", base_time - timedelta(hours=i)) for i in range(5)
    ]
    fake = FakeArxiv(items)
    monkeypatch.setattr(arxiv, "_fetch_window", fake.fetch)

    first = _run(con, run_id="a")
    assert first["rows_new"] == 5
    assert first["exhausted"]

    second = _run(con, run_id="b")
    assert second["rows_new"] == 0, "같은 내용 재수집이 행을 늘렸다"
    assert _bronze_count(con) == 5


def test_backfill_does_not_regress_cursor(con, base_time, monkeypatch):
    """소급 조회로 과거 항목을 회수해도 커서는 후퇴하지 않는다."""
    items = [(f"26{i:04d}", base_time - timedelta(hours=i)) for i in range(5)]
    fake = FakeArxiv(items)
    monkeypatch.setattr(arxiv, "_fetch_window", fake.fetch)

    first = _run(con, run_id="a")
    cursor_after_first = first["cursor_after"]

    # 커서보다 하루 이전 시각으로 뒤늦게 등장한 항목 (모더레이션 지연 시나리오)
    fake.items = sorted(
        fake.items + [("late-0001", base_time - timedelta(days=1))],
        key=lambda x: x[1],
    )

    second = _run(con, run_id="b")

    assert second["cursor_after"] >= cursor_after_first, "커서가 후퇴했다"
    landed = con.execute(
        "SELECT count(*) FROM bronze__raw_item WHERE native_id = 'late-0001'"
    ).fetchone()[0]
    assert landed == 1, "소급 조회가 late-arriving 항목을 놓쳤다"


def test_changed_payload_creates_new_row(con, base_time, monkeypatch):
    """내용이 바뀌면 새 행이 생긴다 (HN 점수 변동 같은 이력 보존)."""
    items = [("2600001", base_time - timedelta(hours=1))]
    fake = FakeArxiv(items)
    monkeypatch.setattr(arxiv, "_fetch_window", fake.fetch)

    _run(con, run_id="a")
    assert _bronze_count(con) == 1

    # 같은 native_id, 다른 payload
    fake.mutate_payload = {"score": 999}
    _run(con, run_id="b")
    assert _bronze_count(con) == 2, "payload 변경이 이력으로 남지 않았다"
