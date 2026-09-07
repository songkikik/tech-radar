"""HN 수집기 검증.

arXiv 와 다른 점만 본다:
  - 커서를 전진시키지 않는다 (mutating 소스)
  - 점수가 바뀌면 새 행, 안 바뀌면 행이 안 는다
  - story 가 아닌 항목(job/comment)과 dead/deleted 는 걸러진다
"""

from __future__ import annotations

import json

from techradar.collect import hackernews


class FakeHN:
    """_get_json 대역. HTTP 경계만 갈아끼워 나머지 코드 경로는 그대로 태운다."""

    def __init__(self, items: list[dict]):
        self.items = {it["id"]: it for it in items}

    def __call__(self, url: str):
        if url.endswith("stories.json"):
            return list(self.items)
        item_id = int(url.rsplit("/", 1)[-1].removesuffix(".json"))
        return self.items.get(item_id)


def _story(item_id: int, score: int, *, comments: int = 0, **over) -> dict:
    base = {
        "id": item_id,
        "type": "story",
        "title": f"story {item_id}",
        "url": f"https://example.com/{item_id}",
        "by": "someone",
        "score": score,
        "descendants": comments,
        "time": 1_780_000_000 + item_id,
    }
    base.update(over)
    return base


def _run(con, fake, monkeypatch, run_id="t"):
    monkeypatch.setattr(hackernews, "_get_json", fake)
    return hackernews.collect(con, listing="top", run_id=run_id, limit=50, max_workers=4)


def _bronze(con):
    return con.execute(
        "SELECT count(*) FROM bronze__raw_item WHERE source_name LIKE 'hackernews:%'"
    ).fetchone()[0]


def test_no_cursor_advance(con, monkeypatch):
    """HN 은 커서를 쓰지 않는다. 커서를 전진시키면 지나간 글의 점수 폭발을 놓친다."""
    fake = FakeHN([_story(1, 10), _story(2, 20)])
    r = _run(con, fake, monkeypatch)

    assert r["rows_new"] == 2
    cursor = con.execute(
        "SELECT cursor_ts FROM ops__source_watermark WHERE source_name = 'hackernews:top'"
    ).fetchone()[0]
    assert cursor is None, f"HN 워터마크에 커서가 생겼다: {cursor}"


def test_score_change_creates_history(con, monkeypatch):
    """점수가 변하면 새 행, 안 변하면 행이 안 는다 — 이게 이력의 원천이다."""
    fake = FakeHN([_story(1, 10), _story(2, 20)])
    _run(con, fake, monkeypatch, run_id="a")
    assert _bronze(con) == 2

    # 아무것도 안 바뀐 재수집
    _run(con, fake, monkeypatch, run_id="b")
    assert _bronze(con) == 2, "변동 없는 재수집이 행을 늘렸다"

    # 1번 글만 점수 상승
    fake.items[1]["score"] = 99
    _run(con, fake, monkeypatch, run_id="c")
    assert _bronze(con) == 3, "점수 변동이 이력으로 남지 않았다"

    scores = con.execute("""
        SELECT list(try_cast(json_extract_string(payload,'$.score') AS INT) ORDER BY fetched_at)
        FROM bronze__raw_item
        WHERE source_name = 'hackernews:top' AND native_id = '1'
    """).fetchone()[0]
    assert scores == [10, 99]


def test_filters_non_stories(con, monkeypatch):
    """story 가 아니거나 dead/deleted 인 항목은 적재하지 않는다."""
    fake = FakeHN([
        _story(1, 10),
        _story(2, 20, type="job"),
        _story(3, 30, dead=True),
        _story(4, 40, deleted=True),
    ])
    r = _run(con, fake, monkeypatch)

    assert r["ids_listed"] == 4
    assert r["rows_new"] == 1, "필터링되어야 할 항목이 적재됐다"


def test_item_failure_does_not_abort(con, monkeypatch):
    """개별 item 실패가 전체 수집을 깨뜨리면 안 된다."""
    fake = FakeHN([_story(1, 10), _story(2, 20)])

    def flaky(url: str):
        if url.endswith("/2.json"):
            raise hackernews.HackerNewsAPIError("simulated")
        return fake(url)

    monkeypatch.setattr(hackernews, "_get_json", flaky)
    r = hackernews.collect(con, listing="top", run_id="t", limit=50, max_workers=2)

    assert r["rows_new"] == 1
    assert r["item_failures"] == 1
    err = con.execute(
        "SELECT error_msg FROM ops__ingest_run_log WHERE source_name='hackernews:top'"
    ).fetchone()[0]
    assert "1건 실패" in err, f"실패 건수가 run_log 에 안 남았다: {err}"
