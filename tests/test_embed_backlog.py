"""증분 임베딩 검증.

모델을 로드하지 않는다 — encode() 를 대역으로 바꿔 '무엇을 인코딩 대상으로
고르는가'만 본다. 그게 이 Phase 의 전부이기도 하다.
"""

from __future__ import annotations

import hashlib

import pytest

from techradar import embed as embed_mod

MODEL = "fake/model-a"


@pytest.fixture
def fake_encode(monkeypatch):
    """인코딩 호출을 기록하는 대역. 벡터 내용은 검증 대상이 아니다."""
    calls: list[list[str]] = []

    def _encode(texts, *, model_name, batch_size):
        calls.append(list(texts))
        return [[0.1, 0.2, 0.3] for _ in texts]

    monkeypatch.setattr(embed_mod, "encode", _encode)
    return calls


def _add_doc(con, doc_id: str, title: str, body: str = "") -> None:
    """silver__document 를 직접 채운다 (dbt 없이 임베딩 로직만 보기 위함)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver__document (
            doc_id VARCHAR, source VARCHAR, title VARCHAR, body VARCHAR,
            content_hash VARCHAR, published_at TIMESTAMPTZ
        )
    """)
    h = hashlib.md5(f"{title}\n{body}".encode()).hexdigest()
    con.execute(
        "INSERT INTO silver__document VALUES (?, 'arxiv', ?, ?, ?, now())",
        [doc_id, title, body, h],
    )


def _create_backlog_view(con, model_name: str = MODEL) -> None:
    """dbt 뷰와 동일한 안티조인. dbt 없이 같은 계약을 재현한다."""
    con.execute(f"""
        CREATE OR REPLACE VIEW silver__embedding_backlog AS
        SELECT d.doc_id, d.source, d.title, d.body, d.content_hash, d.published_at
        FROM silver__document d
        LEFT JOIN ml__embedding e
               ON e.doc_id = d.doc_id
              AND e.content_hash = d.content_hash
              AND e.model_name = '{model_name}'
        WHERE e.doc_id IS NULL
        ORDER BY d.published_at DESC
    """)


def test_only_new_docs_are_embedded(con, fake_encode):
    _add_doc(con, "arxiv:1", "첫 문서")
    _create_backlog_view(con)

    r = embed_mod.run_backlog(con, model_name=MODEL)
    assert r["embedded"] == 1

    # 두 번째 실행: 백로그가 비었으므로 인코딩 호출 자체가 없어야 한다.
    r2 = embed_mod.run_backlog(con, model_name=MODEL)
    assert r2["embedded"] == 0
    assert len(fake_encode) == 1, "백로그가 비었는데 인코딩을 또 돌렸다"

    # 신규 문서만 추가 → 그것만 인코딩
    _add_doc(con, "arxiv:2", "둘째 문서")
    r3 = embed_mod.run_backlog(con, model_name=MODEL)
    assert r3["embedded"] == 1
    assert fake_encode[-1] == ["둘째 문서"]


def test_content_change_reenters_backlog(con, fake_encode):
    """본문이 바뀌면 무효화 로직 없이도 백로그에 자동 재등장한다."""
    _add_doc(con, "arxiv:1", "원본 제목")
    _create_backlog_view(con)
    embed_mod.run_backlog(con, model_name=MODEL)
    assert embed_mod.backlog_size(con) == 0

    # arXiv v2 개정 시나리오 — 같은 doc_id, 다른 본문
    con.execute("DELETE FROM silver__document WHERE doc_id = 'arxiv:1'")
    _add_doc(con, "arxiv:1", "개정된 제목")

    assert embed_mod.backlog_size(con) == 1, "본문 변경이 백로그에 안 잡혔다"
    r = embed_mod.run_backlog(con, model_name=MODEL)
    assert r["embedded"] == 1
    assert fake_encode[-1] == ["개정된 제목"]


def test_model_switch_is_backfill_not_wipe(con, fake_encode):
    """모델을 바꾸면 전량 삭제가 아니라 백필이 되고, 옛 임베딩은 남는다."""
    _add_doc(con, "arxiv:1", "문서")
    _create_backlog_view(con, MODEL)
    embed_mod.run_backlog(con, model_name=MODEL)

    # 리더를 B 모델로 교체
    _create_backlog_view(con, "fake/model-b")
    assert embed_mod.backlog_size(con) == 1, "모델 교체가 백필을 유발하지 않았다"
    embed_mod.run_backlog(con, model_name="fake/model-b")

    models = con.execute(
        "SELECT model_name, count(*) FROM ml__embedding GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert models == [("fake/model-a", 1), ("fake/model-b", 1)], (
        f"두 모델이 공존해야 롤백이 가능하다: {models}"
    )


def test_limit_caps_run_and_leaves_rest(con, fake_encode):
    """런당 캡에 걸려 남은 분량은 다음 실행이 집어간다 (self-healing)."""
    for i in range(5):
        _add_doc(con, f"arxiv:{i}", f"문서 {i}")
    _create_backlog_view(con)

    r = embed_mod.run_backlog(con, model_name=MODEL, limit=2)
    assert r["embedded"] == 2
    assert embed_mod.backlog_size(con) == 3

    embed_mod.run_backlog(con, model_name=MODEL, limit=2)
    embed_mod.run_backlog(con, model_name=MODEL, limit=2)
    assert embed_mod.backlog_size(con) == 0
