"""증분 임베딩.

dbt 뷰 silver__embedding_backlog 가 '아직 임베딩 안 된 문서'를 알려주고,
여기서는 그것만 인코딩해 ml__embedding 에 append 한다.

전신 프로젝트는 매 실행 documents.parquet 전량을 재인코딩했다. 문서가 300건일 땐
견디지만 3만 건이 되면 GH Actions 러너에서 못 돈다. 그보다 중요한 건, 전량 재계산은
'무엇이 바뀌었는지'를 아예 묻지 않는다는 점이다 — 그 질문에 답할 수 있으면
증분이 되고, 못 하면 배치가 된다.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Sequence

import duckdb

from techradar.lake import transaction

#: dbt var embed_model 과 반드시 같은 값이어야 한다. 다르면 백로그가 영원히
#: 비지 않는다(백로그는 A 모델로 조회하는데 적재는 B 모델 이름으로 되므로).
DEFAULT_MODEL = "Qwen/Qwen3-Embedding-0.6B"


@lru_cache(maxsize=2)
def _load_model(model_name: str):
    """모델을 지연 로드한다. import 자체가 무거워 함수 안에서 한다."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "sentence-transformers 가 없습니다. `uv sync --extra embed` 로 설치하세요."
        ) from exc
    return SentenceTransformer(model_name)


def encode(texts: Sequence[str], *, model_name: str, batch_size: int) -> list[list[float]]:
    """텍스트를 L2 정규화된 벡터로 인코딩한다.

    정규화해두면 코사인 유사도가 내적과 같아져서, 조회 쿼리에서 노름 계산을
    생략할 수 있다. 테스트는 이 함수를 대역으로 바꿔 모델 없이 로직을 검증한다.
    """
    model = _load_model(model_name)
    vectors = model.encode(
        list(texts),
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return [[float(x) for x in row] for row in vectors]


# --------------------------------------------------------------------------
# TODO(나): 질의 프리픽스를 붙일지 결정할 것.
#
# Qwen3-Embedding 은 **질의 쪽에만** 지시문 프리픽스를 붙이는 걸 권장한다
# (문서 쪽은 그대로):
#     Instruct: {task 설명}\nQuery: {질의}
#
# 지금은 안 붙이고 있는데, 실측 검색 점수가 0.616~0.628 로 지나치게 압축돼 있다.
# 상위 3건과 하위권의 차이가 0.01 남짓이면 Phase 5 에서 "유사도 임계값 이상만
# 다이제스트에 넣는다" 같은 규칙을 세울 수가 없다.
#
# 판단해야 할 것:
#   - 프리픽스를 붙였을 때 점수 분산이 실제로 벌어지는가 (같은 질의 3~5개로 비교)
#   - 붙인다면 task 설명을 뭐라고 쓸 것인가 (예: "주어진 관심 주제와 의미가
#     가까운 최신 기술 문서를 찾아라")
#   - 붙이면 문서 임베딩은 재계산이 필요 없다(질의 쪽만 바뀜) — 이게 이 결정을
#     나중에 뒤집기 쉽게 만든다. 즉 지금 안 정해도 손해가 없다.
# --------------------------------------------------------------------------


def _embed_text(title: str | None, body: str | None) -> str:
    """임베딩에 넣을 텍스트.

    silver__document.content_hash 가 md5(title + '\\n' + body) 이므로 여기도
    같은 조합이어야 한다. 어긋나면 '본문이 바뀌었는데 재임베딩 안 되는' 버그가 난다.
    """
    return f"{title or ''}\n{body or ''}".strip()


def run_backlog(
    con: duckdb.DuckDBPyConnection,
    *,
    model_name: str = DEFAULT_MODEL,
    batch_size: int = 32,
    limit: int | None = None,
) -> dict[str, Any]:
    """백로그를 읽어 인코딩하고 append 한다.

    상태를 저장하지 않는다. 중간에 죽으면 커밋된 만큼만 남고, 나머지는 다음 실행에서
    여전히 백로그에 있다 → self-healing. 워터마크가 필요 없는 이유가 이것이다.
    """
    started_at = datetime.now(timezone.utc)

    sql = "SELECT doc_id, title, body, content_hash FROM silver__embedding_backlog"
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = con.execute(sql).fetchall()

    if not rows:
        return {"backlog": 0, "embedded": 0, "model": model_name, "dim": None}

    texts = [_embed_text(title, body) for _, title, body, _ in rows]
    vectors = encode(texts, model_name=model_name, batch_size=batch_size)
    dim = len(vectors[0])

    with transaction(con):
        con.executemany(
            "INSERT INTO ml__embedding VALUES (?, ?, ?, ?, ?, ?)",
            [
                (doc_id, model_name, content_hash, dim, vec, started_at)
                for (doc_id, _, _, content_hash), vec in zip(rows, vectors)
            ],
        )

    return {
        "backlog": len(rows),
        "embedded": len(vectors),
        "model": model_name,
        "dim": dim,
        "seconds": (datetime.now(timezone.utc) - started_at).total_seconds(),
    }


def backlog_size(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute("SELECT count(*) FROM silver__embedding_backlog").fetchone()[0]


def similar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    *,
    model_name: str = DEFAULT_MODEL,
    top_k: int = 10,
) -> list[tuple]:
    """질의와 의미가 가까운 문서를 찾는다 (ad-hoc 검증용).

    벡터를 L2 정규화해 저장했으므로 코사인 = 내적이다. DuckDB 의
    array_cosine_similarity 는 고정 크기 ARRAY 를 요구하는데 vec 은 FLOAT[]
    가변 리스트이므로, 저장해둔 dim 을 읽어 조회 시점에 캐스팅한다.

    Phase 5 의 랭킹은 이 유사도에 recency 와 engagement 를 곱해 gold 에서 만든다.
    여기서는 임베딩 자체가 쓸 만한지만 본다.
    """
    dim = con.execute(
        "SELECT dim FROM ml__embedding WHERE model_name = ? LIMIT 1", [model_name]
    ).fetchone()
    if dim is None:
        return []
    dim = dim[0]

    qvec = encode([query], model_name=model_name, batch_size=1)[0]
    return con.execute(
        f"""
        SELECT d.source,
               d.title,
               array_cosine_similarity(e.vec::FLOAT[{dim}], ?::FLOAT[{dim}]) AS score
        FROM ml__embedding e
        JOIN silver__document d USING (doc_id)
        WHERE e.model_name = ?
          AND e.content_hash = d.content_hash
        ORDER BY score DESC
        LIMIT ?
        """,
        [qvec, model_name, top_k],
    ).fetchall()
