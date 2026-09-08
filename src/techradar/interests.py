"""관심 토픽 프로필 로드·임베딩.

문서 임베딩과 같은 모델·같은 벡터 공간을 쓴다. 다른 모델로 임베딩한 질의와
문서를 비교하면 유사도가 무의미해지므로, model_name 을 grain 에 넣어
불일치를 구조적으로 막는다.

관심사는 문서와 달리 수가 적고(수십 개) 자주 안 바뀌므로 증분 로직이 필요 없다.
text_hash 가 바뀐 항목만 다시 임베딩하고, 없어진 항목은 지운다.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import yaml

from techradar.config import REPO_ROOT
from techradar.embed import DEFAULT_MODEL, encode
from techradar.lake import transaction

DEFAULT_PROFILE = REPO_ROOT / "profiles" / "interests.yml"


def load_profile(path: Path | None = None) -> list[dict[str, str]]:
    raw = yaml.safe_load((path or DEFAULT_PROFILE).read_text(encoding="utf-8"))
    items = raw.get("interests") or []
    if not items:
        raise ValueError("interests.yml 에 항목이 없습니다.")
    for it in items:
        it["text"] = " ".join(it["text"].split())  # YAML 접힘 줄바꿈 정리
    return items


def sync(
    con: duckdb.DuckDBPyConnection,
    *,
    model_name: str = DEFAULT_MODEL,
    path: Path | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """프로필을 테이블과 동기화한다.

    rebuild=True 면 해당 모델의 관심사 임베딩을 전부 지우고 다시 만든다
    (모델을 바꿨거나 프리픽스 전략을 바꿨을 때).
    """
    items = load_profile(path)
    now = datetime.now(timezone.utc)

    for it in items:
        it["text_hash"] = hashlib.md5(it["text"].encode()).hexdigest()

    if rebuild:
        with transaction(con):
            con.execute("DELETE FROM ml__interest_embedding WHERE model_name = ?", [model_name])

    existing = {
        k: h
        for k, h in con.execute(
            "SELECT interest_key, text_hash FROM ml__interest_embedding WHERE model_name = ?",
            [model_name],
        ).fetchall()
    }

    stale = [it for it in items if existing.get(it["key"]) != it["text_hash"]]
    removed = set(existing) - {it["key"] for it in items}

    if not stale and not removed:
        return {"total": len(items), "embedded": 0, "removed": 0, "model": model_name}

    vectors = encode([it["text"] for it in stale], model_name=model_name, batch_size=8) if stale else []

    with transaction(con):
        for key in removed:
            con.execute(
                "DELETE FROM ml__interest_embedding WHERE model_name = ? AND interest_key = ?",
                [model_name, key],
            )
        for it, vec in zip(stale, vectors):
            # 같은 키의 옛 벡터를 지우고 새로 넣는다 (관심사는 이력이 필요 없다).
            con.execute(
                "DELETE FROM ml__interest_embedding WHERE model_name = ? AND interest_key = ?",
                [model_name, it["key"]],
            )
            con.execute(
                "INSERT INTO ml__interest_embedding VALUES (?, ?, ?, ?, ?, ?, ?)",
                [it["key"], it["label"], model_name, it["text_hash"], len(vec), vec, now],
            )

    return {
        "total": len(items),
        "embedded": len(stale),
        "removed": len(removed),
        "model": model_name,
    }
