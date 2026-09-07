"""Hacker News 수집기.

arXiv 와 근본적으로 다르다: **HN 은 mutating 소스라 커서가 없다.**

topstories/beststories 는 "지금 이 순간의 상위 500개"이고, 각 글의 score 와
descendants(댓글 수)는 계속 변한다. 커서로 "여기까지 처리했다"를 선언하는 순간
이미 지나간 글의 점수 폭발을 영영 못 본다 — 그런데 그 점수 폭발이야말로
트렌드 신호의 핵심이다. 그래서 매 실행 전량을 다시 받아 upsert 한다.

멱등키에 payload_hash 가 들어있으므로, 점수가 안 바뀐 글은 재수집해도 행이
늘지 않고 바뀐 글만 새 행이 된다. 그 결과 bronze 에 점수 변동 이력이 자연히 쌓인다.

무키 접근(API 키 불필요). Firebase 엔드포인트라 rate limit 문서화된 게 없어
동시성만 제한한다.
"""

from __future__ import annotations

import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import duckdb

from techradar import watermark as wm
from techradar.collect.base import RawItem, insert_items
from techradar.lake import transaction

HN_API = "https://hacker-news.firebaseio.com/v0"
_UA = "tech-radar/0.1 (personal research project; +https://github.com/songkikik)"

LISTINGS = ("top", "best", "new")


class HackerNewsAPIError(RuntimeError):
    pass


def _get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as exc:  # noqa: BLE001
        raise HackerNewsAPIError(f"HN 요청 실패 ({url}): {exc}") from exc


def _fetch_ids(listing: str, limit: int) -> list[int]:
    if listing not in LISTINGS:
        raise ValueError(f"알 수 없는 listing: {listing!r} ({'|'.join(LISTINGS)})")
    return (_get_json(f"{HN_API}/{listing}stories.json") or [])[:limit]


def _fetch_item(item_id: int) -> dict[str, Any] | None:
    """개별 글. 실패는 None 으로 흘린다.

    한 건 실패가 전체 수집을 깨뜨리면 안 된다 — 500건 중 1건 때문에
    나머지 499건을 못 받는 건 손해가 크다. 실패 수는 호출부가 집계해
    run_log 에 남긴다.
    """
    try:
        return _get_json(f"{HN_API}/item/{item_id}.json")
    except HackerNewsAPIError:
        return None


def _fetch_items(ids: list[int], max_workers: int) -> tuple[list[RawItem], int]:
    items: list[RawItem] = []
    failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for raw in pool.map(_fetch_item, ids):
            if raw is None:
                failed += 1
                continue
            # Ask/Show/Job 등은 지금 범위 밖. story 만 받는다.
            if raw.get("type") != "story" or raw.get("dead") or raw.get("deleted"):
                continue
            items.append(
                RawItem(
                    native_id=str(raw["id"]),
                    payload={
                        "source": "hackernews",
                        "hn_id": raw["id"],
                        "title": raw.get("title"),
                        "url": raw.get("url"),
                        "text": raw.get("text"),
                        "by": raw.get("by"),
                        "score": raw.get("score"),
                        "descendants": raw.get("descendants"),
                        "time": raw.get("time"),
                    },
                    event_ts=(
                        datetime.fromtimestamp(raw["time"], tz=timezone.utc)
                        if raw.get("time")
                        else None
                    ),
                )
            )
    return items, failed


def collect(
    con: duckdb.DuckDBPyConnection,
    *,
    listing: str = "top",
    run_id: str,
    limit: int = 200,
    max_workers: int = 16,
) -> dict[str, Any]:
    """한 listing 을 전량 수집한다.

    커서를 전진시키지 않는다(new_cursor=None). 워터마크 행은 그래도 유지하는데,
    consecutive_fail 과 last_success_at 이 Phase 6 의 DQ 체크에 필요하기 때문이다.
    """
    source_name = f"hackernews:{listing}"
    started_at = wm.now_utc()

    wm.ensure(con, source_name)
    cursor = wm.read_cursor(con, source_name)

    try:
        ids = _fetch_ids(listing, limit)
        items, failed = _fetch_items(ids, max_workers)
    except Exception as exc:  # noqa: BLE001
        with transaction(con):
            wm.record_failure(con, source_name, run_id)
            wm.record_run(
                con,
                run_id=run_id,
                source_name=source_name,
                started_at=started_at,
                status="error",
                watermark_before=cursor,
                watermark_after=cursor,
                error_msg=f"{type(exc).__name__}: {exc}",
            )
        raise

    with transaction(con):
        rows_new = insert_items(
            con,
            source_name=source_name,
            items=items,
            run_id=run_id,
            fetched_at=started_at,
        )
        # 커서 없음 — 성공 시각과 연속실패 카운트만 갱신한다.
        wm.advance(con, source_name, None, run_id)
        wm.record_run(
            con,
            run_id=run_id,
            source_name=source_name,
            started_at=started_at,
            status="ok",
            rows_fetched=len(items),
            rows_new=rows_new,
            error_msg=f"item {failed}건 실패" if failed else None,
        )

    return {
        "source_name": source_name,
        "ids_listed": len(ids),
        "rows_fetched": len(items),
        "rows_new": rows_new,
        "item_failures": failed,
    }
