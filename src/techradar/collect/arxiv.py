"""arXiv 증분 수집기.

전신 프로젝트에서 이식하되 두 곳이 결정적으로 다르다.

1. **정렬 방향**: descending → **ascending**.
   descending 은 "최신 N개"를 가져오는 방식이라 커서가 성립하지 않는다. 페이징 도중
   새 논문이 등록되면 결과가 밀려서 조용히 건너뛰는 항목이 생긴다. ascending +
   submittedDate 범위 조회여야 "어디까지 처리했다"를 말할 수 있다.
2. **랜딩 위치**: 파일 → bronze 테이블. 파일 단계가 없으니 워터마크와 같은
   트랜잭션에 묶을 수 있다.

무키 접근(API 키 불필요). arXiv 정책상 User-Agent 와 요청 간 간격이 필요하다.
"""

from __future__ import annotations

import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb

from techradar import watermark as wm
from techradar.collect.base import RawItem, insert_items
from techradar.lake import transaction

ARXIV_API = "https://export.arxiv.org/api/query"
_ATOM = "{http://www.w3.org/2005/Atom}"
_UA = "tech-radar/0.1 (personal research project; +https://github.com/songkikik)"

# --------------------------------------------------------------------------
# TODO(나): 아래 두 값은 직접 근거를 세워서 정할 것.
#
# SAFETY_LAG — 상한을 now 에서 얼마나 뒤로 물릴 것인가.
#   arXiv 는 제출 시각과 API 인덱싱 시각 사이에 지연이 있다. 상한을 now 로 두면
#   "아직 인덱싱 안 된 구간"까지 커서를 전진시켜 그 구간을 영구 유실한다.
#   짧으면 유실, 길면 최신성이 떨어진다.
#
# OVERLAP — 하한을 커서에서 얼마나 소급할 것인가.
#   arXiv 는 모더레이션 때문에 과거 시각으로 뒤늦게 등장하는 항목이 있다.
#   멱등키가 중복을 흡수하므로 넉넉히 잡아도 손해는 재조회 비용뿐이다.
#   짧으면 유실, 길면 매일 같은 데이터를 다시 받는다.
#
# 판단 근거를 만드는 법: 아래 값으로 며칠 돌린 뒤
#   SELECT native_id, min(fetched_at) - event_ts AS lag
#   FROM bronze__raw_item WHERE source_name LIKE 'arxiv:%'
#   ORDER BY lag DESC LIMIT 50;
# 로 실제 인덱싱 지연 분포를 보고 p99 를 덮도록 조정한다.
SAFETY_LAG = timedelta(minutes=15)
OVERLAP = timedelta(days=2)
# --------------------------------------------------------------------------

#: 커서가 없는 최초 실행에서 얼마나 과거부터 볼 것인가.
COLD_START_WINDOW = timedelta(days=7)


class ArxivAPIError(RuntimeError):
    pass


def _http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read()
    except Exception as exc:  # noqa: BLE001
        raise ArxivAPIError(f"arXiv 요청 실패: {exc}") from exc


def _fmt(ts: datetime) -> str:
    """arXiv submittedDate 필터 포맷: YYYYMMDDHHMM (UTC)."""
    return ts.astimezone(timezone.utc).strftime("%Y%m%d%H%M")


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _split_version(arxiv_id: str) -> tuple[str, str | None]:
    """'2601.00001v2' → ('2601.00001', 'v2').

    버전 접미사를 native_id 에서 떼어내야 개정판이 새 문서가 아니라
    같은 문서의 갱신으로 취급된다.
    """
    base, sep, ver = arxiv_id.rpartition("v")
    if sep and ver.isdigit():
        return base, f"v{ver}"
    return arxiv_id, None


def _parse_feed(xml_bytes: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    out: list[dict[str, Any]] = []
    for e in root.findall(f"{_ATOM}entry"):
        abs_id = (e.findtext(f"{_ATOM}id") or "").strip()
        raw_id = abs_id.rsplit("/", 1)[-1]
        if not raw_id:
            continue
        native_id, version = _split_version(raw_id)
        pdf_url = next(
            (
                link.get("href")
                for link in e.findall(f"{_ATOM}link")
                if link.get("title") == "pdf"
            ),
            None,
        )
        out.append(
            {
                "source": "arxiv",
                "arxiv_id": native_id,
                "version": version,
                "title": " ".join((e.findtext(f"{_ATOM}title") or "").split()),
                "summary": (e.findtext(f"{_ATOM}summary") or "").strip(),
                "authors": [
                    (a.findtext(f"{_ATOM}name") or "").strip()
                    for a in e.findall(f"{_ATOM}author")
                ],
                "categories": [
                    c.get("term")
                    for c in e.findall(f"{_ATOM}category")
                    if c.get("term")
                ],
                "published": e.findtext(f"{_ATOM}published"),
                "updated": e.findtext(f"{_ATOM}updated"),
                "abs_url": abs_id,
                "pdf_url": pdf_url,
            }
        )
    return out


def _fetch_window(
    category: str, lower: datetime, upper: datetime, page_size: int, max_pages: int
) -> tuple[list[RawItem], bool]:
    """[lower, upper] 구간을 오래된 것부터 페이징한다.

    반환: (아이템들, 구간을 끝까지 소진했는가).
    소진하지 못했으면(max_pages 도달) 커서를 upper 가 아니라 '실제로 받은 마지막
    항목'까지만 전진시켜야 나머지가 다음 실행에서 이어진다.
    """
    query = f"cat:{category} AND submittedDate:[{_fmt(lower)} TO {_fmt(upper)}]"
    items: list[RawItem] = []
    exhausted = False

    for page in range(max_pages):
        params = {
            "search_query": query,
            "start": page * page_size,
            "max_results": page_size,
            "sortBy": "submittedDate",
            "sortOrder": "ascending",
        }
        entries = _parse_feed(
            _http_get(f"{ARXIV_API}?{urllib.parse.urlencode(params)}")
        )
        for entry in entries:
            items.append(
                RawItem(
                    native_id=entry["arxiv_id"],
                    payload=entry,
                    event_ts=_parse_ts(entry.get("published")),
                )
            )
        if len(entries) < page_size:
            exhausted = True
            break
        time.sleep(1.0)  # arXiv 정책: 요청 간 간격

    return items, exhausted


def collect(
    con: duckdb.DuckDBPyConnection,
    *,
    category: str,
    run_id: str,
    page_size: int = 100,
    max_pages: int = 10,
    backfill_pages: int = 1,
) -> dict[str, Any]:
    """한 카테고리를 증분 수집한다.

    조회를 두 개로 나눈다 — 이게 커서 정체를 구조적으로 막는 핵심이다.

      전방 [cursor, upper]           커서 전진의 **유일한** 근거.
                                     항상 cursor 에서 시작하므로 max(seen) >= cursor 가
                                     보장되고, 따라서 진행이 멈출 수 없다.
      소급 [cursor-OVERLAP, cursor]  모더레이션 지연으로 과거 시각에 뒤늦게 등장한
                                     항목 회수 전용. 커서에 일절 관여하지 않는다.

    둘을 한 쿼리로 합치면(= lower 를 cursor 뒤로 물리면) 오름차순 페이징 예산이
    소급 구간에서 다 소진돼 커서가 영원히 안 움직인다. 실제로 그렇게 짰다가
    3회 실행 만에 정체를 재현했다.

    네트워크 I/O 는 트랜잭션 **밖**에서 끝낸다. 수 분짜리 HTTP 왕복 동안
    트랜잭션을 열어두면 스냅샷 충돌 확률만 올라간다.
    """
    source_name = f"arxiv:{category}"
    started_at = wm.now_utc()

    wm.ensure(con, source_name)
    cursor = wm.read_cursor(con, source_name)

    upper = wm.now_utc() - SAFETY_LAG
    fwd_lower = cursor if cursor else (upper - COLD_START_WINDOW)

    try:
        fwd_items, fwd_exhausted = _fetch_window(
            category, fwd_lower, upper, page_size, max_pages
        )
        # 최초 실행(커서 없음)에는 소급할 대상이 없다.
        back_items: list[RawItem] = []
        if cursor and backfill_pages > 0:
            back_items, _ = _fetch_window(
                category, cursor - OVERLAP, cursor, page_size, backfill_pages
            )
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

    items = fwd_items + back_items

    # 커서 전진 근거는 전방 항목뿐이다. 소급 항목을 섞으면 커서가 과거에 붙잡힌다.
    #
    # 전방을 끝까지 소진했으면 upper 까지 처리 완료가 확정이므로 upper 로 점프한다.
    # (upper 는 이미 SAFETY_LAG 만큼 물러나 있고, 그 아래 늦게 도착하는 항목은
    #  다음 실행의 소급 조회가 잡는다.)
    # 소진하지 못했으면 규칙 2대로 '실제로 처리한' 마지막 이벤트 시각까지만.
    fwd_seen = [it.event_ts for it in fwd_items if it.event_ts]
    if fwd_exhausted:
        new_cursor = upper
    elif fwd_seen:
        new_cursor = max(fwd_seen)
    else:
        new_cursor = None

    # 정체 감지: 전방 조회를 하고도 커서가 안 움직였다면 무증상 실패다.
    # 한 런의 용량(page_size × max_pages)보다 같은 시각의 항목이 많다는 뜻이므로
    # 사람이 용량을 올려야 풀린다. Phase 6 의 DQ 체크가 이 신호를 읽는다.
    stalled = bool(
        cursor and new_cursor and new_cursor <= cursor and not fwd_exhausted
    )

    with transaction(con):
        rows_new = insert_items(
            con,
            source_name=source_name,
            items=items,
            run_id=run_id,
            fetched_at=started_at,
        )
        wm.advance(con, source_name, new_cursor, run_id)
        after = wm.read_cursor(con, source_name)
        wm.record_run(
            con,
            run_id=run_id,
            source_name=source_name,
            started_at=started_at,
            status="stalled" if stalled else "ok",
            rows_fetched=len(items),
            rows_new=rows_new,
            watermark_before=cursor,
            watermark_after=after,
            error_msg="커서 정체: 런 용량 < 동일 시각 항목 수" if stalled else None,
        )

    return {
        "source_name": source_name,
        "forward_window": (fwd_lower, upper),
        "backfill_window": (cursor - OVERLAP, cursor) if back_items else None,
        "exhausted": fwd_exhausted,
        "stalled": stalled,
        "rows_fetched": len(items),
        "rows_forward": len(fwd_items),
        "rows_backfill": len(back_items),
        "rows_new": rows_new,
        "cursor_before": cursor,
        "cursor_after": after,
    }
