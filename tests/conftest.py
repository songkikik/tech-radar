"""테스트 공통 픽스처."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from techradar import lake
from techradar.collect.base import RawItem
from techradar.config import Target


@pytest.fixture
def con(tmp_path):
    """테스트마다 독립된 DuckLake (로컬 카탈로그 + tmp 디렉토리)."""
    data_dir = tmp_path / "lakehouse"
    data_dir.mkdir()
    target = Target(
        name="test",
        catalog_uri=f"ducklake:{tmp_path / 'catalog.ducklake'}",
        data_path=f"{data_dir}/",
    )
    connection = lake.connect(target)
    lake.bootstrap(connection)
    yield connection
    connection.close()


class FakeArxiv:
    """arXiv API 대역. _fetch_window 과 동일한 계약을 지킨다.

    - [lower, upper] 를 event_ts 오름차순으로 반환
    - page_size × max_pages 를 용량 상한으로 자름
    - 잘렸으면 exhausted=False
    """

    def __init__(self, items: list[tuple[str, datetime]]):
        self.items = sorted(items, key=lambda x: x[1])
        self.calls: list[tuple[datetime, datetime]] = []
        #: 설정하면 payload 에 병합된다 — 같은 아이템의 '내용 변경'을 흉내낸다.
        self.mutate_payload: dict | None = None

    def fetch(self, category, lower, upper, page_size, max_pages):
        self.calls.append((lower, upper))
        capacity = page_size * max_pages
        window = [(i, t) for i, t in self.items if lower <= t <= upper]
        exhausted = len(window) <= capacity
        return [
            RawItem(
                native_id=native_id,
                payload={
                    "arxiv_id": native_id,
                    "published": ts.isoformat(),
                    **(self.mutate_payload or {}),
                },
                event_ts=ts,
            )
            for native_id, ts in window[:capacity]
        ], exhausted


@pytest.fixture
def base_time():
    """SAFETY_LAG 를 넉넉히 벗어난 기준 시각 (모든 가짜 항목이 upper 아래에 오도록)."""
    return datetime.now(timezone.utc) - timedelta(hours=6)
