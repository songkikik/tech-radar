"""Slack 다이제스트 발송.

data-pipeline/ingest/naver_monitor/notify/slack.py 의 SlackNotifier 구조를 이식했다.
핵심 두 가지를 그대로 따른다:
  - webhook 미설정이면 조용히 skip 하고 파이프라인은 계속 간다(예외로 안 깬다)
  - 전송 실패가 호출부를 깨뜨리지 않는다

메시지 포맷 로직을 호출부에서 완전히 분리해 두면, 나중에 채널을 Discord 나
Telegram 으로 바꿔도 build_blocks 만 갈아끼우면 된다.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_SOURCE_LABEL = {"arxiv": "논문", "hackernews": "HN", "github": "GitHub"}

#: 다이제스트를 읽는 사람이 한국에 있으므로 표시 시각은 KST 로 고정한다.
#:
#: 명시적으로 변환하지 않으면 DuckDB 가 돌려준 TIMESTAMPTZ 의 tzinfo 를 그대로
#: 따라가는데, 그건 세션 타임존(= 시스템 타임존)에서 온다. 맥에서는 KST 라 맞아
#: 보이지만 GitHub Actions 러너는 UTC 이므로 배포하면 9시간 밀린 시각이 찍힌다.
#: 에러 없이 조용히 틀린다.
_KST = ZoneInfo("Asia/Seoul")


def _escape_mrkdwn(text: str) -> str:
    """Slack mrkdwn 특수문자를 이스케이프한다.

    Slack 은 &, <, > 를 HTML 엔티티로 받는다. 이스케이프하지 않으면 제목에 이런
    문자가 든 항목에서 링크가 깨지는데, 에러가 아니라 그냥 이상하게 보이는 거라
    알아채기 어렵다.

    & 를 먼저 치환해야 한다. 나중에 하면 앞서 만든 &lt; 의 & 까지 다시 치환된다.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_link_label(text: str) -> str:
    """<url|label> 의 label 용 이스케이프.

    label 안의 | 는 Slack 이 구분자로 먹어서 링크가 잘린다. 제거하면 제목이
    어색해지므로 전각 세로줄(U+FF5C)로 바꾼다 — 눈으로는 거의 같아 보인다.
    """
    return _escape_mrkdwn(text).replace("|", "｜")


class SlackNotifier:
    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL", "")

    @property
    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def send(self, text: str, blocks: list[dict] | None = None) -> bool:
        if not self.enabled:
            logger.warning("[slack] SLACK_WEBHOOK_URL 미설정 — 발송 skip")
            return False
        payload: dict[str, Any] = {"text": text}
        if blocks:
            payload["blocks"] = blocks
        req = urllib.request.Request(
            self.webhook_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status == 200
        except Exception as exc:  # noqa: BLE001
            # 알림 전송 실패가 파이프라인을 깨지 않도록 격리한다.
            logger.error("[slack] 전송 실패: %s", exc)
            return False

    def send_digest(
        self, items: list[dict[str, Any]], *, digest_date: date, warnings: list[str] | None = None
    ) -> bool:
        return self.send(
            text=f"오늘의 기술 레이더 {len(items)}건 ({digest_date})",
            blocks=build_blocks(items, digest_date=digest_date, warnings=warnings),
        )


def _fallback_excerpt(body: str | None, limit: int = 220) -> str:
    """요약이 없을 때 원문 앞부분으로 대신한다."""
    text = " ".join((body or "").split())
    if not text:
        return "_(본문 없음 — 링크 참고)_"
    return text[:limit] + ("…" if len(text) > limit else "")


def build_blocks(
    items: list[dict[str, Any]], *, digest_date: date, warnings: list[str] | None = None
) -> list[dict]:
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"오늘의 기술 레이더 · {digest_date}"},
        }
    ]

    # DQ 경고는 항목보다 위에 둔다. 아래에 두면 안 읽는다.
    if warnings:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "⚠️ " + "\n⚠️ ".join(warnings)},
            }
        )

    for item in items:
        label = _SOURCE_LABEL.get(item["source"], item["source"])
        summary = item.get("summary") or _fallback_excerpt(item.get("body"))
        # URL 은 이스케이프하지 않는다 — Slack 이 링크 대상으로 그대로 읽는다.
        # 이스케이프 대상은 사람이 읽는 텍스트뿐이다.
        title = _escape_link_label(item["title"])
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*<{item['url']}|{title}>*\n{_escape_mrkdwn(summary)}",
                },
            }
        )
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"`{label}` · {_escape_mrkdwn(item['best_interest_label'])} · "
                            f"관심도 {item['affinity']:.2f} · "
                            f"{_kst(item['published_at'])} KST"
                        ),
                    }
                ],
            }
        )
    return blocks


def _kst(ts: datetime) -> str:
    return ts.astimezone(_KST).strftime("%m-%d %H:%M")
