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
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

_SOURCE_LABEL = {"arxiv": "논문", "hackernews": "HN", "github": "GitHub"}


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
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*<{item['url']}|{item['title']}>*\n{summary}",
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
                            f"`{label}` · {item['best_interest_label']} · "
                            f"관심도 {item['affinity']:.2f} · "
                            f"{item['published_at']:%m-%d %H:%M}"
                        ),
                    }
                ],
            }
        )
    return blocks
