"""다이제스트 조립·발송·결과 기록.

dbt(gold__digest_dispatch)가 '보낼 목록'까지 만들고, 여기서는 요약을 붙여
보내고 결과를 남기기만 한다. 선정 로직이 SQL 에 있어야 재현·검증이 쉽다.

발송 성공/실패를 ops__digest_delivery 에 남기는 게 중요하다. dispatch 가
'이미 보낸 문서'를 판정할 때 이 테이블을 보므로, 전송 실패한 문서는
다음날 다시 후보로 돌아온다.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import duckdb

from techradar import summarize as summarize_mod
from techradar.lake import transaction
from techradar.notify.slack import SlackNotifier


def pending_items(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """오늘 선정됐지만 아직 발송 성공하지 않은 항목.

    delivery 를 안 본 채 dispatch 만 읽으면, 재실행 시 이미 보낸 걸 또 보낸다.
    """
    rows = con.execute("""
        SELECT p.dispatch_id, p.digest_date, p.doc_id, p.source, p.title, p.url,
               p.published_at, p.best_interest_label, p.affinity, p.score,
               d.body
        FROM gold__digest_dispatch p
        JOIN silver__document d USING (doc_id)
        LEFT JOIN ops__digest_delivery v
               ON v.dispatch_id = p.dispatch_id AND v.status = 'sent'
        WHERE p.digest_date = current_date
          AND v.dispatch_id IS NULL
        ORDER BY p.source, p.rank_in_source
    """).fetchall()
    cols = [
        "dispatch_id", "digest_date", "doc_id", "source", "title", "url",
        "published_at", "best_interest_label", "affinity", "score", "body",
    ]
    return [dict(zip(cols, r)) for r in rows]


def record_delivery(
    con: duckdb.DuckDBPyConnection,
    items: list[dict[str, Any]],
    *,
    status: str,
    error_msg: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with transaction(con):
        con.executemany(
            "INSERT INTO ops__digest_delivery VALUES (?, ?, ?, ?, ?, ?)",
            [
                (it["dispatch_id"], it["doc_id"], it["digest_date"], status, now, error_msg)
                for it in items
            ],
        )


def run(
    con: duckdb.DuckDBPyConnection,
    *,
    dry_run: bool = True,
    summarize: bool = True,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """다이제스트를 만들어 보낸다.

    dry_run 이 기본값 True 인 이유: 발송은 되돌릴 수 없다. price-integrity 의
    slack_notify 도 --send 없으면 페이로드만 출력한다.
    """
    items = pending_items(con)
    if not items:
        return {"items": 0, "sent": False, "summarized": 0, "dry_run": dry_run}

    summary_failures = 0
    summarized = 0
    if summarize:
        items, summary_failures = summarize_mod.summarize_many(items)
        summarized = len(items) - summary_failures
    else:
        for it in items:
            it["summary"] = None

    # 요약이 하나도 안 붙었으면 사용자가 알아야 한다 (조용히 원문만 나가면
    # "왜 요약이 없지?" 를 며칠 뒤에 알게 된다).
    warn = list(warnings or [])
    if summarize and summary_failures == len(items):
        warn.append("요약 생성 실패 — 원문 초록으로 대체했습니다.")

    digest_date: date = items[0]["digest_date"]
    notifier = SlackNotifier()

    if dry_run:
        return {
            "items": len(items),
            "sent": False,
            "summarized": summarized,
            "dry_run": True,
            "payload": items,
            "warnings": warn,
            "notifier_enabled": notifier.enabled,
        }

    ok = notifier.send_digest(items, digest_date=digest_date, warnings=warn)
    record_delivery(
        con,
        items,
        status="sent" if ok else "failed",
        error_msg=None if ok else "Slack 전송 실패(또는 webhook 미설정)",
    )
    return {
        "items": len(items),
        "sent": ok,
        "summarized": summarized,
        "dry_run": False,
        "warnings": warn,
    }
