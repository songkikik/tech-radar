"""다이제스트 조립·발송 검증.

외부 호출(Groq, Slack)은 전부 대역으로 바꾼다. 여기서 볼 것은
'무엇을 보낼 대상으로 고르고, 결과를 어떻게 기록하는가' 뿐이다.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from techradar import digest as digest_mod
from techradar import summarize as summarize_mod
from techradar.notify import slack


@pytest.fixture
def seeded(con):
    """dispatch 2건 + 대응하는 silver 문서를 심어둔다."""
    con.execute("""
        CREATE TABLE silver__document (
            doc_id VARCHAR, source VARCHAR, title VARCHAR, body VARCHAR,
            url VARCHAR, published_at TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE gold__digest_dispatch (
            dispatch_id VARCHAR, digest_date DATE, doc_id VARCHAR, source VARCHAR,
            title VARCHAR, url VARCHAR, published_at TIMESTAMPTZ,
            best_interest_label VARCHAR, affinity DOUBLE, recency_decay DOUBLE,
            engagement_boost DOUBLE, score DOUBLE, rank_in_source INTEGER,
            selected_at TIMESTAMPTZ
        )
    """)
    now = datetime.now(timezone.utc)
    for i in (1, 2):
        con.execute(
            "INSERT INTO silver__document VALUES (?, 'arxiv', ?, ?, ?, ?)",
            [f"arxiv:{i}", f"문서 {i}", f"본문 {i}", f"https://x/{i}", now - timedelta(hours=i)],
        )
        con.execute(
            "INSERT INTO gold__digest_dispatch SELECT ?, current_date, ?, 'arxiv', ?, ?, ?, "
            "'테스트 관심사', 0.6, 0.9, 0.0, 0.54, ?, now()",
            [f"d{i}", f"arxiv:{i}", f"문서 {i}", f"https://x/{i}", now - timedelta(hours=i), i],
        )
    return con


@pytest.fixture
def no_external(monkeypatch):
    """Groq·Slack 을 호출하지 않게 막고, Slack 성공 여부만 제어한다."""
    sent: list[dict] = []

    def fake_send_digest(self, items, *, digest_date, warnings=None):
        sent.append({"items": items, "warnings": warnings})
        return fake_send_digest.ok

    fake_send_digest.ok = True
    monkeypatch.setattr(slack.SlackNotifier, "send_digest", fake_send_digest)
    monkeypatch.setattr(slack.SlackNotifier, "enabled", property(lambda self: True))
    monkeypatch.setattr(summarize_mod, "is_configured", lambda: False)
    return sent, fake_send_digest


def test_dry_run_does_not_record_delivery(seeded, no_external):
    r = digest_mod.run(seeded, dry_run=True, summarize=False)
    assert r["items"] == 2 and r["sent"] is False
    assert seeded.execute("SELECT count(*) FROM ops__digest_delivery").fetchone()[0] == 0


def test_send_records_delivery_and_is_not_resent(seeded, no_external):
    sent, _ = no_external
    r = digest_mod.run(seeded, dry_run=False, summarize=False)
    assert r["sent"] is True
    assert len(sent) == 1 and len(sent[0]["items"]) == 2

    # 재실행: 이미 발송 성공한 항목은 다시 보내지 않는다
    r2 = digest_mod.run(seeded, dry_run=False, summarize=False)
    assert r2["items"] == 0, "이미 보낸 항목을 또 보냈다"
    assert len(sent) == 1


def test_failed_send_stays_pending(seeded, no_external):
    """전송 실패한 항목은 failed 로 기록되고 다음 실행에서 다시 시도된다."""
    sent, fake = no_external
    fake.ok = False

    r = digest_mod.run(seeded, dry_run=False, summarize=False)
    assert r["sent"] is False
    statuses = seeded.execute(
        "SELECT DISTINCT status FROM ops__digest_delivery"
    ).fetchall()
    assert statuses == [("failed",)]

    fake.ok = True
    r2 = digest_mod.run(seeded, dry_run=False, summarize=False)
    assert r2["items"] == 2, "실패한 항목이 재시도 대상에서 빠졌다"
    assert r2["sent"] is True


def test_summary_failure_falls_back_to_excerpt(seeded, no_external, monkeypatch):
    """Groq 가 죽어도 다이제스트는 원문으로 나간다 — 요약은 발송 조건이 아니다."""
    sent, _ = no_external
    # TPM 제약용 sleep 은 테스트에서 순수 낭비다.
    monkeypatch.setattr(summarize_mod.time, "sleep", lambda _: None)
    monkeypatch.setattr(summarize_mod, "is_configured", lambda: True)
    monkeypatch.setattr(
        summarize_mod,
        "summarize_one",
        lambda *a, **k: (_ for _ in ()).throw(summarize_mod.SummarizerUnavailable("boom")),
    )

    r = digest_mod.run(seeded, dry_run=False, summarize=True)
    assert r["sent"] is True, "요약 실패가 발송을 막았다"
    assert r["summarized"] == 0
    assert any("요약 생성 실패" in w for w in r["warnings"])


def test_blocks_use_excerpt_when_no_summary():
    from datetime import date

    blocks = slack.build_blocks(
        [{
            "source": "arxiv", "title": "제목", "url": "https://x",
            "body": "본문 " * 200, "best_interest_label": "관심사",
            "affinity": 0.61, "published_at": datetime.now(timezone.utc),
            "summary": None,
        }],
        digest_date=date.today(),
    )
    text = "".join(str(b) for b in blocks)
    assert "본문" in text and "…" in text, "요약 없을 때 원문 발췌가 안 들어갔다"


def test_escapes_slack_special_chars():
    """제목의 & < > 가 이스케이프되고, 링크 레이블의 | 는 구분자로 안 먹혀야 한다."""
    from datetime import date

    blocks = slack.build_blocks(
        [{
            "source": "hackernews",
            "title": "Rust & Go: a <deep> dive | part 2",
            "url": "https://x?a=1&b=2",
            "body": "본문",
            "best_interest_label": "관심사",
            "affinity": 0.7,
            "published_at": datetime.now(timezone.utc),
            "summary": None,
        }],
        digest_date=date.today(),
    )
    link = blocks[2]["text"]["text"].split("\n")[0]

    assert "&amp;" in link and "&lt;deep&gt;" in link
    # <url|label> 이 정확히 한 번만 끊겨야 링크가 성립한다.
    assert link.count("|") == 1, f"링크 레이블의 | 가 구분자로 먹힌다: {link}"
    # URL 은 이스케이프 대상이 아니다 — Slack 이 링크 대상으로 그대로 읽는다.
    assert "https://x?a=1&b=2" in link


def test_displays_time_in_kst_regardless_of_source_tz():
    """UTC 로 들어와도 KST 로 표시한다 (GH Actions 러너는 UTC 라 안 하면 9시간 밀린다)."""
    from datetime import date

    utc_noon = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
    blocks = slack.build_blocks(
        [{
            "source": "arxiv", "title": "제목", "url": "https://x", "body": "본문",
            "best_interest_label": "관심사", "affinity": 0.6,
            "published_at": utc_noon, "summary": None,
        }],
        digest_date=date.today(),
    )
    context = blocks[3]["elements"][0]["text"]
    assert "09-08 21:00 KST" in context, f"KST 변환이 안 됐다: {context}"
