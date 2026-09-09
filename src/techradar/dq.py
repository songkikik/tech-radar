"""DQ 판정 결과를 다이제스트 배너용 문장으로 바꾼다.

dbt 쪽 분업을 그대로 이어받는다:
  dq__check_result   판정 (멱등)
  dq__alert_dispatch 통보 대상 선별 + cooldown (비멱등, append-only)
  여기                 이미 선별된 것을 **읽어서 사람 문장으로 만들 뿐**

즉 "무엇을 알릴지" 는 SQL 이 정하고 파이썬은 렌더링만 한다. 판정 로직이 파이썬에
섞이면 재현·검증이 어려워지고, cooldown 상태가 두 곳에 생겨 어긋난다.
"""

from __future__ import annotations

import duckdb

#: 배너에 나열할 경고 최대 건수. 넘치면 "외 N건" 으로 접는다.
#:
#: 상한을 두는 이유는 Slack 블록 한도(50)보다 **가독성** 쪽이 크다. 경고가 10줄이면
#: 본문을 밀어내고, 그러면 다이제스트를 안 읽게 된다. 배너는 "지금 뭔가 이상하다"를
#: 알리는 것이 목적이고, 상세 조회는 `techradar status` / dq 테이블의 몫이다.
MAX_LISTED = 5

_SEVERITY_LABEL = {"critical": "심각", "warn": "경고"}


def pending_warnings(con: duckdb.DuckDBPyConnection) -> list[str]:
    """오늘 통보 대상으로 선별된 DQ 경고를 배너 문장 리스트로 돌려준다.

    dq__alert_dispatch 를 읽는 것이지 dq__check_result 를 읽는 것이 아니다.
    check_result 를 직접 읽으면 cooldown 이 무시돼 같은 경고가 매일 붙는다.

    다이제스트 재실행 시 같은 문장이 다시 나오는 것은 의도된 동작이다 —
    발송이 실패해 재시도하는 경우 배너도 같이 가야 한다.
    """
    rows = con.execute("""
        SELECT DISTINCT severity, check_name, source_name, detail
        FROM dq__alert_dispatch
        WHERE alerted_at::date = current_date
        ORDER BY CASE severity WHEN 'critical' THEN 0 ELSE 1 END,
                 check_name, source_name
    """).fetchall()

    lines = [
        f"[{_SEVERITY_LABEL.get(sev, sev)}] {check} · {source} — {detail}"
        for sev, check, source, detail in rows[:MAX_LISTED]
    ]
    if len(rows) > MAX_LISTED:
        lines.append(f"…외 {len(rows) - MAX_LISTED}건 (`techradar status` 또는 dq__alert_dispatch 참고)")

    # 판정 불가는 통보 대상이 아니지만(초기에는 대부분이 그 상태라 매일 알리면
    # 아무도 안 본다) 몇 건인지는 알려준다. 0 이 되는 날이 파이프라인이 성숙한
    # 시점이라, 이 숫자 자체가 진행 지표다.
    (unknown,) = con.execute("""
        SELECT count(*) FROM dq__check_result
        WHERE check_date = current_date AND status = 'insufficient_data'
    """).fetchone()
    if unknown:
        lines.append(f"판정 불가 {unknown}건 (기준선 미확보 — 이력이 쌓이면 자동 해소)")

    return lines
