"""다이제스트 항목 요약 (Groq 무료 티어).

요약은 **부가물**이지 발송 조건이 아니다. Groq 가 죽거나 rate limit 에 걸려도
다이제스트는 원문 초록으로 나가야 한다. data-pipeline 의 slack_notify 가
"webhook 미설정 시 파이프라인은 계속 동작"으로 잡아둔 것과 같은 원칙이다.

Groq 는 OpenAI 호환 API 라 base_url 만 바꾸면 다른 공급자로 갈아탈 수 있다.
무료 티어에 의존하는 이상 이 교체 가능성은 사치가 아니라 필수다.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

#: 무료 티어에서 일일 토큰 한도가 가장 넉넉한 축(20만/일). 다른 모델로 바꾸려면
#: GROQ_MODEL env 로 덮어쓴다. ⚠️ 정확한 모델 ID 는 console.groq.com 에서 확인 필요.
DEFAULT_MODEL = "openai/gpt-oss-120b"

#: 무료 티어 제약은 TPM 6000 이 유일한 병목이다. 요청당 ~900 토큰으로 잡으면
#: 분당 6.6건이 상한 → 요청 간 9초. 다이제스트 8건이면 ~70초로 끝난다.
DEFAULT_PACE_SECONDS = 9.0

_SYSTEM = (
    "너는 데이터 엔지니어를 위한 기술 큐레이터다. 주어진 문서를 한국어로 요약한다.\n"
    "형식을 반드시 지켜라:\n"
    "1줄차: 무엇에 대한 글인지 한 문장.\n"
    "2줄차: 핵심 내용 한 문장.\n"
    "3줄차: '왜 중요한가:' 로 시작해 데이터 파이프라인·분석 실무 관점에서 한 문장.\n"
    "과장하지 말고, 원문에 없는 내용을 지어내지 마라. 불확실하면 그렇게 써라."
)


class SummarizerUnavailable(RuntimeError):
    """요약을 못 하는 상태. 호출부는 이걸 잡아 원문 폴백으로 넘어간다."""


def is_configured() -> bool:
    return bool(os.environ.get("GROQ_API_KEY"))


def _call(prompt: str, *, model: str, timeout: int) -> str:
    body = json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 300,
        }
    ).encode()
    req = urllib.request.Request(
        GROQ_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read())
    return payload["choices"][0]["message"]["content"].strip()


def summarize_one(
    title: str,
    body: str | None,
    *,
    model: str | None = None,
    timeout: int = 30,
) -> str:
    if not is_configured():
        raise SummarizerUnavailable("GROQ_API_KEY 미설정")
    excerpt = (body or "")[:3000]
    prompt = f"제목: {title}\n\n본문:\n{excerpt}"
    try:
        return _call(prompt, model=model or os.environ.get("GROQ_MODEL", DEFAULT_MODEL), timeout=timeout)
    except urllib.error.HTTPError as exc:
        detail = exc.read()[:300].decode("utf-8", "replace")
        raise SummarizerUnavailable(f"HTTP {exc.code}: {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise SummarizerUnavailable(f"{type(exc).__name__}: {exc}") from exc


def summarize_many(
    items: list[dict[str, Any]],
    *,
    model: str | None = None,
    pace_seconds: float = DEFAULT_PACE_SECONDS,
) -> tuple[list[dict[str, Any]], int]:
    """항목마다 summary 를 채운다. 실패한 항목은 summary=None 으로 남긴다.

    한 건 실패가 나머지를 막지 않는다 — 8건 중 3건이 실패해도 5건은 요약이 붙고
    3건은 원문 초록으로 나가는 게, 전체를 원문으로 보내는 것보다 낫다.
    반환: (항목들, 실패 수)
    """
    failures = 0
    for i, item in enumerate(items):
        if not is_configured():
            item["summary"] = None
            failures += 1
            continue
        try:
            item["summary"] = summarize_one(item["title"], item.get("body"), model=model)
        except SummarizerUnavailable:
            item["summary"] = None
            failures += 1
        if i < len(items) - 1 and pace_seconds:
            time.sleep(pace_seconds)  # TPM 6000 제약
    return items, failures
