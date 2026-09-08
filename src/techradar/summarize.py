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
    "정확히 세 줄로만 답하라. 각 줄은 완결된 문장이고, 줄 사이는 줄바꿈으로 구분한다.\n"
    "  첫째 줄: 무엇에 대한 글인지.\n"
    "  둘째 줄: 핵심 내용.\n"
    "  셋째 줄: '왜 중요한가: ' 로 시작해 데이터 파이프라인·분석 실무 관점에서.\n"
    "\n"
    "'첫째 줄', '1줄차' 같은 형식 지시어나 번호, 불릿을 출력에 포함하지 마라.\n"
    "셋째 줄의 '왜 중요한가: ' 만 예외로 그대로 쓴다.\n"
    "과장하지 말고, 원문에 없는 내용을 지어내지 마라. 불확실하면 그렇게 써라."
)


#: 이 길이 미만이면 요약을 시도조차 하지 않는다.
#:
#: HN 링크 글은 text 가 비어 있어 입력이 제목뿐인 경우가 많다. 제목 한 줄로
#: 세 문장을 만들라고 하면 모델은 없는 내용을 지어낸다(실측: "플러그인 구조",
#: "클라우드 동기화" 등 원문에 전혀 없는 서술). 큐레이션 다이제스트에서 환각은
#: 요약 부재보다 훨씬 해롭다 — 사용자가 지어낸 설명을 근거로 읽을지 말지를 정하게 된다.
#:
#: 근본 해결은 링크된 기사 본문을 가져오는 것이지만(플랜의 알려진 한계),
#: 그 전까지는 제목+링크만 보여주는 쪽이 정직하다.
MIN_BODY_CHARS = 200


class SummarizerUnavailable(RuntimeError):
    """요약을 못 하는 상태. 호출부는 이걸 잡아 원문 폴백으로 넘어간다."""


class BodyTooShort(SummarizerUnavailable):
    """요약할 본문이 없다. 실패가 아니라 의도적 생략이다."""


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
            # gpt-oss 계열은 추론 토큰이 max_tokens 예산을 함께 소모한다.
            # 300 으로 잡았더니 추론에 쓰고 남은 몫으로 답을 쓰다 문장 중간에서
            # 잘렸다(finish_reason=length). 요약 자체는 200 토큰이면 충분하므로
            # 나머지는 추론용 여유분이다.
            "max_tokens": 1200,
            # 3줄 요약에 깊은 추론이 필요 없다. 낮추면 응답이 빨라지고
            # TPM 6000 예산도 덜 쓴다.
            "reasoning_effort": "low",
        }
    ).encode()
    req = urllib.request.Request(
        GROQ_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
            "Content-Type": "application/json",
            # urllib 기본 UA("Python-urllib/3.x")는 Groq 앞단 Cloudflare 가
            # 봇으로 간주해 차단한다(HTTP 403 error code 1010, curl 은 통과).
            # arxiv.py 커넥터와 같은 이유로 명시적 UA 를 준다.
            "User-Agent": "tech-radar/0.1 (personal research project; +https://github.com/songkikik)",
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
    if len((body or "").strip()) < MIN_BODY_CHARS:
        raise BodyTooShort(f"본문 {len((body or '').strip())}자 — 요약 생략")
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

    실패(failures)와 생략(skipped)을 구분한다. 본문이 없어서 안 한 건 정상 동작이지
    경고할 일이 아니다. 이 둘을 뭉뚱그리면 "요약 실패" 경고가 매일 뜨고,
    그러면 진짜 실패가 났을 때 아무도 안 본다.

    반환: (항목들, 실패 수, 생략 수)
    """
    failures = 0
    skipped = 0
    for i, item in enumerate(items):
        if not is_configured():
            item["summary"] = None
            failures += 1
            continue
        called = False
        try:
            item["summary"] = summarize_one(item["title"], item.get("body"), model=model)
            called = True
        except BodyTooShort:
            # 실패가 아니다. 요약할 재료가 없어서 안 한 것.
            item["summary"] = None
            skipped += 1
        except SummarizerUnavailable:
            item["summary"] = None
            failures += 1
            called = True
        # API 를 실제로 부른 경우에만 페이싱한다. 생략한 건은 기다릴 이유가 없다.
        if called and i < len(items) - 1 and pace_seconds:
            time.sleep(pace_seconds)  # TPM 6000 제약
    return items, failures, skipped
