# tech-radar

최신 기술 트렌드(arXiv · Hacker News · GitHub)를 **증분 수집**해 레이크하우스에 적재하고,
관심 토픽과의 의미 유사도 + 신선도로 랭킹해 **매일 아침 Slack 다이제스트**로 보내는 파이프라인.

> 상태: Phase 0 (아키텍처 스파이크 진행 중)

## 왜 만드는가

전신 프로젝트(`deepauto-data-track`)는 배치 1회성이었다. 매 실행마다 전량 재스캔·재파싱·재적재·
재인덱싱·재임베딩했고, `published` 시각이 `meta` JSON 문자열에 묻혀 있어 **"최신"이라는 축으로
랭킹하는 것 자체가 불가능**했다.

이 레포는 같은 도메인을 **운영 가능한 증분 파이프라인**으로 다시 짓는다. 핵심 관심사:

- 수집 워터마크를 데이터 적재와 **같은 트랜잭션**에 묶어 at-least-once + 멱등 = effectively-once
- late-arriving 데이터를 이벤트 시각이 아닌 **수집 시각**으로 잘라 유실 방지
- 증분 임베딩을 워터마크가 아닌 **안티조인 백로그**로 구현해 self-healing
- DQ 체크(freshness · 볼륨 급감 · 중복)와 알림을 **감지/발송 분리** 구조로

## 스택

| 레이어 | 선택 |
|---|---|
| 저장 | DuckLake (Postgres 카탈로그 + Cloudflare R2 parquet) |
| 변환 | dbt-duckdb (silver incremental merge / gold) |
| 임베딩 | Qwen3-Embedding-0.6B (sentence-transformers) |
| 요약 | Groq 무료 티어 (OpenAI 호환, 실패 시 원문 폴백) |
| 오케스트레이션 | GitHub Actions cron |
| 서빙 | Slack incoming webhook |

전 구간 무료 티어로 운영된다.

## 로컬 실행

```bash
direnv allow          # uv sync + venv 활성화
uv run python scripts/spike_ducklake.py   # Phase 0 검증
```

`dev` 타깃은 로컬 DuckDB 카탈로그 + `./data/lakehouse` 를 쓰므로 외부 계정 없이 완전 오프라인으로 돈다.
