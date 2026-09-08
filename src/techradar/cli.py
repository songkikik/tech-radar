"""tech-radar CLI."""

from __future__ import annotations

from datetime import datetime, timezone

import typer

from techradar import embed as embed_mod
from techradar import digest as digest_mod
from techradar import interests as interests_mod
from techradar import lake
from techradar.collect import arxiv, hackernews
from techradar.config import load_target, new_run_id

app = typer.Typer(add_completion=False, help="최신 기술 트렌드 증분 파이프라인")


def _utc(ts: datetime | None) -> str:
    """항상 UTC 로 표시한다.

    tz-aware 값에 strftime 을 바로 쓰면 로컬(KST)로 렌더되는데 라벨은 UTC 라
    조용히 9시간 어긋난 로그가 남는다. 시간 경계를 다루는 파이프라인에서
    이건 디버깅을 몇 시간씩 날려먹는 종류의 버그다.
    """
    if ts is None:
        return "None"
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


def _open(target_name: str | None):
    target = load_target(target_name)
    con = lake.connect(target)
    lake.bootstrap(con)
    return target, con


@app.command("collect-arxiv")
def collect_arxiv_cmd(
    category: str = typer.Option(
        ",".join(arxiv.DEFAULT_CATEGORIES),
        "--category",
        help="쉼표로 구분. 카테고리마다 독립된 커서를 갖는다",
    ),
    target: str = typer.Option(None, "--target", help="dev|ci|prod (기본: env)"),
    max_pages: int = typer.Option(10, "--max-pages", help="런당 페이지 상한"),
    page_size: int = typer.Option(100, "--page-size"),
) -> None:
    """arXiv 를 증분 수집해 bronze 에 적재한다."""
    tgt, con = _open(target)
    run_id = new_run_id()
    categories = [c.strip() for c in category.split(",") if c.strip()]
    typer.echo(f"[{tgt.name}] run_id={run_id} · 카테고리 {len(categories)}개")

    total_new = 0
    failed: list[str] = []
    for cat in categories:
        typer.echo(f"\n── {cat} ──")
        try:
            r = arxiv.collect(
                con,
                category=cat,
                run_id=run_id,
                page_size=page_size,
                max_pages=max_pages,
            )
        except Exception as exc:  # noqa: BLE001
            # 한 카테고리 실패가 나머지를 막지 않는다. 커서는 각자 독립이므로
            # 실패한 것만 다음 실행에서 같은 구간을 다시 시도한다.
            failed.append(cat)
            typer.echo(f"  ❌ 실패: {type(exc).__name__}: {exc}")
            continue

        lo, hi = r["forward_window"]
        typer.echo(f"  전방 구간   {_utc(lo)} ~ {_utc(hi)} UTC   ({r['rows_forward']}건)")
        if r["backfill_window"]:
            blo, bhi = r["backfill_window"]
            typer.echo(
                f"  소급 구간   {_utc(blo)} ~ {_utc(bhi)} UTC   ({r['rows_backfill']}건)"
            )
        typer.echo(f"  수신/신규   {r['rows_fetched']} / {r['rows_new']}")
        typer.echo(
            f"  커서        {_utc(r['cursor_before'])} → {_utc(r['cursor_after'])} UTC"
        )
        if r["stalled"]:
            typer.echo("  ❌ 커서 정체 — 런 용량이 부족합니다. --max-pages 를 올리세요")
        elif not r["exhausted"]:
            typer.echo("  ⚠️  전방 구간 미소진 — 다음 실행이 이어서 처리합니다")
        total_new += r["rows_new"]

    typer.echo(f"\n합계 신규 {total_new}건")
    if failed:
        typer.echo(f"⚠️  실패한 카테고리: {', '.join(failed)} (다음 실행에서 재시도)")


@app.command("collect-hn")
def collect_hn_cmd(
    listing: str = typer.Option("top", "--listing", help="top|best|new"),
    limit: int = typer.Option(200, "--limit", help="상위 몇 건까지"),
    target: str = typer.Option(None, "--target"),
    workers: int = typer.Option(16, "--workers", help="item 병렬 조회 수"),
) -> None:
    """Hacker News listing 을 전량 수집한다 (mutating 소스라 커서 없음)."""
    tgt, con = _open(target)
    run_id = new_run_id()
    typer.echo(f"[{tgt.name}] run_id={run_id}")

    r = hackernews.collect(
        con, listing=listing, run_id=run_id, limit=limit, max_workers=workers
    )
    typer.echo(f"  listing     {r['source_name']}  (ID {r['ids_listed']}건 조회)")
    typer.echo(f"  수신/신규   {r['rows_fetched']} / {r['rows_new']}")
    if r["item_failures"]:
        typer.echo(f"  ⚠️  item {r['item_failures']}건 조회 실패 (나머지는 정상 적재)")


@app.command("embed")
def embed_cmd(
    target: str = typer.Option(None, "--target"),
    model: str = typer.Option(embed_mod.DEFAULT_MODEL, "--model"),
    batch_size: int = typer.Option(32, "--batch-size"),
    limit: int = typer.Option(None, "--limit", help="이번 실행에서 처리할 상한"),
) -> None:
    """백로그(미임베딩 문서)만 인코딩해 ml__embedding 에 append 한다."""
    tgt, con = _open(target)
    pending = embed_mod.backlog_size(con)
    typer.echo(f"[{tgt.name}] 백로그 {pending}건, 모델={model}")
    if pending == 0:
        typer.echo("  처리할 문서 없음")
        return

    r = embed_mod.run_backlog(con, model_name=model, batch_size=batch_size, limit=limit)
    typer.echo(f"  임베딩      {r['embedded']}건  dim={r['dim']}  {r['seconds']:.1f}s")
    typer.echo(f"  잔여 백로그 {embed_mod.backlog_size(con)}건")


@app.command("interests")
def interests_cmd(
    target: str = typer.Option(None, "--target"),
    model: str = typer.Option(embed_mod.DEFAULT_MODEL, "--model"),
    rebuild: bool = typer.Option(False, "--rebuild", help="해당 모델 임베딩 전체 재생성"),
) -> None:
    """profiles/interests.yml 을 임베딩해 테이블과 동기화한다."""
    tgt, con = _open(target)
    r = interests_mod.sync(con, model_name=model, rebuild=rebuild)
    typer.echo(
        f"[{tgt.name}] 토픽 {r['total']}개 · 신규/변경 {r['embedded']}개 · 삭제 {r['removed']}개"
    )


@app.command("digest")
def digest_cmd(
    target: str = typer.Option(None, "--target"),
    send: bool = typer.Option(False, "--send", help="실제 발송 (없으면 미리보기만)"),
    no_summary: bool = typer.Option(False, "--no-summary", help="Groq 요약 건너뛰기"),
) -> None:
    """오늘 선정된 항목을 요약해 Slack 으로 보낸다 (기본은 미리보기)."""
    tgt, con = _open(target)
    r = digest_mod.run(con, dry_run=not send, summarize=not no_summary)

    if r["items"] == 0:
        typer.echo(f"[{tgt.name}] 보낼 항목 없음 (오늘 선정분이 이미 발송됐거나 후보가 없음)")
        return

    typer.echo(
        f"[{tgt.name}] {r['items']}건 · 요약 {r['summarized']}건"
        + (f" · 본문없어 생략 {r['skipped']}건" if r.get("skipped") else "")
    )
    for w in r.get("warnings", []):
        typer.echo(f"  ⚠️  {w}")

    if r["dry_run"]:
        typer.echo(f"  (미리보기 — 실제 발송하려면 --send. Slack webhook 설정됨: {r['notifier_enabled']})\n")
        for it in r["payload"]:
            typer.echo(f"  [{it['source']}] {it['title'][:66]}")
            typer.echo(f"     {it['url']}")
            body = (it.get("summary") or (it.get("body") or "")[:160]).replace("\n", "\n     ")
            typer.echo(f"     {body}")
            typer.echo(f"     관심도 {it['affinity']:.2f} · {it['best_interest_label']}\n")
    else:
        typer.echo("  ✅ 발송 완료" if r["sent"] else "  ❌ 발송 실패 — 다음 실행에서 재시도됩니다")


@app.command("status")
def status_cmd(
    target: str = typer.Option(None, "--target"),
    limit: int = typer.Option(10, "--limit", help="표시할 실행 로그 건수"),
) -> None:
    """워터마크와 최근 실행 로그를 본다."""
    tgt, con = _open(target)
    typer.echo(f"[{tgt.name}] catalog={tgt.catalog_uri}\n")

    typer.echo("── 워터마크 ──")
    rows = con.execute("""
        SELECT source_name, cursor_ts, consecutive_fail, last_success_at
        FROM ops__source_watermark ORDER BY source_name
    """).fetchall()
    if not rows:
        typer.echo("  (없음)")
    for src, cur, fail, ok_at in rows:
        flag = f"  ⚠️ 연속실패 {fail}" if fail else ""
        typer.echo(f"  {src:<20} cursor={cur}  last_ok={ok_at}{flag}")

    typer.echo("\n── bronze ──")
    for src, n, lo, hi in con.execute("""
        SELECT source_name, count(*), min(event_ts), max(event_ts)
        FROM bronze__raw_item GROUP BY 1 ORDER BY 1
    """).fetchall():
        typer.echo(f"  {src:<20} {n}건  event_ts {lo} ~ {hi}")

    # late arrival 관측: 그 실행 시점의 커서보다 과거인데 처음 들어온 행.
    # 소급 조회(OVERLAP)가 실제로 뭘 건지고 있는지를 보여준다. 0건이면 OVERLAP 을
    # 줄여도 되고, p99 가 OVERLAP 에 근접하면 늘려야 한다.
    typer.echo("\n── late arrival (소급 조회가 건진 항목) ──")
    late = con.execute("""
        SELECT b.source_name,
               count(*)                                  AS n,
               max(r.watermark_before - b.event_ts)      AS worst
        FROM bronze__raw_item b
        JOIN ops__ingest_run_log r
          ON b.run_id = r.run_id AND b.source_name = r.source_name
        WHERE r.watermark_before IS NOT NULL
          AND b.event_ts < r.watermark_before
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    if not late:
        typer.echo("  (관측 없음 — 아직 데이터가 적거나 소급이 불필요하다는 뜻)")
    for src, n, worst in late:
        typer.echo(f"  {src:<20} {n}건  최대 지연={worst}")

    typer.echo(f"\n── 최근 실행 {limit}건 ──")
    for row in con.execute(
        """
        SELECT started_at, source_name, status, rows_fetched, rows_new, error_msg
        FROM ops__ingest_run_log ORDER BY started_at DESC LIMIT ?
        """,
        [limit],
    ).fetchall():
        started, src, status, fetched, new, err = row
        line = f"  {started:%m-%d %H:%M:%S}  {src:<18} {status:<5} 수신={fetched} 신규={new}"
        typer.echo(line + (f"  {err}" if err else ""))


if __name__ == "__main__":
    app()
