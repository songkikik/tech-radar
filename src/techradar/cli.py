"""tech-radar CLI."""

from __future__ import annotations

from datetime import datetime, timezone

import typer

from techradar import lake
from techradar.collect import arxiv
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


@app.command("collect")
def collect_cmd(
    category: str = typer.Option("cs.AI", "--category", help="arXiv 카테고리"),
    target: str = typer.Option(None, "--target", help="dev|ci|prod (기본: env)"),
    max_pages: int = typer.Option(10, "--max-pages", help="런당 페이지 상한"),
    page_size: int = typer.Option(100, "--page-size"),
) -> None:
    """arXiv 를 증분 수집해 bronze 에 적재한다."""
    tgt, con = _open(target)
    run_id = new_run_id()
    typer.echo(f"[{tgt.name}] run_id={run_id}")

    r = arxiv.collect(
        con,
        category=category,
        run_id=run_id,
        page_size=page_size,
        max_pages=max_pages,
    )
    lo, hi = r["forward_window"]
    typer.echo(f"  전방 구간   {_utc(lo)} ~ {_utc(hi)} UTC   ({r['rows_forward']}건)")
    if r["backfill_window"]:
        blo, bhi = r["backfill_window"]
        typer.echo(
            f"  소급 구간   {_utc(blo)} ~ {_utc(bhi)} UTC   ({r['rows_backfill']}건)"
        )
    typer.echo(f"  수신/신규   {r['rows_fetched']} / {r['rows_new']}")
    typer.echo(f"  커서        {_utc(r['cursor_before'])} → {_utc(r['cursor_after'])} UTC")
    if r["stalled"]:
        typer.echo("  ❌ 커서 정체 — 런 용량이 부족합니다. --max-pages 를 올리세요")
    elif not r["exhausted"]:
        typer.echo("  ⚠️  전방 구간 미소진 — 다음 실행이 이어서 처리합니다")


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
