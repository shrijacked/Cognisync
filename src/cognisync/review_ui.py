from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from functools import partial
from html import escape
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
from pathlib import Path
import posixpath
from urllib.parse import parse_qs, urlparse
from typing import Dict, List, Optional, Sequence

from cognisync.maintenance import (
    MaintenanceError,
    accept_concept_candidate,
    apply_backlink_suggestion,
    dismiss_review_item,
    file_conflict_review,
    reopen_review_item,
    resolve_entity_merge,
)
from cognisync.manifests import read_json_manifest, write_workspace_manifests
from cognisync.linter import lint_snapshot
from cognisync.jobs import list_jobs
from cognisync.planner import build_compile_plan
from cognisync.review_exports import build_review_export_payload
from cognisync.scanner import scan_workspace
from cognisync.sync import list_sync_events
from cognisync.types import IndexSnapshot
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class ReviewUiResult:
    html_path: Path
    export_path: Path
    state_path: Path


def write_review_ui_bundle(
    workspace: Workspace,
    snapshot: IndexSnapshot,
    output_file: Optional[Path] = None,
) -> ReviewUiResult:
    html_path = output_file or (workspace.review_ui_dir / "index.html")
    export_path = html_path.parent / "review-export.json"
    state_path = html_path.parent / "dashboard-state.json"
    html_path.parent.mkdir(parents=True, exist_ok=True)

    review_payload = build_review_export_payload(workspace, snapshot)
    state_payload = build_review_ui_state(workspace, snapshot, review_payload=review_payload)

    export_path.write_text(json.dumps(review_payload, indent=2, sort_keys=True), encoding="utf-8")
    state_path.write_text(json.dumps(state_payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_artifact_preview_pages(workspace, html_path.parent, state_payload)
    _write_graph_detail_pages(workspace, html_path.parent, state_payload)
    _write_run_detail_pages(workspace, html_path.parent, state_payload)
    _write_job_detail_pages(workspace, html_path.parent, state_payload)
    _write_sync_detail_pages(workspace, html_path.parent, state_payload)
    _write_run_timeline_page(html_path.parent, state_payload)
    _write_concept_graph_page(html_path.parent, state_payload)

    html_path.write_text(
        render_review_ui_html(
            payload=state_payload,
            export_href=export_path.name,
            state_href=state_path.name,
        ),
        encoding="utf-8",
    )
    return ReviewUiResult(html_path=html_path, export_path=export_path, state_path=state_path)


def create_review_ui_server(
    directory: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    index_name: str = "index.html",
    workspace: Optional[Workspace] = None,
) -> ThreadingHTTPServer:
    directory = Path(directory).resolve()
    handler = partial(
        _ReviewUiHandler,
        directory=str(directory),
        index_name=index_name,
        workspace=workspace,
    )
    return ThreadingHTTPServer((host, port), handler)


def render_review_ui_html(
    payload: Dict[str, object],
    export_href: str,
    state_href: str,
) -> str:
    review_payload = dict(payload.get("review", {}))
    summary = dict(review_payload.get("summary", {}))
    open_items = list(review_payload.get("open_items", []))
    dismissed_items = list(review_payload.get("dismissed_items", []))
    counts_by_kind = dict(summary.get("open_item_counts_by_kind", {}))
    source_coverage = dict(payload.get("source_coverage", {}))
    compile_health = dict(payload.get("compile_health", {}))
    graph = dict(payload.get("graph", {}))
    graph_nodes = list(graph.get("nodes", []))
    runs = dict(payload.get("runs", {}))
    run_items = list(runs.get("items", []))
    jobs = dict(payload.get("jobs", {}))
    job_items = list(jobs.get("items", []))
    sync = dict(payload.get("sync", {}))
    sync_items = list(sync.get("items", []))
    run_timeline = dict(payload.get("run_timeline", {}))
    concept_graph = dict(payload.get("concept_graph", {}))
    recent_change_summaries = list(payload.get("change_summaries", []))
    serialized_payload = json.dumps(payload, indent=2, sort_keys=True)

    cards = [
        ("Open Review Items", str(summary.get("open_item_count", 0))),
        ("Dismissed Review Items", str(summary.get("dismissed_item_count", 0))),
        ("Graph Nodes", str(graph.get("node_count", 0))),
        ("Graph Edges", str(graph.get("edge_count", 0))),
        ("Recorded Runs", str(runs.get("total_count", 0))),
        ("Queued Jobs", str(jobs.get("queued_count", 0))),
        ("Sync Events", str(sync.get("total_count", 0))),
        ("Known Conflicts", str(graph.get("conflict_count", 0))),
        ("Sources", str(source_coverage.get("source_count", 0))),
    ]

    lines = [
        "<!doctype html>",
        "<html lang=\"en\">",
        "<head>",
        "  <meta charset=\"utf-8\">",
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
        "  <title>Cognisync Review UI</title>",
        "  <style>",
        "    :root { color-scheme: light; --bg: #f4f1e8; --panel: #fffdf7; --ink: #1f2933; --muted: #5c6b73; --line: #d8d2c2; --accent: #165d52; --accent-soft: #dff2ec; --warn: #8f4f18; --warn-soft: #fdf0e3; --danger: #8b2e2e; --danger-soft: #fbeaea; font-family: 'Iowan Old Style', 'Palatino Linotype', 'Book Antiqua', serif; }",
        "    * { box-sizing: border-box; }",
        "    [hidden] { display: none !important; }",
        "    body { margin: 0; background: linear-gradient(180deg, #ece6d8 0%, var(--bg) 100%); color: var(--ink); }",
        "    main { max-width: 1180px; margin: 0 auto; padding: 40px 24px 72px; }",
        "    h1, h2, h3 { margin: 0 0 12px; line-height: 1.1; }",
        "    h1 { font-size: clamp(2.4rem, 5vw, 4.2rem); letter-spacing: -0.04em; }",
        "    h2 { font-size: 1.35rem; }",
        "    p, li, td, th, label { font-size: 0.98rem; line-height: 1.55; }",
        "    .lede { max-width: 72ch; color: var(--muted); margin: 16px 0 28px; }",
        "    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin: 24px 0 32px; }",
        "    .card, .panel { background: rgba(255, 253, 247, 0.88); backdrop-filter: blur(8px); border: 1px solid var(--line); border-radius: 18px; box-shadow: 0 14px 50px rgba(41, 50, 58, 0.07); }",
        "    .card { padding: 18px; }",
        "    .card-label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; }",
        "    .card-value { font-size: 2rem; margin-top: 8px; }",
        "    .grid { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 18px; }",
        "    .stack { display: grid; gap: 18px; }",
        "    .panel { padding: 20px; }",
        "    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: space-between; margin-bottom: 12px; }",
        "    .toolbar-left { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }",
        "    .filter-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 16px 0; }",
        "    .filter-grid label { display: grid; gap: 6px; color: var(--muted); font-size: 0.9rem; }",
        "    .filter-grid input, .filter-grid select { width: 100%; border: 1px solid var(--line); border-radius: 12px; padding: 10px 12px; background: rgba(255, 253, 247, 0.95); color: var(--ink); font: inherit; }",
        "    form { display: grid; gap: 6px; margin-top: 8px; }",
        "    button { border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; background: rgba(22, 93, 82, 0.08); color: var(--accent); font: inherit; cursor: pointer; }",
        "    button:hover { background: rgba(22, 93, 82, 0.14); }",
        "    input[type=\"text\"] { width: 100%; border: 1px solid var(--line); border-radius: 12px; padding: 8px 10px; background: rgba(255, 253, 247, 0.95); color: var(--ink); font: inherit; }",
        "    .pill { display: inline-flex; align-items: center; gap: 6px; padding: 6px 10px; border-radius: 999px; background: var(--accent-soft); color: var(--accent); font-size: 0.82rem; }",
        "    .pill.warn { background: var(--warn-soft); color: var(--warn); }",
        "    .pill.danger { background: var(--danger-soft); color: var(--danger); }",
        "    table { width: 100%; border-collapse: collapse; }",
        "    th, td { text-align: left; padding: 10px 0; border-bottom: 1px solid rgba(216, 210, 194, 0.75); vertical-align: top; }",
        "    th { color: var(--muted); font-weight: 600; }",
        "    code { font-family: 'SFMono-Regular', 'Menlo', monospace; font-size: 0.88em; background: rgba(22, 93, 82, 0.08); padding: 0.12em 0.35em; border-radius: 0.4em; }",
        "    .mono-link { font-family: 'SFMono-Regular', 'Menlo', monospace; font-size: 0.88rem; }",
        "    a { color: var(--accent); }",
        "    details { border-top: 1px solid rgba(216, 210, 194, 0.75); padding: 12px 0 0; }",
        "    details + details { margin-top: 8px; }",
        "    summary { cursor: pointer; font-weight: 600; }",
        "    .muted { color: var(--muted); }",
        "    .empty { color: var(--muted); font-style: italic; }",
        "    .footer { margin-top: 24px; color: var(--muted); }",
        "    @media (max-width: 920px) { .grid { grid-template-columns: 1fr; } main { padding: 28px 16px 48px; } }",
        "  </style>",
        "</head>",
        "<body>",
        "  <main>",
        "    <header>",
        "      <h1>Cognisync Review UI</h1>",
        "      <p class=\"lede\">A lightweight browser surface over the filesystem-native review loop. This dashboard is generated from the current workspace manifests and keeps the queue, dismissals, graph activity, and operator runs readable without scraping terminal output. When served locally, review actions write straight back into the same filesystem state.</p>",
        "    </header>",
        "    <section class=\"cards\">",
    ]
    for label, value in cards:
        lines.extend(
            [
                "      <article class=\"card\">",
                f"        <div class=\"card-label\">{escape(label)}</div>",
                f"        <div class=\"card-value\">{escape(value)}</div>",
                "      </article>",
            ]
        )
    lines.extend(
        [
            "    </section>",
            "    <section class=\"grid\">",
            "      <div class=\"stack\">",
            "        <article class=\"panel\">",
            "          <div class=\"toolbar\">",
            "            <h2>Open Review Items</h2>",
            f"            <span><a class=\"mono-link\" href=\"{escape(export_href)}\">review-export.json</a> <span class=\"muted\">|</span> <a class=\"mono-link\" href=\"{escape(state_href)}\">dashboard-state.json</a></span>",
            "          </div>",
            _render_counts_by_kind(counts_by_kind),
            _render_open_items(open_items),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Dismissed Review Items</h2>",
            _render_dismissed_items(dismissed_items),
            "        </article>",
            "      </div>",
            "      <div class=\"stack\">",
            "        <article class=\"panel\">",
            "          <h2>Graph Overview</h2>",
            _render_graph_overview(graph),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Run History</h2>",
            _render_run_history_summary(runs),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Job Queue</h2>",
            _render_job_history_summary(jobs),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Sync History</h2>",
            _render_sync_history_summary(sync),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Recent Change Summaries</h2>",
            _render_recent_links(recent_change_summaries, empty_label="No change summaries found."),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Compile Health</h2>",
            _render_compile_health(compile_health),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Source Coverage</h2>",
            _render_source_coverage(source_coverage),
            "        </article>",
            "      </div>",
            "    </section>",
            "    <section class=\"stack\" style=\"margin-top: 18px;\">",
            "      <article class=\"panel\">",
            "        <h2>Run Timeline</h2>",
            _render_run_timeline(run_timeline),
            "      </article>",
            "      <article class=\"panel\">",
            "        <h2>Concept Graph</h2>",
            _render_concept_graph_panel(concept_graph),
            "      </article>",
            "      <article class=\"panel\" data-filter-scope=\"graph-nodes\">",
            "        <div class=\"toolbar\">",
            "          <h2>Graph Node Explorer</h2>",
            "          <span class=\"muted\">Filter nodes</span>",
            "        </div>",
            _render_filter_controls(
                scope="graph-nodes",
                search_label="Filter nodes",
                search_placeholder="Search node titles, ids, paths, and collections",
                select_specs=[
                    ("kind", "Node Kind", _collect_filter_values(graph_nodes, "kind")),
                    ("collection", "Collection", _collect_filter_values(graph_nodes, "collection")),
                ],
            ),
            _render_graph_node_explorer(graph_nodes),
            "      </article>",
            "      <article class=\"panel\" data-filter-scope=\"runs\">",
            "        <div class=\"toolbar\">",
            "          <h2>Run Explorer</h2>",
            "          <span class=\"muted\">Filter runs</span>",
            "        </div>",
            _render_filter_controls(
                scope="runs",
                search_label="Filter runs",
                search_placeholder="Search run labels, questions, or paths",
                select_specs=[
                    ("run-kind", "Run Kind", _collect_filter_values(run_items, "run_kind")),
                    ("status", "Status", _collect_filter_values(run_items, "status")),
                ],
            ),
            _render_run_explorer(run_items),
            "      </article>",
            "      <article class=\"panel\" data-filter-scope=\"jobs\">",
            "        <div class=\"toolbar\">",
            "          <h2>Job Explorer</h2>",
            "          <span class=\"muted\">Filter queued and historical jobs</span>",
            "        </div>",
            _render_filter_controls(
                scope="jobs",
                search_label="Filter jobs",
                search_placeholder="Search job ids, titles, and statuses",
                select_specs=[
                    ("job-type", "Job Type", _collect_filter_values(job_items, "job_type")),
                    ("job-status", "Job Status", _collect_filter_values(job_items, "status")),
                ],
            ),
            _render_job_explorer(job_items),
            "      </article>",
            "      <article class=\"panel\" data-filter-scope=\"sync-events\">",
            "        <div class=\"toolbar\">",
            "          <h2>Sync Explorer</h2>",
            "          <span class=\"muted\">Filter import and export history</span>",
            "        </div>",
            _render_filter_controls(
                scope="sync-events",
                search_label="Filter sync events",
                search_placeholder="Search sync ids, bundle paths, and operations",
                select_specs=[
                    ("operation", "Operation", _collect_filter_values(sync_items, "operation")),
                    ("status", "Status", _collect_filter_values(sync_items, "status")),
                ],
            ),
            _render_sync_explorer(sync_items),
            "      </article>",
            "      <article class=\"panel\">",
            "        <h2>Embedded Payload</h2>",
            "        <details>",
            "          <summary>Show JSON snapshot</summary>",
            f"          <pre>{escape(serialized_payload)}</pre>",
            "        </details>",
            "      </article>",
            "    </section>",
            "    <p class=\"footer\">This dashboard is generated into <code>outputs/reports/review-ui/</code> and intentionally ignored by the scanner.</p>",
            "  </main>",
            _render_filter_script(),
            "</body>",
            "</html>",
        ]
    )
    return "\n".join(lines) + "\n"


def build_review_ui_state(
    workspace: Workspace,
    snapshot: IndexSnapshot,
    review_payload: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    review_payload = review_payload or build_review_export_payload(workspace, snapshot)
    runs = _build_run_history(workspace)
    jobs = _build_job_history(workspace)
    sync = _build_sync_history(workspace)
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "workspace": {
            "root": workspace.root.as_posix(),
            "sources_manifest_path": workspace.relative_path(workspace.sources_manifest_path),
            "graph_manifest_path": workspace.relative_path(workspace.graph_manifest_path),
            "review_queue_manifest_path": workspace.relative_path(workspace.review_queue_manifest_path),
            "review_actions_manifest_path": workspace.relative_path(workspace.review_actions_manifest_path),
            "runs_dir": workspace.relative_path(workspace.runs_dir),
            "job_queue_manifest_path": workspace.relative_path(workspace.job_queue_manifest_path),
            "sync_history_manifest_path": workspace.relative_path(workspace.sync_history_manifest_path),
        },
        "review": review_payload,
        "source_coverage": _build_source_coverage(workspace),
        "compile_health": _build_compile_health(workspace, snapshot),
        "graph": _build_graph_summary(workspace),
        "runs": runs,
        "jobs": jobs,
        "sync": sync,
        "run_timeline": _build_run_timeline(workspace),
        "concept_graph": _build_concept_graph(workspace),
        "change_summaries": _read_recent_change_summaries(workspace),
    }


def _render_counts_by_kind(counts_by_kind: Dict[str, object]) -> str:
    if not counts_by_kind:
        return "<p class=\"empty\">No open review items.</p>"
    pills = []
    for kind, count in sorted(counts_by_kind.items()):
        css_class = "pill"
        if "conflict" in kind:
            css_class += " danger"
        elif "backlink" in kind:
            css_class += " warn"
        pills.append(f"<span class=\"{css_class}\">{escape(kind)} <strong>{escape(str(count))}</strong></span>")
    return "          <div class=\"toolbar\">" + "".join(pills) + "</div>"


def _render_open_items(items: List[Dict[str, object]]) -> str:
    if not items:
        return "          <p class=\"empty\">No open review items.</p>"
    lines = [
        "          <table>",
        "            <thead><tr><th>Item</th><th>Path</th><th>Priority</th><th>Actions</th></tr></thead>",
        "            <tbody>",
    ]
    for item in items:
        title = escape(str(item.get("title", "")))
        path = escape(str(item.get("path", item.get("target_path", "-"))))
        priority = escape(str(item.get("priority", "")))
        detail = escape(str(item.get("detail", "")).strip())
        suggestion = escape(str(item.get("suggestion", "")).strip())
        lines.extend(
            [
                "              <tr>",
                f"                <td><strong>{title}</strong><br><span class=\"muted\">{detail}</span><br><span class=\"muted\">{suggestion}</span></td>",
                f"                <td><code>{path}</code></td>",
                f"                <td>{priority}</td>",
                f"                <td>{_render_open_item_actions(item)}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_dismissed_items(items: List[Dict[str, object]]) -> str:
    if not items:
        return "          <p class=\"empty\">No dismissed review items.</p>"
    lines = []
    for item in items:
        title = escape(str(item.get("title", item.get("review_id", ""))))
        review_id = escape(str(item.get("review_id", "")))
        reason = escape(str(item.get("reason", "")).strip() or "No reason recorded.")
        path = escape(str(item.get("path", "")).strip() or "-")
        lines.extend(
            [
                "          <details>",
                f"            <summary>{title}</summary>",
                f"            <p><code>{review_id}</code></p>",
                f"            <p>{reason}</p>",
                f"            <p class=\"muted\">path: <code>{path}</code></p>",
                f"            {_render_inline_form('/api/review/reopen', [('review_id', str(item.get('review_id', '')))], 'Reopen')}",
                "          </details>",
            ]
        )
    return "\n".join(lines)


def _render_recent_links(items: List[Dict[str, str]], empty_label: str) -> str:
    if not items:
        return f"          <p class=\"empty\">{escape(empty_label)}</p>"
    lines = ["          <ul>"]
    for item in items:
        label = escape(item["label"])
        href = escape(item["href"])
        meta = escape(item["meta"])
        detail_href = escape(item.get("detail_href", ""))
        lines.append(
            "            <li><span class=\"mono-link\">"
            + href
            + "</span><br><strong><a href=\""
            + detail_href
            + "\">"
            + label
            + "</a></strong><br><span class=\"muted\">"
            + meta
            + "</span></li>"
        )
    lines.append("          </ul>")
    return "\n".join(lines)


def _render_source_coverage(coverage: Dict[str, object]) -> str:
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">sources <strong>{escape(str(coverage.get('source_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">assets <strong>{escape(str(coverage.get('captured_asset_count', 0)))}</strong></span>",
        f"            <span class=\"pill\">summaries <strong>{escape(str(coverage.get('summary_target_count', 0)))}</strong></span>",
        "          </div>",
        f"          <p class=\"muted\">manifest: <code>{escape(str(coverage.get('manifest_path', '')))}</code></p>",
        _render_kind_table("Source Kinds", dict(coverage.get("counts_by_kind", {}))),
        _render_kind_table("Extraction Status", dict(coverage.get("counts_by_status", {}))),
        _render_tag_table(list(coverage.get("top_tags", []))),
    ]
    return "\n".join(lines)


def _render_compile_health(health: Dict[str, object]) -> str:
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill danger\">issues <strong>{escape(str(health.get('issue_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">tasks <strong>{escape(str(health.get('pending_task_count', 0)))}</strong></span>",
        "          </div>",
        _render_kind_table("Issue Severities", dict(health.get("issue_counts_by_severity", {}))),
        _render_kind_table("Issue Kinds", dict(health.get("issue_counts_by_kind", {}))),
        _render_kind_table("Pending Task Kinds", dict(health.get("task_counts_by_kind", {}))),
        _render_issue_preview(list(health.get("top_issues", []))),
    ]
    return "\n".join(lines)


def _render_run_timeline(timeline: Dict[str, object]) -> str:
    buckets = list(timeline.get("buckets", []))
    detail_href = escape(str(timeline.get("detail_href", "")))
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">days <strong>{escape(str(len(buckets)))}</strong></span>",
        f"            <span class=\"pill warn\">runs <strong>{escape(str(timeline.get('total_count', 0)))}</strong></span>",
        f"            <span><a class=\"mono-link\" href=\"{detail_href}\">open timeline page</a></span>",
        "          </div>",
    ]
    if not buckets:
        lines.append("          <p class=\"empty\">No run timeline data available.</p>")
        return "\n".join(lines)
    lines.extend(
        [
            "          <table>",
            "            <thead><tr><th>Day</th><th>Runs</th><th>Statuses</th></tr></thead>",
            "            <tbody>",
        ]
    )
    for bucket in buckets[:8]:
        lines.extend(
            [
                "              <tr>",
                f"                <td>{escape(str(bucket.get('day', '')))}</td>",
                f"                <td>{escape(str(bucket.get('count', 0)))}</td>",
                f"                <td>{escape(_format_counts_inline(dict(bucket.get('statuses', {}))))}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_concept_graph_panel(concept_graph: Dict[str, object]) -> str:
    map_href = escape(str(concept_graph.get("map_href", "")))
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">nodes <strong>{escape(str(concept_graph.get('selected_node_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">edges <strong>{escape(str(concept_graph.get('selected_edge_count', 0)))}</strong></span>",
        f"            <span><a class=\"mono-link\" href=\"{map_href}\">open concept graph</a></span>",
        "          </div>",
        _render_concept_graph_svg(concept_graph, current_href="index.html"),
    ]
    return "\n".join(lines)


def _render_tag_table(items: List[Dict[str, object]]) -> str:
    if not items:
        return "          <p class=\"empty\">No source tags recorded.</p>"
    lines = [
        "          <h3>Top Tags</h3>",
        "          <table>",
        "            <thead><tr><th>Tag</th><th>Count</th></tr></thead>",
        "            <tbody>",
    ]
    for item in items:
        lines.extend(
            [
                "              <tr>",
                f"                <td>{escape(str(item.get('tag', '')))}</td>",
                f"                <td>{escape(str(item.get('count', 0)))}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_issue_preview(items: List[Dict[str, object]]) -> str:
    if not items:
        return "          <p class=\"empty\">No compile or lint issues detected.</p>"
    lines = ["          <h3>Top Issues</h3>", "          <ul>"]
    for item in items:
        lines.append(
            "            <li><strong>"
            + escape(str(item.get("severity", "")))
            + "</strong> "
            + escape(str(item.get("kind", "")))
            + " <code>"
            + escape(str(item.get("path", "")))
            + "</code><br><span class=\"muted\">"
            + escape(str(item.get("message", "")))
            + "</span></li>"
        )
    lines.append("          </ul>")
    return "\n".join(lines)


def _render_open_item_actions(item: Dict[str, object]) -> str:
    forms: List[str] = []
    kind = str(item.get("kind", ""))
    if kind == "concept_candidate" and str(item.get("slug", "")).strip():
        forms.append(_render_inline_form("/api/review/accept-concept", [("slug", str(item["slug"]))], "Accept"))
    elif kind == "backlink_suggestion" and str(item.get("path", "")).strip():
        forms.append(_render_inline_form("/api/review/apply-backlink", [("target_path", str(item["path"]))], "Apply backlink"))
    elif kind == "entity_merge_candidate" and str(item.get("canonical_label", "")).strip():
        forms.append(
            _render_inline_form(
                "/api/review/resolve-merge",
                [("canonical_label", str(item["canonical_label"]))],
                "Resolve merge",
            )
        )
    elif kind == "conflict_review" and str(item.get("subject", "")).strip():
        forms.append(_render_inline_form("/api/review/file-conflict", [("subject", str(item["subject"]))], "File conflict"))

    review_id = str(item.get("review_id", "")).strip()
    if review_id:
        forms.append(
            _render_inline_form(
                "/api/review/dismiss",
                [("review_id", review_id)],
                "Dismiss",
                text_input=("reason", "reason", "later"),
            )
        )
    if not forms:
        return "<span class=\"muted\">No actions</span>"
    return "<div class=\"stack\">" + "".join(forms) + "</div>"


def _render_inline_form(
    action: str,
    hidden_fields: Sequence[tuple[str, str]],
    button_label: str,
    text_input: Optional[tuple[str, str, str]] = None,
) -> str:
    lines = [f"<form method=\"post\" action=\"{escape(action)}\">"]
    for name, value in hidden_fields:
        lines.append(
            f"<input type=\"hidden\" name=\"{escape(name)}\" value=\"{escape(value)}\">"
        )
    if text_input:
        name, label, default = text_input
        lines.append(
            f"<label class=\"muted\">{escape(label)}<br><input type=\"text\" name=\"{escape(name)}\" value=\"{escape(default)}\"></label>"
        )
    lines.append(f"<button type=\"submit\">{escape(button_label)}</button>")
    lines.append("</form>")
    return "".join(lines)


def _render_graph_overview(graph: Dict[str, object]) -> str:
    node_counts = dict(graph.get("node_counts_by_kind", {}))
    edge_counts = dict(graph.get("edge_counts_by_kind", {}))
    manifest_path = escape(str(graph.get("manifest_path", "")))
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">nodes <strong>{escape(str(graph.get('node_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">edges <strong>{escape(str(graph.get('edge_count', 0)))}</strong></span>",
        f"            <span class=\"pill danger\">conflicts <strong>{escape(str(graph.get('conflict_count', 0)))}</strong></span>",
        "          </div>",
        f"          <p class=\"muted\">manifest: <code>{manifest_path}</code></p>",
        _render_kind_table("Node Kinds", node_counts),
        _render_kind_table("Edge Kinds", edge_counts),
        "          <h3>Connected Artifacts</h3>",
        _render_connected_artifacts(list(graph.get("top_connected_artifacts", []))),
        "          <h3>Conflict Edges</h3>",
        _render_conflict_edges(list(graph.get("conflicts", []))),
    ]
    return "\n".join(lines)


def _render_run_history_summary(runs: Dict[str, object]) -> str:
    items = list(runs.get("items", []))
    if not items:
        return "          <p class=\"empty\">No run manifests found.</p>"
    counts_by_kind = dict(runs.get("counts_by_kind", {}))
    counts_by_status = dict(runs.get("counts_by_status", {}))
    recent_items = items[:6]
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">runs <strong>{escape(str(runs.get('total_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">kinds <strong>{escape(str(len(counts_by_kind)))}</strong></span>",
        f"            <span class=\"pill danger\">statuses <strong>{escape(str(len(counts_by_status)))}</strong></span>",
        "          </div>",
        _render_kind_table("Run Kinds", counts_by_kind),
        _render_kind_table("Run Statuses", counts_by_status),
        "          <h3>Recent Runs</h3>",
        "          <table>",
        "            <thead><tr><th>Run</th><th>Kind</th><th>Status</th><th>Generated</th></tr></thead>",
        "            <tbody>",
    ]
    for item in recent_items:
        detail_href = escape(str(item.get("detail_href", "")))
        label = escape(str(item.get("label", "")))
        path = escape(str(item.get("path", "")))
        run_kind = escape(str(item.get("run_kind", "")))
        status = escape(str(item.get("status", "")))
        generated_at = escape(str(item.get("generated_at", "")))
        lines.extend(
            [
                "              <tr>",
                (
                    "                <td><strong><a href=\""
                    + detail_href
                    + "\">"
                    + label
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + path
                    + "</code></span></td>"
                ),
                f"                <td>{run_kind}</td>",
                f"                <td>{status}</td>",
                f"                <td>{generated_at}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_job_history_summary(jobs: Dict[str, object]) -> str:
    items = list(jobs.get("items", []))
    if not items:
        return "          <p class=\"empty\">No queued or historical jobs found.</p>"
    counts_by_kind = dict(jobs.get("counts_by_kind", {}))
    counts_by_status = dict(jobs.get("counts_by_status", {}))
    recent_items = items[:6]
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">jobs <strong>{escape(str(jobs.get('total_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">queued <strong>{escape(str(jobs.get('queued_count', 0)))}</strong></span>",
        f"            <span class=\"pill danger\">failures <strong>{escape(str(jobs.get('failed_count', 0)))}</strong></span>",
        "          </div>",
        _render_kind_table("Job Types", counts_by_kind),
        _render_kind_table("Job Statuses", counts_by_status),
        "          <h3>Recent Jobs</h3>",
        "          <table>",
        "            <thead><tr><th>Job</th><th>Type</th><th>Status</th><th>Updated</th></tr></thead>",
        "            <tbody>",
    ]
    for item in recent_items:
        detail_href = escape(str(item.get("detail_href", "")))
        label = escape(str(item.get("label", "")))
        path = escape(str(item.get("path", "")))
        job_type = escape(str(item.get("job_type", "")))
        status = escape(str(item.get("status", "")))
        updated_at = escape(str(item.get("updated_at", "")))
        lines.extend(
            [
                "              <tr>",
                (
                    "                <td><strong><a href=\""
                    + detail_href
                    + "\">"
                    + label
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + path
                    + "</code></span></td>"
                ),
                f"                <td>{job_type}</td>",
                f"                <td>{status}</td>",
                f"                <td>{updated_at}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_sync_history_summary(sync: Dict[str, object]) -> str:
    items = list(sync.get("items", []))
    if not items:
        return "          <p class=\"empty\">No sync events found.</p>"
    counts_by_operation = dict(sync.get("counts_by_operation", {}))
    recent_items = items[:6]
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">events <strong>{escape(str(sync.get('total_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">operations <strong>{escape(str(len(counts_by_operation)))}</strong></span>",
        "          </div>",
        _render_kind_table("Sync Operations", counts_by_operation),
        "          <h3>Recent Events</h3>",
        "          <table>",
        "            <thead><tr><th>Event</th><th>Operation</th><th>Files</th><th>Generated</th></tr></thead>",
        "            <tbody>",
    ]
    for item in recent_items:
        detail_href = escape(str(item.get("detail_href", "")))
        label = escape(str(item.get("label", "")))
        path = escape(str(item.get("path", "")))
        operation = escape(str(item.get("operation", "")))
        file_count = escape(str(item.get("file_count", 0)))
        generated_at = escape(str(item.get("generated_at", "")))
        lines.extend(
            [
                "              <tr>",
                (
                    "                <td><strong><a href=\""
                    + detail_href
                    + "\">"
                    + label
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + path
                    + "</code></span></td>"
                ),
                f"                <td>{operation}</td>",
                f"                <td>{file_count}</td>",
                f"                <td>{generated_at}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_kind_table(title: str, values: Dict[str, object]) -> str:
    if not values:
        return f"          <p class=\"empty\">{escape(title)}: none.</p>"
    lines = [
        f"          <h3>{escape(title)}</h3>",
        "          <table>",
        "            <thead><tr><th>Kind</th><th>Count</th></tr></thead>",
        "            <tbody>",
    ]
    for key, value in sorted(values.items()):
        lines.extend(
            [
                "              <tr>",
                f"                <td>{escape(str(key))}</td>",
                f"                <td>{escape(str(value))}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_connected_artifacts(items: List[Dict[str, object]]) -> str:
    if not items:
        return "          <p class=\"empty\">No connected artifact data available.</p>"
    lines = [
        "          <table>",
        "            <thead><tr><th>Artifact</th><th>Collection</th><th>Degree</th></tr></thead>",
        "            <tbody>",
    ]
    for item in items:
        title = escape(str(item.get("title", "")))
        path = escape(str(item.get("path", "")))
        detail_href = escape(str(item.get("detail_href", "")))
        collection = escape(str(item.get("collection", "")))
        degree = escape(str(item.get("degree", 0)))
        lines.extend(
            [
                "              <tr>",
                (
                    "                <td><strong><a href=\""
                    + detail_href
                    + "\">"
                    + title
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + path
                    + "</code></span></td>"
                ),
                f"                <td>{collection}</td>",
                f"                <td>{degree}</td>",
                "              </tr>",
            ]
        )
    lines.extend(["            </tbody>", "          </table>"])
    return "\n".join(lines)


def _render_conflict_edges(items: List[Dict[str, str]]) -> str:
    if not items:
        return "          <p class=\"empty\">No conflict edges recorded.</p>"
    lines: List[str] = []
    for item in items:
        source_href = escape(str(item.get("source_href", "")))
        target_href = escape(str(item.get("target_href", "")))
        subject = escape(item["subject"])
        verb = escape(item["verb"])
        source = escape(item["source"])
        target = escape(item["target"])
        left_value = escape(item["left_value"])
        right_value = escape(item["right_value"])
        lines.extend(
            [
                "          <details>",
                f"            <summary>{subject} {verb}</summary>",
                f"            <p class=\"muted\"><a href=\"{source_href}\"><code>{source}</code></a> says {left_value}</p>",
                f"            <p class=\"muted\"><a href=\"{target_href}\"><code>{target}</code></a> says {right_value}</p>",
                "          </details>",
            ]
        )
    return "\n".join(lines)


def _render_filter_controls(
    scope: str,
    search_label: str,
    search_placeholder: str,
    select_specs: Sequence[tuple[str, str, List[str]]],
) -> str:
    lines = ["        <div class=\"filter-grid\">"]
    lines.extend(
        [
            f"          <label>{escape(search_label)}",
            f"            <input type=\"search\" data-filter-input data-filter-row-scope=\"{escape(scope)}\" placeholder=\"{escape(search_placeholder)}\">",
            "          </label>",
        ]
    )
    for key, label, values in select_specs:
        lines.append(f"          <label>{escape(label)}")
        lines.append(
            f"            <select data-filter-key=\"{escape(key)}\" data-filter-row-scope=\"{escape(scope)}\">"
        )
        lines.append("              <option value=\"\">All</option>")
        for value in values:
            lines.append(f"              <option value=\"{escape(value)}\">{escape(value)}</option>")
        lines.append("            </select>")
        lines.append("          </label>")
    lines.append("        </div>")
    return "\n".join(lines)


def _render_graph_node_explorer(nodes: List[Dict[str, object]]) -> str:
    if not nodes:
        return "        <p class=\"empty\">No graph nodes found.</p>"
    lines = [
        "        <table>",
        "          <thead><tr><th>Node</th><th>Kind</th><th>Collection</th><th>Degree</th></tr></thead>",
        "          <tbody>",
    ]
    for item in nodes:
        lines.extend(
            [
                (
                    "            <tr data-filter-row=\"graph-nodes\" data-kind=\""
                    + escape(str(item.get("kind", "")))
                    + "\" data-collection=\""
                    + escape(str(item.get("collection", "")))
                    + "\" data-search=\""
                    + escape(str(item.get("search_text", "")))
                    + "\">"
                ),
                (
                    "              <td><strong><a href=\""
                    + escape(str(item.get("detail_href", "")))
                    + "\">"
                    + escape(str(item.get("title", item.get("id", ""))))
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + escape(str(item.get("id", "")))
                    + "</code></span></td>"
                ),
                f"              <td>{escape(str(item.get('kind', '')))}</td>",
                f"              <td>{escape(str(item.get('collection', '')) or '-')}</td>",
                f"              <td>{escape(str(item.get('degree', 0)))}</td>",
                "            </tr>",
            ]
        )
    lines.extend(
        [
            "          </tbody>",
            "        </table>",
            "        <p class=\"empty\" data-filter-empty=\"graph-nodes\" hidden>No graph nodes match the current filters.</p>",
        ]
    )
    return "\n".join(lines)


def _render_run_explorer(items: List[Dict[str, object]]) -> str:
    if not items:
        return "        <p class=\"empty\">No run manifests found.</p>"
    lines = [
        "        <table>",
        "          <thead><tr><th>Run</th><th>Kind</th><th>Status</th><th>Generated</th></tr></thead>",
        "          <tbody>",
    ]
    for item in items:
        lines.extend(
            [
                (
                    "            <tr data-filter-row=\"runs\" data-run-kind=\""
                    + escape(str(item.get("run_kind", "")))
                    + "\" data-status=\""
                    + escape(str(item.get("status", "")))
                    + "\" data-search=\""
                    + escape(str(item.get("search_text", "")))
                    + "\">"
                ),
                (
                    "              <td><strong><a href=\""
                    + escape(str(item.get("detail_href", "")))
                    + "\">"
                    + escape(str(item.get("label", "")))
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + escape(str(item.get("path", "")))
                    + "</code></span></td>"
                ),
                f"              <td>{escape(str(item.get('run_kind', '')))}</td>",
                f"              <td>{escape(str(item.get('status', '')))}</td>",
                f"              <td>{escape(str(item.get('generated_at', '')))}</td>",
                "            </tr>",
            ]
        )
    lines.extend(
        [
            "          </tbody>",
            "        </table>",
            "        <p class=\"empty\" data-filter-empty=\"runs\" hidden>No runs match the current filters.</p>",
        ]
    )
    return "\n".join(lines)


def _render_job_explorer(items: List[Dict[str, object]]) -> str:
    if not items:
        return "        <p class=\"empty\">No job manifests found.</p>"
    lines = [
        "        <table>",
        "          <thead><tr><th>Job</th><th>Type</th><th>Status</th><th>Updated</th></tr></thead>",
        "          <tbody>",
    ]
    for item in items:
        lines.extend(
            [
                (
                    "            <tr data-filter-row=\"jobs\" data-job-type=\""
                    + escape(str(item.get("job_type", "")))
                    + "\" data-job-status=\""
                    + escape(str(item.get("status", "")))
                    + "\" data-search=\""
                    + escape(str(item.get("search_text", "")))
                    + "\">"
                ),
                (
                    "              <td><strong><a href=\""
                    + escape(str(item.get("detail_href", "")))
                    + "\">"
                    + escape(str(item.get("label", "")))
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + escape(str(item.get("path", "")))
                    + "</code></span></td>"
                ),
                f"              <td>{escape(str(item.get('job_type', '')))}</td>",
                f"              <td>{escape(str(item.get('status', '')))}</td>",
                f"              <td>{escape(str(item.get('updated_at', '')))}</td>",
                "            </tr>",
            ]
        )
    lines.extend(
        [
            "          </tbody>",
            "        </table>",
            "        <p class=\"empty\" data-filter-empty=\"jobs\" hidden>No jobs match the current filters.</p>",
        ]
    )
    return "\n".join(lines)


def _render_sync_explorer(items: List[Dict[str, object]]) -> str:
    if not items:
        return "        <p class=\"empty\">No sync events found.</p>"
    lines = [
        "        <table>",
        "          <thead><tr><th>Event</th><th>Operation</th><th>Status</th><th>Generated</th></tr></thead>",
        "          <tbody>",
    ]
    for item in items:
        lines.extend(
            [
                (
                    "            <tr data-filter-row=\"sync-events\" data-operation=\""
                    + escape(str(item.get("operation", "")))
                    + "\" data-status=\""
                    + escape(str(item.get("status", "")))
                    + "\" data-search=\""
                    + escape(str(item.get("search_text", "")))
                    + "\">"
                ),
                (
                    "              <td><strong><a href=\""
                    + escape(str(item.get("detail_href", "")))
                    + "\">"
                    + escape(str(item.get("label", "")))
                    + "</a></strong><br><span class=\"muted\"><code>"
                    + escape(str(item.get("path", "")))
                    + "</code></span></td>"
                ),
                f"              <td>{escape(str(item.get('operation', '')))}</td>",
                f"              <td>{escape(str(item.get('status', '')))}</td>",
                f"              <td>{escape(str(item.get('generated_at', '')))}</td>",
                "            </tr>",
            ]
        )
    lines.extend(
        [
            "          </tbody>",
            "        </table>",
            "        <p class=\"empty\" data-filter-empty=\"sync-events\" hidden>No sync events match the current filters.</p>",
        ]
    )
    return "\n".join(lines)


def _render_filter_script() -> str:
    return """  <script>
    (() => {
      const scopes = new Set();
      document.querySelectorAll('[data-filter-row]').forEach((row) => scopes.add(row.dataset.filterRow));
      scopes.forEach((scope) => {
        const controls = document.querySelectorAll(`[data-filter-row-scope="${scope}"]`);
        const rows = Array.from(document.querySelectorAll(`[data-filter-row="${scope}"]`));
        const emptyState = document.querySelector(`[data-filter-empty="${scope}"]`);
        const apply = () => {
          const searchControl = Array.from(controls).find((control) => control.hasAttribute('data-filter-input'));
          const query = searchControl ? searchControl.value.trim().toLowerCase() : '';
          let visibleCount = 0;
          rows.forEach((row) => {
            const searchText = (row.dataset.search || '').toLowerCase();
            let visible = !query || searchText.includes(query);
            controls.forEach((control) => {
              if (!visible || !control.dataset.filterKey) {
                return;
              }
              const key = control.dataset.filterKey;
              const expected = control.value;
              if (expected && row.dataset[key] !== expected) {
                visible = false;
              }
            });
            row.hidden = !visible;
            if (visible) {
              visibleCount += 1;
            }
          });
          if (emptyState) {
            emptyState.hidden = visibleCount !== 0;
          }
        };
        controls.forEach((control) => {
          control.addEventListener(control.tagName === 'SELECT' ? 'change' : 'input', apply);
        });
        apply();
      });
    })();
  </script>"""


def _read_recent_change_summaries(workspace: Workspace, limit: int = 6) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for path in sorted(workspace.change_summaries_dir.glob("*.md"), reverse=True)[:limit]:
        relative = workspace.relative_path(path)
        items.append(
            {
                "label": path.stem,
                "href": relative,
                "detail_href": _artifact_preview_href(relative),
                "meta": path.name,
            }
        )
    return items


def _build_source_coverage(workspace: Workspace, limit: int = 6) -> Dict[str, object]:
    if not workspace.sources_manifest_path.exists():
        return {
            "manifest_path": workspace.relative_path(workspace.sources_manifest_path),
            "source_count": 0,
            "counts_by_kind": {},
            "counts_by_status": {},
            "captured_asset_count": 0,
            "summary_target_count": 0,
            "total_word_count": 0,
            "top_tags": [],
        }

    manifest = read_json_manifest(workspace.sources_manifest_path)
    sources = list(manifest.get("sources", []))
    kind_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    tag_counts: Counter[str] = Counter()
    captured_asset_count = 0
    summary_target_count = 0
    total_word_count = 0
    for source in sources:
        kind_counts[str(source.get("source_kind", "unknown"))] += 1
        status_counts[str(source.get("extraction_status", "unknown"))] += 1
        captured_asset_count += len(list(source.get("captured_assets", [])))
        summary_target_count += len(list(source.get("summary_targets", [])))
        total_word_count += int(source.get("word_count", 0) or 0)
        for tag in list(source.get("tags", [])):
            tag_counts[str(tag)] += 1
    return {
        "manifest_path": workspace.relative_path(workspace.sources_manifest_path),
        "source_count": len(sources),
        "counts_by_kind": dict(kind_counts),
        "counts_by_status": dict(status_counts),
        "captured_asset_count": captured_asset_count,
        "summary_target_count": summary_target_count,
        "total_word_count": total_word_count,
        "top_tags": [{"tag": tag, "count": count} for tag, count in tag_counts.most_common(limit)],
    }


def _build_compile_health(workspace: Workspace, snapshot: IndexSnapshot, limit: int = 6) -> Dict[str, object]:
    issues = lint_snapshot(snapshot, workspace=workspace)
    plan = build_compile_plan(snapshot)
    issue_kind_counts: Counter[str] = Counter(issue.kind for issue in issues)
    issue_severity_counts: Counter[str] = Counter(issue.severity for issue in issues)
    task_kind_counts: Counter[str] = Counter(task.kind for task in plan.tasks)
    return {
        "issue_count": len(issues),
        "pending_task_count": len(plan.tasks),
        "issue_counts_by_kind": dict(issue_kind_counts),
        "issue_counts_by_severity": dict(issue_severity_counts),
        "task_counts_by_kind": dict(task_kind_counts),
        "top_issues": [
            {
                "severity": issue.severity,
                "kind": issue.kind,
                "path": issue.path,
                "message": issue.message,
            }
            for issue in issues[:limit]
        ],
    }


def _build_run_history(workspace: Workspace, limit: int = 24) -> Dict[str, object]:
    items: List[Dict[str, str]] = []
    counts_by_kind: Counter[str] = Counter()
    counts_by_status: Counter[str] = Counter()
    manifests = sorted(workspace.runs_dir.glob("*.json"), reverse=True)
    for path in manifests:
        manifest = read_json_manifest(path)
        run_kind = str(manifest.get("run_kind", "unknown"))
        status = str(manifest.get("status", "unknown"))
        counts_by_kind[run_kind] += 1
        counts_by_status[status] += 1
        relative = workspace.relative_path(path)
        items.append(
            {
                "label": str(manifest.get("run_label", manifest.get("run_kind", path.stem))),
                "run_kind": run_kind,
                "status": status,
                "generated_at": str(manifest.get("generated_at", "")),
                "mode": str(manifest.get("mode", "")),
                "path": relative,
                "detail_href": _run_detail_href(relative),
                "question": str(manifest.get("question", "")),
                "search_text": _normalize_search_text(
                    [
                        manifest.get("run_label"),
                        manifest.get("question"),
                        run_kind,
                        status,
                        manifest.get("mode"),
                        relative,
                    ]
                ),
            }
        )
    return {
        "total_count": len(manifests),
        "counts_by_kind": dict(counts_by_kind),
        "counts_by_status": dict(counts_by_status),
        "items": items[:limit],
    }


def _build_job_history(workspace: Workspace, limit: int = 24) -> Dict[str, object]:
    items: List[Dict[str, str]] = []
    counts_by_kind: Counter[str] = Counter()
    counts_by_status: Counter[str] = Counter()
    jobs = list_jobs(workspace)
    for manifest in jobs:
        job_id = str(manifest.get("job_id", ""))
        job_type = str(manifest.get("job_type", "unknown"))
        status = str(manifest.get("status", "unknown"))
        counts_by_kind[job_type] += 1
        counts_by_status[status] += 1
        relative = workspace.relative_path(workspace.job_manifests_dir / f"{job_id}.json")
        items.append(
            {
                "label": job_id,
                "job_type": job_type,
                "status": status,
                "updated_at": str(manifest.get("updated_at", "")),
                "created_at": str(manifest.get("created_at", "")),
                "path": relative,
                "detail_href": _job_detail_href(job_id),
                "title": str(manifest.get("title", "")),
                "retry_of_job_id": str(manifest.get("retry_of_job_id", "") or ""),
                "search_text": _normalize_search_text(
                    [
                        job_id,
                        job_type,
                        status,
                        manifest.get("title"),
                        manifest.get("error"),
                        manifest.get("retry_of_job_id"),
                        relative,
                    ]
                ),
            }
        )
    return {
        "queue_manifest_path": workspace.relative_path(workspace.job_queue_manifest_path),
        "total_count": len(jobs),
        "queued_count": sum(1 for job in jobs if str(job.get("status", "")) == "queued"),
        "failed_count": sum(1 for job in jobs if str(job.get("status", "")) == "failed"),
        "counts_by_kind": dict(counts_by_kind),
        "counts_by_status": dict(counts_by_status),
        "items": items[:limit],
    }


def _build_sync_history(workspace: Workspace, limit: int = 24) -> Dict[str, object]:
    items: List[Dict[str, str]] = []
    counts_by_operation: Counter[str] = Counter()
    events = list_sync_events(workspace)
    for event in events:
        sync_id = str(event.get("sync_id", ""))
        operation = str(event.get("operation", "unknown"))
        status = str(event.get("status", "unknown"))
        counts_by_operation[operation] += 1
        relative = workspace.relative_path(workspace.sync_manifests_dir / f"{sync_id}.json")
        items.append(
            {
                "label": sync_id,
                "operation": operation,
                "status": status,
                "generated_at": str(event.get("generated_at", "")),
                "file_count": str(event.get("file_count", 0)),
                "path": relative,
                "detail_href": _sync_detail_href(sync_id),
                "bundle_dir_relative": str(event.get("bundle_dir_relative", "")),
                "search_text": _normalize_search_text(
                    [
                        sync_id,
                        operation,
                        status,
                        event.get("bundle_dir_relative"),
                        event.get("bundle_manifest_relative"),
                        relative,
                    ]
                ),
            }
        )
    return {
        "history_manifest_path": workspace.relative_path(workspace.sync_history_manifest_path),
        "total_count": len(events),
        "counts_by_operation": dict(counts_by_operation),
        "items": items[:limit],
    }


def _build_run_timeline(workspace: Workspace, limit: int = 12) -> Dict[str, object]:
    buckets: Dict[str, Dict[str, object]] = {}
    manifests = sorted(workspace.runs_dir.glob("*.json"), reverse=True)
    for path in manifests:
        manifest = read_json_manifest(path)
        generated_at = str(manifest.get("generated_at", ""))
        day = generated_at[:10] if len(generated_at) >= 10 else "unknown"
        entry = buckets.setdefault(day, {"day": day, "count": 0, "statuses": Counter(), "kinds": Counter(), "items": []})
        entry["count"] = int(entry["count"]) + 1
        status = str(manifest.get("status", "unknown"))
        kind = str(manifest.get("run_kind", "unknown"))
        entry["statuses"][status] += 1
        entry["kinds"][kind] += 1
        entry["items"].append(
            {
                "label": str(manifest.get("run_label", path.stem)),
                "path": workspace.relative_path(path),
                "detail_href": _run_detail_href(workspace.relative_path(path)),
                "status": status,
                "run_kind": kind,
                "generated_at": generated_at,
            }
        )

    ordered = []
    for day in sorted(buckets, reverse=True)[:limit]:
        bucket = buckets[day]
        ordered.append(
            {
                "day": day,
                "count": bucket["count"],
                "statuses": dict(bucket["statuses"]),
                "kinds": dict(bucket["kinds"]),
                "items": bucket["items"][:8],
            }
        )
    return {
        "detail_href": "runs/timeline.html",
        "total_count": len(manifests),
        "buckets": ordered,
    }


def _build_graph_summary(workspace: Workspace, limit: int = 6) -> Dict[str, object]:
    if not workspace.graph_manifest_path.exists():
        return {
            "manifest_path": workspace.relative_path(workspace.graph_manifest_path),
            "node_count": 0,
            "edge_count": 0,
            "conflict_count": 0,
            "node_counts_by_kind": {},
            "edge_counts_by_kind": {},
            "top_connected_artifacts": [],
            "conflicts": [],
            "nodes": [],
        }

    manifest = read_json_manifest(workspace.graph_manifest_path)
    nodes = list(manifest.get("nodes", []))
    edges = list(manifest.get("edges", []))
    node_counts: Counter[str] = Counter(str(node.get("kind", "unknown")) for node in nodes)
    edge_counts: Counter[str] = Counter(str(edge.get("kind", "unknown")) for edge in edges)
    node_index = {str(node.get("id", "")): node for node in nodes}
    degrees: Counter[str] = Counter()
    conflicts: List[Dict[str, str]] = []
    for edge in edges:
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if source:
            degrees[source] += 1
        if target:
            degrees[target] += 1
        if str(edge.get("kind", "")) == "conflict":
            conflicts.append(
                {
                    "subject": str(edge.get("subject", "")),
                    "verb": str(edge.get("verb", "")),
                    "source": source,
                    "target": target,
                    "left_value": str(edge.get("left_value", "")),
                    "right_value": str(edge.get("right_value", "")),
                    "source_href": _node_detail_href(source, str(node_index.get(source, {}).get("kind", "node"))),
                    "target_href": _node_detail_href(target, str(node_index.get(target, {}).get("kind", "node"))),
                }
            )

    graph_nodes = []
    for node in sorted(nodes, key=lambda item: (str(item.get("kind", "")), str(item.get("title", item.get("id", ""))).lower(), str(item.get("id", "")))):
        node_id = str(node.get("id", ""))
        kind = str(node.get("kind", "unknown"))
        title = _node_title(node)
        path = str(node.get("path", ""))
        collection = str(node.get("collection", ""))
        detail_href = _node_detail_href(node_id, kind)
        graph_nodes.append(
            {
                "id": node_id,
                "title": title,
                "kind": kind,
                "path": path,
                "collection": collection,
                "degree": int(degrees.get(node_id, 0)),
                "detail_href": detail_href,
                "search_text": _normalize_search_text([title, node_id, kind, path, collection]),
            }
        )

    top_connected_artifacts = []
    artifact_nodes = [node for node in graph_nodes if node.get("path")]
    for item in sorted(artifact_nodes, key=lambda value: (-int(value.get("degree", 0)), str(value.get("title", "")).lower()))[:limit]:
        top_connected_artifacts.append(item)

    return {
        "manifest_path": workspace.relative_path(workspace.graph_manifest_path),
        "generated_at": str(manifest.get("generated_at", "")),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "conflict_count": len(conflicts),
        "node_counts_by_kind": dict(node_counts),
        "edge_counts_by_kind": dict(edge_counts),
        "top_connected_artifacts": top_connected_artifacts,
        "conflicts": conflicts[:limit],
        "nodes": graph_nodes,
    }


def _build_concept_graph(workspace: Workspace, limit: int = 18) -> Dict[str, object]:
    if not workspace.graph_manifest_path.exists():
        return {
            "map_href": "graph/concept-map.html",
            "selected_node_count": 0,
            "selected_edge_count": 0,
            "nodes": [],
            "edges": [],
        }

    manifest = read_json_manifest(workspace.graph_manifest_path)
    nodes = list(manifest.get("nodes", []))
    edges = list(manifest.get("edges", []))
    node_index = {str(node.get("id", "")): node for node in nodes}
    degree_counts: Counter[str] = Counter()
    for edge in edges:
        degree_counts[str(edge.get("source", ""))] += 1
        degree_counts[str(edge.get("target", ""))] += 1

    interesting_nodes = [
        node
        for node in nodes
        if str(node.get("kind", "")) in {"tag", "entity", "concept_candidate", "artifact"}
    ]
    interesting_nodes.sort(
        key=lambda node: (
            str(node.get("kind", "")) == "artifact",
            -degree_counts[str(node.get("id", ""))],
            str(node.get("title", node.get("id", ""))).lower(),
        )
    )
    selected = interesting_nodes[:limit]
    if not selected:
        return {
            "map_href": "graph/concept-map.html",
            "selected_node_count": 0,
            "selected_edge_count": 0,
            "nodes": [],
            "edges": [],
        }

    lanes = {"tag": 120, "entity": 360, "concept_candidate": 620, "artifact": 900}
    lane_groups: Dict[str, List[Dict[str, object]]] = {key: [] for key in lanes}
    for node in selected:
        kind = str(node.get("kind", "artifact"))
        lane_groups.setdefault(kind, []).append(node)

    laid_out_nodes: List[Dict[str, object]] = []
    selected_ids = {str(node.get("id", "")) for node in selected}
    for kind, x in lanes.items():
        group = sorted(lane_groups.get(kind, []), key=lambda node: (-degree_counts[str(node.get("id", ""))], str(node.get("title", node.get("id", ""))).lower()))
        for index, node in enumerate(group):
            node_id = str(node.get("id", ""))
            y = 90 + (index * 90)
            laid_out_nodes.append(
                {
                    "id": node_id,
                    "title": _node_title(node),
                    "kind": kind,
                    "x": x,
                    "y": y,
                    "detail_href": _node_detail_href(node_id, kind),
                }
            )

    selected_edges = []
    seen = set()
    for edge in edges:
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if source not in selected_ids or target not in selected_ids:
            continue
        key = (source, target, str(edge.get("kind", "")))
        if key in seen:
            continue
        seen.add(key)
        selected_edges.append(
            {
                "source": source,
                "target": target,
                "kind": str(edge.get("kind", "")),
            }
        )

    return {
        "map_href": "graph/concept-map.html",
        "selected_node_count": len(laid_out_nodes),
        "selected_edge_count": len(selected_edges),
        "nodes": laid_out_nodes,
        "edges": selected_edges,
    }


def _write_artifact_preview_pages(workspace: Workspace, bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    for relative_path in _collect_preview_targets(workspace, state_payload):
        artifact_path = _resolve_workspace_relative(workspace, relative_path)
        if not artifact_path.exists():
            continue
        preview_path = bundle_dir / _artifact_preview_href(relative_path)
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        preview_path.write_text(
            _render_artifact_preview_html(
                current_href=_artifact_preview_href(relative_path),
                relative_path=relative_path,
                artifact_path=artifact_path,
            ),
            encoding="utf-8",
        )


def _collect_preview_targets(workspace: Workspace, state_payload: Dict[str, object]) -> List[str]:
    targets: List[str] = []
    seen = set()

    def add_target(relative_path: str) -> None:
        normalized = relative_path.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        targets.append(normalized)

    for item in list(state_payload.get("change_summaries", [])):
        add_target(str(item.get("href", "")))

    graph = dict(state_payload.get("graph", {}))
    for node in list(graph.get("nodes", [])):
        add_target(str(node.get("path", "")))

    runs = dict(state_payload.get("runs", {}))
    for item in list(runs.get("items", [])):
        add_target(str(item.get("path", "")))
        manifest_path = _resolve_workspace_relative(workspace, str(item.get("path", "")))
        if not manifest_path.exists():
            continue
        manifest = read_json_manifest(manifest_path)
        for key in (
            "plan_path",
            "plan_json_path",
            "report_path",
            "packet_path",
            "answer_path",
            "slide_path",
            "change_summary_path",
        ):
            add_target(str(manifest.get(key, "") or ""))

    jobs = dict(state_payload.get("jobs", {}))
    for item in list(jobs.get("items", [])):
        add_target(str(item.get("path", "")))
        manifest_path = _resolve_workspace_relative(workspace, str(item.get("path", "")))
        if not manifest_path.exists():
            continue
        manifest = read_json_manifest(manifest_path)
        result = dict(manifest.get("result", {}))
        for key in (
            "run_manifest_path",
            "report_path",
            "answer_path",
            "change_summary_path",
            "training_loop_manifest_path",
        ):
            add_target(str(result.get(key, "") or ""))
        for relative_path in list(result.get("remediation_manifest_paths", [])):
            add_target(str(relative_path or ""))

    return targets


def _write_graph_detail_pages(workspace: Workspace, bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    graph = dict(state_payload.get("graph", {}))
    nodes = list(graph.get("nodes", []))
    if not nodes or not workspace.graph_manifest_path.exists():
        return
    manifest = read_json_manifest(workspace.graph_manifest_path)
    manifest_nodes = {str(node.get("id", "")): dict(node) for node in manifest.get("nodes", [])}
    edges = list(manifest.get("edges", []))
    href_by_node = {str(node.get("id", "")): str(node.get("detail_href", "")) for node in nodes}

    for node in nodes:
        node_id = str(node.get("id", ""))
        node_manifest = manifest_nodes.get(node_id, {})
        detail_path = bundle_dir / str(node.get("detail_href", ""))
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_path.write_text(
            _render_graph_node_detail_html(
                current_href=str(node.get("detail_href", "")),
                node_summary=node,
                node_manifest=node_manifest,
                relationships=_build_node_relationships(
                    current_href=str(node.get("detail_href", "")),
                    node_id=node_id,
                    manifest_nodes=manifest_nodes,
                    edges=edges,
                    href_by_node=href_by_node,
                ),
            ),
            encoding="utf-8",
        )


def _write_run_detail_pages(workspace: Workspace, bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    runs = dict(state_payload.get("runs", {}))
    items = list(runs.get("items", []))
    for item in items:
        manifest_path = _resolve_workspace_relative(workspace, str(item.get("path", "")))
        if not manifest_path.exists():
            continue
        manifest = read_json_manifest(manifest_path)
        detail_path = bundle_dir / str(item.get("detail_href", ""))
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_path.write_text(
            _render_run_detail_html(
                current_href=str(item.get("detail_href", "")),
                item=item,
                manifest=manifest,
            ),
            encoding="utf-8",
        )


def _write_job_detail_pages(workspace: Workspace, bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    jobs = dict(state_payload.get("jobs", {}))
    items = list(jobs.get("items", []))
    for item in items:
        manifest_path = _resolve_workspace_relative(workspace, str(item.get("path", "")))
        if not manifest_path.exists():
            continue
        manifest = read_json_manifest(manifest_path)
        detail_path = bundle_dir / str(item.get("detail_href", ""))
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_path.write_text(
            _render_job_detail_html(
                current_href=str(item.get("detail_href", "")),
                item=item,
                manifest=manifest,
            ),
            encoding="utf-8",
        )


def _write_sync_detail_pages(workspace: Workspace, bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    sync = dict(state_payload.get("sync", {}))
    items = list(sync.get("items", []))
    for item in items:
        manifest_path = _resolve_workspace_relative(workspace, str(item.get("path", "")))
        if not manifest_path.exists():
            continue
        manifest = read_json_manifest(manifest_path)
        detail_path = bundle_dir / str(item.get("detail_href", ""))
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_path.write_text(
            _render_sync_detail_html(
                current_href=str(item.get("detail_href", "")),
                item=item,
                manifest=manifest,
            ),
            encoding="utf-8",
        )


def _write_run_timeline_page(bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    timeline = dict(state_payload.get("run_timeline", {}))
    detail_href = str(timeline.get("detail_href", "")).strip()
    if not detail_href:
        return
    detail_path = bundle_dir / detail_href
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.write_text(
        _render_run_timeline_page_html(current_href=detail_href, timeline=timeline),
        encoding="utf-8",
    )


def _write_concept_graph_page(bundle_dir: Path, state_payload: Dict[str, object]) -> None:
    concept_graph = dict(state_payload.get("concept_graph", {}))
    map_href = str(concept_graph.get("map_href", "")).strip()
    if not map_href:
        return
    map_path = bundle_dir / map_href
    map_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.write_text(
        _render_concept_graph_page_html(current_href=map_href, concept_graph=concept_graph),
        encoding="utf-8",
    )


def _render_graph_node_detail_html(
    current_href: str,
    node_summary: Dict[str, object],
    node_manifest: Dict[str, object],
    relationships: List[Dict[str, str]],
) -> str:
    artifact_path = str(node_summary.get("path", "")).strip()
    artifact_preview = (
        _render_link_value(current_href, artifact_path, _artifact_preview_href(artifact_path))
        if artifact_path
        else "-"
    )
    title = escape(str(node_summary.get("title", node_summary.get("id", "Graph Node"))))
    subtitle = escape(str(node_summary.get("kind", "node")))
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Node Metadata</h2>",
        _render_detail_fields(
            [
                ("ID", _render_code_value(str(node_summary.get("id", "")))),
                ("Kind", _render_code_value(str(node_summary.get("kind", "")))),
                ("Collection", _render_code_value(str(node_summary.get("collection", "")) or "-")),
                ("Path", _render_code_value(str(node_summary.get("path", "")) or "-")),
                ("Artifact Preview", artifact_preview),
                ("Degree", _render_code_value(str(node_summary.get("degree", 0)))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Relationships</h2>",
        _render_relationships_table(relationships),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Node Snapshot</h2>",
        "  <details>",
        "    <summary>Show node JSON</summary>",
        f"    <pre>{escape(json.dumps(node_manifest, indent=2, sort_keys=True))}</pre>",
        "  </details>",
        "</article>",
    ]
    return _render_detail_page(
        title="Graph Node Detail",
        heading=title,
        subtitle=subtitle,
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _render_run_detail_html(
    current_href: str,
    item: Dict[str, object],
    manifest: Dict[str, object],
) -> str:
    validation = dict(manifest.get("validation", {}))
    citations = dict(manifest.get("citations", {}))
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Run Metadata</h2>",
        _render_detail_fields(
            [
                ("Label", _render_code_value(str(item.get("label", "")))),
                ("Kind", _render_code_value(str(item.get("run_kind", "")))),
                ("Status", _render_code_value(str(item.get("status", "")))),
                ("Generated", _render_code_value(str(item.get("generated_at", "")))),
                ("Mode", _render_code_value(str(manifest.get("mode", "")) or "-")),
                ("Question", _render_code_value(str(manifest.get("question", "")) or "-")),
                ("Plan", _render_preview_value(current_href, str(manifest.get("plan_path", "")))),
                ("Plan JSON", _render_preview_value(current_href, str(manifest.get("plan_json_path", "")))),
                ("Report", _render_preview_value(current_href, str(manifest.get("report_path", "")))),
                ("Packet", _render_preview_value(current_href, str(manifest.get("packet_path", "")))),
                ("Answer", _render_preview_value(current_href, str(manifest.get("answer_path", "")))),
                ("Slides", _render_preview_value(current_href, str(manifest.get("slide_path", "")))),
                ("Change Summary", _render_preview_value(current_href, str(manifest.get("change_summary_path", "")))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Validation and Citations</h2>",
        _render_detail_fields(
            [
                ("Validation Passed", _render_code_value(str(validation.get("passed", False)))),
                ("Validation Errors", _render_code_value(str(len(list(validation.get("errors", [])))))),
                ("Validation Warnings", _render_code_value(str(len(list(validation.get("warnings", [])))))),
                ("Available Citations", _render_code_value(str(len(list(citations.get("available", [])))))),
                ("Used Citations", _render_code_value(str(len(list(citations.get("used", [])))))),
                ("Resume Count", _render_code_value(str(manifest.get("resume_count", 0)))),
                ("Attempt Count", _render_code_value(str(manifest.get("attempt_count", 0)))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Run Snapshot</h2>",
        "  <details>",
        "    <summary>Show run JSON</summary>",
        f"    <pre>{escape(json.dumps(manifest, indent=2, sort_keys=True))}</pre>",
        "  </details>",
        "</article>",
    ]
    return _render_detail_page(
        title="Run Detail",
        heading=escape(str(item.get("label", "Run Detail"))),
        subtitle=escape(str(item.get("run_kind", "run"))),
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _render_job_detail_html(
    current_href: str,
    item: Dict[str, object],
    manifest: Dict[str, object],
) -> str:
    result = dict(manifest.get("result", {}))
    audit_entries = list(manifest.get("audit", []))
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Job Metadata</h2>",
        _render_detail_fields(
            [
                ("Job ID", _render_code_value(str(manifest.get("job_id", "")))),
                ("Type", _render_code_value(str(manifest.get("job_type", "")))),
                ("Status", _render_code_value(str(manifest.get("status", "")))),
                ("Title", _render_code_value(str(manifest.get("title", "")))),
                ("Created", _render_code_value(str(manifest.get("created_at", "")))),
                ("Updated", _render_code_value(str(manifest.get("updated_at", "")))),
                ("Attempts", _render_code_value(str(manifest.get("attempts", 0)))),
                ("Retry Of", _render_code_value(str(manifest.get("retry_of_job_id", "")) or "-")),
                ("Error", _render_code_value(str(manifest.get("error", "")) or "-")),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Job Result</h2>",
        _render_detail_fields(
            [
                ("Run Manifest", _render_preview_value(current_href, str(result.get("run_manifest_path", "")))),
                ("Report", _render_preview_value(current_href, str(result.get("report_path", "")))),
                ("Answer", _render_preview_value(current_href, str(result.get("answer_path", "")))),
                ("Change Summary", _render_preview_value(current_href, str(result.get("change_summary_path", "")))),
                ("Training Loop Manifest", _render_preview_value(current_href, str(result.get("training_loop_manifest_path", "")))),
                ("Training Loop Directory", _render_code_value(str(result.get("training_loop_dir", "")) or "-")),
                ("Warning Count", _render_code_value(str(result.get("warning_count", "")) or "-")),
                ("Remediated Count", _render_code_value(str(result.get("remediated_count", "")) or "-")),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Audit Trail</h2>",
        _render_job_audit_table(audit_entries),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Job Snapshot</h2>",
        "  <details>",
        "    <summary>Show job JSON</summary>",
        f"    <pre>{escape(json.dumps(manifest, indent=2, sort_keys=True))}</pre>",
        "  </details>",
        "</article>",
    ]
    return _render_detail_page(
        title="Job Detail",
        heading=escape(str(item.get("label", "Job Detail"))),
        subtitle=escape(str(item.get("job_type", "job"))),
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _render_sync_detail_html(
    current_href: str,
    item: Dict[str, object],
    manifest: Dict[str, object],
) -> str:
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Sync Metadata</h2>",
        _render_detail_fields(
            [
                ("Sync ID", _render_code_value(str(manifest.get("sync_id", "")))),
                ("Operation", _render_code_value(str(manifest.get("operation", "")))),
                ("Status", _render_code_value(str(manifest.get("status", "")))),
                ("Generated", _render_code_value(str(manifest.get("generated_at", "")))),
                ("File Count", _render_code_value(str(manifest.get("file_count", 0)))),
                ("Bundle Directory", _render_code_value(str(manifest.get("bundle_dir_relative", "")) or str(manifest.get("bundle_dir", "")))),
                ("Bundle Manifest", _render_code_value(str(manifest.get("bundle_manifest_relative", "")) or str(manifest.get("bundle_manifest_path", "")))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Included Paths</h2>",
        _render_sync_included_paths(list(manifest.get("included_paths", []))),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Sync Snapshot</h2>",
        "  <details>",
        "    <summary>Show sync JSON</summary>",
        f"    <pre>{escape(json.dumps(manifest, indent=2, sort_keys=True))}</pre>",
        "  </details>",
        "</article>",
    ]
    return _render_detail_page(
        title="Sync Detail",
        heading=escape(str(item.get("label", "Sync Detail"))),
        subtitle=escape(str(item.get("operation", "sync"))),
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _render_run_timeline_page_html(current_href: str, timeline: Dict[str, object]) -> str:
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Timeline Overview</h2>",
        _render_detail_fields(
            [
                ("Days", _render_code_value(str(len(list(timeline.get("buckets", [])))))),
                ("Runs", _render_code_value(str(timeline.get("total_count", 0)))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Timeline Buckets</h2>",
        _render_run_timeline_buckets(list(timeline.get("buckets", [])), current_href=current_href),
        "</article>",
    ]
    return _render_detail_page(
        title="Run Timeline",
        heading="Run Timeline",
        subtitle="chronological run ledger",
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _render_concept_graph_page_html(current_href: str, concept_graph: Dict[str, object]) -> str:
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Graph Overview</h2>",
        _render_detail_fields(
            [
                ("Selected Nodes", _render_code_value(str(concept_graph.get("selected_node_count", 0)))),
                ("Selected Edges", _render_code_value(str(concept_graph.get("selected_edge_count", 0)))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Concept Graph</h2>",
        _render_concept_graph_svg(concept_graph, current_href=current_href),
        "</article>",
    ]
    return _render_detail_page(
        title="Concept Graph",
        heading="Concept Graph",
        subtitle="filesystem-backed graph map",
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _render_detail_page(title: str, heading: str, subtitle: str, current_href: str, body_html: str) -> str:
    dashboard_href = escape(_relative_href(current_href, "index.html"))
    return "\n".join(
        [
            "<!doctype html>",
            "<html lang=\"en\">",
            "<head>",
            "  <meta charset=\"utf-8\">",
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            f"  <title>{escape(title)}</title>",
            "  <style>",
            "    :root { color-scheme: light; --bg: #f4f1e8; --panel: #fffdf7; --ink: #1f2933; --muted: #5c6b73; --line: #d8d2c2; --accent: #165d52; font-family: 'Iowan Old Style', 'Palatino Linotype', 'Book Antiqua', serif; }",
            "    * { box-sizing: border-box; }",
            "    body { margin: 0; background: linear-gradient(180deg, #ece6d8 0%, var(--bg) 100%); color: var(--ink); }",
            "    main { max-width: 980px; margin: 0 auto; padding: 40px 24px 72px; }",
            "    .panel { background: rgba(255, 253, 247, 0.9); border: 1px solid var(--line); border-radius: 18px; padding: 20px; box-shadow: 0 14px 50px rgba(41, 50, 58, 0.07); margin-top: 18px; }",
            "    h1, h2 { margin: 0 0 12px; line-height: 1.1; }",
            "    h1 { font-size: clamp(2rem, 4vw, 3rem); letter-spacing: -0.04em; }",
            "    p, li, td, th { line-height: 1.55; }",
            "    table { width: 100%; border-collapse: collapse; }",
            "    th, td { text-align: left; padding: 10px 0; border-bottom: 1px solid rgba(216, 210, 194, 0.75); vertical-align: top; }",
            "    th { color: var(--muted); font-weight: 600; }",
            "    code, pre { font-family: 'SFMono-Regular', 'Menlo', monospace; }",
            "    code { background: rgba(22, 93, 82, 0.08); padding: 0.12em 0.35em; border-radius: 0.4em; }",
            "    pre { overflow-x: auto; white-space: pre-wrap; }",
            "    a { color: var(--accent); }",
            "    .muted { color: var(--muted); }",
            "  </style>",
            "</head>",
            "<body>",
            "  <main>",
            f"    <p><a href=\"{dashboard_href}\">Back to review dashboard</a></p>",
            f"    <h1>{heading}</h1>",
            f"    <p class=\"muted\">{subtitle} · {escape(title)}</p>",
            body_html,
            "  </main>",
            "</body>",
            "</html>",
            "",
        ]
    )


def _render_detail_fields(rows: Sequence[tuple[str, str]]) -> str:
    lines = ["  <table>", "    <tbody>"]
    for label, value in rows:
        lines.extend(
            [
                "      <tr>",
                f"        <th>{escape(label)}</th>",
                f"        <td>{value}</td>",
                "      </tr>",
            ]
        )
    lines.extend(["    </tbody>", "  </table>"])
    return "\n".join(lines)


def _render_job_audit_table(entries: List[Dict[str, object]]) -> str:
    if not entries:
        return "  <p class=\"muted\">No audit entries found.</p>"
    lines = [
        "  <table>",
        "    <thead><tr><th>Timestamp</th><th>Status</th><th>Message</th></tr></thead>",
        "    <tbody>",
    ]
    for entry in entries:
        lines.extend(
            [
                "      <tr>",
                f"        <td>{escape(str(entry.get('timestamp', '')))}</td>",
                f"        <td>{escape(str(entry.get('status', '')))}</td>",
                f"        <td>{escape(str(entry.get('message', '')))}</td>",
                "      </tr>",
            ]
        )
    lines.extend(["    </tbody>", "  </table>"])
    return "\n".join(lines)


def _render_sync_included_paths(paths: List[object]) -> str:
    if not paths:
        return "  <p class=\"muted\">No included paths recorded.</p>"
    lines = ["  <ul>"]
    for path in paths:
        lines.append(f"    <li><code>{escape(str(path))}</code></li>")
    lines.append("  </ul>")
    return "\n".join(lines)


def _render_code_value(value: str) -> str:
    return f"<code>{escape(value)}</code>"


def _render_link_value(current_href: str, label: str, target_href: str) -> str:
    return f"<a href=\"{escape(_relative_href(current_href, target_href))}\"><code>{escape(label)}</code></a>"


def _render_preview_value(current_href: str, relative_path: str) -> str:
    normalized = relative_path.strip()
    if not normalized:
        return _render_code_value("-")
    return _render_link_value(current_href, normalized, _artifact_preview_href(normalized))


def _render_run_timeline_buckets(buckets: List[Dict[str, object]], current_href: str) -> str:
    if not buckets:
        return "  <p class=\"muted\">No run timeline buckets found.</p>"
    lines = [
        "  <table>",
        "    <thead><tr><th>Day</th><th>Runs</th><th>Statuses</th><th>Recent Items</th></tr></thead>",
        "    <tbody>",
    ]
    for bucket in buckets:
        items = list(bucket.get("items", []))
        recent = "<br>".join(
            [
                "<a href=\""
                + escape(_relative_href(current_href, str(item.get("detail_href", ""))))
                + "\">"
                + escape(str(item.get("label", "")))
                + "</a>"
                for item in items[:3]
            ]
        ) or "-"
        lines.extend(
            [
                "      <tr>",
                f"        <td>{escape(str(bucket.get('day', '')))}</td>",
                f"        <td>{escape(str(bucket.get('count', 0)))}</td>",
                f"        <td>{escape(_format_counts_inline(dict(bucket.get('statuses', {}))))}</td>",
                f"        <td>{recent}</td>",
                "      </tr>",
            ]
        )
    lines.extend(["    </tbody>", "  </table>"])
    return "\n".join(lines)


def _render_concept_graph_svg(concept_graph: Dict[str, object], current_href: str) -> str:
    nodes = list(concept_graph.get("nodes", []))
    edges = list(concept_graph.get("edges", []))
    if not nodes:
        return "<p class=\"empty\">No concept graph data available.</p>"

    by_id = {str(node.get("id", "")): node for node in nodes}
    max_y = max(int(node.get("y", 0)) for node in nodes) + 70
    parts = [
        f"<svg viewBox=\"0 0 1020 {max(240, max_y)}\" width=\"100%\" role=\"img\" aria-label=\"Concept graph\">",
        "<rect x=\"0\" y=\"0\" width=\"1020\" height=\"100%\" fill=\"#fffaf0\"></rect>",
    ]
    for edge in edges:
        source = by_id.get(str(edge.get("source", "")))
        target = by_id.get(str(edge.get("target", "")))
        if source is None or target is None:
            continue
        parts.append(
            "<line x1=\""
            + escape(str(int(source.get("x", 0)) + 70))
            + "\" y1=\""
            + escape(str(int(source.get("y", 0)) + 18))
            + "\" x2=\""
            + escape(str(int(target.get("x", 0)) - 10))
            + "\" y2=\""
            + escape(str(int(target.get("y", 0)) + 18))
            + "\" stroke=\"#d8d2c2\" stroke-width=\"2\"></line>"
        )
    for node in nodes:
        x = int(node.get("x", 0))
        y = int(node.get("y", 0))
        fill = {"tag": "#dff2ec", "entity": "#fdf0e3", "concept_candidate": "#fbeaea", "artifact": "#e9eef4"}.get(
            str(node.get("kind", "")),
            "#f3efe4",
        )
        href = escape(_relative_href(current_href, str(node.get("detail_href", ""))))
        title = escape(str(node.get("title", "")))
        kind = escape(str(node.get("kind", "")))
        parts.extend(
            [
                f"<a href=\"{href}\">",
                f"<rect x=\"{x}\" y=\"{y}\" width=\"150\" height=\"38\" rx=\"14\" fill=\"{fill}\" stroke=\"#d8d2c2\"></rect>",
                f"<text x=\"{x + 12}\" y=\"{y + 16}\" font-size=\"12\" fill=\"#1f2933\">{title}</text>",
                f"<text x=\"{x + 12}\" y=\"{y + 30}\" font-size=\"10\" fill=\"#5c6b73\">{kind}</text>",
                "</a>",
            ]
        )
    parts.append("</svg>")
    return "".join(parts)


def _format_counts_inline(values: Dict[str, object]) -> str:
    if not values:
        return "none"
    return ", ".join(f"{key}:{value}" for key, value in sorted(values.items()))


def _render_artifact_preview_html(current_href: str, relative_path: str, artifact_path: Path) -> str:
    preview_text, binary = _read_artifact_preview(artifact_path)
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Artifact Metadata</h2>",
        _render_detail_fields(
            [
                ("Relative Path", _render_code_value(relative_path)),
                ("Absolute Path", _render_code_value(artifact_path.as_posix())),
                ("Suffix", _render_code_value(artifact_path.suffix or "-")),
                ("Bytes", _render_code_value(str(artifact_path.stat().st_size))),
                ("Binary", _render_code_value(str(binary))),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Artifact Preview</h2>",
        "  <pre>" + escape(preview_text) + "</pre>",
        "</article>",
    ]
    return _render_detail_page(
        title="Artifact Preview",
        heading=escape(artifact_path.name),
        subtitle=escape(relative_path),
        current_href=current_href,
        body_html="\n".join(body_lines),
    )


def _read_artifact_preview(path: Path, max_chars: int = 20000) -> tuple[str, bool]:
    suffix = path.suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf"}:
        return "Binary artifact preview is not rendered inline. Open the original file from the workspace if needed.", True
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return "Could not read artifact preview.", True
    if len(text) > max_chars:
        text = text[:max_chars] + "\n\n[truncated]"
    return text, False


def _render_relationships_table(relationships: List[Dict[str, str]]) -> str:
    if not relationships:
        return "  <p class=\"muted\">No recorded relationships for this node.</p>"
    lines = [
        "  <table>",
        "    <thead><tr><th>Direction</th><th>Kind</th><th>Counterpart</th><th>Detail</th></tr></thead>",
        "    <tbody>",
    ]
    for item in relationships:
        counterpart = item.get("counterpart_label", item.get("counterpart_id", ""))
        counterpart_id = item.get("counterpart_id", "")
        counterpart_href = item.get("counterpart_href")
        counterpart_html = escape(counterpart)
        if counterpart_href:
            counterpart_html = f"<a href=\"{escape(counterpart_href)}\">{counterpart_html}</a>"
        lines.extend(
            [
                "      <tr>",
                f"        <td>{escape(item.get('direction', ''))}</td>",
                f"        <td>{escape(item.get('kind', ''))}</td>",
                f"        <td>{counterpart_html}<br><span class=\"muted\"><code>{escape(counterpart_id)}</code></span></td>",
                f"        <td>{escape(item.get('detail', '-'))}</td>",
                "      </tr>",
            ]
        )
    lines.extend(["    </tbody>", "  </table>"])
    return "\n".join(lines)


def _build_node_relationships(
    current_href: str,
    node_id: str,
    manifest_nodes: Dict[str, Dict[str, object]],
    edges: Sequence[Dict[str, object]],
    href_by_node: Dict[str, str],
) -> List[Dict[str, str]]:
    relationships: List[Dict[str, str]] = []
    for edge in edges:
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if node_id not in {source, target}:
            continue
        counterpart_id = target if source == node_id else source
        counterpart = manifest_nodes.get(counterpart_id, {})
        counterpart_label = _node_title(counterpart) if counterpart else counterpart_id
        counterpart_href = href_by_node.get(counterpart_id)
        relationships.append(
            {
                "direction": "outbound" if source == node_id else "inbound",
                "kind": str(edge.get("kind", "")),
                "counterpart_id": counterpart_id,
                "counterpart_label": counterpart_label,
                "counterpart_href": _relative_href(current_href, counterpart_href) if counterpart_href else "",
                "detail": _edge_detail(edge),
            }
        )
    relationships.sort(key=lambda item: (item["kind"], item["direction"], item["counterpart_label"].lower()))
    return relationships


def _edge_detail(edge: Dict[str, object]) -> str:
    detail_parts = []
    for key in ("subject", "verb", "left_value", "right_value", "evidence_kind", "confidence"):
        value = edge.get(key)
        if value not in (None, ""):
            detail_parts.append(f"{key}={value}")
    if not detail_parts:
        return "-"
    return "; ".join(detail_parts)


def _resolve_workspace_relative(workspace: Workspace, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute():
        return path
    return workspace.root / path


def _collect_filter_values(items: Sequence[Dict[str, object]], key: str) -> List[str]:
    values = {str(item.get(key, "")).strip() for item in items}
    return sorted(value for value in values if value)


def _normalize_search_text(values: Sequence[object]) -> str:
    parts = [str(value).strip().lower() for value in values if str(value).strip()]
    return " ".join(parts)


def _node_title(node: Dict[str, object]) -> str:
    title = str(node.get("title", "")).strip()
    if title:
        return title
    return str(node.get("id", "Node")).strip() or "Node"


def _node_detail_href(node_id: str, kind: str) -> str:
    digest = hashlib.sha1(node_id.encode("utf-8")).hexdigest()[:10]
    slug = slugify(f"{kind}-{node_id}")[:64] or "node"
    return f"graph/{slug}-{digest}.html"


def _artifact_preview_href(relative_path: str) -> str:
    normalized = relative_path.strip()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:10]
    stem = Path(normalized).stem or "artifact"
    slug = slugify(f"{stem}-{normalized}")[:64] or "artifact"
    return f"artifacts/{slug}-{digest}.html"


def _run_detail_href(relative_path: str) -> str:
    stem = Path(relative_path).stem
    return f"runs/{stem}.html"


def _job_detail_href(job_id: str) -> str:
    return f"jobs/{slugify(job_id) or 'job'}.html"


def _sync_detail_href(sync_id: str) -> str:
    return f"sync/{slugify(sync_id) or 'sync'}.html"


def _relative_href(from_href: str, to_href: Optional[str]) -> str:
    if not to_href:
        return ""
    base = posixpath.dirname(from_href) or "."
    return posixpath.relpath(to_href, base)


def _refresh_review_ui_bundle(workspace: Workspace, bundle_dir: Path, index_name: str) -> None:
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    write_review_ui_bundle(workspace, snapshot, output_file=bundle_dir / index_name)


class _ReviewUiHandler(SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args,
        index_name: str = "index.html",
        workspace: Optional[Workspace] = None,
        **kwargs,
    ) -> None:
        self._index_name = index_name
        self._workspace = workspace
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        if self.path in {"", "/"}:
            self.path = f"/{self._index_name}"
        super().do_GET()

    def do_POST(self) -> None:
        if self._workspace is None:
            self.send_error(405, "Review actions are only available when serving a live workspace.")
            return

        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(length).decode("utf-8", errors="ignore")
        payload = {key: values[-1] for key, values in parse_qs(raw_body, keep_blank_values=True).items()}

        try:
            self._apply_review_action(parsed.path, payload)
            _refresh_review_ui_bundle(self._workspace, Path(self.directory), self._index_name)
        except MaintenanceError as error:
            self.send_response(400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(str(error).encode("utf-8"))
            return

        self.send_response(303)
        self.send_header("Location", f"/{self._index_name}")
        self.end_headers()

    def _apply_review_action(self, path: str, payload: Dict[str, str]) -> None:
        workspace = self._workspace
        if workspace is None:
            raise MaintenanceError("No workspace is attached to the review UI server.")

        if path == "/api/review/accept-concept":
            accept_concept_candidate(workspace, payload.get("slug", ""))
            return
        if path == "/api/review/apply-backlink":
            apply_backlink_suggestion(workspace, payload.get("target_path", ""))
            return
        if path == "/api/review/resolve-merge":
            resolve_entity_merge(workspace, payload.get("canonical_label", ""))
            return
        if path == "/api/review/file-conflict":
            file_conflict_review(workspace, payload.get("subject", ""))
            return
        if path == "/api/review/dismiss":
            dismiss_review_item(workspace, payload.get("review_id", ""), payload.get("reason", ""))
            return
        if path == "/api/review/reopen":
            reopen_review_item(workspace, payload.get("review_id", ""))
            return
        raise MaintenanceError(f"Unknown review action endpoint: {path}")

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()
