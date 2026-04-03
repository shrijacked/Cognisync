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
from typing import Dict, List, Optional, Sequence

from cognisync.manifests import read_json_manifest
from cognisync.review_exports import build_review_export_payload
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
    _write_graph_detail_pages(workspace, html_path.parent, state_payload)
    _write_run_detail_pages(workspace, html_path.parent, state_payload)

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
) -> ThreadingHTTPServer:
    directory = Path(directory).resolve()
    handler = partial(_ReviewUiHandler, directory=str(directory), index_name=index_name)
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
    graph = dict(payload.get("graph", {}))
    graph_nodes = list(graph.get("nodes", []))
    runs = dict(payload.get("runs", {}))
    run_items = list(runs.get("items", []))
    recent_change_summaries = list(payload.get("change_summaries", []))
    serialized_payload = json.dumps(payload, indent=2, sort_keys=True)

    cards = [
        ("Open Review Items", str(summary.get("open_item_count", 0))),
        ("Dismissed Review Items", str(summary.get("dismissed_item_count", 0))),
        ("Graph Nodes", str(graph.get("node_count", 0))),
        ("Graph Edges", str(graph.get("edge_count", 0))),
        ("Recorded Runs", str(runs.get("total_count", 0))),
        ("Known Conflicts", str(graph.get("conflict_count", 0))),
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
        "      <p class=\"lede\">A lightweight browser surface over the filesystem-native review loop. This dashboard is generated from the current workspace manifests and keeps the queue, dismissals, graph activity, and operator runs readable without scraping terminal output.</p>",
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
            "          <h2>Recent Change Summaries</h2>",
            _render_recent_links(recent_change_summaries, empty_label="No change summaries found."),
            "        </article>",
            "      </div>",
            "    </section>",
            "    <section class=\"stack\" style=\"margin-top: 18px;\">",
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
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "workspace": {
            "root": workspace.root.as_posix(),
            "graph_manifest_path": workspace.relative_path(workspace.graph_manifest_path),
            "review_queue_manifest_path": workspace.relative_path(workspace.review_queue_manifest_path),
            "review_actions_manifest_path": workspace.relative_path(workspace.review_actions_manifest_path),
            "runs_dir": workspace.relative_path(workspace.runs_dir),
        },
        "review": review_payload,
        "graph": _build_graph_summary(workspace),
        "runs": _build_run_history(workspace),
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
        "            <thead><tr><th>Item</th><th>Path</th><th>Priority</th></tr></thead>",
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
        lines.append(f"            <li><span class=\"mono-link\">{href}</span><br><strong>{label}</strong><br><span class=\"muted\">{meta}</span></li>")
    lines.append("          </ul>")
    return "\n".join(lines)


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
                "meta": path.name,
            }
        )
    return items


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


def _render_graph_node_detail_html(
    current_href: str,
    node_summary: Dict[str, object],
    node_manifest: Dict[str, object],
    relationships: List[Dict[str, str]],
) -> str:
    title = escape(str(node_summary.get("title", node_summary.get("id", "Graph Node"))))
    subtitle = escape(str(node_summary.get("kind", "node")))
    body_lines = [
        "<article class=\"panel\">",
        "  <h2>Node Metadata</h2>",
        _render_detail_fields(
            [
                ("ID", str(node_summary.get("id", ""))),
                ("Kind", str(node_summary.get("kind", ""))),
                ("Collection", str(node_summary.get("collection", "")) or "-"),
                ("Path", str(node_summary.get("path", "")) or "-"),
                ("Degree", str(node_summary.get("degree", 0))),
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
                ("Label", str(item.get("label", ""))),
                ("Kind", str(item.get("run_kind", ""))),
                ("Status", str(item.get("status", ""))),
                ("Generated", str(item.get("generated_at", ""))),
                ("Mode", str(manifest.get("mode", "")) or "-"),
                ("Question", str(manifest.get("question", "")) or "-"),
                ("Plan", str(manifest.get("plan_path", "")) or "-"),
                ("Report", str(manifest.get("report_path", "")) or "-"),
                ("Packet", str(manifest.get("packet_path", "")) or "-"),
                ("Answer", str(manifest.get("answer_path", "")) or "-"),
                ("Change Summary", str(manifest.get("change_summary_path", "")) or "-"),
            ]
        ),
        "</article>",
        "<article class=\"panel\">",
        "  <h2>Validation and Citations</h2>",
        _render_detail_fields(
            [
                ("Validation Passed", str(validation.get("passed", False))),
                ("Validation Errors", str(len(list(validation.get("errors", []))))),
                ("Validation Warnings", str(len(list(validation.get("warnings", []))))),
                ("Available Citations", str(len(list(citations.get("available", []))))),
                ("Used Citations", str(len(list(citations.get("used", []))))),
                ("Resume Count", str(manifest.get("resume_count", 0))),
                ("Attempt Count", str(manifest.get("attempt_count", 0))),
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
                f"        <td><code>{escape(value)}</code></td>",
                "      </tr>",
            ]
        )
    lines.extend(["    </tbody>", "  </table>"])
    return "\n".join(lines)


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


def _run_detail_href(relative_path: str) -> str:
    stem = Path(relative_path).stem
    return f"runs/{stem}.html"


def _relative_href(from_href: str, to_href: Optional[str]) -> str:
    if not to_href:
        return ""
    base = posixpath.dirname(from_href) or "."
    return posixpath.relpath(to_href, base)


class _ReviewUiHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, index_name: str = "index.html", **kwargs) -> None:
        self._index_name = index_name
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        if self.path in {"", "/"}:
            self.path = f"/{self._index_name}"
        super().do_GET()

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()
