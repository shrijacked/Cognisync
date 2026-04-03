from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from functools import partial
from html import escape
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.manifests import read_json_manifest
from cognisync.review_exports import build_review_export_payload
from cognisync.types import IndexSnapshot
from cognisync.utils import utc_timestamp
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
    runs = dict(payload.get("runs", {}))
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
        "    body { margin: 0; background: linear-gradient(180deg, #ece6d8 0%, var(--bg) 100%); color: var(--ink); }",
        "    main { max-width: 1180px; margin: 0 auto; padding: 40px 24px 72px; }",
        "    h1, h2, h3 { margin: 0 0 12px; line-height: 1.1; }",
        "    h1 { font-size: clamp(2.4rem, 5vw, 4.2rem); letter-spacing: -0.04em; }",
        "    h2 { font-size: 1.35rem; }",
        "    p, li, td, th { font-size: 0.98rem; line-height: 1.55; }",
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
        "    .pill { display: inline-flex; align-items: center; gap: 6px; padding: 6px 10px; border-radius: 999px; background: var(--accent-soft); color: var(--accent); font-size: 0.82rem; }",
        "    .pill.warn { background: var(--warn-soft); color: var(--warn); }",
        "    .pill.danger { background: var(--danger-soft); color: var(--danger); }",
        "    table { width: 100%; border-collapse: collapse; }",
        "    th, td { text-align: left; padding: 10px 0; border-bottom: 1px solid rgba(216, 210, 194, 0.75); vertical-align: top; }",
        "    th { color: var(--muted); font-weight: 600; }",
        "    code { font-family: 'SFMono-Regular', 'Menlo', monospace; font-size: 0.88em; background: rgba(22, 93, 82, 0.08); padding: 0.12em 0.35em; border-radius: 0.4em; }",
        "    .mono-link { font-family: 'SFMono-Regular', 'Menlo', monospace; font-size: 0.88rem; }",
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
        "      <p class=\"lede\">A lightweight browser surface over the filesystem-native review loop. This dashboard is generated from the current workspace manifests and keeps the queue, dismissals, and operator actions readable without scraping terminal output.</p>",
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
            _render_run_history(runs),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Recent Change Summaries</h2>",
            _render_recent_links(recent_change_summaries, empty_label="No change summaries found."),
            "        </article>",
            "        <article class=\"panel\">",
            "          <h2>Embedded Payload</h2>",
            "          <details>",
            "            <summary>Show JSON snapshot</summary>",
            f"            <pre>{escape(serialized_payload)}</pre>",
            "          </details>",
            "        </article>",
            "      </div>",
            "    </section>",
            "    <p class=\"footer\">This dashboard is generated into <code>outputs/reports/review-ui/</code> and intentionally ignored by the scanner.</p>",
            "  </main>",
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


def _render_run_history(runs: Dict[str, object]) -> str:
    items = list(runs.get("items", []))
    if not items:
        return "          <p class=\"empty\">No run manifests found.</p>"
    counts_by_kind = dict(runs.get("counts_by_kind", {}))
    counts_by_status = dict(runs.get("counts_by_status", {}))
    lines = [
        "          <div class=\"toolbar\">",
        f"            <span class=\"pill\">runs <strong>{escape(str(runs.get('total_count', 0)))}</strong></span>",
        f"            <span class=\"pill warn\">kinds <strong>{escape(str(len(counts_by_kind)))}</strong></span>",
        f"            <span class=\"pill danger\">statuses <strong>{escape(str(len(counts_by_status)))}</strong></span>",
        "          </div>",
        _render_kind_table("Run Kinds", counts_by_kind),
        _render_kind_table("Run Statuses", counts_by_status),
        "          <table>",
        "            <thead><tr><th>Run</th><th>Kind</th><th>Status</th><th>Generated</th></tr></thead>",
        "            <tbody>",
    ]
    for item in items:
        lines.extend(
            [
                "              <tr>",
                (
                    "                <td><strong>"
                    + escape(str(item.get("label", "")))
                    + "</strong><br><span class=\"muted\"><code>"
                    + escape(str(item.get("path", "")))
                    + "</code></span></td>"
                ),
                f"                <td>{escape(str(item.get('run_kind', '')))}</td>",
                f"                <td>{escape(str(item.get('status', '')))}</td>",
                f"                <td>{escape(str(item.get('generated_at', '')))}</td>",
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


def _build_run_history(workspace: Workspace, limit: int = 12) -> Dict[str, object]:
    items: List[Dict[str, str]] = []
    counts_by_kind: Counter[str] = Counter()
    counts_by_status: Counter[str] = Counter()
    for path in sorted(workspace.runs_dir.glob("*.json"), reverse=True)[:limit]:
        manifest = read_json_manifest(path)
        run_kind = str(manifest.get("run_kind", "unknown"))
        status = str(manifest.get("status", "unknown"))
        counts_by_kind[run_kind] += 1
        counts_by_status[status] += 1
        items.append(
            {
                "label": str(manifest.get("run_label", manifest.get("run_kind", path.stem))),
                "run_kind": run_kind,
                "status": status,
                "generated_at": str(manifest.get("generated_at", "")),
                "mode": str(manifest.get("mode", "")),
                "path": workspace.relative_path(path),
            }
        )
    return {
        "total_count": len(items),
        "counts_by_kind": dict(counts_by_kind),
        "counts_by_status": dict(counts_by_status),
        "items": items,
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
        }

    manifest = read_json_manifest(workspace.graph_manifest_path)
    nodes = list(manifest.get("nodes", []))
    edges = list(manifest.get("edges", []))
    node_counts: Counter[str] = Counter(str(node.get("kind", "unknown")) for node in nodes)
    edge_counts: Counter[str] = Counter(str(edge.get("kind", "unknown")) for edge in edges)
    artifact_index = {
        str(node.get("path", "")): {
            "title": str(node.get("title", "")),
            "collection": str(node.get("collection", "")),
        }
        for node in nodes
        if str(node.get("kind", "")) == "artifact" and str(node.get("path", "")).strip()
    }
    degrees: Counter[str] = Counter()
    conflicts: List[Dict[str, str]] = []
    for edge in edges:
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if source in artifact_index:
            degrees[source] += 1
        if target in artifact_index:
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
                }
            )
    top_connected_artifacts = []
    for path, degree in degrees.most_common(limit):
        info = artifact_index.get(path, {})
        top_connected_artifacts.append(
            {
                "path": path,
                "title": str(info.get("title", path)),
                "collection": str(info.get("collection", "")),
                "degree": degree,
            }
        )
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
    }


def _render_connected_artifacts(items: List[Dict[str, object]]) -> str:
    if not items:
        return "          <p class=\"empty\">No connected artifact data available.</p>"
    lines = [
        "          <table>",
        "            <thead><tr><th>Artifact</th><th>Collection</th><th>Degree</th></tr></thead>",
        "            <tbody>",
    ]
    for item in items:
        lines.extend(
            [
                "              <tr>",
                (
                    "                <td><strong>"
                    + escape(str(item.get("title", "")))
                    + "</strong><br><span class=\"muted\"><code>"
                    + escape(str(item.get("path", "")))
                    + "</code></span></td>"
                ),
                f"                <td>{escape(str(item.get('collection', '')))}</td>",
                f"                <td>{escape(str(item.get('degree', 0)))}</td>",
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
        lines.extend(
            [
                "          <details>",
                f"            <summary>{escape(item['subject'])} {escape(item['verb'])}</summary>",
                f"            <p class=\"muted\"><code>{escape(item['source'])}</code> says {escape(item['left_value'])}</p>",
                f"            <p class=\"muted\"><code>{escape(item['target'])}</code> says {escape(item['right_value'])}</p>",
                "          </details>",
            ]
        )
    return "\n".join(lines)


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
