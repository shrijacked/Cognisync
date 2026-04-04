from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Sequence

from cognisync.review_state import read_review_actions
from cognisync.types import ArtifactRecord, IndexSnapshot
from cognisync.utils import utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


_FRONTMATTER_RE = re.compile(r"^---\n.*?\n---\n", re.DOTALL)
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

_NAVIGATION_PAGES = {
    "wiki/index.md",
    "wiki/sources.md",
    "wiki/concepts.md",
    "wiki/queries.md",
}


def is_navigation_surface_path(path: str) -> bool:
    return path in _NAVIGATION_PAGES


def write_workspace_schema(workspace: "Workspace", force: bool = False) -> Path:
    if workspace.schema_path.exists() and not force:
        existing = workspace.schema_path.read_text(encoding="utf-8")
        rendered = _render_workspace_schema(workspace)
        if existing == rendered:
            return workspace.schema_path
    workspace.schema_path.write_text(_render_workspace_schema(workspace), encoding="utf-8")
    return workspace.schema_path


def ensure_workspace_log(workspace: "Workspace", force: bool = False) -> Path:
    if workspace.log_path.exists() and not force:
        return workspace.log_path
    workspace.log_path.write_text(
        "# Cognisync Activity Log\n\n"
        "This append-only log records important workspace operations in a simple chronological format.\n\n"
        "Use it to see what changed recently without opening every manifest in `.cognisync/`.\n",
        encoding="utf-8",
    )
    return workspace.log_path


def append_workspace_log(
    workspace: "Workspace",
    operation: str,
    title: str,
    details: Sequence[str] | None = None,
    related_paths: Sequence[str] | None = None,
) -> Path:
    ensure_workspace_log(workspace)
    timestamp = utc_timestamp()
    day = timestamp[:10]
    lines = [f"## [{day}] {operation} | {title}", "", f"- timestamp: `{timestamp}`"]
    for detail in list(details or []):
        clean = str(detail).strip()
        if clean:
            lines.append(f"- detail: {clean}")
    for path in list(related_paths or []):
        clean = str(path).strip()
        if clean:
            lines.append(f"- path: `{clean}`")
    lines.append("")
    with workspace.log_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")
    return workspace.log_path


def write_wiki_navigation_surfaces(workspace: "Workspace", snapshot: IndexSnapshot) -> None:
    actions = read_review_actions(workspace)
    applied_backlinks = {str(path) for path in dict(actions.get("applied_backlinks", {})).keys()}
    sections = {
        "sources": _collect_section_pages(workspace, snapshot, "wiki/sources/", applied_backlinks),
        "concepts": _collect_section_pages(workspace, snapshot, "wiki/concepts/", applied_backlinks),
        "queries": _collect_section_pages(workspace, snapshot, "wiki/queries/", applied_backlinks),
    }
    rendered_at = utc_timestamp()
    workspace.wiki_dir.mkdir(parents=True, exist_ok=True)
    (workspace.wiki_dir / "index.md").write_text(
        _render_index_page(workspace, sections, rendered_at),
        encoding="utf-8",
    )
    (workspace.wiki_dir / "sources.md").write_text(
        _render_section_page(
            title="Sources",
            description="Compiled source summaries maintained by Cognisync. Start here when you want the synthesized view of raw material.",
            items=sections["sources"],
            rendered_at=rendered_at,
        ),
        encoding="utf-8",
    )
    (workspace.wiki_dir / "concepts.md").write_text(
        _render_section_page(
            title="Concepts",
            description="Cross-source syntheses, entities, and durable concept pages that the agent keeps refining over time.",
            items=sections["concepts"],
            rendered_at=rendered_at,
        ),
        encoding="utf-8",
    )
    (workspace.wiki_dir / "queries.md").write_text(
        _render_section_page(
            title="Queries",
            description="Filed analyses, reusable answers, and question-driven research artifacts that compound back into the corpus.",
            items=sections["queries"],
            rendered_at=rendered_at,
        ),
        encoding="utf-8",
    )


def _render_workspace_schema(workspace: "Workspace") -> str:
    config = workspace.load_config()
    return "\n".join(
        [
            "# Cognisync Workspace Schema",
            "",
            "This file tells an LLM agent how to maintain this workspace as a persistent, compounding wiki rather than a one-off chat session.",
            "",
            "## Layers",
            "",
            "- `raw/` is immutable source material. Read from it, ingest into the wiki, but do not rewrite or summarize inside `raw/` itself.",
            "- `wiki/` is the maintained knowledge layer. The agent owns these Markdown pages and keeps them internally consistent.",
            "- `outputs/` is for reports, slides, exports, review dashboards, and other generated artifacts that may or may not be filed back into the wiki.",
            "- `.cognisync/` stores machine-readable manifests, runs, queues, review state, and control-plane metadata.",
            "- `prompts/` stores reproducible prompt packets and research packets when a run needs them.",
            "",
            "## Canonical Navigation Files",
            "",
            "- `wiki/index.md` is the primary catalog the agent should read first before broad query work.",
            "- `wiki/sources.md`, `wiki/concepts.md`, and `wiki/queries.md` are maintained catalogs of the durable wiki pages in each section.",
            "- `log.md` is the append-only chronology of important operations such as ingest, research, compile, lint, and maintenance.",
            "",
            "## Default Workflow",
            "",
            "1. Ingest or sync new source material into `raw/`.",
            "2. Refresh or create the relevant source summaries in `wiki/sources/`.",
            "3. Update concept pages, query pages, and cross-links in `wiki/` so the synthesis compounds over time.",
            "4. Keep contradictions, stale claims, missing cross-references, and orphan pages visible instead of silently ignoring them.",
            "5. File valuable answers back into `wiki/queries/` or `outputs/reports/` instead of letting them disappear into chat history.",
            "",
            "## Agent Rules",
            "",
            "- Prefer updating existing wiki pages over creating near-duplicates.",
            "- Treat `raw/` as the source of truth and `wiki/` as the maintained interpretation layer.",
            "- Use citations and explicit source references when answering questions or revising synthesized pages.",
            "- When a query produces a durable artifact, file it back into the workspace and update the relevant indexes.",
            "- Use `log.md` to understand recent work before resuming a session after a break.",
            "",
            "## Workspace Defaults",
            "",
            f"- Workspace name: `{config.workspace_name}`",
            f"- Summary directory: `{config.summary_directory}`",
            f"- Concept directory: `{config.concept_directory}`",
            f"- Query directory: `{config.query_directory}`",
            "",
            "This schema is intentionally lightweight. Extend it as the workspace develops sharper conventions for a specific domain.",
        ]
    )


def _render_index_page(workspace: "Workspace", sections: dict[str, List[dict[str, object]]], rendered_at: str) -> str:
    lines = [
        "# Knowledge Base Index",
        "",
        "This catalog is regenerated by Cognisync and is intended to be the first stop for agents working against the workspace.",
        "",
        f"- updated: `{rendered_at}`",
        "- workspace schema: `../AGENTS.md`",
        "- activity log: `../log.md`",
        "",
        "## Sections",
        "",
        f"- [Sources](sources.md) — compiled source summaries. `{len(sections['sources'])}` page(s).",
        f"- [Concepts](concepts.md) — cross-source synthesis and entity pages. `{len(sections['concepts'])}` page(s).",
        f"- [Queries](queries.md) — reusable analyses and filed answers. `{len(sections['queries'])}` page(s).",
        "",
        "## Recent Pages",
        "",
    ]
    recent_items = sorted(
        [item for values in sections.values() for item in values],
        key=lambda item: str(item.get("path", "")),
    )[:12]
    if not recent_items:
        lines.append("No wiki pages have been cataloged yet.")
        return "\n".join(lines) + "\n"
    for item in recent_items:
        lines.append(_render_page_bullet(item))
    return "\n".join(lines) + "\n"


def _render_section_page(
    title: str,
    description: str,
    items: Sequence[dict[str, object]],
    rendered_at: str,
) -> str:
    lines = [
        f"# {title}",
        "",
        description,
        "",
        f"- updated: `{rendered_at}`",
        f"- page count: `{len(items)}`",
        "",
        "## Entries",
        "",
    ]
    if not items:
        lines.append("No pages cataloged yet.")
        return "\n".join(lines) + "\n"
    for item in items:
        lines.append(_render_page_bullet(item))
    return "\n".join(lines) + "\n"


def _render_page_bullet(item: dict[str, object]) -> str:
    label = str(item.get("title", "")).strip() or str(item.get("path", "")).strip()
    relative_link = str(item.get("relative_link", "")).strip()
    link_style = str(item.get("link_style", "markdown")).strip()
    summary = str(item.get("summary", "")).strip() or "No summary extracted yet."
    tags = [str(tag).strip() for tag in list(item.get("tags", [])) if str(tag).strip()]
    tag_suffix = ""
    if tags:
        tag_suffix = " Tags: " + ", ".join(f"`{tag}`" for tag in tags[:4])
    if link_style == "wikilink":
        wikilink_target = str(item.get("wikilink_target", "")).strip()
        link_text = f"[[{wikilink_target}|{label}]]" if wikilink_target else label
    else:
        link_text = f"[{label}]({relative_link})"
    return f"- {link_text} — {summary}{tag_suffix}"


def _collect_section_pages(
    workspace: "Workspace",
    snapshot: IndexSnapshot,
    prefix: str,
    applied_backlinks: set[str],
) -> List[dict[str, object]]:
    items: List[dict[str, object]] = []
    for artifact in snapshot.artifacts:
        if is_navigation_surface_path(artifact.path):
            continue
        if artifact.collection != "wiki" or artifact.kind != "markdown":
            continue
        if not artifact.path.startswith(prefix):
            continue
        relative_link = artifact.path.removeprefix("wiki/")
        link_style = "wikilink" if artifact.path in applied_backlinks else "markdown"
        items.append(
            {
                "path": artifact.path,
                "title": artifact.title,
                "relative_link": relative_link,
                "wikilink_target": relative_link.removesuffix(".md"),
                "link_style": link_style,
                "summary": _extract_summary_line(workspace.root / artifact.path),
                "tags": list(artifact.tags),
                "word_count": artifact.word_count,
            }
        )
    items.sort(key=lambda item: str(item.get("title", "")).lower())
    return items


def _extract_summary_line(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix not in {".md", ".markdown", ".txt", ".rst"}:
        return "Non-markdown artifact."
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return "Could not read summary."
    text = _FRONTMATTER_RE.sub("", text, count=1)
    text = text.replace("\r\n", "\n")
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(("#", "- ", "* ", "> ", "```")):
            continue
        if stripped.startswith("!"):
            continue
        clean = _MARKDOWN_LINK_RE.sub(r"\1", stripped)
        clean = re.sub(r"`([^`]+)`", r"\1", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        if clean:
            return clean[:180] + ("..." if len(clean) > 180 else "")
    return "No summary extracted yet."
