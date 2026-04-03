from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from cognisync.linter import lint_snapshot
from cognisync.manifests import write_run_manifest, write_workspace_manifests
from cognisync.review_queue import build_review_queue
from cognisync.review_state import (
    canonicalize_review_label,
    preferred_review_label,
    read_review_actions,
    write_review_actions,
)
from cognisync.scanner import scan_workspace
from cognisync.types import ArtifactRecord, IndexSnapshot
from cognisync.utils import relative_markdown_path, slugify, utc_timestamp
from cognisync.workspace import Workspace


class MaintenanceError(RuntimeError):
    pass


@dataclass(frozen=True)
class MaintenanceResult:
    accepted_concept_paths: List[Path]
    resolved_merge_keys: List[str]
    remaining_review_count: int
    issue_count: int
    run_manifest_path: Path


def accept_concept_candidate(workspace: Workspace, slug: str) -> Path:
    snapshot = _refresh_workspace_state(workspace)
    queue = build_review_queue(workspace, snapshot)
    item = next(
        (candidate for candidate in queue["items"] if candidate["kind"] == "concept_candidate" and candidate["slug"] == slug),
        None,
    )
    if item is None:
        raise MaintenanceError(f"No open concept candidate found for slug '{slug}'.")

    target_path = workspace.root / str(item["target_path"])
    if target_path.exists():
        raise MaintenanceError(f"Concept page already exists at {target_path}.")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(_render_concept_page(workspace, snapshot, item), encoding="utf-8")
    _ensure_concepts_navigation_link(workspace, str(item["target_path"]), str(item["title"]).removeprefix("Create concept page for "))

    actions = read_review_actions(workspace)
    actions["accepted_concepts"][slug] = {
        "title": item["title"],
        "target_path": item["target_path"],
        "related_paths": list(item.get("related_paths", [])),
        "accepted_at": utc_timestamp(),
    }
    write_review_actions(workspace, actions)
    _refresh_workspace_state(workspace)
    return target_path


def resolve_entity_merge(workspace: Workspace, canonical_label: str, preferred_label: str | None = None) -> Path:
    snapshot = _refresh_workspace_state(workspace)
    queue = build_review_queue(workspace, snapshot)
    normalized = canonicalize_review_label(canonical_label)
    item = next(
        (
            candidate
            for candidate in queue["items"]
            if candidate["kind"] == "entity_merge_candidate" and candidate["canonical_label"] == normalized
        ),
        None,
    )
    if item is None:
        raise MaintenanceError(f"No open merge candidate found for '{canonical_label}'.")

    labels = list(item.get("labels", []))
    chosen_label = preferred_label.strip() if preferred_label and preferred_label.strip() else preferred_review_label(labels)
    if not chosen_label:
        raise MaintenanceError(f"Could not determine a preferred label for merge candidate '{canonical_label}'.")
    alias_labels = sorted({label for label in labels if label != chosen_label})

    target_path = workspace.wiki_dir / "concepts" / f"{slugify(chosen_label)}.md"
    if target_path.exists():
        _update_concept_frontmatter_aliases(target_path, chosen_label, alias_labels)
    else:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            _render_merge_resolution_page(workspace, snapshot, chosen_label, alias_labels, list(item.get("related_paths", []))),
            encoding="utf-8",
        )

    _ensure_concepts_navigation_link(workspace, workspace.relative_path(target_path), chosen_label)

    actions = read_review_actions(workspace)
    actions["resolved_entity_merges"][normalized] = {
        "preferred_label": chosen_label,
        "aliases": alias_labels,
        "related_paths": list(item.get("related_paths", [])),
        "resolved_at": utc_timestamp(),
    }
    write_review_actions(workspace, actions)
    _refresh_workspace_state(workspace)
    return target_path


def run_maintenance_cycle(
    workspace: Workspace,
    max_concepts: int = 10,
    max_merges: int = 10,
) -> MaintenanceResult:
    snapshot = _refresh_workspace_state(workspace)
    queue = build_review_queue(workspace, snapshot)

    concept_slugs = [
        str(item["slug"]) for item in queue["items"] if item["kind"] == "concept_candidate"
    ][:max_concepts]
    accepted_paths = [accept_concept_candidate(workspace, slug) for slug in concept_slugs]

    snapshot = _refresh_workspace_state(workspace)
    queue = build_review_queue(workspace, snapshot)
    merge_keys = [
        str(item["canonical_label"]) for item in queue["items"] if item["kind"] == "entity_merge_candidate"
    ][:max_merges]
    for merge_key in merge_keys:
        resolve_entity_merge(workspace, merge_key)

    final_snapshot = _refresh_workspace_state(workspace)
    final_queue = build_review_queue(workspace, final_snapshot)
    issues = lint_snapshot(final_snapshot, workspace=workspace)
    run_manifest_path = write_run_manifest(
        workspace,
        "maintenance",
        {
            "run_label": "graph-maintenance",
            "accepted_concept_count": len(accepted_paths),
            "accepted_concept_paths": [workspace.relative_path(path) for path in accepted_paths],
            "resolved_merge_count": len(merge_keys),
            "resolved_merge_keys": merge_keys,
            "remaining_review_count": len(final_queue["items"]),
            "issue_count": len(issues),
            "status": "completed",
        },
    )
    return MaintenanceResult(
        accepted_concept_paths=accepted_paths,
        resolved_merge_keys=merge_keys,
        remaining_review_count=len(final_queue["items"]),
        issue_count=len(issues),
        run_manifest_path=run_manifest_path,
    )


def _refresh_workspace_state(workspace: Workspace) -> IndexSnapshot:
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    return snapshot


def _render_concept_page(workspace: Workspace, snapshot: IndexSnapshot, item: Dict[str, object]) -> str:
    title = str(item["title"]).removeprefix("Create concept page for ")
    target_path = workspace.root / str(item["target_path"])
    related_paths = [str(path) for path in list(item.get("related_paths", []))]
    lines = [
        "---",
        f"title: {title}",
        "tags: [concept, review-accepted]",
        "---",
        f"# {title}",
        "",
        (
            f"{title} was promoted from the Cognisync review queue after it appeared across "
            f"{len(related_paths)} supporting artifacts."
        ),
        "",
        "## Supporting Sources",
        "",
    ]
    lines.extend(_support_source_lines(workspace, snapshot, target_path, related_paths))
    lines.extend(
        [
            "## Review Metadata",
            "",
            f"- Accepted from the review queue on {utc_timestamp()}",
            f"- Target path: `{item['target_path']}`",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _render_merge_resolution_page(
    workspace: Workspace,
    snapshot: IndexSnapshot,
    preferred_label: str,
    aliases: List[str],
    related_paths: List[str],
) -> str:
    target_path = workspace.wiki_dir / "concepts" / f"{slugify(preferred_label)}.md"
    lines = [
        "---",
        f"title: {preferred_label}",
        f"aliases: [{', '.join(aliases)}]" if aliases else "aliases: []",
        "tags: [concept, merge-resolved]",
        "---",
        f"# {preferred_label}",
        "",
        f"{preferred_label} is the canonical label chosen for a resolved entity merge in the review queue.",
        "",
    ]
    if aliases:
        lines.extend(
            [
                "## Aliases",
                "",
                *[f"- {alias}" for alias in aliases],
                "",
            ]
        )
    lines.extend(["## Supporting Sources", ""])
    lines.extend(_support_source_lines(workspace, snapshot, target_path, related_paths))
    return "\n".join(lines).rstrip() + "\n"


def _support_source_lines(
    workspace: Workspace,
    snapshot: IndexSnapshot,
    target_path: Path,
    related_paths: List[str],
) -> List[str]:
    lines: List[str] = []
    for path in related_paths:
        artifact = snapshot.artifact_by_path(path)
        source_path = workspace.root / path
        snippet = _excerpt_text(source_path)
        lines.append(
            f"- [{artifact.title}]({relative_markdown_path(target_path, source_path)}): `{path}`"
        )
        if snippet:
            lines.append(f"  - Evidence: {snippet}")
    lines.append("")
    return lines


def _excerpt_text(path: Path, limit: int = 180) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="ignore")
    compact = " ".join(text.split())
    if compact.startswith("--- "):
        parts = compact.split("--- ", 2)
        compact = parts[-1]
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _ensure_concepts_navigation_link(workspace: Workspace, target_path: str, title: str) -> None:
    concepts_index = workspace.wiki_dir / "concepts.md"
    if not concepts_index.exists():
        return
    link_line = f"- [[concepts/{Path(target_path).stem}|{title}]]"
    existing = concepts_index.read_text(encoding="utf-8")
    if link_line in existing:
        return
    updated = existing.rstrip() + "\n" + link_line + "\n"
    concepts_index.write_text(updated, encoding="utf-8")


def _update_concept_frontmatter_aliases(path: Path, preferred_label: str, aliases: List[str]) -> None:
    text = path.read_text(encoding="utf-8")
    frontmatter, body = _split_frontmatter(text)
    frontmatter["title"] = preferred_label
    existing_aliases = set(_coerce_frontmatter_list(frontmatter.get("aliases")))
    existing_aliases.update(aliases)
    frontmatter["aliases"] = sorted(existing_aliases)
    existing_tags = set(_coerce_frontmatter_list(frontmatter.get("tags")))
    existing_tags.add("concept")
    existing_tags.add("merge-resolved")
    frontmatter["tags"] = sorted(existing_tags)
    rebuilt = _render_frontmatter(frontmatter) + body.lstrip("\n")
    if "## Aliases" not in rebuilt and aliases:
        rebuilt = rebuilt.rstrip() + "\n\n## Aliases\n\n" + "\n".join(f"- {alias}" for alias in aliases) + "\n"
    path.write_text(rebuilt, encoding="utf-8")


def _split_frontmatter(text: str) -> Tuple[Dict[str, object], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    block = text[4:end]
    body = text[end + 5 :]
    data: Dict[str, object] = {}
    for line in block.splitlines():
        stripped = line.strip()
        if not stripped or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        parsed = value.strip()
        if parsed.startswith("[") and parsed.endswith("]"):
            data[key.strip()] = [item.strip() for item in parsed[1:-1].split(",") if item.strip()]
        else:
            data[key.strip()] = parsed
    return data, body


def _render_frontmatter(frontmatter: Dict[str, object]) -> str:
    lines = ["---"]
    for key in sorted(frontmatter):
        value = frontmatter[key]
        if isinstance(value, list):
            lines.append(f"{key}: [{', '.join(str(item) for item in value)}]")
        else:
            lines.append(f"{key}: {value}")
    lines.extend(["---", ""])
    return "\n".join(lines)


def _coerce_frontmatter_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return []
