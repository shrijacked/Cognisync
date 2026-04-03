from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set

from cognisync.linter import lint_snapshot
from cognisync.manifests import build_graph_manifest, build_source_manifest
from cognisync.review_state import read_review_actions
from cognisync.scanner import scan_workspace
from cognisync.types import IndexSnapshot
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class ChangeState:
    artifact_count: int
    source_count: int
    orphan_count: int
    graph_node_count: int
    graph_edge_count: int
    entity_count: int
    assertion_count: int
    tag_count: int
    concept_candidate_count: int
    concept_paths: Set[str]
    resolved_merges: Dict[str, str]
    dismissed_reviews: Dict[str, str]
    conflicts: Dict[str, str]


@dataclass(frozen=True)
class ChangeSummaryResult:
    path: Path
    current_state: ChangeState


def capture_change_state(workspace: Workspace, fallback_to_live_scan: bool = False) -> ChangeState:
    snapshot: Optional[IndexSnapshot] = None
    if workspace.index_path.exists():
        snapshot = workspace.read_index()
    elif fallback_to_live_scan:
        snapshot = scan_workspace(workspace)

    actions = read_review_actions(workspace)
    return _build_change_state(workspace, snapshot, actions)


def write_change_summary(
    workspace: Workspace,
    trigger: str,
    previous_state: ChangeState,
    snapshot: IndexSnapshot,
) -> ChangeSummaryResult:
    current_state = _build_change_state(workspace, snapshot, read_review_actions(workspace))
    timestamp = utc_timestamp()
    summary_path = _next_summary_path(workspace, trigger, timestamp)
    summary_path.write_text(
        _render_change_summary(trigger, timestamp, previous_state, current_state),
        encoding="utf-8",
    )
    return ChangeSummaryResult(path=summary_path, current_state=current_state)


def _build_change_state(
    workspace: Workspace,
    snapshot: Optional[IndexSnapshot],
    actions: Dict[str, object],
) -> ChangeState:
    concept_paths: Set[str] = set()
    artifact_count = 0
    source_count = 0
    orphan_count = 0
    graph_node_count = 0
    graph_edge_count = 0
    entity_count = 0
    assertion_count = 0
    tag_count = 0
    concept_candidate_count = 0
    conflicts: Dict[str, str] = {}

    if snapshot is not None:
        artifact_count = len(snapshot.artifacts)
        source_count = len(build_source_manifest(snapshot)["sources"])
        orphan_count = sum(1 for issue in lint_snapshot(snapshot) if issue.kind == "orphan_page")
        graph_manifest = build_graph_manifest(workspace, snapshot)
        graph_node_count = len(graph_manifest["nodes"])
        graph_edge_count = len(graph_manifest["edges"])
        entity_count = sum(1 for node in graph_manifest["nodes"] if node["kind"] == "entity")
        assertion_count = sum(1 for node in graph_manifest["nodes"] if node["kind"] == "assertion")
        tag_count = sum(1 for node in graph_manifest["nodes"] if node["kind"] == "tag")
        concept_candidate_count = sum(1 for node in graph_manifest["nodes"] if node["kind"] == "concept_candidate")
        concept_paths = {
            artifact.path
            for artifact in snapshot.artifacts
            if artifact.collection == "wiki" and artifact.path.startswith("wiki/concepts/") and artifact.kind == "markdown"
        }
        conflicts = _conflict_descriptions(graph_manifest)

    resolved_merges = {
        str(key): str(dict(value).get("preferred_label", ""))
        for key, value in dict(actions.get("resolved_entity_merges", {})).items()
    }
    dismissed_reviews = {
        str(key): str(dict(value).get("reason", ""))
        for key, value in dict(actions.get("dismissed_reviews", {})).items()
    }
    return ChangeState(
        artifact_count=artifact_count,
        source_count=source_count,
        orphan_count=orphan_count,
        graph_node_count=graph_node_count,
        graph_edge_count=graph_edge_count,
        entity_count=entity_count,
        assertion_count=assertion_count,
        tag_count=tag_count,
        concept_candidate_count=concept_candidate_count,
        concept_paths=concept_paths,
        resolved_merges=resolved_merges,
        dismissed_reviews=dismissed_reviews,
        conflicts=conflicts,
    )


def _conflict_descriptions(graph_manifest: Dict[str, object]) -> Dict[str, str]:
    conflicts: Dict[str, str] = {}
    for edge in graph_manifest["edges"]:
        if edge["kind"] != "conflict":
            continue
        left_path = str(edge["source"])
        right_path = str(edge["target"])
        ordered_paths = sorted([left_path, right_path])
        key = f"{edge['subject']}|{edge['verb']}|{ordered_paths[0]}|{ordered_paths[1]}"
        conflicts[key] = (
            f"{edge['subject']} {edge['verb']}: "
            f"{edge['left_value']} ({left_path}) vs {edge['right_value']} ({right_path})"
        )
    return conflicts


def _render_change_summary(
    trigger: str,
    generated_at: str,
    previous_state: ChangeState,
    current_state: ChangeState,
) -> str:
    new_concepts = sorted(current_state.concept_paths - previous_state.concept_paths)
    resolved_merges = {
        key: value
        for key, value in current_state.resolved_merges.items()
        if previous_state.resolved_merges.get(key) != value
    }
    dismissed_reviews = {
        key: value
        for key, value in current_state.dismissed_reviews.items()
        if previous_state.dismissed_reviews.get(key) != value
    }
    new_conflicts = {
        key: value
        for key, value in current_state.conflicts.items()
        if key not in previous_state.conflicts
    }

    lines = [
        f"# {trigger.title()} Change Summary",
        "",
        f"- Trigger: `{trigger}`",
        f"- Generated: `{generated_at}`",
        "",
        "## Delta Overview",
        "",
        _format_delta_line("Artifact count", previous_state.artifact_count, current_state.artifact_count),
        _format_delta_line("Source count", previous_state.source_count, current_state.source_count),
        _format_delta_line("Orphan pages", previous_state.orphan_count, current_state.orphan_count),
        "",
    ]
    lines.extend(
        [
            "## Graph Delta",
            "",
            _format_delta_line("Graph nodes", previous_state.graph_node_count, current_state.graph_node_count),
            _format_delta_line("Graph edges", previous_state.graph_edge_count, current_state.graph_edge_count),
            _format_delta_line("Entity nodes", previous_state.entity_count, current_state.entity_count),
            _format_delta_line("Assertion nodes", previous_state.assertion_count, current_state.assertion_count),
            _format_delta_line("Tag nodes", previous_state.tag_count, current_state.tag_count),
            _format_delta_line(
                "Concept candidates",
                previous_state.concept_candidate_count,
                current_state.concept_candidate_count,
            ),
            "",
        ]
    )
    lines.extend(_render_list_section("New concept pages", new_concepts))
    lines.extend(_render_mapping_section("Resolved merge decisions", resolved_merges, arrow=True))
    lines.extend(_render_mapping_section("Dismissed review items", dismissed_reviews))
    lines.extend(_render_mapping_section("New conflicts", new_conflicts))
    lines.extend(_render_follow_up_section(new_concepts, resolved_merges, new_conflicts, previous_state, current_state))
    return "\n".join(lines).rstrip() + "\n"


def _render_list_section(title: str, values: list[str]) -> list[str]:
    lines = [f"## {title}", ""]
    if not values:
        lines.append("- None")
    else:
        lines.extend(f"- `{value}`" for value in values)
    lines.append("")
    return lines


def _render_mapping_section(title: str, values: Dict[str, str], arrow: bool = False) -> list[str]:
    lines = [f"## {title}", ""]
    if not values:
        lines.append("- None")
    else:
        for key in sorted(values):
            value = values[key]
            if arrow:
                lines.append(f"- `{key} -> {value}`")
            else:
                lines.append(f"- `{key}`: {value}")
    lines.append("")
    return lines


def _render_follow_up_section(
    new_concepts: list[str],
    resolved_merges: Dict[str, str],
    new_conflicts: Dict[str, str],
    previous_state: ChangeState,
    current_state: ChangeState,
) -> list[str]:
    questions: list[str] = []
    if current_state.orphan_count > previous_state.orphan_count:
        questions.append(
            "Which orphaned pages should gain backlinks so the new graph coverage becomes navigable?"
        )
    if new_concepts:
        questions.append(
            f"Which existing pages should link to the new concept pages: {', '.join(f'`{path}`' for path in new_concepts[:3])}?"
        )
    if resolved_merges:
        questions.append(
            "Do the resolved merge decisions require alias or navigation updates across existing concept pages?"
        )
    for description in list(new_conflicts.values())[:2]:
        subject = description.split(":", 1)[0].strip()
        questions.append(f"How should the corpus acknowledge the disagreement around {subject}?")
    if current_state.assertion_count > previous_state.assertion_count and len(questions) < 5:
        questions.append("Which new assertions deserve promotion into concept pages or operator-facing reports?")
    if not questions:
        questions.append("Should this graph delta trigger a new compile or research pass?")

    lines = ["## Suggested Follow-Up Questions", ""]
    lines.extend(f"- {question}" for question in questions[:5])
    lines.append("")
    return lines


def _format_delta_line(label: str, previous: int, current: int) -> str:
    delta = current - previous
    sign = f"+{delta}" if delta >= 0 else str(delta)
    return f"- {label}: `{previous} -> {current}` (`{sign}`)"


def _filename_timestamp(value: str) -> str:
    return value.replace(":", "").replace("-", "").replace("+", "Z").replace(".", "")


def _next_summary_path(workspace: Workspace, trigger: str, timestamp: str) -> Path:
    directory = workspace.change_summaries_dir
    directory.mkdir(parents=True, exist_ok=True)
    stem = f"{slugify(trigger)}-{_filename_timestamp(timestamp)}"
    candidate = directory / f"{stem}.md"
    index = 2
    while candidate.exists():
        candidate = directory / f"{stem}-{index}.md"
        index += 1
    return candidate
