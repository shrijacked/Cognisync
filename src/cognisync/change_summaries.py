from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from cognisync.linter import lint_snapshot
from cognisync.manifests import build_graph_manifest, build_source_manifest
from cognisync.review_state import read_review_actions
from cognisync.scanner import scan_workspace
from cognisync.types import IndexSnapshot
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


MAX_CHANGE_SECTION_ITEMS = 12
MAX_RECOMPILATION_SUGGESTIONS = 8
GRAPH_NODE_KINDS_FOR_SUMMARY = {"assertion", "concept_candidate", "entity"}


@dataclass(frozen=True)
class ArtifactFingerprint:
    path: str
    collection: str
    kind: str
    title: str
    content_hash: str
    summary_target: Optional[str]
    source_key: Optional[str]


@dataclass(frozen=True)
class SourceGroupFingerprint:
    source_key: str
    source_kind: str
    primary_artifact: Optional[str]
    artifacts: Tuple[str, ...]
    captured_assets: Tuple[str, ...]


@dataclass(frozen=True)
class GraphNodeFingerprint:
    node_id: str
    kind: str
    title: str
    support_paths: Tuple[str, ...]
    incident_artifact_paths: Tuple[str, ...]
    signature: str


@dataclass(frozen=True)
class CompileTaskFingerprint:
    task_id: str
    kind: str
    title: str
    inputs: Tuple[str, ...]
    output_path: str
    rationale: str
    prompt_hint: str


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
    artifact_fingerprints: Dict[str, ArtifactFingerprint]
    source_groups: Dict[str, SourceGroupFingerprint]
    graph_nodes: Dict[str, GraphNodeFingerprint]
    compile_tasks: List[CompileTaskFingerprint]


@dataclass(frozen=True)
class ChangeSummaryResult:
    path: Path
    current_state: ChangeState


def capture_change_state(workspace: Workspace, fallback_to_live_scan: bool = False) -> ChangeState:
    snapshot: Optional[IndexSnapshot] = None
    if fallback_to_live_scan:
        snapshot = scan_workspace(workspace)
    elif workspace.index_path.exists():
        snapshot = workspace.read_index()

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
    artifact_fingerprints: Dict[str, ArtifactFingerprint] = {}
    source_groups: Dict[str, SourceGroupFingerprint] = {}
    graph_nodes: Dict[str, GraphNodeFingerprint] = {}
    compile_tasks: List[CompileTaskFingerprint] = []

    if snapshot is not None:
        from cognisync.planner import build_compile_plan

        artifact_count = len(snapshot.artifacts)
        source_manifest = build_source_manifest(snapshot)
        source_count = len(source_manifest["sources"])
        source_groups, source_key_by_artifact = _source_group_fingerprints(source_manifest)
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
        artifact_fingerprints = _artifact_fingerprints(snapshot, source_key_by_artifact)
        graph_nodes = _graph_node_fingerprints(graph_manifest, set(snapshot.artifact_paths()))
        compile_tasks = [
            CompileTaskFingerprint(
                task_id=task.task_id,
                kind=task.kind,
                title=task.title,
                inputs=tuple(task.inputs),
                output_path=task.output_path,
                rationale=task.rationale,
                prompt_hint=task.prompt_hint,
            )
            for task in build_compile_plan(snapshot).tasks
        ]

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
        artifact_fingerprints=artifact_fingerprints,
        source_groups=source_groups,
        graph_nodes=graph_nodes,
        compile_tasks=compile_tasks,
    )


def _source_group_fingerprints(
    source_manifest: Dict[str, object],
) -> Tuple[Dict[str, SourceGroupFingerprint], Dict[str, str]]:
    groups: Dict[str, SourceGroupFingerprint] = {}
    source_key_by_artifact: Dict[str, str] = {}
    for raw_source in list(source_manifest.get("sources", [])):
        source = dict(raw_source)
        source_key = str(source.get("source_key", ""))
        if not source_key:
            continue
        artifacts = tuple(str(path) for path in list(source.get("artifacts", [])))
        captured_assets = tuple(str(path) for path in list(source.get("captured_assets", [])))
        groups[source_key] = SourceGroupFingerprint(
            source_key=source_key,
            source_kind=str(source.get("source_kind", "artifact")),
            primary_artifact=str(source.get("primary_artifact") or "") or None,
            artifacts=artifacts,
            captured_assets=captured_assets,
        )
        for artifact_path in artifacts:
            source_key_by_artifact[artifact_path] = source_key
    return groups, source_key_by_artifact


def _artifact_fingerprints(
    snapshot: IndexSnapshot,
    source_key_by_artifact: Dict[str, str],
) -> Dict[str, ArtifactFingerprint]:
    return {
        artifact.path: ArtifactFingerprint(
            path=artifact.path,
            collection=artifact.collection,
            kind=artifact.kind,
            title=artifact.title,
            content_hash=artifact.content_hash,
            summary_target=artifact.summary_target,
            source_key=source_key_by_artifact.get(artifact.path),
        )
        for artifact in snapshot.artifacts
    }


def _graph_node_fingerprints(
    graph_manifest: Dict[str, object],
    artifact_paths: Set[str],
) -> Dict[str, GraphNodeFingerprint]:
    incident_artifact_paths: Dict[str, Set[str]] = {}
    for raw_edge in list(graph_manifest.get("edges", [])):
        edge = dict(raw_edge)
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if source in artifact_paths and target:
            incident_artifact_paths.setdefault(target, set()).add(source)
        if target in artifact_paths and source:
            incident_artifact_paths.setdefault(source, set()).add(target)

    nodes: Dict[str, GraphNodeFingerprint] = {}
    for raw_node in list(graph_manifest.get("nodes", [])):
        node = dict(raw_node)
        node_id = str(node.get("id", ""))
        if not node_id:
            continue
        support_paths = tuple(sorted(str(path) for path in list(node.get("support_paths", []))))
        incident_paths = tuple(sorted(incident_artifact_paths.get(node_id, set())))
        signature_payload = {
            "kind": str(node.get("kind", "")),
            "title": str(node.get("title", "")),
            "support_paths": support_paths,
            "incident_artifact_paths": incident_paths,
            "subject": str(node.get("subject", "")),
            "verb": str(node.get("verb", "")),
            "object": str(node.get("object", "")),
            "output_path": str(node.get("output_path", "")),
            "resolved": bool(node.get("resolved", False)),
        }
        nodes[node_id] = GraphNodeFingerprint(
            node_id=node_id,
            kind=str(node.get("kind", "")),
            title=str(node.get("title", "")),
            support_paths=support_paths,
            incident_artifact_paths=incident_paths,
            signature=json.dumps(signature_payload, sort_keys=True),
        )
    return nodes


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
    changed_artifacts = _changed_artifacts(previous_state, current_state)

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
    lines.extend(_render_changed_artifacts_section(previous_state, current_state, changed_artifacts))
    lines.extend(_render_affected_graph_nodes_section(previous_state, current_state, changed_artifacts, new_conflicts))
    lines.extend(_render_recompilation_suggestions_section(current_state, changed_artifacts))
    lines.extend(_render_list_section("New concept pages", new_concepts))
    lines.extend(_render_mapping_section("Resolved merge decisions", resolved_merges, arrow=True))
    lines.extend(_render_mapping_section("Dismissed review items", dismissed_reviews))
    lines.extend(_render_mapping_section("New conflicts", new_conflicts))
    lines.extend(_render_follow_up_section(new_concepts, resolved_merges, new_conflicts, previous_state, current_state))
    return "\n".join(lines).rstrip() + "\n"


def _changed_artifacts(previous_state: ChangeState, current_state: ChangeState) -> Dict[str, str]:
    previous_paths = set(previous_state.artifact_fingerprints)
    current_paths = set(current_state.artifact_fingerprints)
    changes: Dict[str, str] = {}
    for path in sorted(current_paths - previous_paths):
        changes[path] = "added"
    for path in sorted(previous_paths - current_paths):
        changes[path] = "removed"
    for path in sorted(previous_paths & current_paths):
        previous = previous_state.artifact_fingerprints[path]
        current = current_state.artifact_fingerprints[path]
        if previous.content_hash != current.content_hash:
            changes[path] = "modified"
    return changes


def _render_changed_artifacts_section(
    previous_state: ChangeState,
    current_state: ChangeState,
    changed_artifacts: Dict[str, str],
) -> list[str]:
    lines = ["## Changed Artifacts", ""]
    if not changed_artifacts:
        lines.extend(["- None", ""])
        return lines

    sorted_changes = sorted(changed_artifacts.items(), key=lambda item: (item[1], item[0]))
    for path, status in sorted_changes[:MAX_CHANGE_SECTION_ITEMS]:
        fingerprint = current_state.artifact_fingerprints.get(path) or previous_state.artifact_fingerprints[path]
        phrase = _artifact_change_phrase(fingerprint, status)
        details = [f"`{path}` {phrase}"]
        if fingerprint.source_key:
            details.append(f"source group `{fingerprint.source_key}`")
        if fingerprint.summary_target:
            details.append(f"summary target `{fingerprint.summary_target}`")
        lines.append("- " + "; ".join(details))
    omitted = len(sorted_changes) - MAX_CHANGE_SECTION_ITEMS
    if omitted > 0:
        lines.append(f"- ... {omitted} more changed artifact(s) omitted")
    lines.append("")
    return lines


def _artifact_change_phrase(fingerprint: ArtifactFingerprint, status: str) -> str:
    if fingerprint.collection == "raw":
        if status == "added":
            return "added source content"
        if status == "removed":
            return "removed source content"
        return "modified source content"
    if status == "added":
        return "added artifact"
    if status == "removed":
        return "removed artifact"
    return "modified artifact"


def _render_affected_graph_nodes_section(
    previous_state: ChangeState,
    current_state: ChangeState,
    changed_artifacts: Dict[str, str],
    new_conflicts: Dict[str, str],
) -> list[str]:
    changed_paths = set(changed_artifacts)
    affected_nodes: List[Tuple[str, GraphNodeFingerprint, Tuple[str, ...]]] = []
    for node_id, node in current_state.graph_nodes.items():
        if node.kind not in GRAPH_NODE_KINDS_FOR_SUMMARY:
            continue
        support_paths = tuple(sorted(set(node.support_paths) | set(node.incident_artifact_paths)))
        affected_paths = tuple(path for path in support_paths if path in changed_paths)
        previous_node = previous_state.graph_nodes.get(node_id)
        signature_changed = previous_node is None or previous_node.signature != node.signature
        if not affected_paths and not signature_changed:
            continue
        if not affected_paths and previous_node is not None:
            previous_paths = set(previous_node.support_paths) | set(previous_node.incident_artifact_paths)
            affected_paths = tuple(sorted(previous_paths & changed_paths))
        if not affected_paths and not changed_paths:
            continue
        affected_nodes.append((node_id, node, affected_paths))

    lines = ["## Affected Graph Nodes", ""]
    conflict_lines = _affected_conflict_lines(new_conflicts)
    if not affected_nodes and not conflict_lines:
        lines.extend(["- None", ""])
        return lines

    affected_nodes.sort(key=lambda item: (item[1].kind, item[1].title.lower(), item[0]))
    for node_id, node, affected_paths in affected_nodes[:MAX_CHANGE_SECTION_ITEMS]:
        support_detail = _format_path_list(affected_paths) if affected_paths else "graph signature"
        lines.append(
            f"- `{node_id}` {_graph_kind_label(node.kind)} support changed via {support_detail}: {node.title}"
        )
    remaining_budget = max(MAX_CHANGE_SECTION_ITEMS - min(len(affected_nodes), MAX_CHANGE_SECTION_ITEMS), 0)
    lines.extend(conflict_lines[:remaining_budget])
    omitted = len(affected_nodes) + len(conflict_lines) - MAX_CHANGE_SECTION_ITEMS
    if omitted > 0:
        lines.append(f"- ... {omitted} more affected graph item(s) omitted")
    lines.append("")
    return lines


def _affected_conflict_lines(new_conflicts: Dict[str, str]) -> list[str]:
    lines: list[str] = []
    for key in sorted(new_conflicts):
        subject = key.split("|", 1)[0]
        lines.append(f"- `conflict:{slugify(subject)}` conflict changed: {new_conflicts[key]}")
    return lines


def _graph_kind_label(kind: str) -> str:
    if kind == "concept_candidate":
        return "concept candidate"
    return kind


def _render_recompilation_suggestions_section(
    current_state: ChangeState,
    changed_artifacts: Dict[str, str],
) -> list[str]:
    changed_paths = set(changed_artifacts)
    changed_summary_targets = {
        fingerprint.summary_target
        for path, fingerprint in current_state.artifact_fingerprints.items()
        if path in changed_paths and fingerprint.summary_target
    }
    suggestions = [
        task
        for task in current_state.compile_tasks
        if _task_matches_changed_artifacts(task, changed_paths, changed_summary_targets)
    ]
    suggestions.sort(key=lambda task: (task.kind, task.output_path, task.task_id))

    lines = ["## Recompilation Suggestions", ""]
    if not suggestions:
        lines.extend(["- None", ""])
        return lines

    for task in suggestions[:MAX_RECOMPILATION_SUGGESTIONS]:
        source_inputs = tuple(input_path for input_path in task.inputs if input_path in changed_paths)
        if not source_inputs:
            source_inputs = task.inputs[:2]
        lines.append(
            f"- `{task.kind}` `{task.output_path}` from {_format_path_list(source_inputs)}: {task.rationale}"
        )
    omitted = len(suggestions) - MAX_RECOMPILATION_SUGGESTIONS
    if omitted > 0:
        lines.append(f"- ... {omitted} more recompilation suggestion(s) omitted")
    lines.append("")
    return lines


def _task_matches_changed_artifacts(
    task: CompileTaskFingerprint,
    changed_paths: Set[str],
    changed_summary_targets: Set[Optional[str]],
) -> bool:
    task_inputs = set(task.inputs)
    if task_inputs & changed_paths:
        return True
    if task.output_path in changed_paths:
        return True
    if task.output_path in changed_summary_targets:
        return True
    return False


def _format_path_list(paths: Tuple[str, ...]) -> str:
    if not paths:
        return "`unknown`"
    return ", ".join(f"`{path}`" for path in paths)


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
