from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Set

from cognisync.graph_intelligence import (
    _extract_entity_labels,
    _read_artifact_text,
    build_concept_candidates,
    build_graph_semantics,
)
from cognisync.knowledge_surfaces import is_navigation_surface_path
from cognisync.review_state import canonicalize_review_label, normalize_review_label_variant, read_review_actions
from cognisync.types import ArtifactRecord, IndexSnapshot
from cognisync.workspace import Workspace


TEXTUAL_REVIEW_KINDS = {"markdown", "text", "data", "code"}


def build_review_queue(workspace: Workspace, snapshot: IndexSnapshot) -> Dict[str, object]:
    actions = read_review_actions(workspace)
    items = []
    items.extend(_build_concept_candidate_items(snapshot, actions))
    items.extend(_build_entity_merge_items(workspace, snapshot, actions))
    items.extend(_build_conflict_review_items(workspace, snapshot, actions))
    items.extend(_build_backlink_suggestion_items(workspace, snapshot))
    dismissed = {str(key) for key in dict(actions.get("dismissed_reviews", {})).keys()}
    items = [item for item in items if str(item.get("review_id", "")) not in dismissed]
    items.sort(key=lambda item: (_priority_rank(item["priority"]), item["kind"], item["title"]))
    return {
        "schema_version": 1,
        "generated_at": snapshot.generated_at,
        "items": items,
    }


def render_review_queue(queue: Dict[str, object], limit: int | None = None) -> str:
    items = list(queue.get("items", []))
    if limit is not None:
        items = items[:limit]
    if not items:
        return "No review items found."

    lines = [f"Review queue contains {len(queue.get('items', []))} item(s).", ""]
    for item in items:
        location = str(item.get("path") or item.get("target_path") or "-")
        lines.append(f"[{item['priority']}] {item['kind']} {location}: {item['title']}")
        related_paths = ", ".join(item.get("related_paths", []))
        if related_paths:
            lines.append(f"related: {related_paths}")
        detail = str(item.get("detail", "")).strip()
        if detail:
            lines.append(f"detail: {detail}")
        suggestion = str(item.get("suggestion", "")).strip()
        if suggestion:
            lines.append(f"suggestion: {suggestion}")
        lines.append("")
    return "\n".join(lines).rstrip()

def _build_concept_candidate_items(snapshot: IndexSnapshot, actions: Dict[str, object]) -> List[Dict[str, object]]:
    items: List[Dict[str, object]] = []
    accepted = dict(actions.get("accepted_concepts", {}))
    for candidate in build_concept_candidates(snapshot):
        if candidate["resolved"] or str(candidate["slug"]) in accepted:
            continue
        support_paths = list(candidate["support_paths"])
        items.append(
            {
                "review_id": f"concept-candidate:{candidate['slug']}",
                "kind": "concept_candidate",
                "priority": "high" if int(candidate["support_count"]) >= 3 else "medium",
                "status": "open",
                "title": f"Create concept page for {candidate['title']}",
                "slug": candidate["slug"],
                "support_count": int(candidate["support_count"]),
                "target_path": candidate["output_path"],
                "path": candidate["output_path"],
                "evidence_kinds": list(candidate.get("evidence_kinds", [])),
                "related_paths": support_paths,
                "detail": (
                    f"{candidate['title']} is supported by {candidate['support_count']} artifacts "
                    "but does not have a compiled concept page yet."
                ),
                "suggestion": f"Create {candidate['output_path']} and link the supporting artifacts back to it.",
            }
        )
    return items


def _build_entity_merge_items(workspace: Workspace, snapshot: IndexSnapshot, actions: Dict[str, object]) -> List[Dict[str, object]]:
    buckets: Dict[str, Dict[str, object]] = {}
    resolved = dict(actions.get("resolved_entity_merges", {}))
    for artifact in snapshot.artifacts:
        if is_navigation_surface_path(artifact.path):
            continue
        if artifact.kind not in TEXTUAL_REVIEW_KINDS or artifact.collection not in {"raw", "wiki", "outputs"}:
            continue
        text = _read_artifact_text(workspace, artifact)
        if not text:
            continue
        for label in _extract_entity_labels(artifact, text):
            normalized_label = normalize_review_label_variant(label)
            canonical = canonicalize_review_label(normalized_label)
            if not canonical:
                continue
            entry = buckets.setdefault(canonical, {"labels": set(), "paths": set()})
            entry["labels"].add(normalized_label)
            entry["paths"].add(artifact.path)

    items: List[Dict[str, object]] = []
    for canonical, bucket in sorted(buckets.items()):
        labels = sorted(bucket["labels"])
        paths = sorted(bucket["paths"])
        if len(labels) < 2 or len(paths) < 2 or canonical in resolved:
            continue
        items.append(
            {
                "review_id": f"entity-merge:{canonical.replace(' ', '-')}",
                "kind": "entity_merge_candidate",
                "priority": "medium",
                "status": "open",
                "title": f"Merge entity variants for {labels[0]}",
                "canonical_label": canonical,
                "labels": labels,
                "path": paths[0],
                "related_paths": paths,
                "detail": f"Observed nearby entity variants: {', '.join(labels)}.",
                "suggestion": "Consolidate these labels under one concept page or add aliases to keep the graph clean.",
            }
        )
    return items


def _build_conflict_review_items(
    workspace: Workspace,
    snapshot: IndexSnapshot,
    actions: Dict[str, object],
) -> List[Dict[str, object]]:
    semantics = build_graph_semantics(workspace, snapshot)
    filed_conflicts = dict(actions.get("filed_conflicts", {}))
    items: List[Dict[str, object]] = []
    for edge in semantics["edges"]:
        if edge["kind"] != "conflict":
            continue
        conflict_paths = sorted([str(edge["source"]), str(edge["target"])])
        conflict_key = f"{edge['subject']}|{edge['verb']}|{conflict_paths[0]}|{conflict_paths[1]}"
        if conflict_key in filed_conflicts:
            continue
        items.append(
            {
                "review_id": (
                    f"conflict:{edge['source']}:{edge['target']}:{edge['subject']}:{edge['verb']}".replace("/", "-")
                ),
                "kind": "conflict_review",
                "conflict_key": conflict_key,
                "priority": "high",
                "status": "open",
                "title": f"Resolve conflicting claim about {edge['subject']}",
                "path": edge["source"],
                "related_paths": [edge["source"], edge["target"]],
                "subject": edge["subject"],
                "verb": edge["verb"],
                "detail": (
                    f"{edge['source']} says '{edge['subject']} {edge['verb']} {edge['left_value']}' while "
                    f"{edge['target']} says '{edge['subject']} {edge['verb']} {edge['right_value']}'."
                ),
                "suggestion": "Reconcile the claim in a concept page or cite the disagreement explicitly in downstream reports.",
            }
        )
    return items


def _build_backlink_suggestion_items(workspace: Workspace, snapshot: IndexSnapshot) -> List[Dict[str, object]]:
    link_pairs = {
        (artifact.path, link.resolved_path)
        for artifact in snapshot.artifacts
        for link in artifact.links
        if not link.external and link.resolved_path
    }
    signals = {artifact.path: _artifact_signals(workspace, artifact) for artifact in snapshot.artifacts}

    items: List[Dict[str, object]] = []
    for artifact in snapshot.artifacts:
        if artifact.collection != "wiki" or artifact.kind != "markdown":
            continue
        if is_navigation_surface_path(artifact.path):
            continue
        if snapshot.backlinks.get(artifact.path):
            continue

        best_match = None
        best_score = 0
        for other in snapshot.artifacts:
            if other.path == artifact.path:
                continue
            if is_navigation_surface_path(other.path):
                continue
            if (other.path, artifact.path) in link_pairs or (artifact.path, other.path) in link_pairs:
                continue
            score = len(signals[artifact.path] & signals[other.path])
            if score > best_score:
                best_match = other
                best_score = score

        if best_match is None or best_score == 0:
            continue

        items.append(
            {
                "review_id": f"backlink:{artifact.path}:{best_match.path}".replace("/", "-"),
                "kind": "backlink_suggestion",
                "priority": "low",
                "status": "open",
                "title": f"Add a backlink into {artifact.title}",
                "path": artifact.path,
                "related_paths": [best_match.path],
                "detail": (
                    f"{artifact.path} has no backlinks, but it shares graph signals with {best_match.path}."
                ),
                "suggestion": f"Link {best_match.path} to {artifact.path} or file the relationship into an index page.",
            }
        )
    return items


def _artifact_signals(workspace: Workspace, artifact: ArtifactRecord) -> Set[str]:
    if is_navigation_surface_path(artifact.path):
        return set()
    signals = {tag.lower() for tag in artifact.tags}
    for label in [artifact.title, *artifact.headings]:
        canonical = canonicalize_review_label(label)
        if canonical:
            signals.add(canonical)
    if artifact.kind in TEXTUAL_REVIEW_KINDS:
        text = _read_artifact_text(workspace, artifact)
        for label in _extract_entity_labels(artifact, text):
            canonical = canonicalize_review_label(label)
            if canonical:
                signals.add(canonical)
    return {signal for signal in signals if signal}


def _priority_rank(priority: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(priority, 3)
