from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from cognisync.corpus import classify_source_kind, infer_extraction_status, pick_primary_artifact, source_group_key
from cognisync.graph_intelligence import build_graph_semantics
from cognisync.notifications import build_notification_manifest
from cognisync.review_queue import build_review_queue
from cognisync.types import ArtifactRecord, IndexSnapshot
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


def write_workspace_manifests(workspace: Workspace, snapshot: IndexSnapshot) -> Tuple[Path, Path, Path, Path]:
    source_path = workspace.sources_manifest_path
    graph_path = workspace.graph_manifest_path
    review_path = workspace.review_queue_manifest_path
    notifications_path = workspace.notifications_manifest_path
    source_path.parent.mkdir(parents=True, exist_ok=True)
    review_payload = build_review_queue(workspace, snapshot)
    source_path.write_text(
        json.dumps(build_source_manifest(snapshot), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    graph_path.write_text(
        json.dumps(build_graph_manifest(workspace, snapshot), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    review_path.write_text(
        json.dumps(review_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    notifications_path.write_text(
        json.dumps(build_notification_manifest(workspace), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return source_path, graph_path, review_path, notifications_path


def write_run_manifest(
    workspace: Workspace,
    run_kind: str,
    payload: Dict[str, object],
    run_id: Optional[str] = None,
) -> Path:
    run_id = run_id or _run_manifest_name(run_kind, payload)
    path = workspace.runs_dir / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "run_kind": run_kind,
    }
    manifest.update(payload)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return path


def read_json_manifest(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_source_manifest(snapshot: IndexSnapshot) -> Dict[str, object]:
    grouped: Dict[str, List[ArtifactRecord]] = {}
    for artifact in snapshot.artifacts:
        source_key = source_group_key(artifact.path)
        if source_key is None:
            continue
        grouped.setdefault(source_key, []).append(artifact)

    sources = []
    for source_key in sorted(grouped):
        artifacts = sorted(grouped[source_key], key=lambda item: item.path)
        artifact_paths = [artifact.path for artifact in artifacts]
        representative = artifacts[0]
        source_kind = classify_source_kind(representative.path, representative.tags)
        primary_artifact = pick_primary_artifact(artifact_paths, source_kind)
        primary_record = next((artifact for artifact in artifacts if artifact.path == primary_artifact), representative)
        captured_assets = [path for path in artifact_paths if not path.endswith(".md") and path != primary_artifact]
        summary_targets = sorted({artifact.summary_target for artifact in artifacts if artifact.summary_target})
        tags = sorted({tag for artifact in artifacts for tag in artifact.tags})
        sources.append(
            {
                "source_key": source_key,
                "source_kind": source_kind,
                "title": primary_record.title,
                "primary_artifact": primary_artifact,
                "artifacts": artifact_paths,
                "captured_assets": captured_assets,
                "summary_targets": summary_targets,
                "tags": tags,
                "extraction_status": infer_extraction_status(source_kind, artifact_paths),
                "word_count": sum(artifact.word_count for artifact in artifacts),
            }
        )

    return {
        "schema_version": 1,
        "generated_at": snapshot.generated_at,
        "sources": sources,
    }


def build_graph_manifest(workspace: Workspace, snapshot: IndexSnapshot) -> Dict[str, object]:
    nodes = []
    edges = []
    tag_nodes = set()
    seen_edges = set()
    existing_paths = {artifact.path for artifact in snapshot.artifacts}

    for artifact in sorted(snapshot.artifacts, key=lambda item: item.path):
        nodes.append(
            {
                "id": artifact.path,
                "kind": "artifact",
                "path": artifact.path,
                "title": artifact.title,
                "collection": artifact.collection,
                "artifact_kind": artifact.kind,
                "source_kind": classify_source_kind(artifact.path, artifact.tags),
                "tags": artifact.tags,
                "word_count": artifact.word_count,
            }
        )
        for tag in artifact.tags:
            tag_id = f"tag:{tag}"
            if tag_id not in tag_nodes:
                nodes.append({"id": tag_id, "kind": "tag", "title": tag, "tag": tag})
                tag_nodes.add(tag_id)
            edge = (artifact.path, tag_id, "tag")
            if edge not in seen_edges:
                edges.append({"source": artifact.path, "target": tag_id, "kind": "tag"})
                seen_edges.add(edge)
        for link in artifact.links:
            if link.external or not link.resolved_path:
                continue
            if link.resolved_path not in existing_paths:
                continue
            edge = (artifact.path, link.resolved_path, "link")
            if edge in seen_edges:
                continue
            edges.append({"source": artifact.path, "target": link.resolved_path, "kind": "link"})
            seen_edges.add(edge)

    semantics = build_graph_semantics(workspace, snapshot)
    seen_node_ids = {node["id"] for node in nodes}
    for node in semantics["nodes"]:
        if node["id"] in seen_node_ids:
            continue
        nodes.append(node)
        seen_node_ids.add(node["id"])
    for edge in semantics["edges"]:
        edge_key = (
            str(edge["source"]),
            str(edge["target"]),
            str(edge["kind"]),
            str(edge.get("subject", "")),
            str(edge.get("verb", "")),
        )
        if edge_key in seen_edges:
            continue
        edges.append(edge)
        seen_edges.add(edge_key)

    return {
        "schema_version": 1,
        "generated_at": snapshot.generated_at,
        "nodes": nodes,
        "edges": sorted(edges, key=lambda item: (item["source"], item["target"], item["kind"])),
    }


def _run_manifest_name(run_kind: str, payload: Dict[str, object]) -> str:
    seed = str(payload.get("question") or payload.get("run_label") or run_kind)
    timestamp = utc_timestamp().replace(":", "").replace("-", "")
    return f"{run_kind}-{timestamp}-{slugify(seed)[:48]}"
