from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from cognisync.manifests import build_graph_manifest
from cognisync.scanner import scan_workspace
from cognisync.types import IndexSnapshot
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class SyntheticBundleResult:
    directory: Path
    dataset_path: Path
    manifest_path: Path
    record_count: int


def export_synthetic_qa_bundle(workspace: Workspace, output_dir: Optional[Path] = None) -> SyntheticBundleResult:
    snapshot = _ensure_snapshot(workspace)
    graph_manifest = build_graph_manifest(workspace, snapshot)
    assertions = _assertion_nodes(graph_manifest)
    records: List[Dict[str, object]] = []
    for assertion in assertions:
        support_paths = list(assertion.get("support_paths", []))
        if not support_paths:
            continue
        citations = [f"S{index}" for index, _ in enumerate(support_paths, start=1)]
        answer = f"{_sentence_case(assertion['subject'])} {assertion['verb']} {assertion['object']}. " + " ".join(
            f"[{citation}]" for citation in citations
        )
        records.append(
            {
                "task_type": "synthetic_qa",
                "question": _question_for_assertion(assertion["subject"], assertion["verb"], assertion["object"]),
                "answer": answer.strip(),
                "assertion": {
                    "subject": assertion["subject"],
                    "verb": assertion["verb"],
                    "object": assertion["object"],
                },
                "citations": citations,
                "source_paths": support_paths,
                "support_count": len(support_paths),
            }
        )
    return _write_bundle(workspace, "synthetic-qa", records, output_dir=output_dir)


def export_synthetic_contrastive_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> SyntheticBundleResult:
    snapshot = _ensure_snapshot(workspace)
    graph_manifest = build_graph_manifest(workspace, snapshot)
    assertions = _assertion_nodes(graph_manifest)
    all_paths = [artifact.path for artifact in snapshot.artifacts if artifact.collection in {"raw", "wiki"}]
    records: List[Dict[str, object]] = []
    for assertion in assertions:
        support_paths = list(assertion.get("support_paths", []))
        if not support_paths:
            continue
        negative_candidates = [path for path in all_paths if path not in support_paths]
        if not negative_candidates:
            continue
        records.append(
            {
                "task_type": "contrastive_retrieval",
                "query": _question_for_assertion(assertion["subject"], assertion["verb"], assertion["object"]),
                "assertion": {
                    "subject": assertion["subject"],
                    "verb": assertion["verb"],
                    "object": assertion["object"],
                },
                "positive_path": support_paths[0],
                "negative_path": negative_candidates[0],
                "support_paths": support_paths,
            }
        )
    return _write_bundle(workspace, "synthetic-contrastive", records, output_dir=output_dir)


def _ensure_snapshot(workspace: Workspace) -> IndexSnapshot:
    if workspace.index_path.exists():
        return workspace.read_index()
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    return snapshot


def _assertion_nodes(graph_manifest: Dict[str, object]) -> List[Dict[str, object]]:
    return [
        dict(node)
        for node in list(graph_manifest.get("nodes", []))
        if dict(node).get("kind") == "assertion"
    ]


def _question_for_assertion(subject: str, verb: str, obj: str) -> str:
    subject_text = subject.strip()
    if verb == "uses":
        return f"What does {subject_text} use?"
    if verb == "supports":
        return f"What does {subject_text} support?"
    if verb == "requires":
        return f"What does {subject_text} require?"
    if verb == "prefers":
        return f"What does {subject_text} prefer?"
    if verb in {"is", "are"}:
        return f"What is true about {subject_text}?"
    return f"What is the relationship between {subject_text} and {obj}?"


def _sentence_case(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return stripped
    return stripped[:1].upper() + stripped[1:]


def _write_bundle(
    workspace: Workspace,
    prefix: str,
    records: Sequence[Dict[str, object]],
    output_dir: Optional[Path] = None,
) -> SyntheticBundleResult:
    destination = output_dir or _next_bundle_dir(workspace, prefix)
    destination.mkdir(parents=True, exist_ok=True)
    dataset_path = destination / "dataset.jsonl"
    manifest_path = destination / "manifest.json"
    dataset_lines = [json.dumps(record, sort_keys=True) for record in records]
    dataset_path.write_text("\n".join(dataset_lines) + ("\n" if dataset_lines else ""), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": utc_timestamp(),
                "record_count": len(records),
                "dataset_file": dataset_path.name,
                "bundle_type": prefix,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return SyntheticBundleResult(
        directory=destination,
        dataset_path=dataset_path,
        manifest_path=manifest_path,
        record_count=len(records),
    )


def _next_bundle_dir(workspace: Workspace, prefix: str) -> Path:
    slug = utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace(".", "")
    directory = workspace.export_artifacts_dir / f"{prefix}-{slug}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
