from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from cognisync.knowledge_surfaces import is_navigation_surface_path
from cognisync.manifests import build_graph_manifest, read_json_manifest
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
    records = build_synthetic_qa_records(workspace)
    return _write_bundle(workspace, "synthetic-qa", records, output_dir=output_dir)


def export_synthetic_contrastive_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> SyntheticBundleResult:
    records = build_synthetic_contrastive_records(workspace)
    return _write_bundle(workspace, "synthetic-contrastive", records, output_dir=output_dir)


def export_synthetic_graph_completion_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> SyntheticBundleResult:
    records = build_synthetic_graph_completion_records(workspace)
    return _write_bundle(workspace, "synthetic-graph-completion", records, output_dir=output_dir)


def export_synthetic_report_writing_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> SyntheticBundleResult:
    records = build_synthetic_report_writing_records(workspace)
    return _write_bundle(workspace, "synthetic-report-writing", records, output_dir=output_dir)


def build_synthetic_qa_records(workspace: Workspace) -> List[Dict[str, object]]:
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
                "support_paths": support_paths,
                "source_paths": support_paths,
                "support_count": len(support_paths),
            }
        )
    return records


def build_synthetic_contrastive_records(workspace: Workspace) -> List[Dict[str, object]]:
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
    return records


def build_synthetic_graph_completion_records(workspace: Workspace) -> List[Dict[str, object]]:
    snapshot = _ensure_snapshot(workspace)
    graph_manifest = build_graph_manifest(workspace, snapshot)
    assertions = _assertion_nodes(graph_manifest)
    records: List[Dict[str, object]] = []
    for assertion in assertions:
        subject = str(assertion.get("subject", "")).strip()
        verb = str(assertion.get("verb", "")).strip()
        obj = str(assertion.get("object", "")).strip()
        support_paths = list(assertion.get("support_paths", []))
        if not subject or not verb or not obj or not support_paths:
            continue
        records.append(
            {
                "task_type": "graph_completion",
                "prompt": "\n".join(
                    [
                        "Complete the missing graph edge using the support paths.",
                        f"Subject: {subject}",
                        f"Verb: {verb}",
                        "Object: <mask>",
                        f"Support paths: {', '.join(support_paths)}",
                    ]
                ),
                "response": obj,
                "assertion": {
                    "subject": subject,
                    "verb": verb,
                    "object": obj,
                },
                "support_paths": support_paths,
                "source_paths": support_paths,
            }
        )
    return records


def build_synthetic_report_writing_records(workspace: Workspace) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for manifest_path in sorted(workspace.runs_dir.glob("*.json")):
        manifest = read_json_manifest(manifest_path)
        if str(manifest.get("run_kind", "")) != "research":
            continue
        question = str(manifest.get("question", "")).strip()
        mode = str(manifest.get("mode", "report")).strip() or "report"
        response_text = ""
        for candidate in (
            _workspace_path(workspace, manifest.get("answer_path")),
            _workspace_path(workspace, manifest.get("report_path")),
            _workspace_path(workspace, manifest.get("source_packet_path")),
        ):
            response_text = _read_text(candidate)
            if response_text:
                break
        if not question or not response_text:
            continue
        sources = [
            str(source.get("path", "")).strip()
            for source in list(manifest.get("sources", []))
            if isinstance(source, dict) and str(source.get("path", "")).strip()
        ]
        records.append(
            {
                "task_type": "report_writing",
                "prompt": (
                    f"Write a {mode} answering \"{question}\". "
                    "Use the persisted corpus artifacts and preserve inline citations where available."
                ),
                "response": response_text,
                "question": question,
                "mode": mode,
                "job_profile": str(manifest.get("job_profile", "")),
                "source_paths": sources,
                "citations": list(dict(manifest.get("validation", {})).get("used", [])),
                "run_manifest_path": workspace.relative_path(manifest_path),
            }
        )
    return records


def _ensure_snapshot(workspace: Workspace) -> IndexSnapshot:
    return workspace.refresh_index()


def _assertion_nodes(graph_manifest: Dict[str, object]) -> List[Dict[str, object]]:
    nodes = [
        dict(node)
        for node in list(graph_manifest.get("nodes", []))
        if dict(node).get("kind") == "assertion"
    ]
    filtered = [
        node
        for node in nodes
        if not any(is_navigation_surface_path(str(path)) for path in list(node.get("support_paths", [])))
    ]
    filtered.sort(
        key=lambda node: (
            -int(node.get("support_count", 0)),
            str(node.get("subject", "")),
            str(node.get("verb", "")),
            str(node.get("object", "")),
        )
    )
    return filtered


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


def _workspace_path(workspace: Workspace, value: object) -> Optional[Path]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    return workspace.root / normalized


def _read_text(path: Optional[Path]) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


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
