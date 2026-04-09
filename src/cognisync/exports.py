from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict
import json
from pathlib import Path
import shutil
from typing import Dict, List, Optional

from cognisync.manifests import read_json_manifest
from cognisync.synthetic_data import (
    build_synthetic_contrastive_records,
    build_synthetic_graph_completion_records,
    build_synthetic_qa_records,
    build_synthetic_report_writing_records,
)
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


class ExportError(RuntimeError):
    """Raised when an export contract cannot be materialized."""


@dataclass(frozen=True)
class ExportResult:
    path: Path
    record_count: int


@dataclass(frozen=True)
class PresentationBundleResult:
    directory: Path
    manifest_path: Path
    presentation_count: int


@dataclass(frozen=True)
class TrainingBundleResult:
    directory: Path
    dataset_path: Path
    manifest_path: Path
    record_count: int


@dataclass(frozen=True)
class FinetuneBundleResult:
    directory: Path
    supervised_path: Path
    retrieval_path: Path
    manifest_path: Path
    supervised_count: int
    retrieval_count: int
    provider_exports: Dict[str, Path]


@dataclass(frozen=True)
class CorrectionBundleResult:
    directory: Path
    dataset_path: Path
    manifest_path: Path
    record_count: int


def collect_research_export_records(workspace: Workspace) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for manifest_path in sorted(workspace.runs_dir.glob("*.json")):
        manifest = read_json_manifest(manifest_path)
        if str(manifest.get("run_kind", "")) != "research":
            continue
        records.append(_build_research_export_record(workspace, manifest_path, manifest))
    return records


def export_research_jsonl(workspace: Workspace, output_file: Optional[Path] = None) -> ExportResult:
    records = collect_research_export_records(workspace)
    destination = output_file or _next_jsonl_export_path(workspace)
    destination.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record, sort_keys=True) for record in records]
    destination.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return ExportResult(path=destination, record_count=len(records))


def export_presentations_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> PresentationBundleResult:
    destination = output_dir or _next_presentation_bundle_dir(workspace)
    slides_dir = destination / "slides"
    reports_dir = destination / "reports"
    answers_dir = destination / "answers"
    slides_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    answers_dir.mkdir(parents=True, exist_ok=True)

    presentations: List[Dict[str, object]] = []
    for manifest_path in sorted(workspace.runs_dir.glob("*.json")):
        manifest = read_json_manifest(manifest_path)
        if str(manifest.get("run_kind", "")) != "research":
            continue
        slide_path = _resolve_workspace_relative(workspace, str(manifest.get("slide_path", "") or ""))
        if slide_path is None or not slide_path.exists():
            continue

        report_path = _resolve_workspace_relative(workspace, str(manifest.get("report_path", "") or ""))
        answer_path = _resolve_workspace_relative(workspace, str(manifest.get("answer_path", "") or ""))

        slide_target = slides_dir / slide_path.name
        shutil.copy2(slide_path, slide_target)

        report_file = None
        if report_path is not None and report_path.exists():
            report_target = reports_dir / report_path.name
            shutil.copy2(report_path, report_target)
            report_file = report_target.relative_to(destination).as_posix()

        answer_file = None
        if answer_path is not None and answer_path.exists():
            answer_target = answers_dir / answer_path.name
            shutil.copy2(answer_path, answer_target)
            answer_file = answer_target.relative_to(destination).as_posix()

        presentations.append(
            {
                "question": str(manifest.get("question", "")),
                "generated_at": str(manifest.get("generated_at", "")),
                "status": str(manifest.get("status", "")),
                "mode": str(manifest.get("mode", "")),
                "run_manifest_path": workspace.relative_path(manifest_path),
                "slide_file": slide_target.relative_to(destination).as_posix(),
                "report_file": report_file,
                "answer_file": answer_file,
            }
        )

    manifest_payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "presentation_count": len(presentations),
        "presentations": presentations,
    }
    manifest_path = destination / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    return PresentationBundleResult(
        directory=destination,
        manifest_path=manifest_path,
        presentation_count=len(presentations),
    )


def export_training_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> TrainingBundleResult:
    destination = output_dir or _next_training_bundle_dir(workspace)
    destination.mkdir(parents=True, exist_ok=True)
    dataset_path = destination / "dataset.jsonl"
    manifest_path = destination / "manifest.json"

    records = collect_research_export_records(workspace)
    status_counts: Dict[str, int] = defaultdict(int)
    label_counts: Dict[str, int] = defaultdict(int)
    enriched_records: List[Dict[str, object]] = []
    for record in records:
        labels = derive_research_labels(record)
        enriched = dict(record)
        enriched["labels"] = labels
        enriched["source_count"] = len(list(record.get("sources", [])))
        enriched["citation_count"] = len(list(dict(record.get("validation", {})).get("used", [])))
        enriched["error_count"] = len(list(dict(record.get("validation", {})).get("errors", [])))
        enriched["warning_count"] = len(list(dict(record.get("validation", {})).get("warnings", [])))
        enriched_records.append(enriched)
        status_counts[str(record.get("status", ""))] += 1
        for label, value in labels.items():
            if isinstance(value, bool) and value:
                label_counts[label] += 1

    dataset_lines = [json.dumps(record, sort_keys=True) for record in enriched_records]
    dataset_path.write_text("\n".join(dataset_lines) + ("\n" if dataset_lines else ""), encoding="utf-8")
    manifest_payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "record_count": len(enriched_records),
        "dataset_file": dataset_path.name,
        "status_counts": dict(sorted(status_counts.items())),
        "label_counts": dict(sorted(label_counts.items())),
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    return TrainingBundleResult(
        directory=destination,
        dataset_path=dataset_path,
        manifest_path=manifest_path,
        record_count=len(enriched_records),
    )


def export_finetune_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
    provider_formats: Optional[List[str]] = None,
) -> FinetuneBundleResult:
    destination = output_dir or _next_finetune_bundle_dir(workspace)
    destination.mkdir(parents=True, exist_ok=True)
    supervised_path = destination / "supervised.jsonl"
    retrieval_path = destination / "retrieval.jsonl"
    manifest_path = destination / "manifest.json"

    supervised_records = _build_supervised_finetune_records(workspace)
    retrieval_records = _build_retrieval_finetune_records(workspace)
    requested_provider_formats = _normalize_provider_formats(provider_formats)

    supervised_lines = [json.dumps(record, sort_keys=True) for record in supervised_records]
    retrieval_lines = [json.dumps(record, sort_keys=True) for record in retrieval_records]
    supervised_path.write_text("\n".join(supervised_lines) + ("\n" if supervised_lines else ""), encoding="utf-8")
    retrieval_path.write_text("\n".join(retrieval_lines) + ("\n" if retrieval_lines else ""), encoding="utf-8")

    provider_exports: Dict[str, Path] = {}
    for provider_format in requested_provider_formats:
        if provider_format == "openai-chat":
            provider_path = destination / "supervised.openai-chat.jsonl"
            provider_records = _build_openai_chat_supervised_records(supervised_records)
            provider_lines = [json.dumps(record, sort_keys=True) for record in provider_records]
            provider_path.write_text("\n".join(provider_lines) + ("\n" if provider_lines else ""), encoding="utf-8")
            provider_exports[provider_format] = provider_path
            continue
        raise ExportError(f"Unsupported provider format: {provider_format}")

    manifest_payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "bundle_type": "finetune-bundle",
        "supervised_file": supervised_path.name,
        "retrieval_file": retrieval_path.name,
        "supervised_count": len(supervised_records),
        "retrieval_count": len(retrieval_records),
        "supervised_example_types": _count_example_types(supervised_records),
        "retrieval_example_types": _count_example_types(retrieval_records),
        "provider_exports": {name: path.name for name, path in sorted(provider_exports.items())},
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    return FinetuneBundleResult(
        directory=destination,
        supervised_path=supervised_path,
        retrieval_path=retrieval_path,
        manifest_path=manifest_path,
        supervised_count=len(supervised_records),
        retrieval_count=len(retrieval_records),
        provider_exports=provider_exports,
    )


def export_correction_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
) -> CorrectionBundleResult:
    destination = output_dir or _next_correction_bundle_dir(workspace)
    destination.mkdir(parents=True, exist_ok=True)
    dataset_path = destination / "dataset.jsonl"
    manifest_path = destination / "manifest.json"

    correction_records = collect_correction_export_records(workspace)
    dataset_lines = [json.dumps(record, sort_keys=True) for record in correction_records]
    dataset_path.write_text("\n".join(dataset_lines) + ("\n" if dataset_lines else ""), encoding="utf-8")

    target_counts: Dict[str, int] = defaultdict(int)
    status_counts: Dict[str, int] = defaultdict(int)
    for record in correction_records:
        status_counts[str(record.get("status", ""))] += 1
        for target in list(record.get("improvement_targets", [])):
            target_counts[str(target)] += 1

    manifest_payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "bundle_type": "correction-bundle",
        "dataset_file": dataset_path.name,
        "record_count": len(correction_records),
        "status_counts": dict(sorted(status_counts.items())),
        "target_counts": dict(sorted(target_counts.items())),
        "example_types": _count_example_types(correction_records),
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    return CorrectionBundleResult(
        directory=destination,
        dataset_path=dataset_path,
        manifest_path=manifest_path,
        record_count=len(correction_records),
    )


def derive_research_labels(record: Dict[str, object]) -> Dict[str, object]:
    validation = dict(record.get("validation", {}))
    checks = dict(validation.get("checks", {}))
    citation_check = dict(checks.get("citations", {}))
    answer_lint = dict(checks.get("answer_lint", {}))
    unsupported_claims = dict(checks.get("unsupported_claims", {}))
    source_conflicts = dict(checks.get("source_conflicts", {}))
    return {
        "validation_passed": bool(validation.get("passed", False)),
        "has_warnings": bool(validation.get("warnings", [])),
        "has_errors": bool(validation.get("errors", [])),
        "has_citation_errors": bool(citation_check.get("errors", [])),
        "has_answer_lint_errors": bool(answer_lint.get("errors", [])),
        "has_unsupported_claims": bool(unsupported_claims.get("errors", [])),
        "has_conflict_gate": bool(source_conflicts.get("errors", [])),
    }


def collect_correction_export_records(workspace: Workspace) -> List[Dict[str, object]]:
    research_records = {str(record.get("run_id", "")): record for record in collect_research_export_records(workspace)}
    records: List[Dict[str, object]] = []
    for manifest_path in sorted(workspace.remediation_jobs_dir.glob("remediation-*/manifest.json")):
        manifest = read_json_manifest(manifest_path)
        if str(manifest.get("run_kind", "")) != "research_remediation":
            continue
        validation = dict(manifest.get("validation", {}))
        if str(manifest.get("status", "")) != "completed" or not bool(validation.get("passed", False)):
            continue
        source_run_id = str(manifest.get("source_run_id", "")).strip()
        source_record = research_records.get(source_run_id)
        records.append(_build_correction_export_record(workspace, manifest_path, manifest, source_record))
    return records


def _build_supervised_finetune_records(workspace: Workspace) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for record in collect_research_export_records(workspace):
        response_text = str(record.get("answer_text") or record.get("report_text") or "").strip()
        prompt_text = str(record.get("question") or "").strip()
        if not prompt_text or not response_text:
            continue
        records.append(
            {
                "example_type": "research_run",
                "prompt": prompt_text,
                "response": response_text,
                "citations": list(dict(record.get("citations", {})).get("used", [])),
                "source_paths": list(record.get("sources", [])),
                "validation_labels": derive_research_labels(record),
                "metadata": {
                    "run_id": record.get("run_id"),
                    "mode": record.get("mode"),
                    "status": record.get("status"),
                    "job_profile": record.get("job_profile"),
                    "run_manifest_path": record.get("run_manifest_path"),
                    "report_path": record.get("report_path"),
                    "answer_path": record.get("answer_path"),
                },
            }
        )

    for record in collect_correction_export_records(workspace):
        prompt_text = str(record.get("prompt") or "").strip()
        response_text = str(record.get("response") or "").strip()
        if not prompt_text or not response_text:
            continue
        records.append(dict(record))

    for record in build_synthetic_qa_records(workspace):
        prompt_text = str(record.get("question") or "").strip()
        response_text = str(record.get("answer") or "").strip()
        if not prompt_text or not response_text:
            continue
        records.append(
            {
                "example_type": "synthetic_qa",
                "prompt": prompt_text,
                "response": response_text,
                "citations": list(record.get("citations", [])),
                "source_paths": list(record.get("source_paths", [])),
                "metadata": {
                    "assertion": dict(record.get("assertion", {})),
                    "support_count": int(record.get("support_count", 0) or 0),
                },
            }
        )

    for record in build_synthetic_graph_completion_records(workspace):
        prompt_text = str(record.get("prompt") or "").strip()
        response_text = str(record.get("response") or "").strip()
        if not prompt_text or not response_text:
            continue
        records.append(
            {
                "example_type": "graph_completion",
                "prompt": prompt_text,
                "response": response_text,
                "source_paths": list(record.get("source_paths", [])),
                "metadata": {
                    "assertion": dict(record.get("assertion", {})),
                    "support_paths": list(record.get("support_paths", [])),
                },
            }
        )

    for record in build_synthetic_report_writing_records(workspace):
        prompt_text = str(record.get("prompt") or "").strip()
        response_text = str(record.get("response") or "").strip()
        if not prompt_text or not response_text:
            continue
        records.append(
            {
                "example_type": "report_writing",
                "prompt": prompt_text,
                "response": response_text,
                "citations": list(record.get("citations", [])),
                "source_paths": list(record.get("source_paths", [])),
                "metadata": {
                    "question": str(record.get("question", "")),
                    "mode": str(record.get("mode", "")),
                    "job_profile": str(record.get("job_profile", "")),
                    "run_manifest_path": str(record.get("run_manifest_path", "")),
                },
            }
        )
    return records


def _build_correction_export_record(
    workspace: Workspace,
    manifest_path: Path,
    manifest: Dict[str, object],
    source_record: Optional[Dict[str, object]],
) -> Dict[str, object]:
    answer_path = _resolve_workspace_relative(workspace, str(manifest.get("answer_path", "") or ""))
    prompt_path = _resolve_workspace_relative(workspace, str(manifest.get("prompt_path", "") or ""))
    validation_report_path = _resolve_workspace_relative(
        workspace,
        str(manifest.get("validation_report_path", "") or ""),
    )
    validation = dict(manifest.get("validation", {}))
    previous_response = ""
    source_paths: List[str] = []
    if source_record is not None:
        previous_response = str(source_record.get("answer_text") or source_record.get("report_text") or "").strip()
        source_paths = [
            str(source.get("path", "")).strip()
            for source in list(source_record.get("sources", []))
            if isinstance(source, dict) and str(source.get("path", "")).strip()
        ]
    corrected_response = _read_text(answer_path) or ""
    return {
        "example_type": "remediation_correction",
        "question": str(manifest.get("question", "")),
        "prompt": str(manifest.get("question", "")),
        "response": corrected_response.strip(),
        "previous_response": previous_response,
        "status": str(manifest.get("status", "")),
        "improvement_targets": list(manifest.get("improvement_targets", [])),
        "citations": list(validation.get("used", [])),
        "source_paths": source_paths,
        "validation": validation,
        "metadata": {
            "source_run_id": str(manifest.get("source_run_id", "")),
            "source_run_manifest_path": str(manifest.get("source_run_manifest_path", "")),
            "remediation_run_manifest_path": workspace.relative_path(manifest_path),
            "prompt_path": workspace.relative_path(prompt_path) if prompt_path else None,
            "answer_path": workspace.relative_path(answer_path) if answer_path else None,
            "validation_report_path": (
                workspace.relative_path(validation_report_path) if validation_report_path else None
            ),
            "dimension_scores_before": dict(manifest.get("dimension_scores_before", {})),
        },
    }


def _build_retrieval_finetune_records(workspace: Workspace) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for record in build_synthetic_contrastive_records(workspace):
        query_text = str(record.get("query") or "").strip()
        positive_path = str(record.get("positive_path") or "").strip()
        negative_path = str(record.get("negative_path") or "").strip()
        if not query_text or not positive_path or not negative_path:
            continue
        records.append(
            {
                "example_type": "contrastive_retrieval",
                "query": query_text,
                "positive_path": positive_path,
                "negative_path": negative_path,
                "support_paths": list(record.get("support_paths", [])),
                "metadata": {
                    "assertion": dict(record.get("assertion", {})),
                },
            }
        )
    return records


def _count_example_types(records: List[Dict[str, object]]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for record in records:
        counts[str(record.get("example_type", "unknown"))] += 1
    return dict(sorted(counts.items()))


def _build_openai_chat_supervised_records(records: List[Dict[str, object]]) -> List[Dict[str, object]]:
    payload: List[Dict[str, object]] = []
    for record in records:
        prompt_text = str(record.get("prompt") or "").strip()
        response_text = str(record.get("response") or "").strip()
        if not prompt_text or not response_text:
            continue
        payload.append(
            {
                "messages": [
                    {"role": "user", "content": prompt_text},
                    {"role": "assistant", "content": response_text},
                ],
                "metadata": {
                    "example_type": str(record.get("example_type", "")),
                    "source_paths": list(record.get("source_paths", [])),
                    "citations": list(record.get("citations", [])),
                    "details": dict(record.get("metadata", {})),
                },
            }
        )
    return payload


def _normalize_provider_formats(provider_formats: Optional[List[str]]) -> List[str]:
    normalized: List[str] = []
    for value in provider_formats or []:
        item = str(value).strip().lower()
        if not item:
            continue
        if item not in normalized:
            normalized.append(item)
    return normalized


def _build_research_export_record(
    workspace: Workspace,
    manifest_path: Path,
    manifest: Dict[str, object],
) -> Dict[str, object]:
    report_path = _resolve_workspace_relative(workspace, str(manifest.get("report_path", "") or ""))
    answer_path = _resolve_workspace_relative(workspace, str(manifest.get("answer_path", "") or ""))
    packet_path = _resolve_workspace_relative(workspace, str(manifest.get("packet_path", "") or ""))
    slide_path = _resolve_workspace_relative(workspace, str(manifest.get("slide_path", "") or ""))
    change_summary_path = _resolve_workspace_relative(workspace, str(manifest.get("change_summary_path", "") or ""))
    return {
        "run_id": manifest_path.stem,
        "run_kind": str(manifest.get("run_kind", "")),
        "question": str(manifest.get("question", "")),
        "generated_at": str(manifest.get("generated_at", "")),
        "status": str(manifest.get("status", "")),
        "mode": str(manifest.get("mode", "")),
        "job_profile": str(manifest.get("job_profile", "")),
        "resume_count": int(manifest.get("resume_count", 0) or 0),
        "attempt_count": int(manifest.get("attempt_count", 0) or 0),
        "run_manifest_path": workspace.relative_path(manifest_path),
        "report_path": workspace.relative_path(report_path) if report_path else None,
        "answer_path": workspace.relative_path(answer_path) if answer_path else None,
        "packet_path": workspace.relative_path(packet_path) if packet_path else None,
        "slide_path": workspace.relative_path(slide_path) if slide_path else None,
        "change_summary_path": workspace.relative_path(change_summary_path) if change_summary_path else None,
        "notes_dir": str(manifest.get("notes_dir", "")) or None,
        "note_paths": list(manifest.get("note_paths", [])),
        "execution_packet_paths": list(manifest.get("execution_packet_paths", [])),
        "source_packet_path": str(manifest.get("source_packet_path", "")) or None,
        "checkpoints_path": str(manifest.get("checkpoints_path", "")) or None,
        "validation_report_path": str(manifest.get("validation_report_path", "")) or None,
        "report_text": _read_text(report_path),
        "answer_text": _read_text(answer_path),
        "packet_text": _read_text(packet_path),
        "citations": manifest.get("citations", {}),
        "validation": manifest.get("validation", {}),
        "sources": list(manifest.get("sources", [])),
    }


def _read_text(path: Optional[Path]) -> Optional[str]:
    if path is None or not path.exists() or not path.is_file():
        return None
    return path.read_text(encoding="utf-8", errors="ignore")


def _resolve_workspace_relative(workspace: Workspace, relative_path: str) -> Optional[Path]:
    normalized = relative_path.strip()
    if not normalized:
        return None
    path = Path(normalized)
    if path.is_absolute():
        return path
    return workspace.root / path


def _timestamp_slug() -> str:
    return utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace("T", "T")


def _next_jsonl_export_path(workspace: Workspace) -> Path:
    directory = workspace.export_artifacts_dir
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"research-dataset-{_timestamp_slug()}.jsonl"


def _next_presentation_bundle_dir(workspace: Workspace) -> Path:
    directory = workspace.export_artifacts_dir / f"presentations-{_timestamp_slug()}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _next_training_bundle_dir(workspace: Workspace) -> Path:
    directory = workspace.export_artifacts_dir / f"training-bundle-{_timestamp_slug()}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _next_finetune_bundle_dir(workspace: Workspace) -> Path:
    directory = workspace.export_artifacts_dir / f"finetune-bundle-{_timestamp_slug()}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _next_correction_bundle_dir(workspace: Workspace) -> Path:
    directory = workspace.export_artifacts_dir / f"correction-bundle-{_timestamp_slug()}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
