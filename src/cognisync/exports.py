from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
from typing import Dict, List, Optional

from cognisync.manifests import read_json_manifest
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class ExportResult:
    path: Path
    record_count: int


@dataclass(frozen=True)
class PresentationBundleResult:
    directory: Path
    manifest_path: Path
    presentation_count: int


def export_research_jsonl(workspace: Workspace, output_file: Optional[Path] = None) -> ExportResult:
    records: List[Dict[str, object]] = []
    for manifest_path in sorted(workspace.runs_dir.glob("*.json")):
        manifest = read_json_manifest(manifest_path)
        if str(manifest.get("run_kind", "")) != "research":
            continue
        records.append(_build_research_export_record(workspace, manifest_path, manifest))

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
        "resume_count": int(manifest.get("resume_count", 0) or 0),
        "attempt_count": int(manifest.get("attempt_count", 0) or 0),
        "run_manifest_path": workspace.relative_path(manifest_path),
        "report_path": workspace.relative_path(report_path) if report_path else None,
        "answer_path": workspace.relative_path(answer_path) if answer_path else None,
        "packet_path": workspace.relative_path(packet_path) if packet_path else None,
        "slide_path": workspace.relative_path(slide_path) if slide_path else None,
        "change_summary_path": workspace.relative_path(change_summary_path) if change_summary_path else None,
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
