from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.evaluation import collect_feedback_records
from cognisync.exports import collect_research_export_records
from cognisync.research import _verify_research_answer, _write_validation_report
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


class RemediationError(RuntimeError):
    pass


@dataclass(frozen=True)
class RemediationBatchResult:
    job_dirs: List[Path]
    manifest_paths: List[Path]
    remediated_count: int


def remediate_research_runs(
    workspace: Workspace,
    profile_name: str,
    limit: int = 5,
) -> RemediationBatchResult:
    config = workspace.load_config()
    try:
        adapter = adapter_from_config(config, profile_name)
    except AdapterError as error:
        raise RemediationError(str(error)) from error

    feedback_records = collect_feedback_records(workspace)
    if not feedback_records:
        return RemediationBatchResult(job_dirs=[], manifest_paths=[], remediated_count=0)

    research_records = {str(record.get("run_id", "")): record for record in collect_research_export_records(workspace)}
    selected_records = feedback_records[: max(0, limit)]
    job_dirs: List[Path] = []
    manifest_paths: List[Path] = []

    for feedback_record in selected_records:
        run_id = str(feedback_record.get("run_id", "")).strip()
        source_record = research_records.get(run_id)
        if source_record is None:
            raise RemediationError(f"Could not locate the source research record for remediation run '{run_id}'.")
        job_dir, manifest_path = _run_single_remediation(
            workspace=workspace,
            adapter=adapter,
            profile_name=profile_name,
            source_record=source_record,
            feedback_record=feedback_record,
        )
        job_dirs.append(job_dir)
        manifest_paths.append(manifest_path)

    return RemediationBatchResult(
        job_dirs=job_dirs,
        manifest_paths=manifest_paths,
        remediated_count=len(job_dirs),
    )


def _run_single_remediation(
    workspace: Workspace,
    adapter,
    profile_name: str,
    source_record: Dict[str, object],
    feedback_record: Dict[str, object],
) -> tuple[Path, Path]:
    question = str(source_record.get("question", "")).strip()
    run_id = str(source_record.get("run_id", "")).strip()
    job_dir = _next_remediation_job_dir(workspace, question)
    job_dir.mkdir(parents=True, exist_ok=True)

    prompt_path = job_dir / "remediation-packet.md"
    answer_path = job_dir / "answer.md"
    validation_report_path = job_dir / "validation-report.md"
    manifest_path = job_dir / "manifest.json"

    prompt_path.write_text(_render_remediation_packet(source_record, feedback_record), encoding="utf-8")

    result = adapter.run(prompt_file=prompt_path, workspace_root=workspace.root, output_file=answer_path)
    if result.returncode != 0:
        manifest_payload = _build_remediation_manifest(
            source_record=source_record,
            feedback_record=feedback_record,
            profile_name=profile_name,
            prompt_path=workspace.relative_path(prompt_path),
            answer_path=workspace.relative_path(answer_path),
            validation_report_path=workspace.relative_path(validation_report_path),
            status="adapter_failed",
            validation={
                "passed": False,
                "status": "adapter_failed",
                "used": [],
                "errors": [f"Adapter '{profile_name}' exited with code {result.returncode}."],
                "warnings": [],
            },
        )
        manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
        raise RemediationError(f"Adapter '{profile_name}' exited with code {result.returncode}.")

    if not adapter.output_file_flag and result.stdout:
        answer_path.write_text(result.stdout, encoding="utf-8")

    answer_text = answer_path.read_text(encoding="utf-8", errors="ignore") if answer_path.exists() else ""
    sources = list(source_record.get("sources", []))
    validation = _verify_research_answer(workspace, answer_text, sources)
    _write_validation_report(workspace, validation_report_path, question, "remediation", sources, validation)

    status = "completed" if bool(validation.get("passed", False)) else "failed_validation"
    manifest_payload = _build_remediation_manifest(
        source_record=source_record,
        feedback_record=feedback_record,
        profile_name=profile_name,
        prompt_path=workspace.relative_path(prompt_path),
        answer_path=workspace.relative_path(answer_path),
        validation_report_path=workspace.relative_path(validation_report_path),
        status=status,
        validation=validation,
    )
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")

    if not validation.get("passed", False):
        raise RemediationError("Remediation verification failed: " + "; ".join(list(validation.get("errors", []))))

    return job_dir, manifest_path


def _build_remediation_manifest(
    source_record: Dict[str, object],
    feedback_record: Dict[str, object],
    profile_name: str,
    prompt_path: str,
    answer_path: str,
    validation_report_path: str,
    status: str,
    validation: Dict[str, object],
) -> Dict[str, object]:
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "run_kind": "research_remediation",
        "status": status,
        "profile": profile_name,
        "source_run_id": str(source_record.get("run_id", "")),
        "source_run_manifest_path": str(source_record.get("run_manifest_path", "")),
        "question": str(source_record.get("question", "")),
        "improvement_targets": list(feedback_record.get("improvement_targets", [])),
        "dimension_scores_before": dict(feedback_record.get("dimension_scores", {})),
        "prompt_path": prompt_path,
        "answer_path": answer_path,
        "validation_report_path": validation_report_path,
        "validation": validation,
    }


def _render_remediation_packet(source_record: Dict[str, object], feedback_record: Dict[str, object]) -> str:
    question = str(source_record.get("question", "")).strip()
    current_response = str(feedback_record.get("current_response", "")).strip()
    improvement_targets = [str(item) for item in list(feedback_record.get("improvement_targets", []))]
    sources = list(source_record.get("sources", []))
    lines = [
        "# Remediation Prompt",
        "",
        f"Question: {question}",
        "",
        "Rewrite the answer so it passes Cognisync research validation.",
        "",
        "## Improvement Targets",
        "",
    ]
    if improvement_targets:
        lines.extend(f"- `{target}`" for target in improvement_targets)
    else:
        lines.append("- No explicit improvement targets were recorded.")
    lines.extend(
        [
            "",
            "## Current Answer",
            "",
            current_response or "_No prior answer text was stored._",
            "",
            "## Retrieved Sources",
            "",
        ]
    )
    if sources:
        for source in sources:
            lines.extend(
                [
                    f"### [{source['citation']}] {source['title']}",
                    "",
                    f"- Path: `{source['path']}`",
                    f"- Kind: `{source['source_kind']}`",
                    f"- Snippet: {source.get('snippet', '')}",
                    "",
                ]
            )
    else:
        lines.append("No retrieved sources were stored for this run.")
        lines.append("")
    lines.extend(
        [
            "## Instructions",
            "",
            "- Keep a top-level Markdown heading.",
            "- Ground every claim in the retrieved sources.",
            "- Use inline citations like `[S1]` tied to the stored source ids above.",
            "- If sources disagree, acknowledge both sides and cite both.",
            "- Write only the corrected answer artifact.",
            "",
        ]
    )
    return "\n".join(lines)


def _next_remediation_job_dir(workspace: Workspace, question: str) -> Path:
    slug = slugify(question) or "remediation"
    stem = utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace(".", "")
    return workspace.remediation_jobs_dir / f"remediation-{stem}-{slug}"
