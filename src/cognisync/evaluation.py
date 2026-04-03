from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.exports import collect_research_export_records, derive_research_labels
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class ResearchEvaluationResult:
    report_path: Path
    payload_path: Path
    run_count: int


def evaluate_research_runs(
    workspace: Workspace,
    output_file: Optional[Path] = None,
    payload_file: Optional[Path] = None,
) -> ResearchEvaluationResult:
    records = collect_research_export_records(workspace)
    scorecard = _build_scorecard(records)

    report_path = output_file or _next_evaluation_report_path(workspace)
    payload_path = payload_file or report_path.with_suffix(".json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        **scorecard,
        "runs": [_build_run_summary(record) for record in records],
    }
    report_path.write_text(_render_evaluation_report(payload), encoding="utf-8")
    payload_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return ResearchEvaluationResult(report_path=report_path, payload_path=payload_path, run_count=len(records))


def _build_scorecard(records: List[Dict[str, object]]) -> Dict[str, object]:
    run_count = len(records)
    validation_pass_count = 0
    failed_validation_count = 0
    warning_run_count = 0
    total_sources = 0
    total_used_citations = 0
    status_counts: Dict[str, int] = {}
    profile_counts: Dict[str, int] = {}
    label_counts: Dict[str, int] = {}

    for record in records:
        status = str(record.get("status", ""))
        status_counts[status] = status_counts.get(status, 0) + 1
        profile = str(record.get("job_profile", ""))
        profile_counts[profile] = profile_counts.get(profile, 0) + 1
        validation = dict(record.get("validation", {}))
        if bool(validation.get("passed", False)):
            validation_pass_count += 1
        else:
            failed_validation_count += 1
        if validation.get("warnings"):
            warning_run_count += 1
        total_sources += len(list(record.get("sources", [])))
        total_used_citations += len(list(validation.get("used", [])))
        for label, value in derive_research_labels(record).items():
            if isinstance(value, bool) and value:
                label_counts[label] = label_counts.get(label, 0) + 1

    citation_pass_rate = 0.0 if run_count == 0 else validation_pass_count / run_count
    average_source_count = 0.0 if run_count == 0 else total_sources / run_count
    average_used_citation_count = 0.0 if run_count == 0 else total_used_citations / run_count
    return {
        "run_count": run_count,
        "validation_pass_count": validation_pass_count,
        "failed_validation_count": failed_validation_count,
        "warning_run_count": warning_run_count,
        "citation_pass_rate": round(citation_pass_rate, 4),
        "average_source_count": round(average_source_count, 2),
        "average_used_citation_count": round(average_used_citation_count, 2),
        "status_counts": dict(sorted(status_counts.items())),
        "profile_counts": dict(sorted(profile_counts.items())),
        "label_counts": dict(sorted(label_counts.items())),
    }


def _build_run_summary(record: Dict[str, object]) -> Dict[str, object]:
    validation = dict(record.get("validation", {}))
    return {
        "run_id": str(record.get("run_id", "")),
        "question": str(record.get("question", "")),
        "status": str(record.get("status", "")),
        "job_profile": str(record.get("job_profile", "")),
        "validation_passed": bool(validation.get("passed", False)),
        "used_citation_count": len(list(validation.get("used", []))),
        "source_count": len(list(record.get("sources", []))),
        "error_count": len(list(validation.get("errors", []))),
        "warning_count": len(list(validation.get("warnings", []))),
        "labels": derive_research_labels(record),
    }


def _render_evaluation_report(payload: Dict[str, object]) -> str:
    lines = [
        "# Research Evaluation Report",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "## Scorecard",
        "",
        f"- Run count: `{payload['run_count']}`",
        f"- Validation pass rate: `{payload['validation_pass_count']} / {payload['run_count']}`",
        f"- Failed validation runs: `{payload['failed_validation_count']}`",
        f"- Warning-bearing runs: `{payload['warning_run_count']}`",
        f"- Average source count: `{payload['average_source_count']}`",
        f"- Average used citation count: `{payload['average_used_citation_count']}`",
        "",
        "## Status Counts",
        "",
    ]
    status_counts = dict(payload.get("status_counts", {}))
    if status_counts:
        lines.extend(f"- `{status}`: `{count}`" for status, count in status_counts.items())
    else:
        lines.append("- None")
    lines.extend(["", "## Label Counts", ""])
    label_counts = dict(payload.get("label_counts", {}))
    if label_counts:
        lines.extend(f"- `{label}`: `{count}`" for label, count in label_counts.items())
    else:
        lines.append("- None")
    lines.extend(["", "## Run Breakdown", ""])
    runs = list(payload.get("runs", []))
    if not runs:
        lines.append("No research runs were available for evaluation.")
        lines.append("")
        return "\n".join(lines)
    for run in runs:
        lines.extend(
            [
                f"### {run['question']}",
                "",
                f"- Run id: `{run['run_id']}`",
                f"- Status: `{run['status']}`",
                f"- Job profile: `{run['job_profile']}`",
                f"- Validation passed: `{run['validation_passed']}`",
                f"- Source count: `{run['source_count']}`",
                f"- Used citation count: `{run['used_citation_count']}`",
                f"- Error count: `{run['error_count']}`",
                f"- Warning count: `{run['warning_count']}`",
                "",
            ]
        )
    return "\n".join(lines)


def _next_evaluation_report_path(workspace: Workspace) -> Path:
    directory = workspace.export_artifacts_dir
    directory.mkdir(parents=True, exist_ok=True)
    stem = utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace(".", "")
    return directory / f"research-eval-{stem}.md"
