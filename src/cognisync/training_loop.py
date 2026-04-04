from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.evaluation import evaluate_research_runs, export_feedback_bundle
from cognisync.exports import export_correction_bundle, export_finetune_bundle
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


@dataclass(frozen=True)
class TrainingLoopBundleResult:
    directory: Path
    manifest_path: Path
    evaluation_report_path: Path
    evaluation_payload_path: Path
    feedback_manifest_path: Path
    correction_manifest_path: Path
    finetune_manifest_path: Path


def export_training_loop_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
    provider_formats: Optional[List[str]] = None,
) -> TrainingLoopBundleResult:
    destination = output_dir or _next_training_loop_bundle_dir(workspace)
    destination.mkdir(parents=True, exist_ok=True)

    evaluation_dir = destination / "evaluation"
    feedback_dir = destination / "feedback"
    corrections_dir = destination / "corrections"
    finetune_dir = destination / "finetune"

    evaluation_dir.mkdir(parents=True, exist_ok=True)
    feedback_dir.mkdir(parents=True, exist_ok=True)
    corrections_dir.mkdir(parents=True, exist_ok=True)
    finetune_dir.mkdir(parents=True, exist_ok=True)

    evaluation_report_path = evaluation_dir / "research-eval.md"
    evaluation_payload_path = evaluation_dir / "research-eval.json"
    evaluation_result = evaluate_research_runs(
        workspace,
        output_file=evaluation_report_path,
        payload_file=evaluation_payload_path,
    )
    feedback_result = export_feedback_bundle(workspace, output_dir=feedback_dir)
    correction_result = export_correction_bundle(workspace, output_dir=corrections_dir)
    finetune_result = export_finetune_bundle(
        workspace,
        output_dir=finetune_dir,
        provider_formats=provider_formats,
    )

    feedback_manifest = json.loads(feedback_result.manifest_path.read_text(encoding="utf-8"))
    correction_manifest = json.loads(correction_result.manifest_path.read_text(encoding="utf-8"))
    finetune_manifest = json.loads(finetune_result.manifest_path.read_text(encoding="utf-8"))

    manifest_payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "bundle_type": "training-loop-bundle",
        "evaluation": {
            "report_file": evaluation_report_path.relative_to(destination).as_posix(),
            "payload_file": evaluation_payload_path.relative_to(destination).as_posix(),
            "run_count": evaluation_result.run_count,
        },
        "feedback": {
            "dataset_file": feedback_result.dataset_path.relative_to(destination).as_posix(),
            "manifest_file": feedback_result.manifest_path.relative_to(destination).as_posix(),
            "record_count": feedback_result.record_count,
            "target_counts": dict(feedback_manifest.get("target_counts", {})),
        },
        "corrections": {
            "dataset_file": correction_result.dataset_path.relative_to(destination).as_posix(),
            "manifest_file": correction_result.manifest_path.relative_to(destination).as_posix(),
            "record_count": correction_result.record_count,
            "status_counts": dict(correction_manifest.get("status_counts", {})),
            "target_counts": dict(correction_manifest.get("target_counts", {})),
        },
        "finetune": {
            "supervised_file": finetune_result.supervised_path.relative_to(destination).as_posix(),
            "retrieval_file": finetune_result.retrieval_path.relative_to(destination).as_posix(),
            "manifest_file": finetune_result.manifest_path.relative_to(destination).as_posix(),
            "supervised_count": finetune_result.supervised_count,
            "retrieval_count": finetune_result.retrieval_count,
            "supervised_example_types": dict(finetune_manifest.get("supervised_example_types", {})),
            "retrieval_example_types": dict(finetune_manifest.get("retrieval_example_types", {})),
            "provider_exports": {
                name: path.relative_to(destination).as_posix()
                for name, path in sorted(finetune_result.provider_exports.items())
            },
        },
    }

    manifest_path = destination / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    return TrainingLoopBundleResult(
        directory=destination,
        manifest_path=manifest_path,
        evaluation_report_path=evaluation_report_path,
        evaluation_payload_path=evaluation_payload_path,
        feedback_manifest_path=feedback_result.manifest_path,
        correction_manifest_path=correction_result.manifest_path,
        finetune_manifest_path=finetune_result.manifest_path,
    )


def _next_training_loop_bundle_dir(workspace: Workspace) -> Path:
    stem = utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace("T", "T")
    directory = workspace.export_artifacts_dir / f"training-loop-bundle-{stem}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
