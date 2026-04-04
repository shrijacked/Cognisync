from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.research import DEFAULT_RESEARCH_JOB_PROFILE, run_research_cycle
from cognisync.training_loop import improve_research_loop
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


class JobError(RuntimeError):
    pass


@dataclass(frozen=True)
class JobRunResult:
    job_manifest_path: Path
    queue_manifest_path: Path
    job_id: str
    job_type: str
    status: str


def enqueue_research_job(
    workspace: Workspace,
    question: str,
    profile_name: Optional[str] = None,
    limit: int = 5,
    mode: str = "wiki",
    slides: bool = False,
    job_profile: str = DEFAULT_RESEARCH_JOB_PROFILE,
) -> Path:
    parameters = {
        "question": question,
        "profile_name": profile_name,
        "limit": limit,
        "mode": mode,
        "slides": slides,
        "job_profile": job_profile,
    }
    return _enqueue_job(workspace, "research", question, parameters)


def enqueue_improve_research_job(
    workspace: Workspace,
    profile_name: str,
    limit: int = 5,
    provider_formats: Optional[List[str]] = None,
) -> Path:
    parameters = {
        "profile_name": profile_name,
        "limit": limit,
        "provider_formats": list(provider_formats or []),
    }
    return _enqueue_job(workspace, "improve_research", "improve-research", parameters)


def list_jobs(workspace: Workspace) -> List[Dict[str, object]]:
    jobs: List[Dict[str, object]] = []
    if not workspace.job_manifests_dir.exists():
        return jobs
    for manifest_path in sorted(workspace.job_manifests_dir.glob("*.json")):
        jobs.append(json.loads(manifest_path.read_text(encoding="utf-8")))
    jobs.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("job_id", ""))))
    return jobs


def render_jobs_list(workspace: Workspace) -> str:
    jobs = list_jobs(workspace)
    lines = [
        "# Job Queue",
        "",
        f"- Job count: `{len(jobs)}`",
    ]
    if not jobs:
        lines.extend(["", "No queued or historical jobs found."])
        return "\n".join(lines)
    status_counts: Dict[str, int] = {}
    for job in jobs:
        status = str(job.get("status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
    lines.append(f"- Status counts: `{json.dumps(dict(sorted(status_counts.items())), sort_keys=True)}`")
    lines.extend(["", "## Jobs", ""])
    for job in jobs:
        lines.append(
            "- "
            f"`{job['job_id']}` "
            f"`{job['job_type']}` "
            f"`{job['status']}` "
            f"{job.get('title', '')}"
        )
    return "\n".join(lines)


def run_next_job(workspace: Workspace) -> JobRunResult:
    queued_jobs = [job for job in list_jobs(workspace) if str(job.get("status", "")) == "queued"]
    if not queued_jobs:
        raise JobError("No queued jobs found.")
    job = queued_jobs[0]
    manifest_path = workspace.job_manifests_dir / f"{job['job_id']}.json"
    job = _update_job_manifest(
        manifest_path,
        status="running",
        started_at=utc_timestamp(),
        attempts=int(job.get("attempts", 0) or 0) + 1,
        audit_entry="Job execution started.",
    )

    try:
        result_payload = _execute_job(workspace, job)
    except Exception as error:  # pragma: no cover - exercised through CLI/tests
        job = _update_job_manifest(
            manifest_path,
            status="failed",
            finished_at=utc_timestamp(),
            error=str(error),
            audit_entry=f"Job failed: {error}",
        )
        queue_manifest_path = _write_queue_manifest(workspace)
        raise JobError(str(error)) from error

    job = _update_job_manifest(
        manifest_path,
        status="completed",
        finished_at=utc_timestamp(),
        result=result_payload,
        error=None,
        audit_entry="Job execution completed.",
    )
    queue_manifest_path = _write_queue_manifest(workspace)
    return JobRunResult(
        job_manifest_path=manifest_path,
        queue_manifest_path=queue_manifest_path,
        job_id=str(job["job_id"]),
        job_type=str(job["job_type"]),
        status=str(job["status"]),
    )


def _execute_job(workspace: Workspace, job: Dict[str, object]) -> Dict[str, object]:
    job_type = str(job.get("job_type", ""))
    parameters = dict(job.get("parameters", {}))
    if job_type == "research":
        result = run_research_cycle(
            workspace,
            question=str(parameters.get("question", "")),
            limit=int(parameters.get("limit", 5) or 5),
            profile_name=str(parameters.get("profile_name", "")) or None,
            mode=str(parameters.get("mode", "wiki")),
            slides=bool(parameters.get("slides", False)),
            job_profile=str(parameters.get("job_profile", DEFAULT_RESEARCH_JOB_PROFILE)),
        )
        return {
            "run_manifest_path": workspace.relative_path(result.run_manifest_path),
            "report_path": workspace.relative_path(result.report_path),
            "answer_path": workspace.relative_path(result.answer_path) if result.answer_path else None,
            "change_summary_path": workspace.relative_path(result.change_summary_path),
            "status": result.status,
            "warning_count": result.warning_count,
        }
    if job_type == "improve_research":
        result = improve_research_loop(
            workspace,
            profile_name=str(parameters.get("profile_name", "")),
            limit=int(parameters.get("limit", 5) or 5),
            provider_formats=[str(item) for item in list(parameters.get("provider_formats", []))],
        )
        return {
            "remediated_count": result.remediation.remediated_count,
            "remediation_manifest_paths": [
                workspace.relative_path(path) for path in result.remediation.manifest_paths
            ],
            "training_loop_manifest_path": workspace.relative_path(result.bundle.manifest_path),
            "training_loop_dir": workspace.relative_path(result.bundle.directory),
        }
    raise JobError(f"Unsupported job type: {job_type}")


def _enqueue_job(
    workspace: Workspace,
    job_type: str,
    title_seed: str,
    parameters: Dict[str, object],
) -> Path:
    workspace.job_manifests_dir.mkdir(parents=True, exist_ok=True)
    job_id = _job_id(job_type, title_seed)
    manifest_path = workspace.job_manifests_dir / f"{job_id}.json"
    payload = {
        "schema_version": 1,
        "job_id": job_id,
        "job_type": job_type,
        "title": title_seed,
        "status": "queued",
        "created_at": utc_timestamp(),
        "updated_at": utc_timestamp(),
        "attempts": 0,
        "parameters": parameters,
        "result": {},
        "error": None,
        "audit": [
            {
                "timestamp": utc_timestamp(),
                "status": "queued",
                "message": "Job created.",
            }
        ],
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_queue_manifest(workspace)
    return manifest_path


def _update_job_manifest(manifest_path: Path, audit_entry: Optional[str] = None, **updates) -> Dict[str, object]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.update(updates)
    payload["updated_at"] = utc_timestamp()
    if audit_entry:
        audit = list(payload.get("audit", []))
        audit.append(
            {
                "timestamp": utc_timestamp(),
                "status": payload.get("status", "unknown"),
                "message": audit_entry,
            }
        )
        payload["audit"] = audit
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def _write_queue_manifest(workspace: Workspace) -> Path:
    jobs = list_jobs(workspace)
    status_counts: Dict[str, int] = {}
    for job in jobs:
        status = str(job.get("status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "job_count": len(jobs),
        "queued_count": sum(1 for job in jobs if str(job.get("status", "")) == "queued"),
        "status_counts": dict(sorted(status_counts.items())),
        "jobs": [
            {
                "job_id": str(job.get("job_id", "")),
                "job_type": str(job.get("job_type", "")),
                "title": str(job.get("title", "")),
                "status": str(job.get("status", "")),
                "created_at": str(job.get("created_at", "")),
                "updated_at": str(job.get("updated_at", "")),
            }
            for job in jobs
        ],
    }
    workspace.job_queue_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.job_queue_manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return workspace.job_queue_manifest_path


def _job_id(job_type: str, title_seed: str) -> str:
    stamp = utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace(".", "")
    return f"{job_type}-{stamp}-{slugify(title_seed)[:48] or 'job'}"
