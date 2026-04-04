from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.compile_flow import run_compile_cycle
from cognisync.connectors import sync_all_connectors, sync_connector
from cognisync.linter import lint_snapshot
from cognisync.maintenance import run_maintenance_cycle
from cognisync.manifests import write_run_manifest, write_workspace_manifests
from cognisync.notifications import write_notifications_manifest
from cognisync.research import DEFAULT_RESEARCH_JOB_PROFILE, run_research_cycle
from cognisync.scanner import scan_workspace
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


@dataclass(frozen=True)
class JobWorkerResult:
    processed_count: int
    completed_count: int
    failed_count: int
    queue_manifest_path: Path


@dataclass(frozen=True)
class JobClaimResult:
    job_manifest_path: Path
    queue_manifest_path: Path
    job_id: str
    job_type: str
    worker_id: str
    lease_expires_at: str
    status: str


@dataclass(frozen=True)
class JobHeartbeatResult:
    job_manifest_path: Path
    queue_manifest_path: Path
    job_id: str
    job_type: str
    worker_id: str
    lease_expires_at: str
    status: str


def enqueue_research_job(
    workspace: Workspace,
    question: str,
    profile_name: Optional[str] = None,
    limit: int = 5,
    mode: str = "wiki",
    slides: bool = False,
    job_profile: str = DEFAULT_RESEARCH_JOB_PROFILE,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    parameters = {
        "question": question,
        "profile_name": profile_name,
        "limit": limit,
        "mode": mode,
        "slides": slides,
        "job_profile": job_profile,
    }
    return _enqueue_job(workspace, "research", question, parameters, requested_by=requested_by)


def enqueue_improve_research_job(
    workspace: Workspace,
    profile_name: str,
    limit: int = 5,
    provider_formats: Optional[List[str]] = None,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    parameters = {
        "profile_name": profile_name,
        "limit": limit,
        "provider_formats": list(provider_formats or []),
    }
    return _enqueue_job(
        workspace,
        "improve_research",
        "improve-research",
        parameters,
        requested_by=requested_by,
    )


def enqueue_compile_job(
    workspace: Workspace,
    profile_name: Optional[str] = None,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "compile",
        "compile-plan",
        {"profile_name": profile_name},
        requested_by=requested_by,
    )


def enqueue_connector_sync_job(
    workspace: Workspace,
    connector_id: str,
    force: bool = False,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "connector_sync",
        connector_id,
        {
            "connector_id": connector_id,
            "force": force,
        },
        requested_by=requested_by,
    )


def enqueue_connector_sync_all_job(
    workspace: Workspace,
    force: bool = False,
    limit: Optional[int] = None,
    scheduled_only: bool = False,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "connector_sync_all",
        "connector-sync-all",
        {
            "force": force,
            "limit": limit,
            "scheduled_only": scheduled_only,
        },
        requested_by=requested_by,
    )


def enqueue_lint_job(workspace: Workspace, requested_by: Optional[Dict[str, object]] = None) -> Path:
    return _enqueue_job(workspace, "lint", "workspace-lint", {}, requested_by=requested_by)


def enqueue_maintain_job(
    workspace: Workspace,
    max_concepts: int = 10,
    max_merges: int = 10,
    max_backlinks: int = 10,
    max_conflicts: int = 10,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "maintain",
        "graph-maintenance",
        {
            "max_concepts": max_concepts,
            "max_merges": max_merges,
            "max_backlinks": max_backlinks,
            "max_conflicts": max_conflicts,
        },
        requested_by=requested_by,
    )


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
    active_workers = sorted(
        {
            str(dict(job.get("lease", {})).get("worker_id", "")).strip()
            for job in jobs
            if _job_has_active_lease(job) and str(dict(job.get("lease", {})).get("worker_id", "")).strip()
        }
    )
    if active_workers:
        lines.append(f"- Active workers: `{json.dumps(active_workers)}`")
    oldest_queued = next((job for job in jobs if str(job.get("status", "")) == "queued"), None)
    if oldest_queued is not None:
        lines.append(f"- Oldest queued job: `{oldest_queued.get('job_id', '')}`")
    lines.extend(["", "## Jobs", ""])
    for job in jobs:
        retry_of_job_id = str(job.get("retry_of_job_id", "") or "")
        retry_suffix = f" retry-of:{retry_of_job_id}" if retry_of_job_id else ""
        lines.append(
            "- "
            f"`{job['job_id']}` "
            f"`{job['job_type']}` "
            f"`{job['status']}` "
            f"{job.get('title', '')}{retry_suffix}"
        )
    return "\n".join(lines)


def render_worker_registry(workspace: Workspace) -> str:
    payload = read_worker_registry(workspace)
    workers = list(payload.get("workers", []))
    lines = [
        "# Worker Registry",
        "",
        f"- Worker count: `{len(workers)}`",
        f"- Status counts: `{json.dumps(dict(payload.get('counts_by_status', {})), sort_keys=True)}`",
    ]
    if not workers:
        lines.extend(["", "No workers have interacted with the queue yet."])
        return "\n".join(lines)
    lines.extend(["", "## Workers", ""])
    for worker in workers:
        current_job_id = str(worker.get("current_job_id", "") or "")
        current_suffix = f" job:{current_job_id}" if current_job_id else ""
        lines.append(
            "- "
            f"`{worker.get('worker_id', '')}` "
            f"`{worker.get('status', '')}` "
            f"last-seen:{worker.get('last_seen_at', '')}{current_suffix}"
        )
    return "\n".join(lines)


def read_worker_registry(workspace: Workspace) -> Dict[str, object]:
    _write_queue_manifest(workspace)
    return _read_worker_registry(workspace)


def retry_job(
    workspace: Workspace,
    job_id: str,
    profile_name: Optional[str] = None,
    provider_formats: Optional[List[str]] = None,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    original = _read_job_by_id(workspace, job_id)
    if str(original.get("status", "")) not in {"completed", "failed"}:
        raise JobError(f"Job {job_id} is not in a terminal state and cannot be retried.")

    parameters = dict(original.get("parameters", {}))
    job_type = str(original.get("job_type", ""))
    if profile_name:
        parameters["profile_name"] = profile_name
    if provider_formats is not None and job_type == "improve_research":
        parameters["provider_formats"] = list(provider_formats)

    return _enqueue_job(
        workspace,
        job_type=job_type,
        title_seed=str(original.get("title", job_type)),
        parameters=parameters,
        retry_of_job_id=str(original.get("job_id", "")),
        requested_by=requested_by,
    )


def claim_next_job(
    workspace: Workspace,
    worker_id: str,
    lease_seconds: int = 300,
) -> JobClaimResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    if lease_seconds < 1:
        raise JobError("Lease seconds must be at least 1.")

    jobs = list_jobs(workspace)
    active_job = _find_active_job_for_worker(jobs, normalized_worker_id)
    if active_job is not None:
        raise JobError(
            f"Worker {normalized_worker_id} already holds job {active_job.get('job_id', '')}."
        )

    job = _select_claimable_job(jobs)
    if job is None:
        raise JobError("No claimable jobs found.")

    manifest_path = workspace.job_manifests_dir / f"{job['job_id']}.json"
    claim_count = _existing_claim_count(job) + 1
    lease = _build_lease_payload(
        worker_id=normalized_worker_id,
        lease_seconds=lease_seconds,
        claim_count=claim_count,
    )
    job = _update_job_manifest(
        manifest_path,
        status="claimed",
        lease=lease,
        claim_count=claim_count,
        audit_entry=(
            f"Job claimed by {normalized_worker_id} until {lease['lease_expires_at']}."
            if str(job.get("status", "")) == "queued"
            else f"Expired lease reclaimed by {normalized_worker_id} until {lease['lease_expires_at']}."
        ),
    )
    queue_manifest_path = _write_queue_manifest(workspace)
    return JobClaimResult(
        job_manifest_path=manifest_path,
        queue_manifest_path=queue_manifest_path,
        job_id=str(job["job_id"]),
        job_type=str(job["job_type"]),
        worker_id=normalized_worker_id,
        lease_expires_at=str(lease["lease_expires_at"]),
        status=str(job["status"]),
    )


def heartbeat_job(
    workspace: Workspace,
    worker_id: str,
    lease_seconds: int = 300,
) -> JobHeartbeatResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    if lease_seconds < 1:
        raise JobError("Lease seconds must be at least 1.")

    job = _find_active_job_for_worker(list_jobs(workspace), normalized_worker_id)
    if job is None:
        raise JobError(f"No active job lease found for worker {normalized_worker_id}.")

    manifest_path = workspace.job_manifests_dir / f"{job['job_id']}.json"
    current_lease = dict(job.get("lease", {}))
    heartbeat_at = utc_timestamp()
    lease = _build_lease_payload(
        worker_id=normalized_worker_id,
        lease_seconds=lease_seconds,
        claim_count=_existing_claim_count(job) or 1,
        claimed_at=str(current_lease.get("claimed_at", "")) or None,
        last_heartbeat_at=heartbeat_at,
    )
    job = _update_job_manifest(
        manifest_path,
        lease=lease,
        claim_count=lease["claim_count"],
        audit_entry=f"Lease renewed by {normalized_worker_id} until {lease['lease_expires_at']}.",
    )
    queue_manifest_path = _write_queue_manifest(workspace)
    return JobHeartbeatResult(
        job_manifest_path=manifest_path,
        queue_manifest_path=queue_manifest_path,
        job_id=str(job["job_id"]),
        job_type=str(job["job_type"]),
        worker_id=normalized_worker_id,
        lease_expires_at=str(lease["lease_expires_at"]),
        status=str(job["status"]),
    )


def run_job_worker(
    workspace: Workspace,
    max_jobs: Optional[int] = None,
    stop_on_error: bool = False,
    worker_id: str = "local-worker",
    lease_seconds: int = 300,
) -> JobWorkerResult:
    processed_count = 0
    completed_count = 0
    failed_count = 0

    while True:
        if max_jobs is not None and processed_count >= max_jobs:
            break
        if _select_claimable_job(list_jobs(workspace)) is None and _find_active_job_for_worker(
            list_jobs(workspace),
            _normalize_worker_id(worker_id),
        ) is None:
            break
        try:
            run_next_job(workspace, worker_id=worker_id, lease_seconds=lease_seconds)
            completed_count += 1
        except JobError:
            failed_count += 1
            processed_count += 1
            if stop_on_error:
                break
            continue
        processed_count += 1

    _write_queue_manifest(workspace)
    return JobWorkerResult(
        processed_count=processed_count,
        completed_count=completed_count,
        failed_count=failed_count,
        queue_manifest_path=workspace.job_queue_manifest_path,
    )
def run_next_job(
    workspace: Workspace,
    worker_id: str = "local-worker",
    lease_seconds: int = 300,
) -> JobRunResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    owned_job = _find_active_job_for_worker(list_jobs(workspace), normalized_worker_id)
    if owned_job is None:
        claim = claim_next_job(workspace, worker_id=normalized_worker_id, lease_seconds=lease_seconds)
        manifest_path = claim.job_manifest_path
        job = _read_job_by_id(workspace, claim.job_id)
    else:
        manifest_path = workspace.job_manifests_dir / f"{owned_job['job_id']}.json"
        job = owned_job

    claim_count = _existing_claim_count(job)
    lease = _build_lease_payload(
        worker_id=normalized_worker_id,
        lease_seconds=lease_seconds,
        claim_count=claim_count or 1,
        claimed_at=str(dict(job.get("lease", {})).get("claimed_at", "")) or None,
    )
    attempts = int(job.get("attempts", 0) or 0)
    started_at = str(job.get("started_at", "")) or utc_timestamp()
    audit_entry = "Job execution started."
    if str(job.get("status", "")) == "running":
        audit_entry = "Job execution resumed."
    else:
        attempts += 1

    job = _update_job_manifest(
        manifest_path,
        status="running",
        started_at=started_at,
        attempts=attempts,
        lease=lease,
        claim_count=lease["claim_count"],
        audit_entry=f"{audit_entry} Worker: {normalized_worker_id}.",
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
    if job_type == "compile":
        result = run_compile_cycle(
            workspace,
            profile_name=_optional_string(parameters.get("profile_name")),
        )
        return {
            "run_manifest_path": workspace.relative_path(result.run_manifest_path),
            "plan_path": workspace.relative_path(result.plan_path),
            "packet_path": workspace.relative_path(result.packet_path),
            "output_file": workspace.relative_path(result.output_file) if result.output_file else None,
            "issue_count": result.issue_count,
            "task_count": result.task_count,
            "ran_profile": result.ran_profile,
        }
    if job_type == "lint":
        snapshot = scan_workspace(workspace)
        workspace.write_index(snapshot)
        write_workspace_manifests(workspace, snapshot)
        issues = lint_snapshot(snapshot, workspace=workspace)
        issue_counts_by_severity: Dict[str, int] = {}
        for issue in issues:
            issue_counts_by_severity[issue.severity] = issue_counts_by_severity.get(issue.severity, 0) + 1
        run_manifest_path = write_run_manifest(
            workspace,
            "lint",
            {
                "run_label": "workspace-lint",
                "issue_count": len(issues),
                "issue_counts_by_severity": dict(sorted(issue_counts_by_severity.items())),
                "status": "completed" if not issues else "completed_with_issues",
            },
        )
        return {
            "run_manifest_path": workspace.relative_path(run_manifest_path),
            "issue_count": len(issues),
            "issue_counts_by_severity": dict(sorted(issue_counts_by_severity.items())),
            "status": "completed" if not issues else "completed_with_issues",
        }
    if job_type == "maintain":
        result = run_maintenance_cycle(
            workspace,
            max_concepts=int(parameters.get("max_concepts", 10) or 10),
            max_merges=int(parameters.get("max_merges", 10) or 10),
            max_backlinks=int(parameters.get("max_backlinks", 10) or 10),
            max_conflicts=int(parameters.get("max_conflicts", 10) or 10),
        )
        return {
            "run_manifest_path": workspace.relative_path(result.run_manifest_path),
            "change_summary_path": workspace.relative_path(result.change_summary_path),
            "remaining_review_count": result.remaining_review_count,
            "issue_count": result.issue_count,
            "accepted_concept_paths": [workspace.relative_path(path) for path in result.accepted_concept_paths],
            "resolved_merge_keys": list(result.resolved_merge_keys),
            "applied_backlink_targets": list(result.applied_backlink_targets),
            "filed_conflict_keys": list(result.filed_conflict_keys),
        }
    if job_type == "connector_sync":
        requested_by = dict(job.get("requested_by", {})) if isinstance(job.get("requested_by", {}), dict) else None
        result = sync_connector(
            workspace,
            connector_id=str(parameters.get("connector_id", "")),
            force=bool(parameters.get("force", False)),
            actor=requested_by,
        )
        return {
            "run_manifest_path": workspace.relative_path(result.run_manifest_path),
            "change_summary_path": workspace.relative_path(result.change_summary_path),
            "connector_id": result.connector_id,
            "connector_kind": result.connector_kind,
            "synced_count": result.synced_count,
            "registry_path": workspace.relative_path(result.registry_path),
            "result_paths": [workspace.relative_path(path) for path in result.result_paths],
        }
    if job_type == "connector_sync_all":
        requested_by = dict(job.get("requested_by", {})) if isinstance(job.get("requested_by", {}), dict) else None
        result = sync_all_connectors(
            workspace,
            force=bool(parameters.get("force", False)),
            limit=int(parameters["limit"]) if parameters.get("limit") is not None else None,
            scheduled_only=bool(parameters.get("scheduled_only", False)),
            actor=requested_by,
        )
        return {
            "run_manifest_path": workspace.relative_path(result.run_manifest_path),
            "registry_path": workspace.relative_path(result.registry_path),
            "connector_count": result.connector_count,
            "synced_connector_count": result.synced_connector_count,
            "total_result_count": result.total_result_count,
            "scheduled_only": bool(parameters.get("scheduled_only", False)),
            "connector_run_manifest_paths": [
                workspace.relative_path(item.run_manifest_path) for item in result.connector_results
            ],
            "connector_change_summary_paths": [
                workspace.relative_path(item.change_summary_path) for item in result.connector_results
            ],
        }
    if job_type == "research":
        result = run_research_cycle(
            workspace,
            question=str(parameters.get("question", "")),
            limit=int(parameters.get("limit", 5) or 5),
            profile_name=_optional_string(parameters.get("profile_name")),
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
    retry_of_job_id: Optional[str] = None,
    requested_by: Optional[Dict[str, object]] = None,
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
        "claim_count": 0,
        "lease": {},
        "parameters": parameters,
        "requested_by": _serialize_actor(requested_by),
        "result": {},
        "error": None,
        "retry_of_job_id": retry_of_job_id,
        "audit": [
            {
                "timestamp": utc_timestamp(),
                "status": "queued",
                "message": "Job created." if not retry_of_job_id else f"Job re-queued from {retry_of_job_id}.",
            }
        ],
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_queue_manifest(workspace)
    return manifest_path


def _serialize_actor(actor: Optional[Dict[str, object]]) -> Optional[Dict[str, str]]:
    if actor is None:
        return None
    return {
        "principal_id": str(actor.get("principal_id", "")),
        "display_name": str(actor.get("display_name", "")),
        "role": str(actor.get("role", "")),
        "status": str(actor.get("status", "")),
    }


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
    oldest_queued_job_id = ""
    oldest_claimable_job_id = ""
    latest_updated_at = ""
    active_worker_ids: List[str] = []
    for job in jobs:
        status = str(job.get("status", "unknown"))
        status_counts[status] = status_counts.get(status, 0) + 1
        updated_at = str(job.get("updated_at", ""))
        if updated_at and updated_at > latest_updated_at:
            latest_updated_at = updated_at
        if not oldest_queued_job_id and status == "queued":
            oldest_queued_job_id = str(job.get("job_id", ""))
        if not oldest_claimable_job_id and _job_is_claimable(job):
            oldest_claimable_job_id = str(job.get("job_id", ""))
        if _job_has_active_lease(job):
            worker_id = str(dict(job.get("lease", {})).get("worker_id", "")).strip()
            if worker_id and worker_id not in active_worker_ids:
                active_worker_ids.append(worker_id)
    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "job_count": len(jobs),
        "queued_count": sum(1 for job in jobs if str(job.get("status", "")) == "queued"),
        "claimed_count": sum(1 for job in jobs if str(job.get("status", "")) == "claimed"),
        "running_count": sum(1 for job in jobs if str(job.get("status", "")) == "running"),
        "oldest_queued_job_id": oldest_queued_job_id,
        "oldest_claimable_job_id": oldest_claimable_job_id,
        "latest_updated_at": latest_updated_at,
        "active_worker_ids": sorted(active_worker_ids),
        "status_counts": dict(sorted(status_counts.items())),
        "jobs": [
            {
                "job_id": str(job.get("job_id", "")),
                "job_type": str(job.get("job_type", "")),
                "title": str(job.get("title", "")),
                "status": str(job.get("status", "")),
                "created_at": str(job.get("created_at", "")),
                "updated_at": str(job.get("updated_at", "")),
                "worker_id": str(dict(job.get("lease", {})).get("worker_id", "")),
                "lease_expires_at": str(dict(job.get("lease", {})).get("lease_expires_at", "")),
                "last_heartbeat_at": str(dict(job.get("lease", {})).get("last_heartbeat_at", "")),
                "retry_of_job_id": str(job.get("retry_of_job_id", "") or ""),
            }
            for job in jobs
        ],
    }
    workspace.job_queue_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.job_queue_manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_worker_registry(workspace, jobs)
    write_notifications_manifest(workspace)
    return workspace.job_queue_manifest_path


def _job_id(job_type: str, title_seed: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{job_type}-{stamp}-{slugify(title_seed)[:48] or 'job'}"


def _read_job_by_id(workspace: Workspace, job_id: str) -> Dict[str, object]:
    normalized = job_id.strip()
    if not normalized:
        raise JobError("A job id is required.")
    manifest_path = workspace.job_manifests_dir / f"{normalized}.json"
    if not manifest_path.exists():
        raise JobError(f"Could not find a job manifest for {normalized}.")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _optional_string(value: object) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.lower() == "none":
        return None
    return normalized


def _normalize_worker_id(worker_id: str) -> str:
    normalized = worker_id.strip()
    if not normalized:
        raise JobError("A worker id is required.")
    return normalized


def _existing_claim_count(job: Dict[str, object]) -> int:
    lease = dict(job.get("lease", {}))
    return int(lease.get("claim_count", job.get("claim_count", 0)) or 0)


def _build_lease_payload(
    worker_id: str,
    lease_seconds: int,
    claim_count: int,
    claimed_at: Optional[str] = None,
    last_heartbeat_at: Optional[str] = None,
) -> Dict[str, object]:
    claimed_at = claimed_at or utc_timestamp()
    expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
    ).replace(microsecond=0).isoformat()
    payload = {
        "worker_id": worker_id,
        "claimed_at": claimed_at,
        "lease_expires_at": expires_at,
        "lease_seconds": lease_seconds,
        "claim_count": claim_count,
    }
    if last_heartbeat_at:
        payload["last_heartbeat_at"] = last_heartbeat_at
    return payload


def _select_claimable_job(jobs: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
    for job in jobs:
        if _job_is_claimable(job):
            return job
    return None


def _find_active_job_for_worker(jobs: List[Dict[str, object]], worker_id: str) -> Optional[Dict[str, object]]:
    for job in jobs:
        if str(job.get("status", "")) not in {"claimed", "running"}:
            continue
        lease = dict(job.get("lease", {}))
        if str(lease.get("worker_id", "")) != worker_id:
            continue
        if _job_has_active_lease(job):
            return job
    return None


def _job_is_claimable(job: Dict[str, object]) -> bool:
    status = str(job.get("status", ""))
    if status == "queued":
        return True
    if status in {"claimed", "running"} and not _job_has_active_lease(job):
        return True
    return False


def _job_has_active_lease(job: Dict[str, object]) -> bool:
    status = str(job.get("status", ""))
    if status not in {"claimed", "running"}:
        return False
    lease = dict(job.get("lease", {}))
    expires_at = str(lease.get("lease_expires_at", "")).strip()
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > datetime.now(timezone.utc)
    except ValueError:
        return False


def _write_worker_registry(workspace: Workspace, jobs: Optional[List[Dict[str, object]]] = None) -> None:
    jobs = jobs if jobs is not None else list_jobs(workspace)
    workers_by_id: Dict[str, Dict[str, object]] = {}

    for job in jobs:
        lease = dict(job.get("lease", {}))
        worker_id = str(lease.get("worker_id", "")).strip()
        if not worker_id:
            continue
        worker = workers_by_id.setdefault(
            worker_id,
            {
                "worker_id": worker_id,
                "status": "idle",
                "current_job_id": "",
                "current_job_type": "",
                "lease_expires_at": "",
                "claim_count": 0,
                "last_seen_at": "",
            },
        )
        last_seen_at = (
            str(lease.get("last_heartbeat_at", ""))
            or str(job.get("updated_at", ""))
            or str(lease.get("claimed_at", ""))
        )
        if last_seen_at > str(worker.get("last_seen_at", "")):
            worker["last_seen_at"] = last_seen_at
        claim_count = int(lease.get("claim_count", job.get("claim_count", 0)) or 0)
        if claim_count > int(worker.get("claim_count", 0) or 0):
            worker["claim_count"] = claim_count
        if _job_has_active_lease(job):
            worker["status"] = str(job.get("status", "claimed"))
            worker["current_job_id"] = str(job.get("job_id", ""))
            worker["current_job_type"] = str(job.get("job_type", ""))
            worker["lease_expires_at"] = str(lease.get("lease_expires_at", ""))

    workers = sorted(workers_by_id.values(), key=lambda item: str(item.get("worker_id", "")))
    counts_by_status: Dict[str, int] = {}
    for worker in workers:
        status = str(worker.get("status", "unknown"))
        counts_by_status[status] = counts_by_status.get(status, 0) + 1

    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "worker_count": len(workers),
        "counts_by_status": dict(sorted(counts_by_status.items())),
        "workers": workers,
    }
    workspace.worker_registry_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.worker_registry_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _read_worker_registry(workspace: Workspace) -> Dict[str, object]:
    if not workspace.worker_registry_path.exists():
        return {
            "schema_version": 1,
            "generated_at": utc_timestamp(),
            "worker_count": 0,
            "counts_by_status": {},
            "workers": [],
        }
    return json.loads(workspace.worker_registry_path.read_text(encoding="utf-8"))
