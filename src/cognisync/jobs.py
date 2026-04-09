from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.access import DEFAULT_LOCAL_OPERATOR_ID
from cognisync.change_summaries import capture_change_state, write_change_summary
from cognisync.compile_flow import run_compile_cycle
from cognisync.connectors import sync_all_connectors, sync_connector
from cognisync.ingest import ingest_repo, ingest_sitemap, ingest_url
from cognisync.knowledge_surfaces import append_workspace_log
from cognisync.linter import lint_snapshot
from cognisync.maintenance import run_maintenance_cycle
from cognisync.manifests import write_run_manifest, write_workspace_manifests
from cognisync.notifications import write_notifications_manifest
from cognisync.research import DEFAULT_RESEARCH_JOB_PROFILE, run_research_cycle
from cognisync.scanner import scan_workspace
from cognisync.sharing import pull_attached_remote
from cognisync.sync import export_sync_bundle, import_sync_bundle_archive
from cognisync.training_loop import improve_research_loop
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


class JobError(RuntimeError):
    pass


WORKER_CAPABILITY_BY_JOB_TYPE = {
    "research": "research",
    "improve_research": "research",
    "compile": "workspace",
    "lint": "workspace",
    "maintain": "workspace",
    "connector_sync": "connector",
    "connector_sync_all": "connector",
    "sync_export": "sync",
    "remote_sync_pull": "sync",
    "ingest_url": "ingest",
    "ingest_repo": "ingest",
    "ingest_sitemap": "ingest",
}


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


@dataclass(frozen=True)
class JobDispatchResult:
    job_manifest_path: Path
    queue_manifest_path: Path
    job_id: str
    job_type: str
    worker_id: str
    lease_expires_at: str
    status: str
    parameters: Dict[str, object]
    requested_by: Optional[Dict[str, str]]
    worker_capability: str


@dataclass(frozen=True)
class WorkerReleaseResult:
    worker_id: str
    session: Optional[Dict[str, object]]
    requeued_job_ids: List[str]
    queue_manifest_path: Path


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


def enqueue_sync_export_job(
    workspace: Workspace,
    peer_ref: Optional[str] = None,
    output_dir: Optional[str] = None,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "sync_export",
        peer_ref or "workspace-sync-export",
        {
            "peer_ref": peer_ref,
            "output_dir": output_dir,
        },
        requested_by=requested_by,
    )


def enqueue_remote_sync_pull_job(
    workspace: Workspace,
    remote_ref: str,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "remote_sync_pull",
        remote_ref,
        {
            "remote_ref": remote_ref,
        },
        requested_by=requested_by,
    )


def enqueue_ingest_url_job(
    workspace: Workspace,
    url: str,
    name: Optional[str] = None,
    force: bool = False,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "ingest_url",
        name or url,
        {
            "url": url,
            "name": name,
            "force": force,
        },
        requested_by=requested_by,
    )


def enqueue_ingest_repo_job(
    workspace: Workspace,
    source: str,
    name: Optional[str] = None,
    force: bool = False,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "ingest_repo",
        name or source,
        {
            "source": source,
            "name": name,
            "force": force,
        },
        requested_by=requested_by,
    )


def enqueue_ingest_sitemap_job(
    workspace: Workspace,
    source: str,
    force: bool = False,
    limit: Optional[int] = None,
    requested_by: Optional[Dict[str, object]] = None,
) -> Path:
    return _enqueue_job(
        workspace,
        "ingest_sitemap",
        source,
        {
            "source": source,
            "force": force,
            "limit": limit,
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
        capability = str(job.get("worker_capability", "")).strip()
        capability_suffix = f" capability:{capability}" if capability else ""
        lines.append(
            "- "
            f"`{job['job_id']}` "
            f"`{job['job_type']}` "
            f"`{job['status']}` "
            f"{job.get('title', '')}{retry_suffix}{capability_suffix}"
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
        capabilities = [str(item) for item in list(worker.get("declared_capabilities", []))]
        capability_suffix = f" capabilities:{json.dumps(capabilities)}" if capabilities else ""
        lines.append(
            "- "
            f"`{worker.get('worker_id', '')}` "
            f"`{worker.get('status', '')}` "
            f"last-seen:{worker.get('last_seen_at', '')}{current_suffix}{capability_suffix}"
        )
    return "\n".join(lines)


def read_worker_registry(workspace: Workspace) -> Dict[str, object]:
    _write_queue_manifest(workspace)
    return _read_worker_registry(workspace)


def register_worker_session(
    workspace: Workspace,
    worker_id: str,
    status: str = "idle",
    worker_capabilities: Optional[List[str]] = None,
    ttl_seconds: int = 120,
    current_job_id: str = "",
    current_job_type: str = "",
    origin: str = "remote-http",
    workspace_root: Optional[str] = None,
) -> Dict[str, object]:
    normalized_worker_id = _normalize_worker_id(worker_id)
    if ttl_seconds < 1:
        raise JobError("Worker session ttl_seconds must be at least 1.")
    payload = _read_worker_sessions(workspace)
    sessions = {str(item.get("worker_id", "")): dict(item) for item in list(payload.get("sessions", []))}
    existing = sessions.get(normalized_worker_id, {})
    now = utc_timestamp()
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).replace(microsecond=0).isoformat()
    sessions[normalized_worker_id] = {
        "worker_id": normalized_worker_id,
        "status": str(status or "idle").strip().lower() or "idle",
        "origin": str(origin or "remote-http").strip() or "remote-http",
        "registered_at": str(existing.get("registered_at", now)) or now,
        "updated_at": now,
        "last_seen_at": now,
        "expires_at": expires_at,
        "released_at": "",
        "release_reason": "",
        "current_job_id": str(current_job_id or "").strip(),
        "current_job_type": str(current_job_type or "").strip(),
        "declared_capabilities": _normalize_worker_capabilities(
            worker_capabilities if worker_capabilities is not None else list(existing.get("declared_capabilities", []))
        ),
        "workspace_root": str(workspace_root or existing.get("workspace_root", "")).strip(),
    }
    _write_worker_sessions(workspace, {"sessions": list(sessions.values())})
    _write_worker_registry(workspace)
    return sessions[normalized_worker_id]


def heartbeat_worker_session(
    workspace: Workspace,
    worker_id: str,
    status: Optional[str] = None,
    worker_capabilities: Optional[List[str]] = None,
    ttl_seconds: int = 120,
    current_job_id: Optional[str] = None,
    current_job_type: Optional[str] = None,
    workspace_root: Optional[str] = None,
) -> Dict[str, object]:
    normalized_worker_id = _normalize_worker_id(worker_id)
    payload = _read_worker_sessions(workspace)
    sessions = {str(item.get("worker_id", "")): dict(item) for item in list(payload.get("sessions", []))}
    existing = sessions.get(normalized_worker_id)
    if existing is None:
        return register_worker_session(
            workspace,
            worker_id=normalized_worker_id,
            status=status or "idle",
            worker_capabilities=worker_capabilities,
            ttl_seconds=ttl_seconds,
            current_job_id=current_job_id or "",
            current_job_type=current_job_type or "",
            workspace_root=workspace_root,
        )
    now = utc_timestamp()
    existing["status"] = str(status or existing.get("status", "idle")).strip().lower() or "idle"
    existing["updated_at"] = now
    existing["last_seen_at"] = now
    existing["expires_at"] = (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(ttl_seconds)))).replace(
        microsecond=0
    ).isoformat()
    existing["released_at"] = ""
    existing["release_reason"] = ""
    if current_job_id is not None:
        existing["current_job_id"] = str(current_job_id).strip()
    if current_job_type is not None:
        existing["current_job_type"] = str(current_job_type).strip()
    if worker_capabilities is not None:
        existing["declared_capabilities"] = _normalize_worker_capabilities(worker_capabilities)
    if workspace_root is not None:
        existing["workspace_root"] = str(workspace_root).strip()
    sessions[normalized_worker_id] = existing
    _write_worker_sessions(workspace, {"sessions": list(sessions.values())})
    _write_worker_registry(workspace)
    return existing


def release_worker_session(
    workspace: Workspace,
    worker_id: str,
    reason: str = "stopped",
) -> Dict[str, object]:
    normalized_worker_id = _normalize_worker_id(worker_id)
    payload = _read_worker_sessions(workspace)
    sessions = {str(item.get("worker_id", "")): dict(item) for item in list(payload.get("sessions", []))}
    existing = sessions.get(normalized_worker_id)
    if existing is None:
        raise JobError(f"Worker session {normalized_worker_id} does not exist.")
    now = utc_timestamp()
    existing["status"] = "released"
    existing["updated_at"] = now
    existing["last_seen_at"] = now
    existing["expires_at"] = ""
    existing["released_at"] = now
    existing["release_reason"] = str(reason).strip() or "stopped"
    existing["current_job_id"] = ""
    existing["current_job_type"] = ""
    sessions[normalized_worker_id] = existing
    _write_worker_sessions(workspace, {"sessions": list(sessions.values())})
    _write_worker_registry(workspace)
    return existing


def release_worker(
    workspace: Workspace,
    worker_id: str,
    reason: str = "stopped",
    requeue_active_jobs: bool = False,
) -> WorkerReleaseResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    session: Optional[Dict[str, object]]
    try:
        session = release_worker_session(
            workspace,
            worker_id=normalized_worker_id,
            reason=reason,
        )
    except JobError:
        session = None

    requeued_job_ids: List[str] = []
    if requeue_active_jobs:
        requeued_job_ids = _requeue_active_jobs_for_worker(
            workspace,
            worker_id=normalized_worker_id,
            reason=reason,
        )

    if session is None and not requeued_job_ids:
        raise JobError(f"Worker session {normalized_worker_id} does not exist.")

    queue_manifest_path = _write_queue_manifest(workspace)
    return WorkerReleaseResult(
        worker_id=normalized_worker_id,
        session=session,
        requeued_job_ids=requeued_job_ids,
        queue_manifest_path=queue_manifest_path,
    )


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
    worker_capabilities: Optional[List[str]] = None,
) -> JobClaimResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    normalized_worker_capabilities = _normalize_worker_capabilities(worker_capabilities)
    if lease_seconds < 1:
        raise JobError("Lease seconds must be at least 1.")

    jobs = list_jobs(workspace)
    active_job = _find_active_job_for_worker(jobs, normalized_worker_id)
    if active_job is not None:
        raise JobError(
            f"Worker {normalized_worker_id} already holds job {active_job.get('job_id', '')}."
        )

    job = _select_claimable_job(jobs, worker_capabilities=normalized_worker_capabilities)
    if job is None:
        raise JobError("No claimable jobs found.")

    manifest_path = workspace.job_manifests_dir / f"{job['job_id']}.json"
    claim_count = _existing_claim_count(job) + 1
    lease = _build_lease_payload(
        worker_id=normalized_worker_id,
        lease_seconds=lease_seconds,
        claim_count=claim_count,
        worker_capabilities=normalized_worker_capabilities,
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
    worker_capabilities: Optional[List[str]] = None,
) -> JobHeartbeatResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    normalized_worker_capabilities = _normalize_worker_capabilities(worker_capabilities)
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
        worker_capabilities=normalized_worker_capabilities or list(current_lease.get("worker_capabilities", [])),
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
    worker_capabilities: Optional[List[str]] = None,
) -> JobWorkerResult:
    processed_count = 0
    completed_count = 0
    failed_count = 0
    normalized_worker_capabilities = _normalize_worker_capabilities(worker_capabilities)

    while True:
        if max_jobs is not None and processed_count >= max_jobs:
            break
        if _select_claimable_job(list_jobs(workspace), worker_capabilities=normalized_worker_capabilities) is None and _find_active_job_for_worker(
            list_jobs(workspace),
            _normalize_worker_id(worker_id),
        ) is None:
            break
        try:
            run_next_job(
                workspace,
                worker_id=worker_id,
                lease_seconds=lease_seconds,
                worker_capabilities=normalized_worker_capabilities,
            )
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


def dispatch_next_job(
    workspace: Workspace,
    worker_id: str = "local-worker",
    lease_seconds: int = 300,
    worker_capabilities: Optional[List[str]] = None,
) -> JobDispatchResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    normalized_worker_capabilities = _normalize_worker_capabilities(worker_capabilities)
    owned_job = _find_active_job_for_worker(list_jobs(workspace), normalized_worker_id)
    if owned_job is None:
        claim = claim_next_job(
            workspace,
            worker_id=normalized_worker_id,
            lease_seconds=lease_seconds,
            worker_capabilities=normalized_worker_capabilities,
        )
        manifest_path = claim.job_manifest_path
        job = _read_job_by_id(workspace, claim.job_id)
        lease_expires_at = claim.lease_expires_at
    else:
        manifest_path = workspace.job_manifests_dir / f"{owned_job['job_id']}.json"
        job = owned_job
        lease_expires_at = str(dict(job.get("lease", {})).get("lease_expires_at", ""))

    claim_count = _existing_claim_count(job)
    lease = _build_lease_payload(
        worker_id=normalized_worker_id,
        lease_seconds=lease_seconds,
        claim_count=claim_count or 1,
        claimed_at=str(dict(job.get("lease", {})).get("claimed_at", "")) or None,
        worker_capabilities=normalized_worker_capabilities or list(dict(job.get("lease", {})).get("worker_capabilities", [])),
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
    queue_manifest_path = _write_queue_manifest(workspace)
    return JobDispatchResult(
        job_manifest_path=manifest_path,
        queue_manifest_path=queue_manifest_path,
        job_id=str(job["job_id"]),
        job_type=str(job["job_type"]),
        worker_id=normalized_worker_id,
        lease_expires_at=str(lease["lease_expires_at"]),
        status=str(job["status"]),
        parameters=dict(job.get("parameters", {})),
        requested_by=dict(job.get("requested_by", {})) if isinstance(job.get("requested_by", {}), dict) else None,
        worker_capability=str(job.get("worker_capability", "")),
    )


def complete_dispatched_job(
    workspace: Workspace,
    job_id: str,
    worker_id: str,
    result_payload: Dict[str, object],
    sync_archive_bytes: Optional[bytes] = None,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> JobRunResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    manifest_path = workspace.job_manifests_dir / f"{job_id}.json"
    job = _read_job_by_id(workspace, job_id)
    _require_job_worker(job, normalized_worker_id)

    final_result = dict(result_payload)
    if sync_archive_bytes:
        import_result = import_sync_bundle_archive(
            workspace,
            archive_bytes=sync_archive_bytes,
            actor_id=actor_id,
        )
        final_result["imported_sync_manifest_path"] = workspace.relative_path(import_result.manifest_path)
        final_result["imported_sync_event_path"] = workspace.relative_path(import_result.event_manifest_path)
        final_result["imported_sync_history_path"] = workspace.relative_path(import_result.history_manifest_path)
        snapshot = workspace.refresh_index()
        write_workspace_manifests(workspace, snapshot)

    job = _update_job_manifest(
        manifest_path,
        status="completed",
        finished_at=utc_timestamp(),
        result=final_result,
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


def fail_dispatched_job(
    workspace: Workspace,
    job_id: str,
    worker_id: str,
    error_message: str,
) -> JobRunResult:
    normalized_worker_id = _normalize_worker_id(worker_id)
    manifest_path = workspace.job_manifests_dir / f"{job_id}.json"
    job = _read_job_by_id(workspace, job_id)
    _require_job_worker(job, normalized_worker_id)
    job = _update_job_manifest(
        manifest_path,
        status="failed",
        finished_at=utc_timestamp(),
        error=error_message,
        audit_entry=f"Job failed: {error_message}",
    )
    queue_manifest_path = _write_queue_manifest(workspace)
    return JobRunResult(
        job_manifest_path=manifest_path,
        queue_manifest_path=queue_manifest_path,
        job_id=str(job["job_id"]),
        job_type=str(job["job_type"]),
        status=str(job["status"]),
    )


def execute_job_payload(
    workspace: Workspace,
    job_type: str,
    parameters: Dict[str, object],
    requested_by: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    result = _execute_job(
        workspace,
        {
            "job_type": job_type,
            "parameters": dict(parameters),
            "requested_by": dict(requested_by or {}),
        },
    )
    return _with_artifact_paths(result)


def run_next_job(
    workspace: Workspace,
    worker_id: str = "local-worker",
    lease_seconds: int = 300,
    worker_capabilities: Optional[List[str]] = None,
) -> JobRunResult:
    dispatch = dispatch_next_job(
        workspace,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
        worker_capabilities=worker_capabilities,
    )
    try:
        result_payload = execute_job_payload(
            workspace,
            job_type=dispatch.job_type,
            parameters=dispatch.parameters,
            requested_by=dispatch.requested_by,
        )
    except Exception as error:  # pragma: no cover - exercised through CLI/tests
        fail_dispatched_job(
            workspace,
            job_id=dispatch.job_id,
            worker_id=dispatch.worker_id,
            error_message=str(error),
        )
        raise JobError(str(error)) from error

    return complete_dispatched_job(
        workspace,
        job_id=dispatch.job_id,
        worker_id=dispatch.worker_id,
        result_payload=result_payload,
    )


def _execute_job(workspace: Workspace, job: Dict[str, object]) -> Dict[str, object]:
    job_type = str(job.get("job_type", ""))
    parameters = dict(job.get("parameters", {}))
    if job_type == "ingest_url":
        previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
        result = ingest_url(
            workspace,
            url=str(parameters.get("url", "")),
            name=_optional_string(parameters.get("name")),
            force=bool(parameters.get("force", False)),
        )
        snapshot = workspace.refresh_index()
        write_workspace_manifests(workspace, snapshot)
        change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
        run_manifest_path = write_run_manifest(
            workspace,
            "ingest",
            {
                "run_label": f"ingest-url-{result.path.stem}",
                "status": "completed",
                "source_kind": "url",
                "result_paths": [workspace.relative_path(result.path)],
                "change_summary_path": workspace.relative_path(change_summary.path),
            },
        )
        append_workspace_log(
            workspace,
            operation="ingest",
            title=f"Executed queued URL ingest for {result.path.name}",
            details=[f"Queued ingest captured URL source {parameters.get('url', '')}."],
            related_paths=[workspace.relative_path(result.path), workspace.relative_path(change_summary.path)],
        )
        return {
            "run_manifest_path": workspace.relative_path(run_manifest_path),
            "change_summary_path": workspace.relative_path(change_summary.path),
            "result_paths": [workspace.relative_path(result.path)],
            "source_kind": "url",
        }
    if job_type == "ingest_repo":
        previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
        result = ingest_repo(
            workspace,
            repo_path=str(parameters.get("source", "")),
            name=_optional_string(parameters.get("name")),
            force=bool(parameters.get("force", False)),
        )
        snapshot = workspace.refresh_index()
        write_workspace_manifests(workspace, snapshot)
        change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
        run_manifest_path = write_run_manifest(
            workspace,
            "ingest",
            {
                "run_label": f"ingest-repo-{result.path.stem}",
                "status": "completed",
                "source_kind": "repo",
                "result_paths": [workspace.relative_path(result.path)],
                "change_summary_path": workspace.relative_path(change_summary.path),
            },
        )
        append_workspace_log(
            workspace,
            operation="ingest",
            title=f"Executed queued repo ingest for {result.path.name}",
            details=[f"Queued ingest captured repository metadata from {parameters.get('source', '')}."],
            related_paths=[workspace.relative_path(result.path), workspace.relative_path(change_summary.path)],
        )
        return {
            "run_manifest_path": workspace.relative_path(run_manifest_path),
            "change_summary_path": workspace.relative_path(change_summary.path),
            "result_paths": [workspace.relative_path(result.path)],
            "source_kind": "repo",
        }
    if job_type == "ingest_sitemap":
        previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
        results = ingest_sitemap(
            workspace,
            source=str(parameters.get("source", "")),
            force=bool(parameters.get("force", False)),
            limit=int(parameters["limit"]) if parameters.get("limit") is not None else None,
        )
        snapshot = workspace.refresh_index()
        write_workspace_manifests(workspace, snapshot)
        change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
        run_manifest_path = write_run_manifest(
            workspace,
            "ingest",
            {
                "run_label": f"ingest-sitemap-{slugify(str(parameters.get('source', '')))[:32] or 'batch'}",
                "status": "completed",
                "source_kind": "sitemap",
                "result_paths": [workspace.relative_path(result.path) for result in results],
                "result_count": len(results),
                "change_summary_path": workspace.relative_path(change_summary.path),
            },
        )
        append_workspace_log(
            workspace,
            operation="ingest",
            title="Executed queued sitemap ingest",
            details=[f"Queued ingest imported {len(results)} URL source(s) from sitemap {parameters.get('source', '')}."],
            related_paths=[workspace.relative_path(change_summary.path)] + [workspace.relative_path(result.path) for result in results[:5]],
        )
        return {
            "run_manifest_path": workspace.relative_path(run_manifest_path),
            "change_summary_path": workspace.relative_path(change_summary.path),
            "result_paths": [workspace.relative_path(result.path) for result in results],
            "result_count": len(results),
            "source_kind": "sitemap",
        }
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
        snapshot = workspace.refresh_index()
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
        append_workspace_log(
            workspace,
            operation="lint",
            title="Executed queued lint job",
            details=[f"Queued lint found {len(issues)} issue(s)."],
            related_paths=[workspace.relative_path(run_manifest_path)],
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
    if job_type == "sync_export":
        requested_by = dict(job.get("requested_by", {})) if isinstance(job.get("requested_by", {}), dict) else None
        actor_id = str((requested_by or {}).get("principal_id", "")).strip() or "local-operator"
        output_dir = _optional_string(parameters.get("output_dir"))
        result = export_sync_bundle(
            workspace,
            output_dir=Path(output_dir).expanduser().resolve() if output_dir else None,
            actor_id=actor_id,
            peer_ref=_optional_string(parameters.get("peer_ref")),
        )
        return {
            "directory": workspace.relative_path(result.directory),
            "manifest_path": workspace.relative_path(result.manifest_path),
            "event_manifest_path": workspace.relative_path(result.event_manifest_path),
            "history_manifest_path": workspace.relative_path(result.history_manifest_path),
            "file_count": result.file_count,
            "peer_ref": _optional_string(parameters.get("peer_ref")),
        }
    if job_type == "remote_sync_pull":
        requested_by = dict(job.get("requested_by", {})) if isinstance(job.get("requested_by", {}), dict) else None
        actor_id = str((requested_by or {}).get("principal_id", "")).strip() or "local-operator"
        result = pull_attached_remote(
            workspace,
            remote_ref=str(parameters.get("remote_ref", "")),
            actor_id=actor_id,
        )
        return {
            "manifest_path": workspace.relative_path(result["manifest_path"]),
            "event_manifest_path": workspace.relative_path(result["event_manifest_path"]),
            "history_manifest_path": workspace.relative_path(result["history_manifest_path"]),
            "file_count": int(result.get("file_count", 0) or 0),
            "remote_ref": str(parameters.get("remote_ref", "")),
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
            "plan_path": workspace.relative_path(result.plan_path),
            "packet_path": workspace.relative_path(result.packet_path),
            "report_path": workspace.relative_path(result.report_path),
            "answer_path": workspace.relative_path(result.answer_path) if result.answer_path else None,
            "slide_path": workspace.relative_path(result.slide_path) if result.slide_path else None,
            "notes_dir": workspace.relative_path(result.notes_dir),
            "source_packet_path": workspace.relative_path(result.source_packet_path),
            "checkpoints_path": workspace.relative_path(result.checkpoints_path),
            "validation_report_path": workspace.relative_path(result.validation_report_path),
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
        "worker_capability": _job_worker_capability(job_type),
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
                "worker_capability": str(job.get("worker_capability", "")),
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


def _with_artifact_paths(result_payload: Dict[str, object]) -> Dict[str, object]:
    enriched = dict(result_payload)
    artifact_paths: List[str] = []
    for key, value in enriched.items():
        if key == "artifact_paths":
            continue
        if key.endswith(("_path", "_file", "_dir")):
            candidate = _optional_string(value)
            if candidate:
                artifact_paths.append(candidate)
        elif key.endswith(("_paths", "_files", "_dirs")) and isinstance(value, list):
            for item in value:
                candidate = _optional_string(item)
                if candidate:
                    artifact_paths.append(candidate)
    artifact_paths.extend(["log.md", "wiki/index.md", "wiki/sources.md", "wiki/concepts.md", "wiki/queries.md"])
    enriched["artifact_paths"] = sorted({path for path in artifact_paths if path})
    return enriched


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
    worker_capabilities: Optional[List[str]] = None,
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
        "worker_capabilities": _normalize_worker_capabilities(worker_capabilities),
    }
    if last_heartbeat_at:
        payload["last_heartbeat_at"] = last_heartbeat_at
    return payload


def _requeue_active_jobs_for_worker(
    workspace: Workspace,
    worker_id: str,
    reason: str,
) -> List[str]:
    normalized_worker_id = _normalize_worker_id(worker_id)
    requeued_job_ids: List[str] = []
    for job in list_jobs(workspace):
        lease = dict(job.get("lease", {}))
        if str(lease.get("worker_id", "")).strip() != normalized_worker_id:
            continue
        if not _job_has_active_lease(job):
            continue
        manifest_path = workspace.job_manifests_dir / f"{job['job_id']}.json"
        _update_job_manifest(
            manifest_path,
            status="queued",
            lease={},
            finished_at="",
            started_at="",
            result={},
            error=None,
            audit_entry=(
                f"Worker {normalized_worker_id} was released ({reason}) and its active lease was re-queued."
            ),
        )
        requeued_job_ids.append(str(job.get("job_id", "")))
    return requeued_job_ids


def _select_claimable_job(
    jobs: List[Dict[str, object]],
    worker_capabilities: Optional[List[str]] = None,
) -> Optional[Dict[str, object]]:
    normalized_worker_capabilities = _normalize_worker_capabilities(worker_capabilities)
    for job in jobs:
        if _job_is_claimable(job) and _job_matches_worker_capabilities(job, normalized_worker_capabilities):
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


def _require_job_worker(job: Dict[str, object], worker_id: str) -> None:
    lease = dict(job.get("lease", {}))
    current_worker_id = str(lease.get("worker_id", "")).strip()
    if current_worker_id != worker_id:
        raise JobError(
            f"Job {job.get('job_id', '')} is owned by {current_worker_id or 'no active worker'} and cannot be updated by {worker_id}."
        )
    if str(job.get("status", "")) not in {"claimed", "running"}:
        raise JobError(f"Job {job.get('job_id', '')} is not actively running.")


def _job_is_claimable(job: Dict[str, object]) -> bool:
    status = str(job.get("status", ""))
    if status == "queued":
        return True
    if status in {"claimed", "running"} and not _job_has_active_lease(job):
        return True
    return False


def _job_matches_worker_capabilities(job: Dict[str, object], worker_capabilities: List[str]) -> bool:
    if not worker_capabilities:
        return True
    required_capability = str(job.get("worker_capability", "")).strip() or _job_worker_capability(
        str(job.get("job_type", ""))
    )
    if not required_capability:
        return True
    return required_capability in worker_capabilities


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

    for session in _active_worker_sessions(workspace):
        worker_id = str(session.get("worker_id", "")).strip()
        if not worker_id:
            continue
        workers_by_id[worker_id] = {
            "worker_id": worker_id,
            "status": str(session.get("status", "idle")).strip() or "idle",
            "current_job_id": str(session.get("current_job_id", "")).strip(),
            "current_job_type": str(session.get("current_job_type", "")).strip(),
            "lease_expires_at": str(session.get("expires_at", "")).strip(),
            "claim_count": 0,
            "last_seen_at": str(session.get("last_seen_at", "")).strip(),
            "declared_capabilities": _normalize_worker_capabilities(list(session.get("declared_capabilities", []))),
            "session_origin": str(session.get("origin", "")).strip(),
            "workspace_root": str(session.get("workspace_root", "")).strip(),
        }

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
                "declared_capabilities": [],
                "session_origin": "",
                "workspace_root": "",
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
        declared_capabilities = _normalize_worker_capabilities(list(lease.get("worker_capabilities", [])))
        if declared_capabilities:
            worker["declared_capabilities"] = declared_capabilities
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


def _read_worker_sessions(workspace: Workspace) -> Dict[str, object]:
    if not workspace.worker_sessions_manifest_path.exists():
        return {
            "schema_version": 1,
            "generated_at": utc_timestamp(),
            "sessions": [],
        }
    return json.loads(workspace.worker_sessions_manifest_path.read_text(encoding="utf-8"))


def _write_worker_sessions(workspace: Workspace, payload: Dict[str, object]) -> None:
    sessions = []
    for session in list(payload.get("sessions", [])):
        if not isinstance(session, dict):
            continue
        worker_id = str(session.get("worker_id", "")).strip()
        if not worker_id:
            continue
        sessions.append(
            {
                "worker_id": worker_id,
                "status": str(session.get("status", "idle")).strip().lower() or "idle",
                "origin": str(session.get("origin", "remote-http")).strip() or "remote-http",
                "registered_at": str(session.get("registered_at", "")) or utc_timestamp(),
                "updated_at": str(session.get("updated_at", "")) or utc_timestamp(),
                "last_seen_at": str(session.get("last_seen_at", "")) or utc_timestamp(),
                "expires_at": str(session.get("expires_at", "")),
                "released_at": str(session.get("released_at", "")),
                "release_reason": str(session.get("release_reason", "")),
                "current_job_id": str(session.get("current_job_id", "")),
                "current_job_type": str(session.get("current_job_type", "")),
                "declared_capabilities": _normalize_worker_capabilities(list(session.get("declared_capabilities", []))),
                "workspace_root": str(session.get("workspace_root", "")),
            }
        )
    workspace.worker_sessions_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.worker_sessions_manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": utc_timestamp(),
                "session_count": len(sessions),
                "sessions": sorted(sessions, key=lambda item: item["worker_id"]),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _active_worker_sessions(workspace: Workspace) -> List[Dict[str, object]]:
    payload = _read_worker_sessions(workspace)
    active_sessions: List[Dict[str, object]] = []
    now = datetime.now(timezone.utc)
    for session in list(payload.get("sessions", [])):
        if not isinstance(session, dict):
            continue
        if str(session.get("status", "")).strip() == "released":
            continue
        expires_at = str(session.get("expires_at", "")).strip()
        if not expires_at:
            continue
        try:
            if datetime.fromisoformat(expires_at) <= now:
                continue
        except ValueError:
            continue
        active_sessions.append(dict(session))
    return active_sessions


def _normalize_worker_capabilities(worker_capabilities: Optional[List[str]]) -> List[str]:
    return sorted(
        {
            str(capability).strip()
            for capability in list(worker_capabilities or [])
            if str(capability).strip()
        }
    )


def _job_worker_capability(job_type: str) -> str:
    return WORKER_CAPABILITY_BY_JOB_TYPE.get(str(job_type).strip(), "workspace")
