from __future__ import annotations

from dataclasses import dataclass
import base64
import json
from pathlib import Path
import time
from typing import List, Optional
from urllib import error, request

from cognisync.jobs import execute_job_payload
from cognisync.sync import encode_sync_bundle_archive, export_sync_bundle
from cognisync.workspace import Workspace


class RemoteWorkerError(RuntimeError):
    pass


@dataclass(frozen=True)
class RemoteWorkerResult:
    processed_count: int
    completed_count: int
    stopped_reason: str


def run_remote_worker(
    server_url: str,
    token: str,
    worker_id: str,
    max_jobs: Optional[int] = None,
    lease_seconds: int = 300,
    poll_interval_seconds: float = 0.0,
    max_idle_polls: int = 0,
    worker_capabilities: Optional[List[str]] = None,
    workspace_root: Optional[Path] = None,
) -> RemoteWorkerResult:
    normalized_server = server_url.rstrip("/")
    normalized_token = token.strip()
    normalized_worker = worker_id.strip()
    if not normalized_server:
        raise RemoteWorkerError("A control-plane server url is required.")
    if not normalized_token:
        raise RemoteWorkerError("A bearer token is required.")
    if not normalized_worker:
        raise RemoteWorkerError("A worker id is required.")
    normalized_worker_capabilities = sorted(
        {
            str(capability).strip()
            for capability in list(worker_capabilities or [])
            if str(capability).strip()
        }
    )
    resolved_workspace_root = Path(workspace_root).expanduser().resolve() if workspace_root is not None else None

    processed_count = 0
    completed_count = 0
    stopped_reason = "idle"
    idle_polls = 0

    while True:
        if max_jobs is not None and processed_count >= max_jobs:
            stopped_reason = "max_jobs_reached"
            break
        try:
            if resolved_workspace_root is None:
                payload = _post_json(
                    f"{normalized_server}/api/jobs/run-next",
                    token=normalized_token,
                    payload={
                        "worker_id": normalized_worker,
                        "lease_seconds": lease_seconds,
                        "worker_capabilities": normalized_worker_capabilities,
                    },
                )
            else:
                payload = _run_mirrored_remote_job(
                    server_url=normalized_server,
                    token=normalized_token,
                    worker_id=normalized_worker,
                    lease_seconds=lease_seconds,
                    worker_capabilities=normalized_worker_capabilities,
                    workspace_root=resolved_workspace_root,
                )
        except RemoteWorkerError as error_message:
            if "409:" in str(error_message):
                if poll_interval_seconds > 0 and idle_polls < max(0, int(max_idle_polls)):
                    idle_polls += 1
                    time.sleep(poll_interval_seconds)
                    continue
                stopped_reason = "no_jobs"
                break
            raise
        processed_count += 1
        idle_polls = 0
        if str(payload.get("status", "")) == "completed":
            completed_count += 1

    return RemoteWorkerResult(
        processed_count=processed_count,
        completed_count=completed_count,
        stopped_reason=stopped_reason,
    )


def _run_mirrored_remote_job(
    server_url: str,
    token: str,
    worker_id: str,
    lease_seconds: int,
    worker_capabilities: List[str],
    workspace_root: Path,
) -> dict:
    dispatch_payload = _post_json(
        f"{server_url}/api/jobs/dispatch-next",
        token=token,
        payload={
            "worker_id": worker_id,
            "lease_seconds": lease_seconds,
            "worker_capabilities": worker_capabilities,
        },
    )
    job_payload = dict(dispatch_payload.get("job", {}))
    workspace = Workspace(workspace_root)
    try:
        result_payload = execute_job_payload(
            workspace,
            job_type=str(job_payload.get("job_type", "")),
            parameters=dict(job_payload.get("parameters", {})),
            requested_by=dict(job_payload.get("requested_by", {})) if isinstance(job_payload.get("requested_by", {}), dict) else None,
        )
        bundle = export_sync_bundle(
            workspace,
            actor_id="local-operator",
            included_paths=[str(item) for item in list(result_payload.get("artifact_paths", []))],
        )
        archive_base64 = base64.b64encode(encode_sync_bundle_archive(bundle.directory)).decode("ascii")
        return _post_json(
            f"{server_url}/api/jobs/complete",
            token=token,
            payload={
                "job_id": str(job_payload.get("job_id", "")),
                "worker_id": worker_id,
                "result": result_payload,
                "sync_archive_base64": archive_base64,
            },
        )
    except Exception as error:
        try:
            _post_json(
                f"{server_url}/api/jobs/fail",
                token=token,
                payload={
                    "job_id": str(job_payload.get("job_id", "")),
                    "worker_id": worker_id,
                    "error": str(error),
                },
            )
        except RemoteWorkerError:
            pass
        raise RemoteWorkerError(str(error)) from error


def _post_json(url: str, token: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as http_error:
        body = http_error.read().decode("utf-8", errors="ignore")
        message = body.strip()
        try:
            payload = json.loads(body)
            message = str(payload.get("error", message))
        except json.JSONDecodeError:
            pass
        raise RemoteWorkerError(f"{http_error.code}: {message}") from http_error
