from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Optional
from urllib import error, request


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

    processed_count = 0
    completed_count = 0
    stopped_reason = "idle"

    while True:
        if max_jobs is not None and processed_count >= max_jobs:
            stopped_reason = "max_jobs_reached"
            break
        try:
            payload = _post_json(
                f"{normalized_server}/api/jobs/run-next",
                token=normalized_token,
                payload={"worker_id": normalized_worker, "lease_seconds": lease_seconds},
            )
        except RemoteWorkerError as error_message:
            if "409:" in str(error_message):
                stopped_reason = "no_jobs"
                break
            raise
        processed_count += 1
        if str(payload.get("status", "")) == "completed":
            completed_count += 1

    return RemoteWorkerResult(
        processed_count=processed_count,
        completed_count=completed_count,
        stopped_reason=stopped_reason,
    )


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
