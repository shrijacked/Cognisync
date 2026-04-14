from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Dict, List, Optional

from cognisync.notifications import build_notification_manifest
from cognisync.sharing import load_shared_workspace_manifest
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


MAX_RENDERED_FINDINGS = 12
STALE_WORKER_THRESHOLD = timedelta(minutes=15)
SEVERITY_RANK = {"high": 0, "medium": 1, "info": 2}
BROAD_OPERATOR_SCOPES = {
    "connectors.sync",
    "control.admin",
    "jobs.claim",
    "jobs.run",
    "scheduler.run",
    "sync.export",
}


def build_hosted_hardening_report(
    workspace: Workspace,
    control_plane_payload: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    control_payload = (
        control_plane_payload
        if control_plane_payload is not None
        else _read_json(workspace.control_plane_manifest_path)
    )
    sharing_payload = load_shared_workspace_manifest(workspace)
    worker_payload = _read_json(workspace.worker_registry_path)
    queue_payload = _read_json(workspace.job_queue_manifest_path)
    notification_payload = build_notification_manifest(workspace)

    findings: List[Dict[str, object]] = []
    findings.extend(_token_findings(workspace, _as_list(control_payload.get("tokens", []))))
    findings.extend(_trust_policy_findings(workspace, sharing_payload))
    findings.extend(_worker_findings(workspace, worker_payload))
    findings.extend(_queue_findings(workspace, queue_payload))
    findings.extend(_notification_findings(workspace, notification_payload))
    findings = _sorted_findings(findings)

    counts_by_severity: Dict[str, int] = {}
    for finding in findings:
        severity = str(finding.get("severity", "info"))
        counts_by_severity[severity] = counts_by_severity.get(severity, 0) + 1

    status = "ok"
    if findings:
        status = "attention"
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "status": status,
        "summary": {
            "finding_count": len(findings),
            "counts_by_severity": dict(
                sorted(counts_by_severity.items(), key=lambda item: SEVERITY_RANK.get(item[0], 99))
            ),
            "high_count": counts_by_severity.get("high", 0),
            "medium_count": counts_by_severity.get("medium", 0),
            "info_count": counts_by_severity.get("info", 0),
        },
        "findings": findings,
    }


def render_hosted_hardening_report(report: Dict[str, object]) -> str:
    summary = dict(report.get("summary", {}))
    findings = list(report.get("findings", []))
    lines = [
        "## Hosted Hardening",
        "",
        f"- Status: `{report.get('status', 'ok')}`",
        f"- Findings: `{summary.get('finding_count', 0)}`",
        f"- High findings: `{summary.get('high_count', 0)}`",
        f"- Medium findings: `{summary.get('medium_count', 0)}`",
    ]
    if not findings:
        lines.extend(["", "No hosted-alpha hardening findings detected."])
        return "\n".join(lines)

    lines.extend(["", "### Findings", ""])
    for finding in findings[:MAX_RENDERED_FINDINGS]:
        lines.append(
            "- "
            f"`{finding.get('severity', 'info')}` "
            f"`{finding.get('finding_id', '')}` "
            f"{finding.get('title', '')}"
        )
        lines.append(f"  path: `{finding.get('path', '')}`")
        recommendation = str(finding.get("recommendation", "")).strip()
        if recommendation:
            lines.append(f"  recommendation: {recommendation}")
    omitted = len(findings) - MAX_RENDERED_FINDINGS
    if omitted > 0:
        lines.append(f"- ... {omitted} more hosted hardening finding(s) omitted")
    return "\n".join(lines)


def _token_findings(workspace: Workspace, tokens: List[object]) -> List[Dict[str, object]]:
    findings: List[Dict[str, object]] = []
    for raw_token in tokens:
        token = _as_dict(raw_token)
        if str(token.get("status", "")) != "active":
            continue
        principal_id = str(token.get("principal_id", "")).strip() or "unknown"
        role = str(token.get("role", "")).strip().lower()
        scopes = {str(scope) for scope in _as_list(token.get("scopes", []))}
        expires_at = str(token.get("expires_at", "")).strip()
        token_id = str(token.get("token_id", "")).strip()

        if not expires_at:
            findings.append(
                _finding(
                    finding_id=f"token-no-expiry:{principal_id}",
                    severity="medium",
                    title="Active control-plane token has no expiry.",
                    detail=(
                        "Long-lived bearer tokens increase the blast radius if copied into shell history, "
                        "bundles, or logs."
                    ),
                    recommendation="Re-issue the token with `--expires-in-hours` and revoke the long-lived token.",
                    path=workspace.relative_path(workspace.control_plane_manifest_path),
                    related_ids=[token_id] if token_id else [],
                )
            )
            if role == "operator" and scopes & BROAD_OPERATOR_SCOPES:
                findings.append(
                    _finding(
                        finding_id=f"token-operator-no-expiry:{principal_id}",
                        severity="high",
                        title="Operator token has broad hosted scopes and no expiry.",
                        detail=(
                            "Operator tokens can mutate hosted-alpha state and should be time-bounded "
                            "before remote use."
                        ),
                        recommendation=(
                            "Re-issue the token with `--expires-in-hours` and only the scopes needed by "
                            "that worker or operator."
                        ),
                        path=workspace.relative_path(workspace.control_plane_manifest_path),
                        related_ids=[token_id] if token_id else [],
                    )
                )
        elif _timestamp_is_past(expires_at):
            findings.append(
                _finding(
                    finding_id=f"token-expired-active:{principal_id}",
                    severity="high",
                    title="Active control-plane token is past its expiry.",
                    detail=(
                        "The token should be marked expired on validation, but it is still recorded as active "
                        "in the manifest."
                    ),
                    recommendation="Validate or revoke the token so hosted status reflects the real access posture.",
                    path=workspace.relative_path(workspace.control_plane_manifest_path),
                    related_ids=[token_id] if token_id else [],
                )
            )
    return findings


def _trust_policy_findings(workspace: Workspace, sharing_payload: Dict[str, object]) -> List[Dict[str, object]]:
    findings: List[Dict[str, object]] = []
    trust_policy = _as_dict(sharing_payload.get("trust_policy", {}))
    if not bool(trust_policy.get("require_secure_control_plane", True)):
        findings.append(
            _finding(
                finding_id="trust-policy-insecure-control-plane",
                severity="high",
                title="Shared-workspace policy allows insecure control-plane URLs.",
                detail="Remote peers and attached remotes can use non-HTTPS control-plane URLs under this policy.",
                recommendation="Enable `--require-secure-control-plane` before issuing hosted peer bundles.",
                path=workspace.relative_path(workspace.shared_workspace_manifest_path),
            )
        )
    allowed_peer_capabilities = _as_list(trust_policy.get("allowed_peer_capabilities", []))
    if bool(trust_policy.get("allow_remote_workers", True)) and not allowed_peer_capabilities:
        findings.append(
            _finding(
                finding_id="trust-policy-peer-capabilities-any",
                severity="medium",
                title="Remote workers are allowed without a peer capability allowlist.",
                detail="Accepted peers can request any supported peer capability unless the trust policy narrows them.",
                recommendation=(
                    "Set explicit `--allow-peer-capability` values for the hosted-alpha work you intend "
                    "to delegate."
                ),
                path=workspace.relative_path(workspace.shared_workspace_manifest_path),
            )
        )
    if str(trust_policy.get("max_peer_role", "operator")) == "operator":
        findings.append(
            _finding(
                finding_id="trust-policy-max-peer-role-operator",
                severity="medium",
                title="Shared-workspace policy permits operator peers.",
                detail=(
                    "Operator peers can mutate queue, sharing, and control-plane state when granted "
                    "matching scopes."
                ),
                recommendation=(
                    "Lower `--max-peer-role` to `reviewer` unless remote operators are intentionally part "
                    "of this workspace."
                ),
                path=workspace.relative_path(workspace.shared_workspace_manifest_path),
            )
        )
    return findings


def _worker_findings(workspace: Workspace, worker_payload: Dict[str, object]) -> List[Dict[str, object]]:
    findings: List[Dict[str, object]] = []
    for raw_worker in _as_list(worker_payload.get("workers", [])):
        worker = _as_dict(raw_worker)
        status = str(worker.get("status", "")).strip().lower()
        if status not in {"claimed", "running"}:
            continue
        last_seen_at = str(worker.get("last_seen_at", "")).strip()
        if not _timestamp_is_older_than(last_seen_at, STALE_WORKER_THRESHOLD):
            continue
        worker_id = str(worker.get("worker_id", "")).strip() or "unknown"
        findings.append(
            _finding(
                finding_id=f"worker-stale-active:{worker_id}",
                severity="high",
                title="Hosted worker owns active work but has not checked in recently.",
                detail="A stale active worker can leave hosted jobs leased without visible progress.",
                recommendation="Use `control-plane release-worker --requeue-active-jobs` if the worker is gone.",
                path=workspace.relative_path(workspace.worker_registry_path),
                related_ids=[str(worker.get("current_job_id", ""))] if worker.get("current_job_id") else [],
            )
        )
    return findings


def _queue_findings(workspace: Workspace, queue_payload: Dict[str, object]) -> List[Dict[str, object]]:
    findings: List[Dict[str, object]] = []
    queued_count = _safe_int(queue_payload.get("queued_count", 0))
    if queued_count > 0:
        findings.append(
            _finding(
                finding_id="job-queue-backlog",
                severity="info",
                title=f"Hosted queue has {queued_count} queued job(s).",
                detail="Queued hosted work is waiting for a compatible local or remote worker.",
                recommendation=(
                    "Start a worker with matching `--capability` values or intentionally leave the backlog queued."
                ),
                path=workspace.relative_path(workspace.job_queue_manifest_path),
            )
        )
    return findings


def _notification_findings(workspace: Workspace, notification_payload: Dict[str, object]) -> List[Dict[str, object]]:
    findings: List[Dict[str, object]] = []
    for raw_notification in _as_list(notification_payload.get("notifications", [])):
        notification = _as_dict(raw_notification)
        if str(notification.get("severity", "")) != "high":
            continue
        kind = str(notification.get("kind", "notification")).strip() or "notification"
        findings.append(
            _finding(
                finding_id=f"notification-high:{kind}",
                severity="high",
                title=str(notification.get("title", "High-severity hosted notification")),
                detail=str(notification.get("detail", "")),
                recommendation=(
                    "Resolve or intentionally dismiss the underlying workspace condition before exposing "
                    "hosted workflows."
                ),
                path=str(notification.get("path", ""))
                or workspace.relative_path(workspace.notifications_manifest_path),
                related_ids=[
                    str(item)
                    for item in _as_list(notification.get("related_paths", []))
                    if str(item).strip()
                ],
            )
        )
    return findings


def _finding(
    finding_id: str,
    severity: str,
    title: str,
    detail: str,
    recommendation: str,
    path: str,
    related_ids: Optional[List[str]] = None,
) -> Dict[str, object]:
    return {
        "finding_id": finding_id,
        "severity": severity,
        "title": title,
        "detail": detail,
        "recommendation": recommendation,
        "path": path,
        "related_ids": list(related_ids or []),
    }


def _sorted_findings(findings: List[Dict[str, object]]) -> List[Dict[str, object]]:
    findings.sort(
        key=lambda item: (
            SEVERITY_RANK.get(str(item.get("severity", "info")), 99),
            str(item.get("finding_id", "")),
        )
    )
    return findings


def _timestamp_is_past(value: str) -> bool:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return False
    return parsed <= datetime.now(timezone.utc)


def _timestamp_is_older_than(value: str, threshold: timedelta) -> bool:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return False
    return parsed <= datetime.now(timezone.utc) - threshold


def _parse_timestamp(value: str) -> Optional[datetime]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_list(value: object) -> List[object]:
    if isinstance(value, list):
        return value
    return []


def _as_dict(value: object) -> Dict[str, object]:
    if isinstance(value, dict):
        return value
    return {}


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _read_json(path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}
