from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import mimetypes
from pathlib import Path
import secrets
from typing import Dict, List, Optional, TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

from cognisync.access import (
    AccessError,
    DEFAULT_LOCAL_OPERATOR_ID,
    OPERATOR_ACTION_ROLES,
    REVIEW_ACTION_ROLES,
    grant_access_member,
    load_access_manifest,
    require_access_role,
    revoke_access_member,
)
from cognisync.collaboration import (
    CollaborationError,
    add_comment,
    load_collaboration_manifest,
    record_decision,
    request_review,
    resolve_review,
)
from cognisync.connectors import (
    ConnectorSyncResult,
    add_connector,
    connector_subscription_is_due,
    list_connectors,
    list_due_connectors,
    subscribe_connector,
    sync_all_connectors,
    sync_connector,
    unsubscribe_connector,
)
from cognisync.jobs import (
    JobError,
    claim_next_job,
    complete_dispatched_job,
    dispatch_next_job,
    enqueue_compile_job,
    enqueue_connector_sync_job,
    enqueue_connector_sync_all_job,
    enqueue_ingest_repo_job,
    enqueue_ingest_sitemap_job,
    enqueue_ingest_url_job,
    fail_dispatched_job,
    enqueue_lint_job,
    enqueue_maintain_job,
    enqueue_research_job,
    enqueue_sync_export_job,
    heartbeat_job,
    read_worker_registry,
    run_next_job,
)
from cognisync.maintenance import (
    MaintenanceError,
    accept_concept_candidate,
    apply_backlink_suggestion,
    clear_dismissed_review_item,
    dismiss_review_item,
    file_conflict_review,
    list_dismissed_review_items,
    reopen_review_item,
    resolve_entity_merge,
)
from cognisync.notifications import write_notifications_manifest
from cognisync.review_queue import build_review_queue
from cognisync.review_state import read_review_actions
from cognisync.research import DEFAULT_RESEARCH_JOB_PROFILE
from cognisync.scanner import scan_workspace
from cognisync.sharing import (
    accept_shared_peer,
    ensure_shared_workspace_manifest,
    invite_shared_peer,
    issue_shared_peer_bundle,
    list_due_shared_peer_syncs,
    load_shared_workspace_manifest,
    remove_shared_peer,
    record_shared_peer_scheduler_tick,
    set_shared_peer_role,
    set_shared_trust_policy,
    sharing_summary,
    subscribe_shared_peer_sync,
    suspend_shared_peer,
    unsubscribe_shared_peer_sync,
)
from cognisync.sync import encode_sync_bundle_archive, export_sync_bundle, import_sync_bundle_archive, list_sync_events
from cognisync.utils import slugify, utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


DEFAULT_CONTROL_SCOPES = {
    "operator": [
        "connectors.sync",
        "control.admin",
        "control.read",
        "jobs.claim",
        "jobs.heartbeat",
        "jobs.run",
        "review.run",
        "scheduler.run",
    ],
    "reviewer": ["control.read", "review.run"],
    "editor": ["control.read"],
    "viewer": ["control.read"],
}


class ControlPlaneError(RuntimeError):
    pass


@dataclass(frozen=True)
class SchedulerTickResult:
    action: str
    due_connector_count: int
    due_connector_ids: List[str]
    due_peer_sync_ids: List[str]
    due_job_subscription_count: int
    due_job_subscription_ids: List[str]
    enqueued_job_ids: List[str]
    executed_run_manifest_path: Optional[Path]


def ensure_control_plane_manifest(workspace: "Workspace") -> Dict[str, object]:
    payload = load_control_plane_manifest(workspace)
    _write_control_plane_manifest(workspace, payload)
    return payload


def load_control_plane_manifest(workspace: "Workspace") -> Dict[str, object]:
    if workspace.control_plane_manifest_path.exists():
        payload = json.loads(workspace.control_plane_manifest_path.read_text(encoding="utf-8"))
    else:
        payload = default_control_plane_manifest()
    return _normalize_control_plane_manifest(payload)


def render_control_plane_status(workspace: "Workspace") -> str:
    payload = ensure_control_plane_manifest(workspace)
    sharing = sharing_summary(workspace)
    summary = dict(payload.get("summary", {}))
    scheduler = dict(payload.get("scheduler", {}))
    lines = [
        "# Control Plane",
        "",
        f"- Workspace id: `{payload.get('workspace_id', '')}`",
        f"- Active tokens: `{summary.get('active_token_count', 0)}`",
        f"- Expired tokens: `{summary.get('expired_token_count', 0)}`",
        f"- Pending invites: `{summary.get('pending_invite_count', 0)}`",
        f"- Accepted invites: `{summary.get('accepted_invite_count', 0)}`",
        f"- Published control plane URL: `{sharing.get('published_control_plane_url', '') or 'unbound'}`",
        f"- Accepted peers: `{sharing.get('accepted_peer_count', 0)}`",
        f"- Suspended peers: `{sharing.get('suspended_peer_count', 0)}`",
        f"- Last scheduler action: `{scheduler.get('last_action', 'never')}`",
    ]
    return "\n".join(lines)


def render_control_plane_workers(workspace: "Workspace") -> str:
    payload = read_worker_registry(workspace)
    workers = list(payload.get("workers", []))
    lines = [
        "# Control Plane Workers",
        "",
        f"- Worker count: `{len(workers)}`",
        f"- Status counts: `{json.dumps(dict(payload.get('counts_by_status', {})), sort_keys=True)}`",
    ]
    if not workers:
        lines.extend(["", "No workers have registered with the queue yet."])
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


def list_control_plane_tokens(workspace: "Workspace") -> List[Dict[str, object]]:
    payload = ensure_control_plane_manifest(workspace)
    return [dict(item) for item in list(payload.get("tokens", []))]


def create_control_plane_invite(
    workspace: "Workspace",
    principal_id: str,
    role: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    actor = require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "create control-plane invites")
    normalized_principal = principal_id.strip()
    normalized_role = role.strip().lower()
    if not normalized_principal:
        raise ControlPlaneError("A principal id is required.")
    payload = ensure_control_plane_manifest(workspace)
    invites = [dict(item) for item in list(payload.get("invites", []))]
    now = utc_timestamp()
    invite = {
        "invite_id": f"invite-{slugify(normalized_principal)[:32] or 'principal'}-{now.replace(':', '').replace('-', '')}",
        "principal_id": normalized_principal,
        "role": normalized_role,
        "status": "pending",
        "created_at": now,
        "accepted_at": "",
        "created_by": _serialize_actor(actor),
    }
    invites.append(invite)
    payload["invites"] = _sorted_invites(invites)
    _write_control_plane_manifest(workspace, payload)
    return invite


def accept_control_plane_invite(
    workspace: "Workspace",
    invite_ref: str,
    actor_id: Optional[str] = None,
) -> Dict[str, object]:
    normalized_ref = invite_ref.strip()
    if not normalized_ref:
        raise ControlPlaneError("An invite id or principal id is required.")
    payload = ensure_control_plane_manifest(workspace)
    invites = [dict(item) for item in list(payload.get("invites", []))]
    invite = next(
        (
            item
            for item in invites
            if str(item.get("invite_id", "")) == normalized_ref or str(item.get("principal_id", "")) == normalized_ref
        ),
        None,
    )
    if invite is None:
        raise ControlPlaneError(f"Could not find invite '{normalized_ref}'.")
    if str(invite.get("status", "")) == "accepted":
        return invite
    principal_id = str(invite.get("principal_id", ""))
    grant_access_member(
        workspace,
        principal_id=principal_id,
        role=str(invite.get("role", "viewer")),
        display_name=principal_id,
    )
    now = utc_timestamp()
    invite["status"] = "accepted"
    invite["accepted_at"] = now
    invite["accepted_by"] = actor_id or principal_id
    payload["invites"] = _sorted_invites(invites)
    _write_control_plane_manifest(workspace, payload)
    return invite


def issue_control_plane_token(
    workspace: "Workspace",
    principal_id: str,
    scopes: Optional[List[str]] = None,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    description: str = "",
    expires_in_hours: Optional[int] = None,
) -> tuple[Dict[str, object], str]:
    actor = require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "issue control-plane tokens")
    principal = require_access_role(workspace, principal_id, ("viewer", "editor", "reviewer", "operator"), "receive control-plane tokens")
    if expires_in_hours is not None and int(expires_in_hours) < 1:
        raise ControlPlaneError("Control-plane token expiry must be at least 1 hour.")
    raw_token = f"cp_{secrets.token_hex(24)}"
    token_hash = _hash_token(raw_token)
    normalized_scopes = sorted(set(scopes or DEFAULT_CONTROL_SCOPES.get(str(principal.get("role", "")), ["control.read"])))
    created_at = utc_timestamp()
    expires_at = ""
    if expires_in_hours is not None:
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=int(expires_in_hours))).isoformat(timespec="seconds")
    token = {
        "token_id": f"token-{slugify(principal_id)[:24] or 'principal'}-{utc_timestamp().replace(':', '').replace('-', '')}",
        "principal_id": str(principal.get("principal_id", "")),
        "role": str(principal.get("role", "")),
        "status": "active",
        "description": description.strip(),
        "created_at": created_at,
        "issued_by": _serialize_actor(actor),
        "last_used_at": "",
        "expires_at": expires_at,
        "token_prefix": raw_token[:10],
        "token_hash": token_hash,
        "scopes": normalized_scopes,
    }
    payload = ensure_control_plane_manifest(workspace)
    tokens = [dict(item) for item in list(payload.get("tokens", []))]
    tokens.append(token)
    payload["tokens"] = _sorted_tokens(tokens)
    _write_control_plane_manifest(workspace, payload)
    safe_token = dict(token)
    safe_token.pop("token_hash", None)
    return safe_token, raw_token


def revoke_control_plane_token(
    workspace: "Workspace",
    token_id: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "revoke control-plane tokens")
    normalized_id = token_id.strip()
    payload = ensure_control_plane_manifest(workspace)
    tokens = [dict(item) for item in list(payload.get("tokens", []))]
    token = next((item for item in tokens if str(item.get("token_id", "")) == normalized_id), None)
    if token is None:
        raise ControlPlaneError(f"Could not find token '{normalized_id}'.")
    token["status"] = "revoked"
    token["revoked_at"] = utc_timestamp()
    payload["tokens"] = _sorted_tokens(tokens)
    _write_control_plane_manifest(workspace, payload)
    safe_token = dict(token)
    safe_token.pop("token_hash", None)
    return safe_token


def revoke_control_plane_tokens_for_principal(
    workspace: "Workspace",
    principal_id: str,
    reason: str = "",
) -> List[Dict[str, object]]:
    normalized_principal = principal_id.strip()
    if not normalized_principal:
        return []
    payload = ensure_control_plane_manifest(workspace)
    tokens = [dict(item) for item in list(payload.get("tokens", []))]
    revoked: List[Dict[str, object]] = []
    changed = False
    for token in tokens:
        if str(token.get("principal_id", "")) != normalized_principal:
            continue
        if str(token.get("status", "")) != "active":
            continue
        token["status"] = "revoked"
        token["revoked_at"] = utc_timestamp()
        if reason:
            token["revocation_reason"] = reason
        revoked.append(dict(token))
        changed = True
    if changed:
        payload["tokens"] = _sorted_tokens(tokens)
        _write_control_plane_manifest(workspace, payload)
    for token in revoked:
        token.pop("token_hash", None)
    return revoked


def validate_control_plane_token(
    workspace: "Workspace",
    token_value: str,
    required_scopes: List[str],
) -> Dict[str, object]:
    normalized_token = token_value.strip()
    if not normalized_token:
        raise ControlPlaneError("A bearer token is required.")
    payload = ensure_control_plane_manifest(workspace)
    hashed = _hash_token(normalized_token)
    tokens = [dict(item) for item in list(payload.get("tokens", []))]
    token = next(
        (
            item
            for item in tokens
            if str(item.get("status", "")) == "active" and str(item.get("token_hash", "")) == hashed
        ),
        None,
    )
    if token is None:
        raise ControlPlaneError("Invalid or revoked control-plane token.")
    if _token_is_expired(token):
        token["status"] = "expired"
        token["expired_at"] = utc_timestamp()
        payload["tokens"] = _sorted_tokens(tokens)
        _write_control_plane_manifest(workspace, payload)
        raise ControlPlaneError("Expired control-plane token.")
    scopes = {str(item) for item in list(token.get("scopes", []))}
    missing_scopes = [scope for scope in required_scopes if scope not in scopes]
    if missing_scopes:
        raise ControlPlaneError(f"Token is missing required scopes: {', '.join(missing_scopes)}.")
    token["last_used_at"] = utc_timestamp()
    payload["tokens"] = _sorted_tokens(tokens)
    _write_control_plane_manifest(workspace, payload)
    principal_id = str(token.get("principal_id", ""))
    return require_access_role(
        workspace,
        principal_id,
        ("viewer", "editor", "reviewer", "operator"),
        "use the control plane",
    )


def schedule_job_subscription(
    workspace: "Workspace",
    job_type: str,
    every_hours: int,
    parameters: Optional[Dict[str, object]] = None,
    label: Optional[str] = None,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    actor = require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "manage scheduled control-plane jobs")
    normalized_type = str(job_type).strip().lower()
    if normalized_type not in {"research", "compile", "lint", "maintain"}:
        raise ControlPlaneError("Scheduled jobs currently support research, compile, lint, and maintain.")
    if int(every_hours) < 1:
        raise ControlPlaneError("Scheduled job intervals must be at least 1 hour.")

    payload = ensure_control_plane_manifest(workspace)
    subscriptions = [
        _normalize_job_subscription(item)
        for item in list(dict(payload.get("scheduler", {})).get("job_subscriptions", []))
    ]
    subscribed_at = utc_timestamp()
    normalized_parameters = dict(parameters or {})
    if normalized_type == "research" and not str(normalized_parameters.get("question", "")).strip():
        raise ControlPlaneError("Scheduled research jobs require a question.")
    subscription_id = (
        f"scheduled-{normalized_type}-"
        f"{slugify(label or normalized_parameters.get('question') or normalized_type)[:40] or normalized_type}-"
        f"{subscribed_at.replace(':', '').replace('-', '')}"
    )
    subscription = _normalize_job_subscription(
        {
            "subscription_id": subscription_id,
            "job_type": normalized_type,
            "label": label or normalized_parameters.get("question") or normalized_type,
            "parameters": normalized_parameters,
            "enabled": True,
            "interval_hours": int(every_hours),
            "subscribed_at": subscribed_at,
            "next_run_at": subscribed_at,
            "last_enqueued_at": "",
            "last_scheduler_tick_at": "",
            "last_due_at": "",
            "last_tick_status": "",
            "created_by": _serialize_actor(actor),
            "updated_by": _serialize_actor(actor),
        }
    )
    subscriptions.append(subscription)
    scheduler = dict(payload.get("scheduler", {}))
    scheduler["job_subscriptions"] = _sorted_job_subscriptions(subscriptions)
    payload["scheduler"] = scheduler
    _write_control_plane_manifest(workspace, payload)
    return subscription


def disable_job_subscription(
    workspace: "Workspace",
    subscription_id: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    actor = require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "manage scheduled control-plane jobs")
    normalized_id = str(subscription_id).strip()
    if not normalized_id:
        raise ControlPlaneError("A scheduled job subscription id is required.")
    payload = ensure_control_plane_manifest(workspace)
    subscriptions = [
        _normalize_job_subscription(item)
        for item in list(dict(payload.get("scheduler", {})).get("job_subscriptions", []))
    ]
    subscription = next((item for item in subscriptions if str(item.get("subscription_id", "")) == normalized_id), None)
    if subscription is None:
        raise ControlPlaneError(f"Could not find scheduled job '{normalized_id}'.")
    subscription["enabled"] = False
    subscription["next_run_at"] = ""
    subscription["updated_by"] = _serialize_actor(actor)
    scheduler = dict(payload.get("scheduler", {}))
    scheduler["job_subscriptions"] = _sorted_job_subscriptions(subscriptions)
    payload["scheduler"] = scheduler
    _write_control_plane_manifest(workspace, payload)
    return subscription


def list_job_subscriptions(workspace: "Workspace") -> List[Dict[str, object]]:
    payload = ensure_control_plane_manifest(workspace)
    return _sorted_job_subscriptions(
        [
            _normalize_job_subscription(item)
            for item in list(dict(payload.get("scheduler", {})).get("job_subscriptions", []))
        ]
    )


def list_due_job_subscriptions(workspace: "Workspace") -> List[Dict[str, object]]:
    due: List[Dict[str, object]] = []
    for subscription in list_job_subscriptions(workspace):
        if _job_subscription_is_due(subscription):
            due.append(subscription)
    return due


def _normalize_job_subscription(subscription: object) -> Dict[str, object]:
    payload = dict(subscription) if isinstance(subscription, dict) else {}
    interval_hours = payload.get("interval_hours")
    try:
        normalized_interval = int(interval_hours) if interval_hours is not None and str(interval_hours) != "" else None
    except (TypeError, ValueError):
        normalized_interval = None
    return {
        "subscription_id": str(payload.get("subscription_id", "")),
        "job_type": str(payload.get("job_type", "")),
        "label": str(payload.get("label", "")),
        "parameters": dict(payload.get("parameters", {})) if isinstance(payload.get("parameters", {}), dict) else {},
        "enabled": bool(payload.get("enabled", False)),
        "interval_hours": normalized_interval,
        "subscribed_at": str(payload.get("subscribed_at", "")) or "",
        "next_run_at": str(payload.get("next_run_at", "")) or "",
        "last_enqueued_at": str(payload.get("last_enqueued_at", "")) or "",
        "last_scheduler_tick_at": str(payload.get("last_scheduler_tick_at", "")) or "",
        "last_due_at": str(payload.get("last_due_at", "")) or "",
        "last_tick_status": str(payload.get("last_tick_status", "")) or "",
        "created_by": _serialize_actor(payload.get("created_by")),
        "updated_by": _serialize_actor(payload.get("updated_by")),
    }


def _sorted_job_subscriptions(subscriptions: List[Dict[str, object]]) -> List[Dict[str, object]]:
    subscriptions.sort(
        key=lambda item: (
            str(item.get("job_type", "")),
            str(item.get("label", "")),
            str(item.get("subscription_id", "")),
        )
    )
    return subscriptions


def _job_subscription_is_due(subscription: Dict[str, object]) -> bool:
    normalized = _normalize_job_subscription(subscription)
    if not bool(normalized.get("enabled", False)):
        return False
    next_run_at = str(normalized.get("next_run_at", "")).strip()
    if not next_run_at:
        return True
    try:
        return datetime.fromisoformat(next_run_at) <= datetime.now(timezone.utc)
    except ValueError:
        return True


def _advance_job_subscription(subscription: Dict[str, object], scheduled_at: str) -> Dict[str, object]:
    normalized = _normalize_job_subscription(subscription)
    normalized["last_enqueued_at"] = scheduled_at
    interval_hours = normalized.get("interval_hours")
    if interval_hours:
        try:
            base_time = datetime.fromisoformat(scheduled_at)
        except ValueError:
            base_time = datetime.now(timezone.utc)
        normalized["next_run_at"] = (
            base_time + timedelta(hours=int(interval_hours))
        ).replace(microsecond=0).isoformat()
    return normalized


def _enqueue_scheduled_job(
    workspace: "Workspace",
    scheduled_job: Dict[str, object],
    actor: Dict[str, object],
) -> Path:
    normalized = _normalize_job_subscription(scheduled_job)
    job_type = str(normalized.get("job_type", ""))
    parameters = dict(normalized.get("parameters", {}))
    if job_type == "research":
        question = str(parameters.get("question", "")).strip()
        if not question:
            raise ControlPlaneError("Scheduled research jobs require a question.")
        return enqueue_research_job(
            workspace,
            question=question,
            profile_name=str(parameters.get("profile_name", "")) or None,
            limit=int(parameters.get("limit", 5) or 5),
            mode=str(parameters.get("mode", "wiki") or "wiki"),
            slides=bool(parameters.get("slides", False)),
            job_profile=str(parameters.get("job_profile", "")) or DEFAULT_RESEARCH_JOB_PROFILE,
            requested_by=actor,
        )
    if job_type == "compile":
        return enqueue_compile_job(
            workspace,
            profile_name=str(parameters.get("profile_name", "")) or None,
            requested_by=actor,
        )
    if job_type == "lint":
        return enqueue_lint_job(workspace, requested_by=actor)
    if job_type == "maintain":
        return enqueue_maintain_job(
            workspace,
            max_concepts=int(parameters.get("max_concepts", 10) or 10),
            max_merges=int(parameters.get("max_merges", 10) or 10),
            max_backlinks=int(parameters.get("max_backlinks", 10) or 10),
            max_conflicts=int(parameters.get("max_conflicts", 10) or 10),
            requested_by=actor,
        )
    raise ControlPlaneError(f"Unsupported scheduled job type '{job_type}'.")


def run_scheduler_tick(
    workspace: "Workspace",
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    enqueue_only: bool = True,
    force: bool = False,
    limit: Optional[int] = None,
) -> SchedulerTickResult:
    actor = require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "run the control-plane scheduler")
    due_connectors = [dict(item) for item in list_due_connectors(workspace)]
    due_peer_syncs = [dict(item) for item in list_due_shared_peer_syncs(workspace)]
    due_job_subscriptions = [dict(item) for item in list_due_job_subscriptions(workspace)]
    if limit is not None:
        due_connectors = due_connectors[: max(0, int(limit))]
        remaining = max(0, int(limit) - len(due_connectors))
        due_peer_syncs = due_peer_syncs[:remaining] if remaining else []
        remaining = max(0, remaining - len(due_peer_syncs))
        due_job_subscriptions = due_job_subscriptions[:remaining] if remaining else []
    due_connector_ids = [str(item.get("connector_id", "")) for item in due_connectors]
    due_peer_sync_ids = [str(item.get("peer_id", "")) for item in due_peer_syncs]
    due_job_subscription_ids = [str(item.get("subscription_id", "")) for item in due_job_subscriptions]
    executed_run_manifest_path: Optional[Path] = None
    enqueued_job_ids: List[str] = []
    action = "noop"
    tick_at = utc_timestamp()
    if due_connectors or due_peer_syncs or due_job_subscriptions:
        if enqueue_only:
            if due_connectors:
                manifest_path = enqueue_connector_sync_all_job(
                    workspace,
                    force=force,
                    limit=len(due_connectors),
                    scheduled_only=True,
                    requested_by=actor,
                )
                enqueued_job_ids.append(manifest_path.stem)
            for peer_sync in due_peer_syncs:
                manifest_path = enqueue_sync_export_job(
                    workspace,
                    peer_ref=str(peer_sync.get("peer_id", "")),
                    requested_by=actor,
                )
                enqueued_job_ids.append(manifest_path.stem)
            for scheduled_job in due_job_subscriptions:
                manifest_path = _enqueue_scheduled_job(workspace, scheduled_job, actor)
                enqueued_job_ids.append(manifest_path.stem)
        else:
            if due_connectors:
                result = sync_all_connectors(
                    workspace,
                    force=force,
                    limit=len(due_connectors),
                    scheduled_only=True,
                    actor=actor,
                )
                executed_run_manifest_path = result.run_manifest_path
            for peer_sync in due_peer_syncs:
                manifest_path = enqueue_sync_export_job(
                    workspace,
                    peer_ref=str(peer_sync.get("peer_id", "")),
                    requested_by=actor,
                )
                enqueued_job_ids.append(manifest_path.stem)
            for scheduled_job in due_job_subscriptions:
                manifest_path = _enqueue_scheduled_job(workspace, scheduled_job, actor)
                enqueued_job_ids.append(manifest_path.stem)
        if executed_run_manifest_path is not None:
            action = "executed"
        elif enqueued_job_ids:
            action = "enqueued"
    _record_scheduler_observations(workspace, due_connector_ids)
    record_shared_peer_scheduler_tick(workspace, due_peer_sync_ids)
    payload = ensure_control_plane_manifest(workspace)
    scheduler_payload = dict(payload.get("scheduler", {}))
    history = [dict(item) for item in list(scheduler_payload.get("history", []))]
    subscriptions = [
        _normalize_job_subscription(item)
        for item in list(scheduler_payload.get("job_subscriptions", []))
    ]
    due_ids = {subscription_id for subscription_id in due_job_subscription_ids if subscription_id}
    updated_subscriptions: List[Dict[str, object]] = []
    for subscription in subscriptions:
        updated = _normalize_job_subscription(subscription)
        if not bool(updated.get("enabled", False)):
            updated_subscriptions.append(updated)
            continue
        updated["last_scheduler_tick_at"] = tick_at
        if str(updated.get("subscription_id", "")) in due_ids:
            updated["last_due_at"] = tick_at
            updated["last_tick_status"] = "enqueued" if enqueued_job_ids else "due"
            updated = _advance_job_subscription(updated, tick_at)
        else:
            updated["last_tick_status"] = "waiting"
        updated_subscriptions.append(updated)
    history.insert(
        0,
        {
            "tick_at": tick_at,
            "action": action,
            "due_connector_ids": due_connector_ids,
            "due_peer_sync_ids": due_peer_sync_ids,
            "due_job_subscription_ids": due_job_subscription_ids,
            "enqueued_job_ids": enqueued_job_ids,
            "executed_run_manifest_path": workspace.relative_path(executed_run_manifest_path) if executed_run_manifest_path else "",
        },
    )
    scheduler_payload.update(
        {
            "last_tick_at": tick_at,
            "last_action": action,
            "last_due_connector_ids": due_connector_ids,
            "last_due_peer_sync_ids": due_peer_sync_ids,
            "last_due_job_subscription_ids": due_job_subscription_ids,
            "last_enqueued_job_ids": enqueued_job_ids,
            "last_executed_run_manifest_path": workspace.relative_path(executed_run_manifest_path)
            if executed_run_manifest_path
            else "",
            "job_subscriptions": _sorted_job_subscriptions(updated_subscriptions),
            "history": history[:50],
        }
    )
    payload["scheduler"] = scheduler_payload
    _write_control_plane_manifest(workspace, payload)
    return SchedulerTickResult(
        action=action,
        due_connector_count=len(due_connectors),
        due_connector_ids=due_connector_ids,
        due_peer_sync_ids=due_peer_sync_ids,
        due_job_subscription_count=len(due_job_subscriptions),
        due_job_subscription_ids=due_job_subscription_ids,
        enqueued_job_ids=enqueued_job_ids,
        executed_run_manifest_path=executed_run_manifest_path,
    )


def create_control_plane_server(
    workspace: "Workspace",
    host: str = "127.0.0.1",
    port: int = 8766,
) -> ThreadingHTTPServer:
    handler = partial(_ControlPlaneHandler, workspace=workspace)
    return ThreadingHTTPServer((host, port), handler)


def default_control_plane_manifest() -> Dict[str, object]:
    now = utc_timestamp()
    return {
        "schema_version": 1,
        "workspace_id": f"workspace-{secrets.token_hex(8)}",
        "created_at": now,
        "generated_at": now,
        "invites": [],
        "tokens": [],
        "scheduler": {
            "last_tick_at": "",
            "last_action": "",
            "last_due_connector_ids": [],
            "last_due_peer_sync_ids": [],
            "last_due_job_subscription_ids": [],
            "last_enqueued_job_ids": [],
            "last_executed_run_manifest_path": "",
            "job_subscriptions": [],
            "history": [],
        },
        "summary": _build_summary([], []),
    }


def _normalize_control_plane_manifest(payload: Dict[str, object]) -> Dict[str, object]:
    invites = _sorted_invites([dict(item) for item in list(payload.get("invites", [])) if dict(item).get("invite_id")])
    tokens = _sorted_tokens([dict(item) for item in list(payload.get("tokens", [])) if dict(item).get("token_id")])
    scheduler = dict(payload.get("scheduler", {}))
    return {
        "schema_version": 1,
        "workspace_id": str(payload.get("workspace_id", "")) or f"workspace-{secrets.token_hex(8)}",
        "created_at": str(payload.get("created_at", "")) or utc_timestamp(),
        "generated_at": str(payload.get("generated_at", "")) or utc_timestamp(),
        "invites": invites,
        "tokens": tokens,
        "scheduler": {
            "last_tick_at": str(scheduler.get("last_tick_at", "")),
            "last_action": str(scheduler.get("last_action", "")),
            "last_due_connector_ids": [str(item) for item in list(scheduler.get("last_due_connector_ids", []))],
            "last_due_peer_sync_ids": [str(item) for item in list(scheduler.get("last_due_peer_sync_ids", []))],
            "last_due_job_subscription_ids": [str(item) for item in list(scheduler.get("last_due_job_subscription_ids", []))],
            "last_enqueued_job_ids": [str(item) for item in list(scheduler.get("last_enqueued_job_ids", []))],
            "last_executed_run_manifest_path": str(scheduler.get("last_executed_run_manifest_path", "")),
            "job_subscriptions": _sorted_job_subscriptions(
                [
                    _normalize_job_subscription(item)
                    for item in list(scheduler.get("job_subscriptions", []))
                    if isinstance(item, dict)
                ]
            ),
            "history": [
                {
                    "tick_at": str(dict(item).get("tick_at", "")),
                    "action": str(dict(item).get("action", "")),
                    "due_connector_ids": [str(connector_id) for connector_id in list(dict(item).get("due_connector_ids", []))],
                    "due_peer_sync_ids": [str(peer_id) for peer_id in list(dict(item).get("due_peer_sync_ids", []))],
                    "due_job_subscription_ids": [
                        str(subscription_id) for subscription_id in list(dict(item).get("due_job_subscription_ids", []))
                    ],
                    "enqueued_job_ids": [str(job_id) for job_id in list(dict(item).get("enqueued_job_ids", []))],
                    "executed_run_manifest_path": str(dict(item).get("executed_run_manifest_path", "")),
                }
                for item in list(scheduler.get("history", []))
                if isinstance(item, dict)
            ],
        },
        "summary": _build_summary(invites, tokens),
    }


def _build_summary(invites: List[Dict[str, object]], tokens: List[Dict[str, object]]) -> Dict[str, object]:
    return {
        "invite_count": len(invites),
        "pending_invite_count": sum(1 for item in invites if str(item.get("status", "")) == "pending"),
        "accepted_invite_count": sum(1 for item in invites if str(item.get("status", "")) == "accepted"),
        "token_count": len(tokens),
        "active_token_count": sum(1 for item in tokens if str(item.get("status", "")) == "active"),
        "revoked_token_count": sum(1 for item in tokens if str(item.get("status", "")) == "revoked"),
        "expired_token_count": sum(1 for item in tokens if str(item.get("status", "")) == "expired"),
    }


def _write_control_plane_manifest(workspace: "Workspace", payload: Dict[str, object]) -> None:
    normalized = _normalize_control_plane_manifest(payload)
    normalized["generated_at"] = utc_timestamp()
    workspace.control_plane_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.control_plane_manifest_path.write_text(
        json.dumps(normalized, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _scheduler_status_payload(workspace: "Workspace") -> Dict[str, object]:
    payload = ensure_control_plane_manifest(workspace)
    scheduler = dict(payload.get("scheduler", {}))
    due_connector_ids = [str(item.get("connector_id", "")) for item in list_due_connectors(workspace)]
    due_peer_sync_ids = [str(item.get("peer_id", "")) for item in list_due_shared_peer_syncs(workspace)]
    subscriptions = list_job_subscriptions(workspace)
    due_job_subscription_ids = [str(item.get("subscription_id", "")) for item in list_due_job_subscriptions(workspace)]
    return {
        "due_connector_count": len(due_connector_ids),
        "due_connector_ids": due_connector_ids,
        "due_peer_sync_count": len(due_peer_sync_ids),
        "due_peer_sync_ids": due_peer_sync_ids,
        "scheduled_job_count": len(subscriptions),
        "due_job_subscription_count": len(due_job_subscription_ids),
        "due_job_subscription_ids": due_job_subscription_ids,
        "last_tick_at": str(scheduler.get("last_tick_at", "")),
        "last_action": str(scheduler.get("last_action", "")),
        "last_enqueued_job_ids": [str(item) for item in list(scheduler.get("last_enqueued_job_ids", []))],
        "history": [dict(item) for item in list(scheduler.get("history", []))],
    }


def _scheduler_jobs_payload(workspace: "Workspace") -> Dict[str, object]:
    items = list_job_subscriptions(workspace)
    return {
        "items": items,
        "summary": {
            "subscription_count": len(items),
            "enabled_count": sum(1 for item in items if bool(item.get("enabled", False))),
            "due_count": sum(1 for item in items if _job_subscription_is_due(item)),
        },
        "state_paths": {
            "control_plane_manifest_path": workspace.relative_path(workspace.control_plane_manifest_path),
        },
    }


def _find_collaboration_thread(workspace: "Workspace", artifact_path: str) -> Dict[str, object]:
    normalized_path = str(artifact_path).strip()
    payload = load_collaboration_manifest(workspace)
    for thread in list(payload.get("threads", [])):
        if str(thread.get("artifact_path", "")) == normalized_path:
            return dict(thread)
    return {}


def _connector_summary_payload(workspace: "Workspace") -> Dict[str, object]:
    connectors = list_connectors(workspace)
    due_connector_ids = [str(item.get("connector_id", "")) for item in list_due_connectors(workspace)]
    return {
        "connector_count": len(connectors),
        "due_connector_count": len(due_connector_ids),
        "due_connector_ids": due_connector_ids,
    }


def _review_payload(workspace: "Workspace") -> Dict[str, object]:
    snapshot = scan_workspace(workspace)
    queue = build_review_queue(workspace, snapshot)
    dismissed_items = list_dismissed_review_items(workspace)
    actions = read_review_actions(workspace)

    counts_by_kind: Dict[str, int] = {}
    for item in list(queue.get("items", [])):
        kind = str(item.get("kind", "unknown"))
        counts_by_kind[kind] = counts_by_kind.get(kind, 0) + 1

    return {
        "open_items": [dict(item) for item in list(queue.get("items", []))],
        "dismissed_items": dismissed_items,
        "action_state": actions,
        "summary": {
            "open_item_count": len(list(queue.get("items", []))),
            "dismissed_item_count": len(dismissed_items),
            "counts_by_kind": dict(sorted(counts_by_kind.items())),
        },
        "state_paths": {
            "review_queue_path": workspace.relative_path(workspace.review_queue_manifest_path),
            "review_actions_path": workspace.relative_path(workspace.review_actions_manifest_path),
        },
    }


def _run_history_payload(workspace: "Workspace") -> Dict[str, object]:
    items: List[Dict[str, object]] = []
    counts_by_kind: Dict[str, int] = {}
    counts_by_status: Dict[str, int] = {}
    manifests = sorted(workspace.runs_dir.glob("*.json"), reverse=True)
    for path in manifests:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        run_kind = str(manifest.get("run_kind", "unknown"))
        status = str(manifest.get("status", "unknown"))
        counts_by_kind[run_kind] = counts_by_kind.get(run_kind, 0) + 1
        counts_by_status[status] = counts_by_status.get(status, 0) + 1
        items.append(
            {
                "run_id": path.stem,
                "run_kind": run_kind,
                "status": status,
                "generated_at": str(manifest.get("generated_at", "")),
                "mode": str(manifest.get("mode", "")),
                "label": str(manifest.get("run_label", manifest.get("question", path.stem))),
                "question": str(manifest.get("question", "")),
                "path": workspace.relative_path(path),
            }
        )
    return {
        "items": items,
        "summary": {
            "total_count": len(items),
            "counts_by_kind": dict(sorted(counts_by_kind.items())),
            "counts_by_status": dict(sorted(counts_by_status.items())),
        },
        "state_paths": {
            "runs_dir": workspace.relative_path(workspace.runs_dir),
        },
    }


def _sync_history_payload(workspace: "Workspace") -> Dict[str, object]:
    events = list_sync_events(workspace)
    counts_by_operation: Dict[str, int] = {}
    items: List[Dict[str, object]] = []
    for event in events:
        operation = str(event.get("operation", "unknown"))
        counts_by_operation[operation] = counts_by_operation.get(operation, 0) + 1
        items.append(
            {
                "sync_id": str(event.get("sync_id", "")),
                "operation": operation,
                "status": str(event.get("status", "")),
                "generated_at": str(event.get("generated_at", "")),
                "file_count": int(event.get("file_count", 0) or 0),
                "path": workspace.relative_path(workspace.sync_manifests_dir / f"{event.get('sync_id', '')}.json"),
                "bundle_dir_relative": str(event.get("bundle_dir_relative", "")),
            }
        )
    return {
        "items": items,
        "summary": {
            "total_count": len(items),
            "counts_by_operation": dict(sorted(counts_by_operation.items())),
        },
        "state_paths": {
            "history_manifest_path": workspace.relative_path(workspace.sync_history_manifest_path),
            "manifests_dir": workspace.relative_path(workspace.sync_manifests_dir),
        },
    }


def _change_summary_payload(workspace: "Workspace") -> Dict[str, object]:
    items: List[Dict[str, object]] = []
    counts_by_trigger: Dict[str, int] = {}
    files = sorted(workspace.change_summaries_dir.glob("*.md"), reverse=True) if workspace.change_summaries_dir.exists() else []
    for path in files:
        trigger = path.stem.split("-", 1)[0] if "-" in path.stem else path.stem
        counts_by_trigger[trigger] = counts_by_trigger.get(trigger, 0) + 1
        items.append(
            {
                "name": path.name,
                "trigger": trigger,
                "path": workspace.relative_path(path),
            }
        )
    return {
        "items": items,
        "summary": {
            "total_count": len(items),
            "counts_by_trigger": dict(sorted(counts_by_trigger.items())),
        },
        "state_paths": {
            "change_summaries_dir": workspace.relative_path(workspace.change_summaries_dir),
        },
    }


def _serialize_connector_sync_result(workspace: "Workspace", result: ConnectorSyncResult) -> Dict[str, object]:
    return {
        "connector_id": result.connector_id,
        "connector_kind": result.connector_kind,
        "synced_count": result.synced_count,
        "registry_path": workspace.relative_path(result.registry_path),
        "change_summary_path": workspace.relative_path(result.change_summary_path),
        "run_manifest_path": workspace.relative_path(result.run_manifest_path),
        "result_paths": [workspace.relative_path(path) for path in result.result_paths],
    }


def _require_review_actor(workspace: "Workspace", actor: Dict[str, object], action_label: str) -> Dict[str, object]:
    return require_access_role(
        workspace,
        str(actor.get("principal_id", "")),
        REVIEW_ACTION_ROLES,
        action_label,
    )


def _require_control_admin_actor(workspace: "Workspace", actor: Dict[str, object], action_label: str) -> Dict[str, object]:
    return require_access_role(
        workspace,
        str(actor.get("principal_id", "")),
        OPERATOR_ACTION_ROLES,
        action_label,
    )


def _load_job_manifest(manifest_path: Path) -> Dict[str, object]:
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _record_scheduler_observations(workspace: "Workspace", due_connector_ids: List[str]) -> None:
    if not workspace.connector_registry_path.exists():
        return
    payload = json.loads(workspace.connector_registry_path.read_text(encoding="utf-8"))
    tick_at = utc_timestamp()
    normalized_due_ids = {connector_id for connector_id in due_connector_ids if connector_id}
    changed = False
    for connector in list(payload.get("connectors", [])):
        subscription = dict(connector.get("subscription", {}))
        if not bool(subscription.get("enabled", False)):
            continue
        subscription["last_scheduler_tick_at"] = tick_at
        if str(connector.get("connector_id", "")) in normalized_due_ids:
            subscription["last_due_at"] = tick_at
            subscription["last_tick_status"] = "due"
        else:
            subscription["last_tick_status"] = "waiting"
        connector["subscription"] = subscription
        changed = True
    if changed:
        workspace.connector_registry_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def _serialize_actor(actor: Dict[str, object]) -> Dict[str, str]:
    return {
        "principal_id": str(actor.get("principal_id", "")),
        "display_name": str(actor.get("display_name", "")),
        "role": str(actor.get("role", "")),
        "status": str(actor.get("status", "")),
    }


def _sorted_invites(invites: List[Dict[str, object]]) -> List[Dict[str, object]]:
    invites.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("invite_id", ""))), reverse=True)
    return invites


def _sorted_tokens(tokens: List[Dict[str, object]]) -> List[Dict[str, object]]:
    tokens.sort(key=lambda item: (str(item.get("created_at", "")), str(item.get("token_id", ""))), reverse=True)
    return tokens


def _token_is_expired(token: Dict[str, object]) -> bool:
    expires_at = str(token.get("expires_at", "")).strip()
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) <= datetime.now(timezone.utc)
    except ValueError:
        return False


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _resolve_workspace_relative_path(workspace: "Workspace", relative_path: str) -> Path:
    normalized = relative_path.strip()
    if not normalized:
        raise ControlPlaneError("A workspace-relative path is required.")
    candidate = (workspace.root / normalized).resolve()
    try:
        candidate.relative_to(workspace.root)
    except ValueError as error:
        raise ControlPlaneError("Artifact paths must stay within the workspace root.") from error
    return candidate


def _artifact_preview_payload(workspace: "Workspace", relative_path: str) -> Dict[str, object]:
    artifact_path = _resolve_workspace_relative_path(workspace, relative_path)
    if not artifact_path.exists() or not artifact_path.is_file():
        raise ControlPlaneError(f"Could not find artifact at {relative_path}.")

    mime_type = mimetypes.guess_type(artifact_path.name)[0] or "application/octet-stream"
    size_bytes = artifact_path.stat().st_size
    artifact: Dict[str, object] = {
        "path": workspace.relative_path(artifact_path),
        "size_bytes": size_bytes,
        "mime_type": mime_type,
        "kind": "binary",
    }

    raw = artifact_path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        artifact["sha256"] = hashlib.sha256(raw).hexdigest()
        return artifact

    artifact["kind"] = "text"
    artifact["line_count"] = len(text.splitlines()) or (1 if text else 0)
    artifact["excerpt"] = text[:4000]
    return artifact


class _ControlPlaneHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, workspace: "Workspace", **kwargs) -> None:
        self._workspace = workspace
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            actor = self._authenticate(["control.read"])
        except ControlPlaneError as error:
            self._send_error(403, str(error))
            return

        if parsed.path == "/api/status":
            payload = ensure_control_plane_manifest(self._workspace)
            self._send_json(
                200,
                {
                    "workspace": {
                        "root": self._workspace.root.as_posix(),
                        "workspace_id": payload.get("workspace_id", ""),
                    },
                    "actor": _serialize_actor(actor),
                    "summary": dict(payload.get("summary", {})),
                },
            )
            return

        if parsed.path == "/api/workspace":
            payload = ensure_control_plane_manifest(self._workspace)
            ensure_shared_workspace_manifest(self._workspace)
            self._send_json(
                200,
                {
                    "workspace": {
                        "root": self._workspace.root.as_posix(),
                        "workspace_id": payload.get("workspace_id", ""),
                    },
                    "sharing": sharing_summary(self._workspace),
                    "actor": _serialize_actor(actor),
                },
            )
            return

        if parsed.path == "/api/share":
            payload = load_shared_workspace_manifest(self._workspace)
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    "workspace_id": str(payload.get("workspace_id", "")),
                    "workspace_name": str(payload.get("workspace_name", "")),
                    "published_control_plane_url": str(payload.get("published_control_plane_url", "")),
                    "trust_policy": dict(payload.get("trust_policy", {})),
                    "summary": sharing_summary(self._workspace),
                    "peers": [dict(item) for item in list(payload.get("peers", []))],
                },
            )
            return

        if parsed.path == "/api/access":
            payload = load_access_manifest(self._workspace)
            members = [dict(item) for item in list(payload.get("members", []))]
            counts_by_role: Dict[str, int] = {}
            for member in members:
                role = str(member.get("role", "viewer"))
                counts_by_role[role] = counts_by_role.get(role, 0) + 1
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    "members": members,
                    "summary": {
                        "member_count": len(members),
                        "counts_by_role": dict(sorted(counts_by_role.items())),
                    },
                },
            )
            return

        if parsed.path == "/api/invites":
            payload = ensure_control_plane_manifest(self._workspace)
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    "invites": [dict(item) for item in list(payload.get("invites", []))],
                    "summary": dict(payload.get("summary", {})),
                },
            )
            return

        if parsed.path == "/api/tokens":
            tokens = list_control_plane_tokens(self._workspace)
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    "tokens": tokens,
                    "summary": {
                        "total_count": len(tokens),
                        "active_count": sum(1 for item in tokens if str(item.get("status", "")) == "active"),
                        "revoked_count": sum(1 for item in tokens if str(item.get("status", "")) == "revoked"),
                        "expired_count": sum(1 for item in tokens if str(item.get("status", "")) == "expired"),
                    },
                },
            )
            return

        if parsed.path == "/api/collab":
            payload = load_collaboration_manifest(self._workspace)
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    "threads": [dict(item) for item in list(payload.get("threads", []))],
                    "summary": dict(payload.get("summary", {})),
                },
            )
            return

        if parsed.path == "/api/review":
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    **_review_payload(self._workspace),
                },
            )
            return

        if parsed.path == "/api/notifications":
            write_notifications_manifest(self._workspace)
            payload = json.loads(self._workspace.notifications_manifest_path.read_text(encoding="utf-8"))
            self._send_json(200, {"actor": _serialize_actor(actor), **payload})
            return

        if parsed.path == "/api/audit":
            from cognisync.observability import write_audit_manifest

            write_audit_manifest(self._workspace)
            payload = json.loads(self._workspace.audit_manifest_path.read_text(encoding="utf-8"))
            self._send_json(200, {"actor": _serialize_actor(actor), **payload})
            return

        if parsed.path == "/api/usage":
            from cognisync.observability import write_usage_manifest

            write_usage_manifest(self._workspace)
            payload = json.loads(self._workspace.usage_manifest_path.read_text(encoding="utf-8"))
            self._send_json(200, {"actor": _serialize_actor(actor), **payload})
            return

        if parsed.path == "/api/connectors":
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    "summary": _connector_summary_payload(self._workspace),
                    "connectors": [dict(item) for item in list_connectors(self._workspace)],
                },
            )
            return

        if parsed.path == "/api/scheduler":
            self._send_json(200, _scheduler_status_payload(self._workspace))
            return

        if parsed.path == "/api/scheduler/jobs":
            self._send_json(
                200,
                {
                    "actor": _serialize_actor(actor),
                    **_scheduler_jobs_payload(self._workspace),
                },
            )
            return

        if parsed.path == "/api/jobs":
            queue_payload = {}
            if self._workspace.job_queue_manifest_path.exists():
                queue_payload = json.loads(self._workspace.job_queue_manifest_path.read_text(encoding="utf-8"))
            self._send_json(200, queue_payload or {"jobs": [], "queued_count": 0, "job_count": 0})
            return

        if parsed.path == "/api/runs":
            self._send_json(200, {"actor": _serialize_actor(actor), **_run_history_payload(self._workspace)})
            return

        if parsed.path == "/api/sync":
            self._send_json(200, {"actor": _serialize_actor(actor), **_sync_history_payload(self._workspace)})
            return

        if parsed.path == "/api/change-summaries":
            self._send_json(200, {"actor": _serialize_actor(actor), **_change_summary_payload(self._workspace)})
            return

        if parsed.path == "/api/artifacts/preview":
            requested_path = str(query.get("path", [""])[0] or "")
            try:
                artifact_payload = _artifact_preview_payload(self._workspace, requested_path)
            except ControlPlaneError as error:
                self._send_error(400, str(error))
                return
            self._send_json(200, {"actor": _serialize_actor(actor), "artifact": artifact_payload})
            return

        if parsed.path == "/api/workers":
            self._send_json(200, read_worker_registry(self._workspace))
            return

        self._send_error(404, f"Unknown control-plane endpoint: {parsed.path}")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self._read_json_body()
            if parsed.path == "/api/access/grant":
                actor = self._authenticate(["control.admin"])
                actor = _require_control_admin_actor(self._workspace, actor, "manage workspace access over the control plane")
                member = grant_access_member(
                    self._workspace,
                    principal_id=str(payload.get("principal_id", "")),
                    role=str(payload.get("role", "")),
                    display_name=str(payload.get("display_name", "")) or None,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "member": member})
                return
            if parsed.path == "/api/access/revoke":
                actor = self._authenticate(["control.admin"])
                actor = _require_control_admin_actor(self._workspace, actor, "manage workspace access over the control plane")
                member = revoke_access_member(self._workspace, str(payload.get("principal_id", "")))
                self._send_json(200, {"actor": _serialize_actor(actor), "member": member})
                return
            if parsed.path == "/api/invites/create":
                actor = self._authenticate(["control.admin"])
                actor = _require_control_admin_actor(self._workspace, actor, "manage control-plane invites over the control plane")
                invite = create_control_plane_invite(
                    self._workspace,
                    principal_id=str(payload.get("principal_id", "")),
                    role=str(payload.get("role", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "invite": invite})
                return
            if parsed.path == "/api/invites/accept":
                actor = self._authenticate(["control.admin"])
                actor = _require_control_admin_actor(self._workspace, actor, "manage control-plane invites over the control plane")
                invite = accept_control_plane_invite(
                    self._workspace,
                    invite_ref=str(payload.get("invite_ref", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "invite": invite})
                return
            if parsed.path == "/api/tokens/issue":
                actor = self._authenticate(["control.admin"])
                actor = _require_control_admin_actor(self._workspace, actor, "issue control-plane tokens over the control plane")
                token_metadata, raw_token = issue_control_plane_token(
                    self._workspace,
                    principal_id=str(payload.get("principal_id", "")),
                    scopes=[str(item) for item in list(payload.get("scopes", []))] or None,
                    actor_id=str(actor.get("principal_id", "")),
                    description=str(payload.get("description", "")),
                    expires_in_hours=int(payload["expires_in_hours"]) if payload.get("expires_in_hours") is not None else None,
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "token": raw_token,
                        "token_metadata": token_metadata,
                    },
                )
                return
            if parsed.path == "/api/tokens/revoke":
                actor = self._authenticate(["control.admin"])
                actor = _require_control_admin_actor(self._workspace, actor, "revoke control-plane tokens over the control plane")
                token = revoke_control_plane_token(
                    self._workspace,
                    token_id=str(payload.get("token_id", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "token": token})
                return
            if parsed.path == "/api/jobs/enqueue/research":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                question = str(payload.get("question", "")).strip()
                if not question:
                    raise ControlPlaneError("A research question is required.")
                manifest_path = enqueue_research_job(
                    self._workspace,
                    question=question,
                    profile_name=str(payload.get("profile_name", "")) or None,
                    limit=int(payload.get("limit", 5) or 5),
                    mode=str(payload.get("mode", "wiki") or "wiki"),
                    slides=bool(payload.get("slides", False)),
                    job_profile=str(payload.get("job_profile", "")) or None,
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/compile":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                manifest_path = enqueue_compile_job(
                    self._workspace,
                    profile_name=str(payload.get("profile_name", "")) or None,
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/lint":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                manifest_path = enqueue_lint_job(self._workspace, requested_by=actor)
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/maintain":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                manifest_path = enqueue_maintain_job(
                    self._workspace,
                    max_concepts=int(payload.get("max_concepts", 10) or 10),
                    max_merges=int(payload.get("max_merges", 10) or 10),
                    max_backlinks=int(payload.get("max_backlinks", 10) or 10),
                    max_conflicts=int(payload.get("max_conflicts", 10) or 10),
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/connector-sync":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                connector_id = str(payload.get("connector_id", "")).strip()
                if not connector_id:
                    raise ControlPlaneError("A connector id is required.")
                manifest_path = enqueue_connector_sync_job(
                    self._workspace,
                    connector_id=connector_id,
                    force=bool(payload.get("force", False)),
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/connector-sync-all":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                manifest_path = enqueue_connector_sync_all_job(
                    self._workspace,
                    force=bool(payload.get("force", False)),
                    limit=int(payload["limit"]) if payload.get("limit") is not None else None,
                    scheduled_only=bool(payload.get("scheduled_only", False)),
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/sync-export":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                manifest_path = enqueue_sync_export_job(
                    self._workspace,
                    peer_ref=str(payload.get("peer_ref", "")) or None,
                    output_dir=str(payload.get("output_dir", "")) or None,
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/ingest-url":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                url = str(payload.get("url", "")).strip()
                if not url:
                    raise ControlPlaneError("A source URL is required.")
                manifest_path = enqueue_ingest_url_job(
                    self._workspace,
                    url=url,
                    name=str(payload.get("name", "")) or None,
                    force=bool(payload.get("force", False)),
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/ingest-repo":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                source = str(payload.get("source", "")).strip()
                if not source:
                    raise ControlPlaneError("A repository source is required.")
                manifest_path = enqueue_ingest_repo_job(
                    self._workspace,
                    source=source,
                    name=str(payload.get("name", "")) or None,
                    force=bool(payload.get("force", False)),
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/jobs/enqueue/ingest-sitemap":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "enqueue jobs over the control plane",
                )
                source = str(payload.get("source", "")).strip()
                if not source:
                    raise ControlPlaneError("A sitemap source is required.")
                manifest_path = enqueue_ingest_sitemap_job(
                    self._workspace,
                    source=source,
                    force=bool(payload.get("force", False)),
                    limit=int(payload["limit"]) if payload.get("limit") is not None else None,
                    requested_by=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "job": _load_job_manifest(manifest_path)})
                return
            if parsed.path == "/api/sync/export":
                actor = self._authenticate(["jobs.run"])
                result = export_sync_bundle(
                    self._workspace,
                    actor_id=str(actor.get("principal_id", "")),
                    peer_ref=str(payload.get("peer_ref", "")) or None,
                )
                response_payload: Dict[str, object] = {
                    "actor": _serialize_actor(actor),
                    "bundle": json.loads(result.manifest_path.read_text(encoding="utf-8")),
                    "sync_event": json.loads(result.event_manifest_path.read_text(encoding="utf-8")),
                    "history_path": self._workspace.relative_path(result.history_manifest_path),
                }
                if bool(payload.get("inline_archive", False)):
                    response_payload["archive_base64"] = base64.b64encode(
                        encode_sync_bundle_archive(result.directory)
                    ).decode("ascii")
                self._send_json(200, response_payload)
                return
            if parsed.path == "/api/sync/import":
                actor = self._authenticate(["jobs.run"])
                archive_base64 = str(payload.get("archive_base64", "")).strip()
                if not archive_base64:
                    raise ControlPlaneError("A base64-encoded sync archive is required.")
                try:
                    archive_bytes = base64.b64decode(archive_base64.encode("ascii"), validate=True)
                except (ValueError, binascii.Error) as error:
                    raise ControlPlaneError("The provided sync archive is not valid base64.") from error
                result = import_sync_bundle_archive(
                    self._workspace,
                    archive_bytes=archive_bytes,
                    actor_id=str(actor.get("principal_id", "")),
                    from_peer=str(payload.get("from_peer", "")) or None,
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "bundle_manifest": json.loads(result.manifest_path.read_text(encoding="utf-8")),
                        "sync_event": json.loads(result.event_manifest_path.read_text(encoding="utf-8")),
                        "history_path": self._workspace.relative_path(result.history_manifest_path),
                    },
                )
                return
            if parsed.path == "/api/review/accept-concept":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                concept_path = accept_concept_candidate(self._workspace, str(payload.get("slug", "")))
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "concept_path": self._workspace.relative_path(concept_path),
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/review/resolve-merge":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                concept_path = resolve_entity_merge(
                    self._workspace,
                    canonical_label=str(payload.get("canonical_label", "")),
                    preferred_label=str(payload.get("preferred_label", "")) or None,
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "concept_path": self._workspace.relative_path(concept_path),
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/review/apply-backlink":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                path = apply_backlink_suggestion(self._workspace, str(payload.get("target_path", "")))
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "path": self._workspace.relative_path(path),
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/review/file-conflict":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                note_path = file_conflict_review(self._workspace, str(payload.get("subject", "")))
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "note_path": self._workspace.relative_path(note_path),
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/review/dismiss":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                dismissed = dismiss_review_item(
                    self._workspace,
                    review_id=str(payload.get("review_id", "")),
                    reason=str(payload.get("reason", "")),
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "dismissed": dismissed,
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/review/reopen":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                reopened = reopen_review_item(self._workspace, str(payload.get("review_id", "")))
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "reopened": reopened,
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/review/clear-dismissed":
                actor = self._authenticate(["review.run"])
                actor = _require_review_actor(self._workspace, actor, "apply review actions over the control plane")
                cleared = clear_dismissed_review_item(self._workspace, str(payload.get("review_id", "")))
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "cleared": cleared,
                        "review": _review_payload(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/jobs/claim-next":
                actor = self._authenticate(["jobs.claim"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "claim jobs over the control plane",
                )
                result = claim_next_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
                    worker_capabilities=[str(item) for item in list(payload.get("worker_capabilities", []))],
                )
                self._send_json(200, {"actor": _serialize_actor(actor), **result.__dict__})
                return
            if parsed.path == "/api/jobs/dispatch-next":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "dispatch jobs over the control plane",
                )
                result = dispatch_next_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
                    worker_capabilities=[str(item) for item in list(payload.get("worker_capabilities", []))],
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "job": {
                            "job_id": result.job_id,
                            "job_type": result.job_type,
                            "worker_id": result.worker_id,
                            "lease_expires_at": result.lease_expires_at,
                            "status": result.status,
                            "worker_capability": result.worker_capability,
                            "parameters": result.parameters,
                            "requested_by": result.requested_by,
                            "job_manifest_path": self._workspace.relative_path(result.job_manifest_path),
                            "queue_manifest_path": self._workspace.relative_path(result.queue_manifest_path),
                        },
                    },
                )
                return
            if parsed.path == "/api/jobs/heartbeat":
                actor = self._authenticate(["jobs.heartbeat"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "renew job leases over the control plane",
                )
                result = heartbeat_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
                    worker_capabilities=[str(item) for item in list(payload.get("worker_capabilities", []))],
                )
                self._send_json(200, {"actor": _serialize_actor(actor), **result.__dict__})
                return
            if parsed.path == "/api/jobs/run-next":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "run jobs over the control plane",
                )
                result = run_next_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
                    worker_capabilities=[str(item) for item in list(payload.get("worker_capabilities", []))],
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "job_id": result.job_id,
                        "job_type": result.job_type,
                        "status": result.status,
                        "job_manifest_path": self._workspace.relative_path(result.job_manifest_path),
                        "queue_manifest_path": self._workspace.relative_path(result.queue_manifest_path),
                    },
                )
                return
            if parsed.path == "/api/jobs/complete":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "complete dispatched jobs over the control plane",
                )
                archive_base64 = str(payload.get("sync_archive_base64", "")).strip()
                archive_bytes: Optional[bytes] = None
                if archive_base64:
                    try:
                        archive_bytes = base64.b64decode(archive_base64.encode("ascii"), validate=True)
                    except (ValueError, binascii.Error) as error:
                        raise ControlPlaneError("The provided sync archive is not valid base64.") from error
                result = complete_dispatched_job(
                    self._workspace,
                    job_id=str(payload.get("job_id", "")),
                    worker_id=str(payload.get("worker_id", "")),
                    result_payload=dict(payload.get("result", {})),
                    sync_archive_bytes=archive_bytes,
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "job_id": result.job_id,
                        "job_type": result.job_type,
                        "status": result.status,
                        "job_manifest_path": self._workspace.relative_path(result.job_manifest_path),
                        "queue_manifest_path": self._workspace.relative_path(result.queue_manifest_path),
                        "job": _load_job_manifest(result.job_manifest_path),
                    },
                )
                return
            if parsed.path == "/api/jobs/fail":
                actor = self._authenticate(["jobs.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "fail dispatched jobs over the control plane",
                )
                result = fail_dispatched_job(
                    self._workspace,
                    job_id=str(payload.get("job_id", "")),
                    worker_id=str(payload.get("worker_id", "")),
                    error_message=str(payload.get("error", "")).strip() or "remote execution failed",
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "job_id": result.job_id,
                        "job_type": result.job_type,
                        "status": result.status,
                        "job_manifest_path": self._workspace.relative_path(result.job_manifest_path),
                        "queue_manifest_path": self._workspace.relative_path(result.queue_manifest_path),
                        "job": _load_job_manifest(result.job_manifest_path),
                    },
                )
                return
            if parsed.path == "/api/scheduler/tick":
                actor = self._authenticate(["scheduler.run"])
                result = run_scheduler_tick(
                    self._workspace,
                    actor_id=str(actor.get("principal_id", "")),
                    enqueue_only=bool(payload.get("enqueue_only", True)),
                    force=bool(payload.get("force", False)),
                    limit=int(payload["limit"]) if payload.get("limit") is not None else None,
                )
                self._send_json(
                    200,
                    {
                        "action": result.action,
                        "due_connector_count": result.due_connector_count,
                        "due_connector_ids": result.due_connector_ids,
                        "due_peer_sync_count": len(result.due_peer_sync_ids),
                        "due_peer_sync_ids": result.due_peer_sync_ids,
                        "due_job_subscription_count": result.due_job_subscription_count,
                        "due_job_subscription_ids": result.due_job_subscription_ids,
                        "enqueued_job_ids": result.enqueued_job_ids,
                        "executed_run_manifest_path": self._workspace.relative_path(result.executed_run_manifest_path)
                        if result.executed_run_manifest_path
                        else "",
                    },
                )
                return
            if parsed.path == "/api/scheduler/jobs/research":
                actor = self._authenticate(["scheduler.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "manage scheduled jobs over the control plane",
                )
                subscription = schedule_job_subscription(
                    self._workspace,
                    job_type="research",
                    every_hours=int(payload.get("every_hours", 0) or 0),
                    parameters={
                        "question": str(payload.get("question", "")),
                        "profile_name": str(payload.get("profile_name", "")) or None,
                        "limit": int(payload.get("limit", 5) or 5),
                        "mode": str(payload.get("mode", "wiki") or "wiki"),
                        "slides": bool(payload.get("slides", False)),
                        "job_profile": str(payload.get("job_profile", "")) or DEFAULT_RESEARCH_JOB_PROFILE,
                    },
                    label=str(payload.get("label", "")) or None,
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "subscription": subscription})
                return
            if parsed.path == "/api/scheduler/jobs/compile":
                actor = self._authenticate(["scheduler.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "manage scheduled jobs over the control plane",
                )
                subscription = schedule_job_subscription(
                    self._workspace,
                    job_type="compile",
                    every_hours=int(payload.get("every_hours", 0) or 0),
                    parameters={
                        "profile_name": str(payload.get("profile_name", "")) or None,
                    },
                    label=str(payload.get("label", "")) or None,
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "subscription": subscription})
                return
            if parsed.path == "/api/scheduler/jobs/lint":
                actor = self._authenticate(["scheduler.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "manage scheduled jobs over the control plane",
                )
                subscription = schedule_job_subscription(
                    self._workspace,
                    job_type="lint",
                    every_hours=int(payload.get("every_hours", 0) or 0),
                    parameters={},
                    label=str(payload.get("label", "")) or None,
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "subscription": subscription})
                return
            if parsed.path == "/api/scheduler/jobs/maintain":
                actor = self._authenticate(["scheduler.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "manage scheduled jobs over the control plane",
                )
                subscription = schedule_job_subscription(
                    self._workspace,
                    job_type="maintain",
                    every_hours=int(payload.get("every_hours", 0) or 0),
                    parameters={
                        "max_concepts": int(payload.get("max_concepts", 10) or 10),
                        "max_merges": int(payload.get("max_merges", 10) or 10),
                        "max_backlinks": int(payload.get("max_backlinks", 10) or 10),
                        "max_conflicts": int(payload.get("max_conflicts", 10) or 10),
                    },
                    label=str(payload.get("label", "")) or None,
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "subscription": subscription})
                return
            if parsed.path == "/api/scheduler/jobs/remove":
                actor = self._authenticate(["scheduler.run"])
                actor = _require_control_admin_actor(
                    self._workspace,
                    actor,
                    "manage scheduled jobs over the control plane",
                )
                subscription = disable_job_subscription(
                    self._workspace,
                    subscription_id=str(payload.get("subscription_id", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "subscription": subscription})
                return
            if parsed.path == "/api/collab/request-review":
                actor = self._authenticate(["control.read"])
                thread = request_review(
                    self._workspace,
                    artifact_path=str(payload.get("artifact_path", "")),
                    actor_id=str(actor.get("principal_id", "")),
                    assignee_ids=[str(item) for item in list(payload.get("assignee_ids", []))],
                    note=str(payload.get("note", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "thread": thread})
                return
            if parsed.path == "/api/collab/comment":
                actor = self._authenticate(["control.read"])
                comment = add_comment(
                    self._workspace,
                    artifact_path=str(payload.get("artifact_path", "")),
                    actor_id=str(actor.get("principal_id", "")),
                    message=str(payload.get("message", "")),
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "comment": comment,
                        "thread": _find_collaboration_thread(self._workspace, str(payload.get("artifact_path", ""))),
                    },
                )
                return
            if parsed.path == "/api/collab/approve":
                actor = self._authenticate(["control.read"])
                decision = record_decision(
                    self._workspace,
                    artifact_path=str(payload.get("artifact_path", "")),
                    actor_id=str(actor.get("principal_id", "")),
                    decision="approved",
                    summary=str(payload.get("summary", "")),
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "decision": decision,
                        "thread": _find_collaboration_thread(self._workspace, str(payload.get("artifact_path", ""))),
                    },
                )
                return
            if parsed.path == "/api/collab/request-changes":
                actor = self._authenticate(["control.read"])
                decision = record_decision(
                    self._workspace,
                    artifact_path=str(payload.get("artifact_path", "")),
                    actor_id=str(actor.get("principal_id", "")),
                    decision="changes_requested",
                    summary=str(payload.get("summary", "")),
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "decision": decision,
                        "thread": _find_collaboration_thread(self._workspace, str(payload.get("artifact_path", ""))),
                    },
                )
                return
            if parsed.path == "/api/collab/resolve":
                actor = self._authenticate(["control.read"])
                thread = resolve_review(
                    self._workspace,
                    artifact_path=str(payload.get("artifact_path", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "thread": thread})
                return
            if parsed.path == "/api/share/set-policy":
                actor = self._authenticate(["control.read"])
                updated = set_shared_trust_policy(
                    self._workspace,
                    actor_id=str(actor.get("principal_id", "")),
                    allow_remote_workers=payload.get("allow_remote_workers"),
                    allow_sync_imports_from_peers=payload.get("allow_sync_imports_from_peers"),
                    default_peer_role=str(payload.get("default_peer_role", "")) or None,
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "trust_policy": dict(updated.get("trust_policy", {})),
                        "summary": sharing_summary(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/share/invite-peer":
                actor = self._authenticate(["control.read"])
                peer = invite_shared_peer(
                    self._workspace,
                    peer_id=str(payload.get("peer_id", "")),
                    role=str(payload.get("role", "")),
                    actor_id=str(actor.get("principal_id", "")),
                    base_url=str(payload.get("base_url", "")) or None,
                    capabilities=[str(item) for item in list(payload.get("capabilities", []))],
                    display_name=str(payload.get("display_name", "")) or None,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "peer": peer})
                return
            if parsed.path == "/api/share/accept-peer":
                actor = self._authenticate(["control.read"])
                peer = accept_shared_peer(
                    self._workspace,
                    peer_ref=str(payload.get("peer_ref", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "peer": peer})
                return
            if parsed.path == "/api/share/peers/role":
                actor = self._authenticate(["control.read"])
                peer = set_shared_peer_role(
                    self._workspace,
                    peer_ref=str(payload.get("peer_id", "")),
                    role=str(payload.get("role", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(
                    200,
                    {"actor": _serialize_actor(actor), "peer": peer, "sharing": sharing_summary(self._workspace)},
                )
                return
            if parsed.path == "/api/share/peers/suspend":
                actor = self._authenticate(["control.read"])
                peer = suspend_shared_peer(
                    self._workspace,
                    peer_ref=str(payload.get("peer_id", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(
                    200,
                    {"actor": _serialize_actor(actor), "peer": peer, "sharing": sharing_summary(self._workspace)},
                )
                return
            if parsed.path == "/api/share/peers/remove":
                actor = self._authenticate(["control.read"])
                removed = remove_shared_peer(
                    self._workspace,
                    peer_ref=str(payload.get("peer_id", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "removed_peer_id": str(removed.get("peer_id", "")),
                        "sharing": sharing_summary(self._workspace),
                    },
                )
                return
            if parsed.path == "/api/share/issue-peer-bundle":
                actor = self._authenticate(["control.read"])
                bundle = issue_shared_peer_bundle(
                    self._workspace,
                    peer_ref=str(payload.get("peer_ref", "")),
                    actor_id=str(actor.get("principal_id", "")),
                    scopes=[str(item) for item in list(payload.get("scopes", []))] or None,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "bundle": bundle})
                return
            if parsed.path == "/api/share/subscribe-sync":
                actor = self._authenticate(["control.read"])
                peer = subscribe_shared_peer_sync(
                    self._workspace,
                    peer_ref=str(payload.get("peer_ref", "")),
                    every_hours=int(payload.get("every_hours", 0) or 0),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "peer": peer})
                return
            if parsed.path == "/api/share/unsubscribe-sync":
                actor = self._authenticate(["control.read"])
                peer = unsubscribe_shared_peer_sync(
                    self._workspace,
                    peer_ref=str(payload.get("peer_ref", "")),
                    actor_id=str(actor.get("principal_id", "")),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "peer": peer})
                return
            if parsed.path == "/api/connectors/sync":
                actor = self._authenticate(["connectors.sync"])
                result = sync_connector(
                    self._workspace,
                    connector_id=str(payload.get("connector_id", "")),
                    force=bool(payload.get("force", False)),
                    actor=actor,
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "result": _serialize_connector_sync_result(self._workspace, result),
                    },
                )
                return
            if parsed.path == "/api/connectors/add":
                actor = self._authenticate(["connectors.sync"])
                connector = add_connector(
                    self._workspace,
                    kind=str(payload.get("kind", "")),
                    source=str(payload.get("source", "")),
                    name=str(payload.get("name", "")) or None,
                    actor=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "connector": connector})
                return
            if parsed.path == "/api/connectors/subscribe":
                actor = self._authenticate(["connectors.sync"])
                connector = subscribe_connector(
                    self._workspace,
                    connector_id=str(payload.get("connector_id", "")),
                    every_hours=int(payload["every_hours"]) if payload.get("every_hours") is not None else None,
                    weekdays=[str(item) for item in list(payload.get("weekdays", []))],
                    hour=int(payload["hour"]) if payload.get("hour") is not None else None,
                    minute=int(payload.get("minute", 0) or 0),
                    actor=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "connector": connector})
                return
            if parsed.path == "/api/connectors/unsubscribe":
                actor = self._authenticate(["connectors.sync"])
                connector = unsubscribe_connector(
                    self._workspace,
                    connector_id=str(payload.get("connector_id", "")),
                    actor=actor,
                )
                self._send_json(200, {"actor": _serialize_actor(actor), "connector": connector})
                return
            if parsed.path == "/api/connectors/sync-all":
                actor = self._authenticate(["connectors.sync"])
                result = sync_all_connectors(
                    self._workspace,
                    force=bool(payload.get("force", False)),
                    limit=int(payload["limit"]) if payload.get("limit") is not None else None,
                    scheduled_only=bool(payload.get("scheduled_only", False)),
                    actor=actor,
                )
                self._send_json(
                    200,
                    {
                        "actor": _serialize_actor(actor),
                        "summary": {
                            "connector_count": result.connector_count,
                            "synced_connector_count": result.synced_connector_count,
                            "total_result_count": result.total_result_count,
                            "registry_path": self._workspace.relative_path(result.registry_path),
                            "run_manifest_path": self._workspace.relative_path(result.run_manifest_path),
                        },
                        "connector_results": [
                            _serialize_connector_sync_result(self._workspace, item)
                            for item in result.connector_results
                        ],
                    },
                )
                return
        except (CollaborationError, AccessError) as error:
            message = str(error)
            status = 403 if "permission" in message or "active member" in message else 400
            self._send_error(status, message)
            return
        except ControlPlaneError as error:
            self._send_error(403, str(error))
            return
        except JobError as error:
            status = 409 if "No claimable jobs found" in str(error) or "No active job lease found" in str(error) else 400
            self._send_error(status, str(error))
            return
        except MaintenanceError as error:
            self._send_error(400, str(error))
            return
        except Exception as error:  # pragma: no cover - server fallback
            self._send_error(400, str(error))
            return

        self._send_error(404, f"Unknown control-plane endpoint: {parsed.path}")

    def log_message(self, format: str, *args) -> None:  # pragma: no cover - noisy in tests
        return

    def _authenticate(self, required_scopes: List[str]) -> Dict[str, object]:
        header = str(self.headers.get("Authorization", "")).strip()
        if not header.startswith("Bearer "):
            raise ControlPlaneError("Missing bearer token.")
        return validate_control_plane_token(self._workspace, header.removeprefix("Bearer ").strip(), required_scopes)

    def _read_json_body(self) -> Dict[str, object]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="ignore")
        if not raw.strip():
            return {}
        return json.loads(raw)

    def _send_json(self, status: int, payload: Dict[str, object]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: int, message: str) -> None:
        self._send_json(status, {"error": message})
