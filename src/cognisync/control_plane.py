from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
from pathlib import Path
import secrets
from typing import Dict, List, Optional, TYPE_CHECKING
from urllib.parse import urlparse

from cognisync.access import (
    AccessError,
    DEFAULT_LOCAL_OPERATOR_ID,
    OPERATOR_ACTION_ROLES,
    grant_access_member,
    load_access_manifest,
    require_access_role,
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
    connector_subscription_is_due,
    list_connectors,
    list_due_connectors,
    sync_all_connectors,
    sync_connector,
)
from cognisync.jobs import (
    JobError,
    claim_next_job,
    enqueue_connector_sync_all_job,
    enqueue_sync_export_job,
    heartbeat_job,
    read_worker_registry,
    run_next_job,
)
from cognisync.notifications import write_notifications_manifest
from cognisync.sharing import (
    ensure_shared_workspace_manifest,
    list_due_shared_peer_syncs,
    load_shared_workspace_manifest,
    record_shared_peer_scheduler_tick,
    set_shared_trust_policy,
    sharing_summary,
    subscribe_shared_peer_sync,
    unsubscribe_shared_peer_sync,
)
from cognisync.utils import slugify, utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


DEFAULT_CONTROL_SCOPES = {
    "operator": ["connectors.sync", "control.read", "jobs.claim", "jobs.heartbeat", "jobs.run", "scheduler.run"],
    "reviewer": ["control.read"],
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
        f"- Pending invites: `{summary.get('pending_invite_count', 0)}`",
        f"- Accepted invites: `{summary.get('accepted_invite_count', 0)}`",
        f"- Published control plane URL: `{sharing.get('published_control_plane_url', '') or 'unbound'}`",
        f"- Accepted peers: `{sharing.get('accepted_peer_count', 0)}`",
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
) -> tuple[Dict[str, object], str]:
    actor = require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "issue control-plane tokens")
    principal = require_access_role(workspace, principal_id, ("viewer", "editor", "reviewer", "operator"), "receive control-plane tokens")
    raw_token = f"cp_{secrets.token_hex(24)}"
    token_hash = _hash_token(raw_token)
    normalized_scopes = sorted(set(scopes or DEFAULT_CONTROL_SCOPES.get(str(principal.get("role", "")), ["control.read"])))
    token = {
        "token_id": f"token-{slugify(principal_id)[:24] or 'principal'}-{utc_timestamp().replace(':', '').replace('-', '')}",
        "principal_id": str(principal.get("principal_id", "")),
        "role": str(principal.get("role", "")),
        "status": "active",
        "description": description.strip(),
        "created_at": utc_timestamp(),
        "issued_by": _serialize_actor(actor),
        "last_used_at": "",
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
    if limit is not None:
        due_connectors = due_connectors[: max(0, int(limit))]
        remaining = max(0, int(limit) - len(due_connectors))
        due_peer_syncs = due_peer_syncs[:remaining] if remaining else []
    due_connector_ids = [str(item.get("connector_id", "")) for item in due_connectors]
    due_peer_sync_ids = [str(item.get("peer_id", "")) for item in due_peer_syncs]
    executed_run_manifest_path: Optional[Path] = None
    enqueued_job_ids: List[str] = []
    action = "noop"
    if due_connectors or due_peer_syncs:
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
            action = "enqueued"
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
                enqueue_sync_export_job(
                    workspace,
                    peer_ref=str(peer_sync.get("peer_id", "")),
                    requested_by=actor,
                )
            action = "executed"
    _record_scheduler_observations(workspace, due_connector_ids)
    record_shared_peer_scheduler_tick(workspace, due_peer_sync_ids)
    payload = ensure_control_plane_manifest(workspace)
    history = [dict(item) for item in list(dict(payload.get("scheduler", {})).get("history", []))]
    history.insert(
        0,
        {
            "tick_at": utc_timestamp(),
            "action": action,
            "due_connector_ids": due_connector_ids,
            "due_peer_sync_ids": due_peer_sync_ids,
            "enqueued_job_ids": enqueued_job_ids,
            "executed_run_manifest_path": workspace.relative_path(executed_run_manifest_path) if executed_run_manifest_path else "",
        },
    )
    payload["scheduler"] = {
        "last_tick_at": utc_timestamp(),
        "last_action": action,
        "last_due_connector_ids": due_connector_ids,
        "last_due_peer_sync_ids": due_peer_sync_ids,
        "last_enqueued_job_ids": enqueued_job_ids,
        "last_executed_run_manifest_path": workspace.relative_path(executed_run_manifest_path) if executed_run_manifest_path else "",
        "history": history[:50],
    }
    _write_control_plane_manifest(workspace, payload)
    return SchedulerTickResult(
        action=action,
        due_connector_count=len(due_connectors),
        due_connector_ids=due_connector_ids,
        due_peer_sync_ids=due_peer_sync_ids,
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
            "last_enqueued_job_ids": [],
            "last_executed_run_manifest_path": "",
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
            "last_enqueued_job_ids": [str(item) for item in list(scheduler.get("last_enqueued_job_ids", []))],
            "last_executed_run_manifest_path": str(scheduler.get("last_executed_run_manifest_path", "")),
            "history": [
                {
                    "tick_at": str(dict(item).get("tick_at", "")),
                    "action": str(dict(item).get("action", "")),
                    "due_connector_ids": [str(connector_id) for connector_id in list(dict(item).get("due_connector_ids", []))],
                    "due_peer_sync_ids": [str(peer_id) for peer_id in list(dict(item).get("due_peer_sync_ids", []))],
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
    return {
        "due_connector_count": len(due_connector_ids),
        "due_connector_ids": due_connector_ids,
        "due_peer_sync_count": len(due_peer_sync_ids),
        "due_peer_sync_ids": due_peer_sync_ids,
        "last_tick_at": str(scheduler.get("last_tick_at", "")),
        "last_action": str(scheduler.get("last_action", "")),
        "last_enqueued_job_ids": [str(item) for item in list(scheduler.get("last_enqueued_job_ids", []))],
        "history": [dict(item) for item in list(scheduler.get("history", []))],
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


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class _ControlPlaneHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, workspace: "Workspace", **kwargs) -> None:
        self._workspace = workspace
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
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

        if parsed.path == "/api/jobs":
            queue_payload = {}
            if self._workspace.job_queue_manifest_path.exists():
                queue_payload = json.loads(self._workspace.job_queue_manifest_path.read_text(encoding="utf-8"))
            self._send_json(200, queue_payload or {"jobs": [], "queued_count": 0, "job_count": 0})
            return

        if parsed.path == "/api/workers":
            self._send_json(200, read_worker_registry(self._workspace))
            return

        self._send_error(404, f"Unknown control-plane endpoint: {parsed.path}")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self._read_json_body()
            if parsed.path == "/api/jobs/claim-next":
                actor = self._authenticate(["jobs.claim"])
                result = claim_next_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), **result.__dict__})
                return
            if parsed.path == "/api/jobs/heartbeat":
                actor = self._authenticate(["jobs.heartbeat"])
                result = heartbeat_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
                )
                self._send_json(200, {"actor": _serialize_actor(actor), **result.__dict__})
                return
            if parsed.path == "/api/jobs/run-next":
                actor = self._authenticate(["jobs.run"])
                result = run_next_job(
                    self._workspace,
                    worker_id=str(payload.get("worker_id", "")),
                    lease_seconds=int(payload.get("lease_seconds", 300) or 300),
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
                        "enqueued_job_ids": result.enqueued_job_ids,
                        "executed_run_manifest_path": self._workspace.relative_path(result.executed_run_manifest_path)
                        if result.executed_run_manifest_path
                        else "",
                    },
                )
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
