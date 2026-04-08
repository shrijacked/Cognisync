from __future__ import annotations

import base64
import binascii
import json
import secrets
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING
from urllib import error, request

from cognisync.access import (
    AccessError,
    DEFAULT_LOCAL_OPERATOR_ID,
    OPERATOR_ACTION_ROLES,
    VALID_ACCESS_ROLES,
    find_access_member,
    grant_access_member,
    require_access_role,
    revoke_access_member,
)
from cognisync.config import load_config
from cognisync.utils import slugify, utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


class SharingError(RuntimeError):
    pass


PEER_CAPABILITY_SCOPE_ALIASES = {
    "jobs.remote": {"control.read", "jobs.run", "jobs.claim", "jobs.heartbeat"},
    "review.remote": {"control.read", "review.run"},
    "scheduler.remote": {"control.read", "scheduler.run"},
    "connectors.sync": {"control.read", "connectors.sync"},
    "control.admin": {"control.read", "control.admin"},
    "sync.import": {"control.read", "sync.export"},
}


def ensure_shared_workspace_manifest(workspace: "Workspace") -> Dict[str, object]:
    payload = load_shared_workspace_manifest(workspace)
    _write_shared_workspace_manifest(workspace, payload)
    return payload


def load_shared_workspace_manifest(workspace: "Workspace") -> Dict[str, object]:
    if workspace.shared_workspace_manifest_path.exists():
        payload = json.loads(workspace.shared_workspace_manifest_path.read_text(encoding="utf-8"))
    else:
        payload = default_shared_workspace_manifest(workspace)
    return _normalize_shared_workspace_manifest(workspace, payload)


def render_shared_workspace_status(workspace: "Workspace") -> str:
    payload = ensure_shared_workspace_manifest(workspace)
    summary = sharing_summary(workspace)
    lines = [
        "# Shared Workspace",
        "",
        f"- Workspace id: `{payload.get('workspace_id', '')}`",
        f"- Workspace name: `{payload.get('workspace_name', '')}`",
        f"- Published control plane: `{payload.get('published_control_plane_url', '') or 'unbound'}`",
        f"- Peer count: `{summary['peer_count']}`",
        f"- Accepted peers: `{summary['accepted_peer_count']}`",
        f"- Pending peers: `{summary['pending_peer_count']}`",
        f"- Suspended peers: `{summary['suspended_peer_count']}`",
        f"- Attached remotes: `{summary['attached_remote_count']}`",
        f"- Due remote pulls: `{summary['due_remote_pull_count']}`",
        f"- Issued peer bundles: `{summary['issued_bundle_count']}`",
        f"- Remote workers allowed: `{summary['allow_remote_workers']}`",
        f"- Sync imports from peers allowed: `{summary['allow_sync_imports_from_peers']}`",
    ]
    return "\n".join(lines)


def list_shared_peers(workspace: "Workspace", status: Optional[str] = None) -> List[Dict[str, object]]:
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    if status:
        peers = [peer for peer in peers if str(peer.get("status", "")) == status]
    return peers


def list_attached_remotes(workspace: "Workspace", status: Optional[str] = None) -> List[Dict[str, object]]:
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    if status:
        remotes = [remote for remote in remotes if str(remote.get("status", "")) == status]
    return remotes


def sharing_summary(workspace: "Workspace") -> Dict[str, object]:
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    attached_remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    trust_policy = dict(payload.get("trust_policy", {}))
    return {
        "workspace_id": str(payload.get("workspace_id", "")),
        "workspace_name": str(payload.get("workspace_name", "")),
        "published_control_plane_url": str(payload.get("published_control_plane_url", "")),
        "peer_count": len(peers),
        "accepted_peer_count": sum(1 for peer in peers if str(peer.get("status", "")) == "accepted"),
        "pending_peer_count": sum(1 for peer in peers if str(peer.get("status", "")) == "pending"),
        "suspended_peer_count": sum(1 for peer in peers if str(peer.get("status", "")) == "suspended"),
        "peer_ids": [str(peer.get("peer_id", "")) for peer in peers if str(peer.get("status", "")) == "accepted"],
        "attached_remote_count": len(attached_remotes),
        "attached_remote_ids": [
            str(remote.get("remote_id", "")) for remote in attached_remotes if str(remote.get("status", "")) == "attached"
        ],
        "due_remote_pull_count": len(list_due_attached_remote_pulls(workspace)),
        "issued_bundle_count": sum(1 for peer in peers if str(peer.get("last_token_id", "")).strip()),
        "scheduled_sync_peer_count": sum(
            1 for peer in peers if bool(dict(peer.get("sync_subscription", {})).get("enabled", False))
        ),
        "due_sync_peer_count": len(list_due_shared_peer_syncs(workspace)),
        "allow_remote_workers": bool(trust_policy.get("allow_remote_workers", True)),
        "allow_sync_imports_from_peers": bool(trust_policy.get("allow_sync_imports_from_peers", True)),
    }


def bind_shared_control_plane_url(
    workspace: "Workspace",
    url: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "bind a shared control-plane URL")
    normalized_url = url.strip()
    if not normalized_url:
        raise SharingError("A control-plane URL is required.")
    payload = ensure_shared_workspace_manifest(workspace)
    payload["published_control_plane_url"] = normalized_url
    payload["updated_at"] = utc_timestamp()
    _write_shared_workspace_manifest(workspace, payload)
    return payload


def invite_shared_peer(
    workspace: "Workspace",
    peer_id: str,
    role: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    base_url: Optional[str] = None,
    capabilities: Optional[List[str]] = None,
    display_name: Optional[str] = None,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "invite shared-workspace peers")
    normalized_peer_id = peer_id.strip()
    normalized_role = role.strip().lower()
    if not normalized_peer_id:
        raise SharingError("A peer id is required.")
    if normalized_role not in VALID_ACCESS_ROLES:
        raise SharingError(
            f"Unsupported shared-workspace role '{role}'. Expected one of: {', '.join(VALID_ACCESS_ROLES)}."
        )
    payload = ensure_shared_workspace_manifest(workspace)
    peers = {str(item.get("peer_id", "")): dict(item) for item in list(payload.get("peers", []))}
    existing = peers.get(normalized_peer_id) or {}
    now = utc_timestamp()
    peer = {
        "peer_id": normalized_peer_id,
        "display_name": display_name or str(existing.get("display_name", "")) or normalized_peer_id,
        "role": normalized_role,
        "status": "pending",
        "base_url": (base_url or str(existing.get("base_url", ""))).strip(),
        "capabilities": _sorted_capabilities(capabilities or list(existing.get("capabilities", []))),
        "shared_at": str(existing.get("shared_at", now)) or now,
        "updated_at": now,
        "accepted_at": str(existing.get("accepted_at", "")),
    }
    peers[normalized_peer_id] = peer
    payload["peers"] = _sorted_peers(peers.values())
    payload["updated_at"] = now
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def accept_shared_peer(
    workspace: "Workspace",
    peer_ref: str,
    actor_id: Optional[str] = None,
) -> Dict[str, object]:
    normalized_ref = peer_ref.strip()
    if not normalized_ref:
        raise SharingError("A peer id is required.")
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = next((item for item in peers if str(item.get("peer_id", "")) == normalized_ref), None)
    if peer is None:
        raise SharingError(f"Could not find shared peer '{normalized_ref}'.")
    if str(peer.get("status", "")) == "accepted":
        return peer
    principal_id = str(peer.get("peer_id", ""))
    grant_access_member(
        workspace,
        principal_id=principal_id,
        role=str(peer.get("role", "viewer")),
        display_name=str(peer.get("display_name", "")) or principal_id,
    )
    peer["status"] = "accepted"
    peer["accepted_at"] = utc_timestamp()
    peer["suspended_at"] = ""
    peer["updated_at"] = peer["accepted_at"]
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = peer["accepted_at"]
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def set_shared_peer_role(
    workspace: "Workspace",
    peer_ref: str,
    role: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "update shared-workspace peer roles")
    normalized_role = role.strip().lower()
    if normalized_role not in VALID_ACCESS_ROLES:
        raise SharingError(
            f"Unsupported shared-workspace role '{role}'. Expected one of: {', '.join(VALID_ACCESS_ROLES)}."
        )
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = _find_peer_in_list(peers, peer_ref)
    peer["role"] = normalized_role
    peer["updated_at"] = utc_timestamp()
    if str(peer.get("status", "")) == "accepted":
        principal_id = str(peer.get("peer_id", ""))
        grant_access_member(
            workspace,
            principal_id=principal_id,
            role=normalized_role,
            display_name=str(peer.get("display_name", "")) or principal_id,
        )
        _revoke_peer_tokens(workspace, principal_id, reason="peer_role_changed")
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = str(peer.get("updated_at", ""))
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def suspend_shared_peer(
    workspace: "Workspace",
    peer_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "suspend shared-workspace peers")
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = _find_peer_in_list(peers, peer_ref)
    now = utc_timestamp()
    peer["status"] = "suspended"
    peer["suspended_at"] = now
    peer["updated_at"] = now
    subscription = _normalize_sync_subscription(dict(peer.get("sync_subscription", {})))
    subscription["enabled"] = False
    subscription["next_sync_at"] = ""
    subscription["last_tick_status"] = "suspended"
    peer["sync_subscription"] = subscription
    principal_id = str(peer.get("peer_id", ""))
    if find_access_member(workspace, principal_id) is not None:
        try:
            revoke_access_member(workspace, principal_id)
        except AccessError:
            pass
    _revoke_peer_tokens(workspace, principal_id, reason="peer_suspended")
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = now
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def remove_shared_peer(
    workspace: "Workspace",
    peer_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "remove shared-workspace peers")
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = _find_peer_in_list(peers, peer_ref)
    principal_id = str(peer.get("peer_id", ""))
    if find_access_member(workspace, principal_id) is not None:
        try:
            revoke_access_member(workspace, principal_id)
        except AccessError:
            pass
    _revoke_peer_tokens(workspace, principal_id, reason="peer_removed")
    payload["peers"] = _sorted_peers(
        [item for item in peers if str(item.get("peer_id", "")) != principal_id]
    )
    payload["updated_at"] = utc_timestamp()
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def issue_shared_peer_bundle(
    workspace: "Workspace",
    peer_ref: str,
    output_file: Optional[str] = None,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    scopes: Optional[List[str]] = None,
) -> Dict[str, object]:
    from cognisync.control_plane import DEFAULT_CONTROL_SCOPES, issue_control_plane_token

    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "issue shared-workspace peer bundles")
    payload = ensure_shared_workspace_manifest(workspace)
    control_plane_url = str(payload.get("published_control_plane_url", "")).strip()
    if not control_plane_url:
        raise SharingError("Bind a shared control-plane URL before issuing peer bundles.")
    trust_policy = dict(payload.get("trust_policy", {}))
    if not bool(trust_policy.get("allow_remote_workers", True)):
        raise SharingError("Remote worker bundles are disabled by the shared-workspace trust policy.")
    normalized_ref = peer_ref.strip()
    if not normalized_ref:
        raise SharingError("A peer id is required.")
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = next((item for item in peers if str(item.get("peer_id", "")) == normalized_ref), None)
    if peer is None:
        raise SharingError(f"Could not find shared peer '{normalized_ref}'.")
    if str(peer.get("status", "")) != "accepted":
        raise SharingError(f"Peer '{normalized_ref}' must be accepted before a bundle can be issued.")
    allowed_scopes = peer_allowed_control_scopes(peer)
    requested_scopes = sorted({str(item).strip() for item in list(scopes or []) if str(item).strip()}) or allowed_scopes
    disallowed_scopes = sorted(scope for scope in requested_scopes if scope not in allowed_scopes)
    if disallowed_scopes:
        raise SharingError(
            "Requested scopes are not permitted by peer capabilities: "
            + ", ".join(disallowed_scopes)
            + "."
        )
    if not requested_scopes:
        requested_scopes = list(DEFAULT_CONTROL_SCOPES.get(str(peer.get("role", "")), ["control.read"]))

    token_payload, raw_token = issue_control_plane_token(
        workspace,
        principal_id=str(peer.get("peer_id", "")),
        scopes=requested_scopes,
        actor_id=actor_id,
        description=f"shared-workspace bundle for {peer.get('peer_id', '')}",
    )
    issued_at = utc_timestamp()
    peer["last_bundle_issued_at"] = issued_at
    peer["last_token_id"] = str(token_payload.get("token_id", ""))
    peer["last_bundle_scopes"] = [str(item) for item in list(token_payload.get("scopes", []))]
    peer["last_allowed_bundle_scopes"] = allowed_scopes
    peer["updated_at"] = issued_at
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = issued_at
    _write_shared_workspace_manifest(workspace, payload)

    bundle = {
        "workspace_id": str(payload.get("workspace_id", "")),
        "workspace_name": str(payload.get("workspace_name", "")),
        "server_url": control_plane_url,
        "principal_id": str(peer.get("peer_id", "")),
        "display_name": str(peer.get("display_name", "")),
        "role": str(peer.get("role", "")),
        "capabilities": [str(item) for item in list(peer.get("capabilities", []))],
        "base_url": str(peer.get("base_url", "")),
        "token": raw_token,
        "token_id": str(token_payload.get("token_id", "")),
        "scopes": [str(item) for item in list(token_payload.get("scopes", []))],
        "issued_at": issued_at,
        "manifest_path": workspace.relative_path(workspace.shared_workspace_manifest_path),
    }
    if output_file:
        bundle["output_file"] = output_file
    return bundle


def attach_remote_bundle(
    workspace: "Workspace",
    bundle_file: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "attach remote shared-workspace bundles")
    bundle = _load_remote_bundle_file(workspace, bundle_file)
    return attach_remote_payload(workspace, bundle=bundle, actor_id=actor_id)


def attach_remote_payload(
    workspace: "Workspace",
    bundle: Dict[str, object],
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "attach remote shared-workspace bundles")
    return _upsert_attached_remote(workspace, bundle=bundle, require_existing=False)


def refresh_attached_remote_bundle(
    workspace: "Workspace",
    bundle_file: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "refresh attached remote bundles")
    bundle = _load_remote_bundle_file(workspace, bundle_file)
    return refresh_attached_remote_payload(workspace, bundle=bundle, actor_id=actor_id)


def refresh_attached_remote_payload(
    workspace: "Workspace",
    bundle: Dict[str, object],
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "refresh attached remote bundles")
    return _upsert_attached_remote(workspace, bundle=bundle, require_existing=True)


def pull_attached_remote(
    workspace: "Workspace",
    remote_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "pull attached remote workspace state")
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    remote = _find_attached_remote_in_list(remotes, remote_ref)
    if str(remote.get("status", "")) != "attached":
        raise SharingError(f"Attached remote '{remote_ref}' is not active.")
    principal_id = str(remote.get("principal_id", "")).strip()
    token = str(remote.get("token", "")).strip()
    server_url = str(remote.get("server_url", "")).strip()
    if not principal_id or not token or not server_url:
        raise SharingError(f"Attached remote '{remote_ref}' is missing control-plane connection details.")

    try:
        export_payload = _post_remote_json(
            f"{_control_plane_api_root(server_url)}/sync/export",
            token=token,
            payload={
                "peer_ref": principal_id,
                "inline_archive": True,
            },
        )
        archive_base64 = str(export_payload.get("archive_base64", "")).strip()
        if not archive_base64:
            raise SharingError("Remote control plane did not return an inline sync archive.")
        try:
            archive_bytes = base64.b64decode(archive_base64.encode("ascii"), validate=True)
        except (ValueError, binascii.Error) as error:
            raise SharingError("Remote control plane returned an invalid sync archive payload.") from error
        from cognisync.manifests import write_workspace_manifests
        from cognisync.sync import SyncError, import_sync_bundle_archive

        result = import_sync_bundle_archive(
            workspace,
            archive_bytes=archive_bytes,
            actor_id=actor_id,
            from_peer=principal_id,
        )
        snapshot = workspace.refresh_index()
        write_workspace_manifests(workspace, snapshot)
        now = utc_timestamp()
        subscription = _normalize_pull_subscription(dict(remote.get("pull_subscription", {})))
        subscription["last_pull_at"] = now
        subscription["last_tick_status"] = "imported"
        if bool(subscription.get("enabled", False)):
            interval_hours = int(subscription.get("interval_hours", 0) or 0)
            subscription["next_pull_at"] = _next_interval_timestamp(hours=interval_hours) if interval_hours > 0 else ""
        remote["pull_subscription"] = subscription
        remote["last_pull_at"] = now
        remote["last_pull_status"] = "imported"
        remote["last_imported_sync_event_path"] = workspace.relative_path(result.event_manifest_path)
        remote["last_imported_sync_history_path"] = workspace.relative_path(result.history_manifest_path)
        remote["updated_at"] = now
        payload["attached_remotes"] = _sorted_attached_remotes(remotes)
        payload["updated_at"] = now
        _write_shared_workspace_manifest(workspace, payload)
        return {
            "remote": remote,
            "manifest_path": result.manifest_path,
            "event_manifest_path": result.event_manifest_path,
            "history_manifest_path": result.history_manifest_path,
            "file_count": result.file_count,
        }
    except (SharingError, SyncError) as error:
        _record_attached_remote_pull_failure(workspace, payload, remotes, remote, str(error))
        raise


def subscribe_attached_remote_pull(
    workspace: "Workspace",
    remote_ref: str,
    every_hours: int,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "subscribe attached remote pulls")
    if every_hours < 1:
        raise SharingError("Attached remote pull subscription hours must be at least 1.")
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    remote = _find_attached_remote_in_list(remotes, remote_ref)
    if str(remote.get("status", "")) != "attached":
        raise SharingError(f"Attached remote '{remote_ref}' is not active.")
    subscription = _normalize_pull_subscription(
        {
            "enabled": True,
            "interval_hours": int(every_hours),
            "next_pull_at": _next_interval_timestamp(hours=every_hours),
            "last_pull_at": str(dict(remote.get("pull_subscription", {})).get("last_pull_at", "")),
            "last_scheduler_tick_at": str(dict(remote.get("pull_subscription", {})).get("last_scheduler_tick_at", "")),
            "last_tick_status": str(dict(remote.get("pull_subscription", {})).get("last_tick_status", "")),
        }
    )
    remote["pull_subscription"] = subscription
    remote["updated_at"] = utc_timestamp()
    payload["attached_remotes"] = _sorted_attached_remotes(remotes)
    payload["updated_at"] = str(remote.get("updated_at", ""))
    _write_shared_workspace_manifest(workspace, payload)
    return remote


def unsubscribe_attached_remote_pull(
    workspace: "Workspace",
    remote_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "unsubscribe attached remote pulls")
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    remote = _find_attached_remote_in_list(remotes, remote_ref)
    subscription = _normalize_pull_subscription(dict(remote.get("pull_subscription", {})))
    subscription["enabled"] = False
    subscription["next_pull_at"] = ""
    remote["pull_subscription"] = subscription
    remote["updated_at"] = utc_timestamp()
    payload["attached_remotes"] = _sorted_attached_remotes(remotes)
    payload["updated_at"] = str(remote.get("updated_at", ""))
    _write_shared_workspace_manifest(workspace, payload)
    return remote


def suspend_attached_remote(
    workspace: "Workspace",
    remote_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "suspend attached remote workspaces")
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    remote = _find_attached_remote_in_list(remotes, remote_ref)
    now = utc_timestamp()
    remote["status"] = "suspended"
    remote["suspended_at"] = now
    remote["updated_at"] = now
    subscription = _normalize_pull_subscription(dict(remote.get("pull_subscription", {})))
    subscription["enabled"] = False
    subscription["next_pull_at"] = ""
    subscription["last_tick_status"] = "suspended"
    remote["pull_subscription"] = subscription
    payload["attached_remotes"] = _sorted_attached_remotes(remotes)
    payload["updated_at"] = now
    _write_shared_workspace_manifest(workspace, payload)
    return remote


def detach_attached_remote(
    workspace: "Workspace",
    remote_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "detach attached remote workspaces")
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    remote = _find_attached_remote_in_list(remotes, remote_ref)
    remote_id = str(remote.get("remote_id", ""))
    payload["attached_remotes"] = _sorted_attached_remotes(
        [item for item in remotes if str(item.get("remote_id", "")) != remote_id]
    )
    payload["updated_at"] = utc_timestamp()
    _write_shared_workspace_manifest(workspace, payload)
    return remote


def list_due_attached_remote_pulls(workspace: "Workspace") -> List[Dict[str, object]]:
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    return [remote for remote in remotes if attached_remote_pull_is_due(remote)]


def attached_remote_pull_is_due(remote: Dict[str, object]) -> bool:
    if str(remote.get("status", "")) != "attached":
        return False
    subscription = _normalize_pull_subscription(dict(remote.get("pull_subscription", {})))
    if not bool(subscription.get("enabled", False)):
        return False
    next_pull_at = str(subscription.get("next_pull_at", "")).strip()
    if not next_pull_at:
        return False
    try:
        from datetime import datetime, timezone

        return datetime.fromisoformat(next_pull_at) <= datetime.now(timezone.utc)
    except ValueError:
        return False


def record_attached_remote_scheduler_tick(workspace: "Workspace", due_remote_ids: List[str]) -> None:
    payload = ensure_shared_workspace_manifest(workspace)
    due_set = {remote_id for remote_id in due_remote_ids if remote_id}
    tick_at = utc_timestamp()
    changed = False
    remotes = [dict(item) for item in list(payload.get("attached_remotes", []))]
    for remote in remotes:
        subscription = _normalize_pull_subscription(dict(remote.get("pull_subscription", {})))
        if not bool(subscription.get("enabled", False)):
            continue
        subscription["last_scheduler_tick_at"] = tick_at
        remote_id = str(remote.get("remote_id", "")).strip()
        principal_id = str(remote.get("principal_id", "")).strip()
        subscription["last_tick_status"] = "due" if remote_id in due_set or principal_id in due_set else "waiting"
        remote["pull_subscription"] = subscription
        changed = True
    if changed:
        payload["attached_remotes"] = _sorted_attached_remotes(remotes)
        payload["updated_at"] = tick_at
        _write_shared_workspace_manifest(workspace, payload)


def set_shared_trust_policy(
    workspace: "Workspace",
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    allow_remote_workers: Optional[bool] = None,
    allow_sync_imports_from_peers: Optional[bool] = None,
    default_peer_role: Optional[str] = None,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "update shared-workspace trust policy")
    payload = ensure_shared_workspace_manifest(workspace)
    trust_policy = dict(payload.get("trust_policy", {}))
    if allow_remote_workers is not None:
        trust_policy["allow_remote_workers"] = bool(allow_remote_workers)
    if allow_sync_imports_from_peers is not None:
        trust_policy["allow_sync_imports_from_peers"] = bool(allow_sync_imports_from_peers)
    if default_peer_role is not None:
        normalized_role = default_peer_role.strip().lower()
        if normalized_role not in VALID_ACCESS_ROLES:
            raise SharingError(
                f"Unsupported default peer role '{default_peer_role}'. Expected one of: {', '.join(VALID_ACCESS_ROLES)}."
            )
        trust_policy["default_peer_role"] = normalized_role
    payload["trust_policy"] = trust_policy
    payload["updated_at"] = utc_timestamp()
    _write_shared_workspace_manifest(workspace, payload)
    return payload


def subscribe_shared_peer_sync(
    workspace: "Workspace",
    peer_ref: str,
    every_hours: int,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "subscribe shared-peer sync exports")
    if every_hours < 1:
        raise SharingError("Shared-peer sync subscription hours must be at least 1.")
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = _find_peer_in_list(peers, peer_ref)
    if str(peer.get("status", "")) != "accepted":
        raise SharingError(f"Peer '{peer_ref}' must be accepted before sync scheduling is enabled.")
    peer["sync_subscription"] = _normalize_sync_subscription(
        {
            "enabled": True,
            "interval_hours": int(every_hours),
            "next_sync_at": _next_interval_timestamp(hours=every_hours),
            "last_sync_at": str(dict(peer.get("sync_subscription", {})).get("last_sync_at", "")),
            "last_scheduler_tick_at": str(dict(peer.get("sync_subscription", {})).get("last_scheduler_tick_at", "")),
            "last_tick_status": str(dict(peer.get("sync_subscription", {})).get("last_tick_status", "")),
        }
    )
    peer["updated_at"] = utc_timestamp()
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = peer["updated_at"]
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def unsubscribe_shared_peer_sync(
    workspace: "Workspace",
    peer_ref: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> Dict[str, object]:
    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "unsubscribe shared-peer sync exports")
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = _find_peer_in_list(peers, peer_ref)
    subscription = _normalize_sync_subscription(dict(peer.get("sync_subscription", {})))
    subscription["enabled"] = False
    subscription["next_sync_at"] = ""
    peer["sync_subscription"] = subscription
    peer["updated_at"] = utc_timestamp()
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = peer["updated_at"]
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def list_due_shared_peer_syncs(workspace: "Workspace") -> List[Dict[str, object]]:
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    return [peer for peer in peers if shared_peer_sync_is_due(peer)]


def shared_peer_sync_is_due(peer: Dict[str, object]) -> bool:
    if str(peer.get("status", "")) != "accepted":
        return False
    subscription = _normalize_sync_subscription(dict(peer.get("sync_subscription", {})))
    if not bool(subscription.get("enabled", False)):
        return False
    next_sync_at = str(subscription.get("next_sync_at", "")).strip()
    if not next_sync_at:
        return False
    try:
        from datetime import datetime, timezone

        return datetime.fromisoformat(next_sync_at) <= datetime.now(timezone.utc)
    except ValueError:
        return False


def mark_shared_peer_sync_exported(workspace: "Workspace", peer_ref: str) -> Dict[str, object]:
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = _find_peer_in_list(peers, peer_ref)
    subscription = _normalize_sync_subscription(dict(peer.get("sync_subscription", {})))
    if not bool(subscription.get("enabled", False)):
        return peer
    interval_hours = int(subscription.get("interval_hours", 0) or 0)
    now = utc_timestamp()
    subscription["last_sync_at"] = now
    subscription["next_sync_at"] = _next_interval_timestamp(hours=interval_hours) if interval_hours > 0 else ""
    subscription["last_tick_status"] = "exported"
    peer["sync_subscription"] = subscription
    peer["updated_at"] = now
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = now
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def record_shared_peer_scheduler_tick(workspace: "Workspace", due_peer_ids: List[str]) -> None:
    payload = ensure_shared_workspace_manifest(workspace)
    due_set = {peer_id for peer_id in due_peer_ids if peer_id}
    tick_at = utc_timestamp()
    changed = False
    peers = [dict(item) for item in list(payload.get("peers", []))]
    for peer in peers:
        subscription = _normalize_sync_subscription(dict(peer.get("sync_subscription", {})))
        if not bool(subscription.get("enabled", False)):
            continue
        subscription["last_scheduler_tick_at"] = tick_at
        subscription["last_tick_status"] = "due" if str(peer.get("peer_id", "")) in due_set else "waiting"
        peer["sync_subscription"] = subscription
        changed = True
    if changed:
        payload["peers"] = _sorted_peers(peers)
        payload["updated_at"] = tick_at
        _write_shared_workspace_manifest(workspace, payload)


def default_shared_workspace_manifest(workspace: "Workspace") -> Dict[str, object]:
    now = utc_timestamp()
    return {
        "schema_version": 1,
        "workspace_id": f"shared-{slugify(workspace.root.name)[:24] or 'workspace'}-{secrets.token_hex(6)}",
        "workspace_name": workspace.root.name,
        "created_at": now,
        "updated_at": now,
        "generated_at": now,
        "published_control_plane_url": "",
        "trust_policy": {
            "allow_remote_workers": True,
            "allow_sync_imports_from_peers": True,
            "default_peer_role": "viewer",
        },
        "peers": [],
        "attached_remotes": [],
    }


def _normalize_shared_workspace_manifest(workspace: "Workspace", payload: Dict[str, object]) -> Dict[str, object]:
    trust_policy = dict(payload.get("trust_policy", {}))
    configured_name = workspace.root.name
    if workspace.config_path.exists():
        configured_name = load_config(workspace.config_path).workspace_name
    return {
        "schema_version": 1,
        "workspace_id": str(payload.get("workspace_id", "")) or f"shared-{slugify(workspace.root.name)[:24] or 'workspace'}-{secrets.token_hex(6)}",
        "workspace_name": configured_name,
        "created_at": str(payload.get("created_at", "")) or utc_timestamp(),
        "updated_at": str(payload.get("updated_at", "")) or utc_timestamp(),
        "generated_at": str(payload.get("generated_at", "")) or utc_timestamp(),
        "published_control_plane_url": str(payload.get("published_control_plane_url", "")).strip(),
        "trust_policy": {
            "allow_remote_workers": bool(trust_policy.get("allow_remote_workers", True)),
            "allow_sync_imports_from_peers": bool(trust_policy.get("allow_sync_imports_from_peers", True)),
            "default_peer_role": str(trust_policy.get("default_peer_role", "viewer")).strip().lower() or "viewer",
        },
        "peers": _sorted_peers([dict(item) for item in list(payload.get("peers", []))]),
        "attached_remotes": _sorted_attached_remotes(
            [dict(item) for item in list(payload.get("attached_remotes", []))]
        ),
    }


def _sorted_capabilities(capabilities: List[str]) -> List[str]:
    return sorted({str(item).strip() for item in capabilities if str(item).strip()})


def peer_has_capability(peer: Dict[str, object], capability: str) -> bool:
    normalized_capability = str(capability).strip()
    if not normalized_capability:
        return False
    capabilities = {str(item).strip() for item in list(peer.get("capabilities", [])) if str(item).strip()}
    return normalized_capability in capabilities


def peer_allowed_control_scopes(peer: Dict[str, object]) -> List[str]:
    from cognisync.control_plane import DEFAULT_CONTROL_SCOPES

    role = str(peer.get("role", "viewer")).strip().lower() or "viewer"
    role_scopes = set(DEFAULT_CONTROL_SCOPES.get(role, ["control.read"]))
    known_scopes = {scope for scopes in DEFAULT_CONTROL_SCOPES.values() for scope in scopes}
    allowed = {"control.read"}
    for capability in _sorted_capabilities(list(peer.get("capabilities", []))):
        if capability in known_scopes:
            allowed.add(capability)
        allowed.update(PEER_CAPABILITY_SCOPE_ALIASES.get(capability, set()))
    return sorted(scope for scope in allowed if scope in role_scopes or scope == "control.read")


def _sorted_peers(peers: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    for peer in peers:
        peer_id = str(peer.get("peer_id", "")).strip()
        if not peer_id:
            continue
        normalized.append(
            {
                "peer_id": peer_id,
                "display_name": str(peer.get("display_name", "")).strip() or peer_id,
                "role": str(peer.get("role", "viewer")).strip().lower() or "viewer",
                "status": str(peer.get("status", "pending")).strip().lower() or "pending",
                "base_url": str(peer.get("base_url", "")).strip(),
                "capabilities": _sorted_capabilities(list(peer.get("capabilities", []))),
                "shared_at": str(peer.get("shared_at", "")) or utc_timestamp(),
                "accepted_at": str(peer.get("accepted_at", "")),
                "suspended_at": str(peer.get("suspended_at", "")),
                "last_bundle_issued_at": str(peer.get("last_bundle_issued_at", "")),
                "last_token_id": str(peer.get("last_token_id", "")),
                "last_bundle_scopes": [str(item) for item in list(peer.get("last_bundle_scopes", []))],
                "sync_subscription": _normalize_sync_subscription(dict(peer.get("sync_subscription", {}))),
                "updated_at": str(peer.get("updated_at", "")) or utc_timestamp(),
            }
        )
    status_rank = {"accepted": 0, "pending": 1, "suspended": 2}
    normalized.sort(key=lambda item: (status_rank.get(item["status"], 9), item["peer_id"]))
    return normalized


def _sorted_attached_remotes(remotes: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    for remote in remotes:
        workspace_id = str(remote.get("workspace_id", "")).strip()
        principal_id = str(remote.get("principal_id", "")).strip()
        remote_id = str(remote.get("remote_id", "")).strip() or (
            f"{workspace_id}:{principal_id}" if workspace_id and principal_id else ""
        )
        if not remote_id or not workspace_id or not principal_id:
            continue
        normalized.append(
            {
                "remote_id": remote_id,
                "workspace_id": workspace_id,
                "workspace_name": str(remote.get("workspace_name", "")).strip() or workspace_id,
                "principal_id": principal_id,
                "display_name": str(remote.get("display_name", "")).strip() or principal_id,
                "role": str(remote.get("role", "viewer")).strip().lower() or "viewer",
                "status": str(remote.get("status", "attached")).strip().lower() or "attached",
                "server_url": str(remote.get("server_url", "")).strip(),
                "base_url": str(remote.get("base_url", "")).strip(),
                "capabilities": _sorted_capabilities(list(remote.get("capabilities", []))),
                "scopes": _sorted_capabilities(list(remote.get("scopes", []))),
                "token": str(remote.get("token", "")).strip(),
                "token_id": str(remote.get("token_id", "")).strip(),
                "attached_at": str(remote.get("attached_at", "")) or utc_timestamp(),
                "suspended_at": str(remote.get("suspended_at", "")),
                "last_pull_at": str(remote.get("last_pull_at", "")),
                "last_pull_status": str(remote.get("last_pull_status", "")),
                "last_imported_sync_event_path": str(remote.get("last_imported_sync_event_path", "")),
                "last_imported_sync_history_path": str(remote.get("last_imported_sync_history_path", "")),
                "pull_subscription": _normalize_pull_subscription(dict(remote.get("pull_subscription", {}))),
                "updated_at": str(remote.get("updated_at", "")) or utc_timestamp(),
            }
        )
    status_rank = {"attached": 0, "suspended": 1}
    normalized.sort(key=lambda item: (status_rank.get(item["status"], 9), item["workspace_name"], item["principal_id"]))
    return normalized


def _find_peer(payload: Dict[str, object], peer_ref: str) -> Dict[str, object]:
    normalized_ref = peer_ref.strip()
    if not normalized_ref:
        raise SharingError("A peer id is required.")
    peers = [dict(item) for item in list(payload.get("peers", []))]
    return _find_peer_in_list(peers, normalized_ref)


def _find_peer_in_list(peers: List[Dict[str, object]], peer_ref: str) -> Dict[str, object]:
    normalized_ref = peer_ref.strip()
    peer = next((item for item in peers if str(item.get("peer_id", "")) == normalized_ref), None)
    if peer is None:
        raise SharingError(f"Could not find shared peer '{normalized_ref}'.")
    return peer


def _find_attached_remote_in_list(remotes: List[Dict[str, object]], remote_ref: str) -> Dict[str, object]:
    normalized_ref = remote_ref.strip()
    if not normalized_ref:
        raise SharingError("A remote id is required.")
    exact = next((item for item in remotes if str(item.get("remote_id", "")) == normalized_ref), None)
    if exact is not None:
        return exact
    principal_matches = [item for item in remotes if str(item.get("principal_id", "")) == normalized_ref]
    if len(principal_matches) == 1:
        return principal_matches[0]
    workspace_matches = [item for item in remotes if str(item.get("workspace_id", "")) == normalized_ref]
    if len(workspace_matches) == 1:
        return workspace_matches[0]
    raise SharingError(f"Could not find attached remote '{normalized_ref}'.")


def _normalize_sync_subscription(payload: Dict[str, object]) -> Dict[str, object]:
    interval_hours = payload.get("interval_hours")
    if interval_hours is not None:
        interval_hours = int(interval_hours)
    return {
        "enabled": bool(payload.get("enabled", False)),
        "interval_hours": interval_hours,
        "next_sync_at": str(payload.get("next_sync_at", "")),
        "last_sync_at": str(payload.get("last_sync_at", "")),
        "last_scheduler_tick_at": str(payload.get("last_scheduler_tick_at", "")),
        "last_tick_status": str(payload.get("last_tick_status", "")),
    }


def _normalize_pull_subscription(payload: Dict[str, object]) -> Dict[str, object]:
    interval_hours = payload.get("interval_hours")
    if interval_hours is not None:
        interval_hours = int(interval_hours)
    return {
        "enabled": bool(payload.get("enabled", False)),
        "interval_hours": interval_hours,
        "next_pull_at": str(payload.get("next_pull_at", "")),
        "last_pull_at": str(payload.get("last_pull_at", "")),
        "last_scheduler_tick_at": str(payload.get("last_scheduler_tick_at", "")),
        "last_tick_status": str(payload.get("last_tick_status", "")),
    }


def _revoke_peer_tokens(workspace: "Workspace", principal_id: str, reason: str) -> None:
    from cognisync.control_plane import revoke_control_plane_tokens_for_principal

    revoke_control_plane_tokens_for_principal(workspace, principal_id, reason=reason)


def _next_interval_timestamp(hours: int) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) + timedelta(hours=max(1, int(hours)))).replace(microsecond=0).isoformat()


def _control_plane_api_root(server_url: str) -> str:
    normalized = server_url.rstrip("/")
    if normalized.endswith("/api"):
        return normalized
    return f"{normalized}/api"


def _load_remote_bundle_file(workspace: "Workspace", bundle_file: str) -> Dict[str, object]:
    bundle_path = Path(bundle_file).expanduser()
    if not bundle_path.is_absolute():
        bundle_path = workspace.root / bundle_path
    bundle_path = bundle_path.resolve()
    if not bundle_path.exists():
        raise SharingError(f"Could not find remote peer bundle at {bundle_path}.")
    try:
        return json.loads(bundle_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise SharingError(f"Remote peer bundle {bundle_path} is not valid JSON.") from error


def _validated_remote_bundle(bundle: Dict[str, object]) -> Dict[str, object]:
    principal_id = str(bundle.get("principal_id", "")).strip()
    workspace_id = str(bundle.get("workspace_id", "")).strip()
    server_url = str(bundle.get("server_url", "")).strip()
    token = str(bundle.get("token", "")).strip()
    token_id = str(bundle.get("token_id", "")).strip()
    if not principal_id:
        raise SharingError("Remote peer bundles must include a principal_id.")
    if not workspace_id:
        raise SharingError("Remote peer bundles must include a workspace_id.")
    if not server_url:
        raise SharingError("Remote peer bundles must include a server_url.")
    if not token:
        raise SharingError("Remote peer bundles must include a bearer token.")
    if not token_id:
        raise SharingError("Remote peer bundles must include a token_id.")
    normalized = dict(bundle)
    normalized["principal_id"] = principal_id
    normalized["workspace_id"] = workspace_id
    normalized["server_url"] = server_url
    normalized["token"] = token
    normalized["token_id"] = token_id
    return normalized


def _upsert_attached_remote(
    workspace: "Workspace",
    bundle: Dict[str, object],
    require_existing: bool,
) -> Dict[str, object]:
    normalized_bundle = _validated_remote_bundle(bundle)
    principal_id = str(normalized_bundle.get("principal_id", ""))
    workspace_id = str(normalized_bundle.get("workspace_id", ""))
    remote_id = f"{workspace_id}:{principal_id}"
    payload = ensure_shared_workspace_manifest(workspace)
    remotes = {str(item.get("remote_id", "")): dict(item) for item in list(payload.get("attached_remotes", []))}
    existing = remotes.get(remote_id) or {}
    if require_existing and not existing:
        raise SharingError(f"Could not find attached remote '{remote_id}' to refresh.")
    now = utc_timestamp()
    remote = {
        "remote_id": remote_id,
        "workspace_id": workspace_id,
        "workspace_name": str(normalized_bundle.get("workspace_name", "")).strip() or workspace_id,
        "principal_id": principal_id,
        "display_name": str(normalized_bundle.get("display_name", "")).strip() or principal_id,
        "role": str(normalized_bundle.get("role", "viewer")).strip().lower() or "viewer",
        "status": "attached",
        "server_url": str(normalized_bundle.get("server_url", "")).strip(),
        "base_url": str(normalized_bundle.get("base_url", "")).strip(),
        "capabilities": _sorted_capabilities(list(normalized_bundle.get("capabilities", []))),
        "scopes": _sorted_capabilities(list(normalized_bundle.get("scopes", []))),
        "token": str(normalized_bundle.get("token", "")).strip(),
        "token_id": str(normalized_bundle.get("token_id", "")).strip(),
        "attached_at": str(existing.get("attached_at", now)) or now,
        "suspended_at": "",
        "updated_at": now,
        "last_pull_at": str(existing.get("last_pull_at", "")),
        "last_pull_status": str(existing.get("last_pull_status", "")),
        "last_imported_sync_event_path": str(existing.get("last_imported_sync_event_path", "")),
        "last_imported_sync_history_path": str(existing.get("last_imported_sync_history_path", "")),
        "pull_subscription": _normalize_pull_subscription(dict(existing.get("pull_subscription", {}))),
    }
    remotes[remote_id] = remote
    payload["attached_remotes"] = _sorted_attached_remotes(list(remotes.values()))
    payload["updated_at"] = now
    _write_shared_workspace_manifest(workspace, payload)
    return remote


def _post_remote_json(url: str, token: str, payload: Dict[str, object]) -> Dict[str, object]:
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
        raise SharingError(f"{http_error.code}: {message}") from http_error
    except error.URLError as url_error:
        raise SharingError(f"Could not reach remote control plane: {url_error.reason}") from url_error


def _record_attached_remote_pull_failure(
    workspace: "Workspace",
    payload: Dict[str, object],
    remotes: List[Dict[str, object]],
    remote: Dict[str, object],
    message: str,
) -> None:
    now = utc_timestamp()
    subscription = _normalize_pull_subscription(dict(remote.get("pull_subscription", {})))
    subscription["last_tick_status"] = "failed"
    remote["pull_subscription"] = subscription
    remote["last_pull_status"] = f"failed: {message}"
    remote["updated_at"] = now
    payload["attached_remotes"] = _sorted_attached_remotes(remotes)
    payload["updated_at"] = now
    _write_shared_workspace_manifest(workspace, payload)


def _write_shared_workspace_manifest(workspace: "Workspace", payload: Dict[str, object]) -> None:
    normalized = _normalize_shared_workspace_manifest(workspace, payload)
    normalized["generated_at"] = utc_timestamp()
    workspace.shared_workspace_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.shared_workspace_manifest_path.write_text(
        json.dumps(normalized, indent=2, sort_keys=True),
        encoding="utf-8",
    )
