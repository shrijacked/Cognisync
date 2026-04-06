from __future__ import annotations

import json
import secrets
from typing import Dict, List, Optional, TYPE_CHECKING

from cognisync.access import DEFAULT_LOCAL_OPERATOR_ID, OPERATOR_ACTION_ROLES, VALID_ACCESS_ROLES, grant_access_member, require_access_role
from cognisync.config import load_config
from cognisync.utils import slugify, utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


class SharingError(RuntimeError):
    pass


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


def sharing_summary(workspace: "Workspace") -> Dict[str, object]:
    payload = ensure_shared_workspace_manifest(workspace)
    peers = [dict(item) for item in list(payload.get("peers", []))]
    trust_policy = dict(payload.get("trust_policy", {}))
    return {
        "workspace_id": str(payload.get("workspace_id", "")),
        "workspace_name": str(payload.get("workspace_name", "")),
        "published_control_plane_url": str(payload.get("published_control_plane_url", "")),
        "peer_count": len(peers),
        "accepted_peer_count": sum(1 for peer in peers if str(peer.get("status", "")) == "accepted"),
        "pending_peer_count": sum(1 for peer in peers if str(peer.get("status", "")) == "pending"),
        "peer_ids": [str(peer.get("peer_id", "")) for peer in peers if str(peer.get("status", "")) == "accepted"],
        "issued_bundle_count": sum(1 for peer in peers if str(peer.get("last_token_id", "")).strip()),
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
    peer["updated_at"] = peer["accepted_at"]
    payload["peers"] = _sorted_peers(peers)
    payload["updated_at"] = peer["accepted_at"]
    _write_shared_workspace_manifest(workspace, payload)
    return peer


def issue_shared_peer_bundle(
    workspace: "Workspace",
    peer_ref: str,
    output_file: Optional[str] = None,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    scopes: Optional[List[str]] = None,
) -> Dict[str, object]:
    from cognisync.control_plane import issue_control_plane_token

    require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, "issue shared-workspace peer bundles")
    payload = ensure_shared_workspace_manifest(workspace)
    control_plane_url = str(payload.get("published_control_plane_url", "")).strip()
    if not control_plane_url:
        raise SharingError("Bind a shared control-plane URL before issuing peer bundles.")
    normalized_ref = peer_ref.strip()
    if not normalized_ref:
        raise SharingError("A peer id is required.")
    peers = [dict(item) for item in list(payload.get("peers", []))]
    peer = next((item for item in peers if str(item.get("peer_id", "")) == normalized_ref), None)
    if peer is None:
        raise SharingError(f"Could not find shared peer '{normalized_ref}'.")
    if str(peer.get("status", "")) != "accepted":
        raise SharingError(f"Peer '{normalized_ref}' must be accepted before a bundle can be issued.")

    token_payload, raw_token = issue_control_plane_token(
        workspace,
        principal_id=str(peer.get("peer_id", "")),
        scopes=scopes,
        actor_id=actor_id,
        description=f"shared-workspace bundle for {peer.get('peer_id', '')}",
    )
    issued_at = utc_timestamp()
    peer["last_bundle_issued_at"] = issued_at
    peer["last_token_id"] = str(token_payload.get("token_id", ""))
    peer["last_bundle_scopes"] = [str(item) for item in list(token_payload.get("scopes", []))]
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
    }


def _sorted_capabilities(capabilities: List[str]) -> List[str]:
    return sorted({str(item).strip() for item in capabilities if str(item).strip()})


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
                "last_bundle_issued_at": str(peer.get("last_bundle_issued_at", "")),
                "last_token_id": str(peer.get("last_token_id", "")),
                "last_bundle_scopes": [str(item) for item in list(peer.get("last_bundle_scopes", []))],
                "updated_at": str(peer.get("updated_at", "")) or utc_timestamp(),
            }
        )
    normalized.sort(key=lambda item: (item["status"] != "accepted", item["peer_id"]))
    return normalized


def _write_shared_workspace_manifest(workspace: "Workspace", payload: Dict[str, object]) -> None:
    normalized = _normalize_shared_workspace_manifest(workspace, payload)
    normalized["generated_at"] = utc_timestamp()
    workspace.shared_workspace_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.shared_workspace_manifest_path.write_text(
        json.dumps(normalized, indent=2, sort_keys=True),
        encoding="utf-8",
    )
