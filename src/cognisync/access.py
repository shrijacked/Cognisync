from __future__ import annotations

import json
from typing import Dict, List, Optional, TYPE_CHECKING

from cognisync.utils import utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


VALID_ACCESS_ROLES = ("viewer", "editor", "reviewer", "operator")
DEFAULT_LOCAL_OPERATOR_ID = "local-operator"


class AccessError(RuntimeError):
    pass


def ensure_access_manifest(workspace: "Workspace") -> Dict[str, object]:
    payload = load_access_manifest(workspace)
    _write_access_manifest(workspace, payload)
    return payload


def load_access_manifest(workspace: "Workspace") -> Dict[str, object]:
    if workspace.access_manifest_path.exists():
        payload = json.loads(workspace.access_manifest_path.read_text(encoding="utf-8"))
    else:
        payload = default_access_manifest()
    return _normalize_access_manifest(payload)


def render_access_roster(workspace: "Workspace") -> str:
    payload = ensure_access_manifest(workspace)
    members = list(payload.get("members", []))
    counts_by_role: Dict[str, int] = {}
    for member in members:
        role = str(member.get("role", "viewer"))
        counts_by_role[role] = counts_by_role.get(role, 0) + 1

    lines = [
        "# Access Roster",
        "",
        f"- Member count: `{len(members)}`",
        f"- Roles: `{json.dumps(dict(sorted(counts_by_role.items())), sort_keys=True)}`",
    ]
    if not members:
        lines.extend(["", "No access members recorded."])
        return "\n".join(lines)

    lines.extend(["", "## Members", ""])
    for member in members:
        display_name = str(member.get("display_name", "")) or str(member.get("principal_id", ""))
        lines.append(
            "- "
            f"`{member.get('principal_id', '')}` "
            f"`{member.get('role', '')}` "
            f"{display_name}"
        )
    return "\n".join(lines)


def grant_access_member(
    workspace: "Workspace",
    principal_id: str,
    role: str,
    display_name: Optional[str] = None,
) -> Dict[str, object]:
    normalized_id = principal_id.strip()
    normalized_role = role.strip().lower()
    if not normalized_id:
        raise AccessError("A principal id is required.")
    if normalized_role not in VALID_ACCESS_ROLES:
        raise AccessError(
            f"Unsupported access role '{role}'. Expected one of: {', '.join(VALID_ACCESS_ROLES)}."
        )

    payload = ensure_access_manifest(workspace)
    members = {str(item.get("principal_id", "")): dict(item) for item in list(payload.get("members", []))}
    existing = members.get(normalized_id)
    now = utc_timestamp()
    record = {
        "principal_id": normalized_id,
        "display_name": display_name or (str(existing.get("display_name", "")) if existing else normalized_id),
        "role": normalized_role,
        "status": "active",
        "added_at": str(existing.get("added_at", now)) if existing else now,
        "updated_at": now,
    }
    members[normalized_id] = record
    payload["members"] = _sorted_members(members.values())
    _write_access_manifest(workspace, payload)
    return record


def revoke_access_member(workspace: "Workspace", principal_id: str) -> Dict[str, object]:
    normalized_id = principal_id.strip()
    if not normalized_id:
        raise AccessError("A principal id is required.")
    if normalized_id == DEFAULT_LOCAL_OPERATOR_ID:
        raise AccessError("The default local operator cannot be revoked.")

    payload = ensure_access_manifest(workspace)
    members = {str(item.get("principal_id", "")): dict(item) for item in list(payload.get("members", []))}
    record = members.pop(normalized_id, None)
    if record is None:
        raise AccessError(f"Could not find access member '{normalized_id}'.")
    payload["members"] = _sorted_members(members.values())
    _write_access_manifest(workspace, payload)
    return record


def default_access_manifest() -> Dict[str, object]:
    now = utc_timestamp()
    return {
        "schema_version": 1,
        "generated_at": now,
        "members": [
            {
                "principal_id": DEFAULT_LOCAL_OPERATOR_ID,
                "display_name": "Local Operator",
                "role": "operator",
                "status": "active",
                "added_at": now,
                "updated_at": now,
            }
        ],
    }


def _normalize_access_manifest(payload: Dict[str, object]) -> Dict[str, object]:
    members = [dict(item) for item in list(payload.get("members", [])) if isinstance(item, dict)]
    if not any(str(item.get("principal_id", "")) == DEFAULT_LOCAL_OPERATOR_ID for item in members):
        members.append(default_access_manifest()["members"][0])
    normalized_payload = {
        "schema_version": 1,
        "generated_at": str(payload.get("generated_at", "")) or utc_timestamp(),
        "members": _sorted_members(members),
    }
    return normalized_payload


def _sorted_members(members: List[Dict[str, object]]) -> List[Dict[str, object]]:
    normalized: List[Dict[str, object]] = []
    for member in members:
        principal_id = str(member.get("principal_id", "")).strip()
        if not principal_id:
            continue
        normalized.append(
            {
                "principal_id": principal_id,
                "display_name": str(member.get("display_name", "")).strip() or principal_id,
                "role": str(member.get("role", "viewer")).strip().lower() or "viewer",
                "status": str(member.get("status", "active")).strip().lower() or "active",
                "added_at": str(member.get("added_at", "")) or utc_timestamp(),
                "updated_at": str(member.get("updated_at", "")) or str(member.get("added_at", "")) or utc_timestamp(),
            }
        )
    normalized.sort(key=lambda item: (item["role"] != "operator", item["principal_id"]))
    return normalized


def _write_access_manifest(workspace: "Workspace", payload: Dict[str, object]) -> None:
    normalized_payload = _normalize_access_manifest(payload)
    normalized_payload["generated_at"] = utc_timestamp()
    workspace.access_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.access_manifest_path.write_text(
        json.dumps(normalized_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
