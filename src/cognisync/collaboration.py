from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

from cognisync.access import DEFAULT_LOCAL_OPERATOR_ID, REVIEW_ACTION_ROLES, require_access_role
from cognisync.utils import slugify, utc_timestamp

if TYPE_CHECKING:
    from cognisync.workspace import Workspace


COLLABORATION_ACTION_ROLES = ("editor", "reviewer", "operator")
COLLABORATION_RESOLVE_ROLES = ("editor", "operator")


class CollaborationError(RuntimeError):
    pass


def ensure_collaboration_manifest(workspace: "Workspace") -> Dict[str, object]:
    payload = load_collaboration_manifest(workspace)
    _write_collaboration_manifest(workspace, payload)
    return payload


def load_collaboration_manifest(workspace: "Workspace") -> Dict[str, object]:
    if workspace.collaboration_manifest_path.exists():
        payload = json.loads(workspace.collaboration_manifest_path.read_text(encoding="utf-8"))
    else:
        payload = default_collaboration_manifest()
    return _normalize_collaboration_manifest(payload)


def render_collaboration_threads(workspace: "Workspace") -> str:
    payload = ensure_collaboration_manifest(workspace)
    threads = list(payload.get("threads", []))
    summary = dict(payload.get("summary", {}))
    lines = [
        "# Collaboration",
        "",
        f"- Thread count: `{summary.get('thread_count', 0)}`",
        f"- Statuses: `{json.dumps(summary.get('counts_by_status', {}), sort_keys=True)}`",
        f"- Decisions: `{summary.get('decision_count', 0)}`",
        f"- Comments: `{summary.get('comment_count', 0)}`",
    ]
    if not threads:
        lines.extend(["", "No collaboration threads found."])
        return "\n".join(lines)
    lines.extend(["", "## Threads", ""])
    for thread in threads:
        lines.append(
            "- "
            f"`{thread.get('status', '')}` "
            f"`{thread.get('artifact_path', '')}` "
            f"{thread.get('artifact_title', thread.get('artifact_path', ''))}"
        )
        requested_by = dict(thread.get("requested_by", {}))
        if requested_by:
            lines.append(f"  requested_by: `{requested_by.get('principal_id', '')}`")
        if thread.get("assignees"):
            assignees = ", ".join(
                f"`{dict(assignee).get('principal_id', '')}`"
                for assignee in list(thread.get("assignees", []))
                if dict(assignee).get("principal_id")
            )
            lines.append(f"  assignees: {assignees}")
        decisions = list(thread.get("decisions", []))
        if decisions:
            last_decision = decisions[-1]
            lines.append(
                "  last_decision: "
                f"`{last_decision.get('decision', '')}` by "
                f"`{dict(last_decision.get('actor', {})).get('principal_id', '')}`"
            )
    return "\n".join(lines)


def list_collaboration_threads(workspace: "Workspace") -> List[Dict[str, object]]:
    payload = ensure_collaboration_manifest(workspace)
    return [dict(item) for item in list(payload.get("threads", []))]


def request_review(
    workspace: "Workspace",
    artifact_path: str,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
    assignee_ids: Optional[List[str]] = None,
    note: str = "",
) -> Dict[str, object]:
    actor = require_access_role(
        workspace,
        actor_id,
        COLLABORATION_ACTION_ROLES,
        "request artifact reviews",
    )
    normalized_path = _normalize_artifact_path(workspace, artifact_path)
    assignees = _resolve_assignees(workspace, assignee_ids or [])
    payload = ensure_collaboration_manifest(workspace)
    threads = {str(item.get("artifact_path", "")): dict(item) for item in list(payload.get("threads", []))}
    existing = dict(threads.get(normalized_path, {}))
    now = utc_timestamp()
    request_entry = {
        "requested_at": now,
        "requested_by": _serialize_actor(actor),
        "assignee_ids": [str(item.get("principal_id", "")) for item in assignees],
        "note": note.strip(),
    }
    requests = [dict(item) for item in list(existing.get("requests", []))]
    requests.append(request_entry)
    thread = {
        "artifact_path": normalized_path,
        "artifact_title": Path(normalized_path).name,
        "status": "pending_review",
        "requested_by": _serialize_actor(actor),
        "requested_at": str(existing.get("requested_at", now)) if existing else now,
        "assignees": assignees,
        "requests": requests,
        "comments": [dict(item) for item in list(existing.get("comments", []))],
        "decisions": [dict(item) for item in list(existing.get("decisions", []))],
        "created_at": str(existing.get("created_at", now)) if existing else now,
        "updated_at": now,
        "resolved_at": "",
        "resolved_by": {},
    }
    threads[normalized_path] = thread
    payload["threads"] = _sorted_threads(threads.values())
    _write_collaboration_manifest(workspace, payload)
    return thread


def add_comment(
    workspace: "Workspace",
    artifact_path: str,
    actor_id: str,
    message: str,
) -> Dict[str, object]:
    actor = require_access_role(
        workspace,
        actor_id,
        COLLABORATION_ACTION_ROLES,
        "comment on artifact reviews",
    )
    normalized_path = _normalize_artifact_path(workspace, artifact_path)
    normalized_message = message.strip()
    if not normalized_message:
        raise CollaborationError("A non-empty comment message is required.")
    payload = ensure_collaboration_manifest(workspace)
    thread = _require_thread(payload, normalized_path)
    now = utc_timestamp()
    comment = {
        "comment_id": _manifest_id("comment", normalized_path, actor_id, len(list(thread.get("comments", []))) + 1),
        "created_at": now,
        "actor": _serialize_actor(actor),
        "message": normalized_message,
    }
    comments = [dict(item) for item in list(thread.get("comments", []))]
    comments.append(comment)
    thread["comments"] = comments
    thread["updated_at"] = now
    _upsert_thread(workspace, payload, thread)
    return comment


def record_decision(
    workspace: "Workspace",
    artifact_path: str,
    actor_id: str,
    decision: str,
    summary: str = "",
) -> Dict[str, object]:
    normalized_decision = decision.strip().lower()
    if normalized_decision not in {"approved", "changes_requested"}:
        raise CollaborationError("Decision must be either 'approved' or 'changes_requested'.")
    actor = require_access_role(
        workspace,
        actor_id,
        REVIEW_ACTION_ROLES,
        "record review decisions",
    )
    normalized_path = _normalize_artifact_path(workspace, artifact_path)
    payload = ensure_collaboration_manifest(workspace)
    thread = _require_thread(payload, normalized_path)
    now = utc_timestamp()
    decision_entry = {
        "decision_id": _manifest_id("decision", normalized_path, actor_id, len(list(thread.get("decisions", []))) + 1),
        "created_at": now,
        "actor": _serialize_actor(actor),
        "decision": normalized_decision,
        "summary": summary.strip(),
    }
    decisions = [dict(item) for item in list(thread.get("decisions", []))]
    decisions.append(decision_entry)
    thread["decisions"] = decisions
    thread["status"] = normalized_decision
    thread["resolved_at"] = ""
    thread["resolved_by"] = {}
    thread["updated_at"] = now
    _upsert_thread(workspace, payload, thread)
    return decision_entry


def resolve_review(
    workspace: "Workspace",
    artifact_path: str,
    actor_id: str,
) -> Dict[str, object]:
    actor = require_access_role(
        workspace,
        actor_id,
        COLLABORATION_RESOLVE_ROLES,
        "resolve collaboration threads",
    )
    normalized_path = _normalize_artifact_path(workspace, artifact_path)
    payload = ensure_collaboration_manifest(workspace)
    thread = _require_thread(payload, normalized_path)
    now = utc_timestamp()
    thread["status"] = "resolved"
    thread["resolved_at"] = now
    thread["resolved_by"] = _serialize_actor(actor)
    thread["updated_at"] = now
    _upsert_thread(workspace, payload, thread)
    return thread


def default_collaboration_manifest() -> Dict[str, object]:
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "threads": [],
        "summary": _build_summary([]),
    }


def _normalize_collaboration_manifest(payload: Dict[str, object]) -> Dict[str, object]:
    threads = _sorted_threads(
        _normalize_thread(dict(item))
        for item in list(payload.get("threads", []))
        if isinstance(item, dict) and str(item.get("artifact_path", "")).strip()
    )
    return {
        "schema_version": 1,
        "generated_at": str(payload.get("generated_at", "")) or utc_timestamp(),
        "threads": threads,
        "summary": _build_summary(threads),
    }


def _normalize_thread(thread: Dict[str, object]) -> Dict[str, object]:
    created_at = str(thread.get("created_at", "")) or utc_timestamp()
    updated_at = str(thread.get("updated_at", "")) or created_at
    requested_by = dict(thread.get("requested_by", {}))
    assignees = [_serialize_actor(dict(item)) for item in list(thread.get("assignees", [])) if dict(item).get("principal_id")]
    requests = []
    for item in list(thread.get("requests", [])):
        request = dict(item)
        requests.append(
            {
                "requested_at": str(request.get("requested_at", "")) or created_at,
                "requested_by": _serialize_actor(dict(request.get("requested_by", requested_by))),
                "assignee_ids": [str(value) for value in list(request.get("assignee_ids", [])) if str(value).strip()],
                "note": str(request.get("note", "")),
            }
        )
    comments = []
    for item in list(thread.get("comments", [])):
        comment = dict(item)
        comments.append(
            {
                "comment_id": str(comment.get("comment_id", "")) or _manifest_id("comment", str(thread.get("artifact_path", "")), "", len(comments) + 1),
                "created_at": str(comment.get("created_at", "")) or updated_at,
                "actor": _serialize_actor(dict(comment.get("actor", {}))),
                "message": str(comment.get("message", "")),
            }
        )
    decisions = []
    for item in list(thread.get("decisions", [])):
        decision = dict(item)
        decisions.append(
            {
                "decision_id": str(decision.get("decision_id", "")) or _manifest_id("decision", str(thread.get("artifact_path", "")), "", len(decisions) + 1),
                "created_at": str(decision.get("created_at", "")) or updated_at,
                "actor": _serialize_actor(dict(decision.get("actor", {}))),
                "decision": str(decision.get("decision", "approved")) or "approved",
                "summary": str(decision.get("summary", "")),
            }
        )
    resolved_by = dict(thread.get("resolved_by", {}))
    return {
        "artifact_path": str(thread.get("artifact_path", "")).strip(),
        "artifact_title": str(thread.get("artifact_title", "")).strip() or Path(str(thread.get("artifact_path", ""))).name,
        "status": str(thread.get("status", "pending_review")).strip() or "pending_review",
        "requested_by": _serialize_actor(requested_by),
        "requested_at": str(thread.get("requested_at", "")) or created_at,
        "assignees": assignees,
        "requests": requests,
        "comments": comments,
        "decisions": decisions,
        "created_at": created_at,
        "updated_at": updated_at,
        "resolved_at": str(thread.get("resolved_at", "")),
        "resolved_by": _serialize_actor(resolved_by) if resolved_by else {},
    }


def _build_summary(threads: List[Dict[str, object]]) -> Dict[str, object]:
    counts_by_status: Dict[str, int] = {}
    assignee_count = 0
    request_count = 0
    comment_count = 0
    decision_count = 0
    for thread in threads:
        status = str(thread.get("status", "pending_review"))
        counts_by_status[status] = counts_by_status.get(status, 0) + 1
        assignee_count += len(list(thread.get("assignees", [])))
        request_count += len(list(thread.get("requests", [])))
        comment_count += len(list(thread.get("comments", [])))
        decision_count += len(list(thread.get("decisions", [])))
    return {
        "thread_count": len(threads),
        "counts_by_status": dict(sorted(counts_by_status.items())),
        "assignee_count": assignee_count,
        "request_count": request_count,
        "comment_count": comment_count,
        "decision_count": decision_count,
    }


def _write_collaboration_manifest(workspace: "Workspace", payload: Dict[str, object]) -> None:
    normalized_payload = _normalize_collaboration_manifest(payload)
    normalized_payload["generated_at"] = utc_timestamp()
    workspace.collaboration_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.collaboration_manifest_path.write_text(
        json.dumps(normalized_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _normalize_artifact_path(workspace: "Workspace", artifact_path: str) -> str:
    raw_path = Path(artifact_path.strip()).expanduser()
    if not str(raw_path):
        raise CollaborationError("An artifact path is required.")
    candidate = raw_path if raw_path.is_absolute() else (workspace.root / raw_path)
    candidate = candidate.resolve()
    try:
        relative = candidate.relative_to(workspace.root).as_posix()
    except ValueError as error:
        raise CollaborationError("Artifact paths must stay inside the workspace root.") from error
    if not candidate.exists() or not candidate.is_file():
        raise CollaborationError(f"Could not find artifact '{relative}' in the workspace.")
    return relative


def _resolve_assignees(workspace: "Workspace", assignee_ids: List[str]) -> List[Dict[str, str]]:
    assignees: List[Dict[str, str]] = []
    for principal_id in assignee_ids:
        member = require_access_role(
            workspace,
            principal_id,
            REVIEW_ACTION_ROLES,
            "receive artifact review assignments",
        )
        assignees.append(_serialize_actor(member))
    assignees.sort(key=lambda item: item["principal_id"])
    return assignees


def _serialize_actor(actor: Dict[str, object]) -> Dict[str, str]:
    principal_id = str(actor.get("principal_id", "")).strip()
    if not principal_id:
        return {}
    return {
        "principal_id": principal_id,
        "display_name": str(actor.get("display_name", "")).strip() or principal_id,
        "role": str(actor.get("role", "")).strip(),
        "status": str(actor.get("status", "")).strip() or "active",
    }


def _sorted_threads(threads) -> List[Dict[str, object]]:
    normalized = [dict(item) for item in threads if str(dict(item).get("artifact_path", "")).strip()]
    normalized.sort(
        key=lambda item: (
            str(item.get("updated_at", "")),
            str(item.get("artifact_path", "")),
        ),
        reverse=True,
    )
    return normalized


def _require_thread(payload: Dict[str, object], artifact_path: str) -> Dict[str, object]:
    for item in list(payload.get("threads", [])):
        if str(item.get("artifact_path", "")) == artifact_path:
            return dict(item)
    raise CollaborationError(f"Could not find a collaboration thread for '{artifact_path}'.")


def _upsert_thread(workspace: "Workspace", payload: Dict[str, object], thread: Dict[str, object]) -> None:
    threads = {
        str(item.get("artifact_path", "")): dict(item)
        for item in list(payload.get("threads", []))
        if str(item.get("artifact_path", "")).strip()
    }
    threads[str(thread.get("artifact_path", ""))] = dict(thread)
    payload["threads"] = _sorted_threads(threads.values())
    _write_collaboration_manifest(workspace, payload)


def _manifest_id(prefix: str, artifact_path: str, actor_id: str, sequence: int) -> str:
    artifact_slug = slugify(Path(artifact_path).stem or "artifact")[:32] or "artifact"
    actor_slug = slugify(actor_id)[:24] or "actor"
    return f"{prefix}-{artifact_slug}-{actor_slug}-{sequence}"
