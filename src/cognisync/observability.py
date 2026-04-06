from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from cognisync.access import load_access_manifest
from cognisync.collaboration import load_collaboration_manifest
from cognisync.control_plane import load_control_plane_manifest
from cognisync.connectors import list_connectors
from cognisync.jobs import list_jobs
from cognisync.manifests import read_json_manifest
from cognisync.sharing import load_shared_workspace_manifest
from cognisync.sync import list_sync_events
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


def write_audit_manifest(workspace: Workspace) -> str:
    payload = build_audit_manifest(workspace)
    workspace.audit_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.audit_manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return workspace.relative_path(workspace.audit_manifest_path)


def render_audit_history(workspace: Workspace) -> str:
    write_audit_manifest(workspace)
    payload = read_json_manifest(workspace.audit_manifest_path)
    events = list(payload.get("events", []))
    summary = dict(payload.get("summary", {}))
    lines = [
        "# Audit History",
        "",
        f"- Event count: `{summary.get('total_count', 0)}`",
        f"- Kinds: `{json.dumps(summary.get('counts_by_kind', {}), sort_keys=True)}`",
        f"- Statuses: `{json.dumps(summary.get('counts_by_status', {}), sort_keys=True)}`",
    ]
    if not events:
        lines.extend(["", "No audit events found."])
        return "\n".join(lines)
    lines.extend(["", "## Events", ""])
    for event in events[:12]:
        lines.append(
            "- "
            f"`{event.get('event_kind', '')}` "
            f"`{event.get('status', '')}` "
            f"`{event.get('category', '')}` "
            f"{event.get('label', '')}"
        )
        lines.append(f"  path: `{event.get('path', '')}`")
    return "\n".join(lines)


def build_audit_manifest(workspace: Workspace, limit: int = 200) -> Dict[str, object]:
    events: List[Dict[str, object]] = []

    access_payload = load_access_manifest(workspace)
    for member in list(access_payload.get("members", [])):
        events.append(
            {
                "event_kind": "access",
                "category": str(member.get("role", "")),
                "label": str(member.get("display_name", member.get("principal_id", ""))),
                "status": str(member.get("status", "active")),
                "generated_at": str(member.get("updated_at", member.get("added_at", ""))),
                "path": workspace.relative_path(workspace.access_manifest_path),
            }
        )

    collaboration_payload = load_collaboration_manifest(workspace)
    for thread in list(collaboration_payload.get("threads", [])):
        artifact_path = str(thread.get("artifact_path", ""))
        artifact_label = str(thread.get("artifact_title", artifact_path))
        for request in list(thread.get("requests", [])):
            actor = dict(request.get("requested_by", {}))
            events.append(
                {
                    "event_kind": "collaboration",
                    "category": "request_review",
                    "label": artifact_label,
                    "status": str(thread.get("status", "")),
                    "generated_at": str(request.get("requested_at", "")),
                    "path": workspace.relative_path(workspace.collaboration_manifest_path),
                    "actor_id": str(actor.get("principal_id", "")),
                    "artifact_path": artifact_path,
                }
            )
        for comment in list(thread.get("comments", [])):
            actor = dict(comment.get("actor", {}))
            events.append(
                {
                    "event_kind": "collaboration",
                    "category": "comment",
                    "label": artifact_label,
                    "status": str(thread.get("status", "")),
                    "generated_at": str(comment.get("created_at", "")),
                    "path": workspace.relative_path(workspace.collaboration_manifest_path),
                    "actor_id": str(actor.get("principal_id", "")),
                    "artifact_path": artifact_path,
                }
            )
        for decision in list(thread.get("decisions", [])):
            actor = dict(decision.get("actor", {}))
            events.append(
                {
                    "event_kind": "collaboration",
                    "category": str(decision.get("decision", "")),
                    "label": artifact_label,
                    "status": str(thread.get("status", "")),
                    "generated_at": str(decision.get("created_at", "")),
                    "path": workspace.relative_path(workspace.collaboration_manifest_path),
                    "actor_id": str(actor.get("principal_id", "")),
                    "artifact_path": artifact_path,
                }
            )
        resolved_at = str(thread.get("resolved_at", "")).strip()
        resolved_by = dict(thread.get("resolved_by", {}))
        if resolved_at:
            events.append(
                {
                    "event_kind": "collaboration",
                    "category": "resolved",
                    "label": artifact_label,
                    "status": str(thread.get("status", "")),
                    "generated_at": resolved_at,
                    "path": workspace.relative_path(workspace.collaboration_manifest_path),
                    "actor_id": str(resolved_by.get("principal_id", "")),
                    "artifact_path": artifact_path,
                }
            )

    sharing_payload = load_shared_workspace_manifest(workspace)
    for peer in list(sharing_payload.get("peers", [])):
        events.append(
            {
                "event_kind": "sharing",
                "category": str(peer.get("role", "")),
                "label": str(peer.get("peer_id", "")),
                "status": str(peer.get("status", "")),
                "generated_at": str(peer.get("updated_at", peer.get("shared_at", ""))),
                "path": workspace.relative_path(workspace.shared_workspace_manifest_path),
            }
        )

    control_plane_payload = load_control_plane_manifest(workspace)
    for invite in list(control_plane_payload.get("invites", [])):
        events.append(
            {
                "event_kind": "control_plane",
                "category": "invite",
                "label": str(invite.get("principal_id", "")),
                "status": str(invite.get("status", "")),
                "generated_at": str(invite.get("accepted_at", invite.get("created_at", ""))),
                "path": workspace.relative_path(workspace.control_plane_manifest_path),
            }
        )
    for token in list(control_plane_payload.get("tokens", [])):
        events.append(
            {
                "event_kind": "control_plane",
                "category": "token",
                "label": str(token.get("principal_id", "")),
                "status": str(token.get("status", "")),
                "generated_at": str(token.get("created_at", "")),
                "path": workspace.relative_path(workspace.control_plane_manifest_path),
            }
        )

    for connector in list_connectors(workspace):
        events.append(
            {
                "event_kind": "connector",
                "category": str(connector.get("kind", "")),
                "label": str(connector.get("connector_id", "")),
                "status": "synced" if connector.get("last_synced_at") else "registered",
                "generated_at": str(connector.get("last_synced_at") or connector.get("updated_at") or connector.get("created_at", "")),
                "path": workspace.relative_path(workspace.connector_registry_path),
            }
        )

    for job in list_jobs(workspace):
        events.append(
            {
                "event_kind": "job",
                "category": str(job.get("job_type", "")),
                "label": str(job.get("title", job.get("job_id", ""))),
                "status": str(job.get("status", "")),
                "generated_at": str(job.get("updated_at", job.get("created_at", ""))),
                "path": workspace.relative_path(workspace.job_manifests_dir / f"{job.get('job_id', '')}.json"),
            }
        )

    for path in sorted(workspace.runs_dir.glob("*.json")):
        manifest = read_json_manifest(path)
        events.append(
            {
                "event_kind": "run",
                "category": str(manifest.get("run_kind", "")),
                "label": str(manifest.get("question", manifest.get("run_label", path.stem))),
                "status": str(manifest.get("status", "")),
                "generated_at": str(manifest.get("generated_at", "")),
                "path": workspace.relative_path(path),
            }
        )

    for event in list_sync_events(workspace):
        sync_id = str(event.get("sync_id", ""))
        events.append(
            {
                "event_kind": "sync",
                "category": str(event.get("operation", "")),
                "label": sync_id,
                "status": str(event.get("status", "")),
                "generated_at": str(event.get("generated_at", "")),
                "path": workspace.relative_path(workspace.sync_manifests_dir / f"{sync_id}.json"),
            }
        )

    events.sort(
        key=lambda item: (
            str(item.get("generated_at", "")),
            str(item.get("event_kind", "")),
            str(item.get("label", "")),
        ),
        reverse=True,
    )
    limited_events = events[:limit]
    counts_by_kind: Dict[str, int] = {}
    counts_by_status: Dict[str, int] = {}
    for event in limited_events:
        event_kind = str(event.get("event_kind", "unknown"))
        status = str(event.get("status", "unknown"))
        counts_by_kind[event_kind] = counts_by_kind.get(event_kind, 0) + 1
        counts_by_status[status] = counts_by_status.get(status, 0) + 1
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "events": limited_events,
        "summary": {
            "total_count": len(limited_events),
            "counts_by_kind": dict(sorted(counts_by_kind.items())),
            "counts_by_status": dict(sorted(counts_by_status.items())),
        },
    }


def write_usage_manifest(workspace: Workspace) -> str:
    payload = build_usage_manifest(workspace)
    workspace.usage_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.usage_manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return workspace.relative_path(workspace.usage_manifest_path)


def render_usage_report(workspace: Workspace) -> str:
    write_usage_manifest(workspace)
    payload = read_json_manifest(workspace.usage_manifest_path)
    summary = dict(payload.get("summary", {}))
    lines = [
        "# Usage Summary",
        "",
        f"- Access members: `{summary.get('access_member_count', 0)}`",
        f"- Runs: `{summary.get('run_count', 0)}`",
        f"- Jobs: `{summary.get('job_count', 0)}`",
        f"- Sync events: `{summary.get('sync_event_count', 0)}`",
        f"- Connectors: `{summary.get('connector_count', 0)}`",
        f"- Collaboration threads: `{summary.get('collaboration_thread_count', 0)}`",
        f"- Storage bytes: `{summary.get('storage_total_bytes', 0)}`",
        f"- Run kinds: `{json.dumps(summary.get('run_counts_by_kind', {}), sort_keys=True)}`",
        f"- Job types: `{json.dumps(summary.get('job_counts_by_type', {}), sort_keys=True)}`",
        f"- Connector kinds: `{json.dumps(summary.get('connector_counts_by_kind', {}), sort_keys=True)}`",
        f"- Collaboration statuses: `{json.dumps(summary.get('collaboration_counts_by_status', {}), sort_keys=True)}`",
    ]
    return "\n".join(lines)


def build_usage_manifest(workspace: Workspace) -> Dict[str, object]:
    access_payload = load_access_manifest(workspace)
    collaboration_payload = load_collaboration_manifest(workspace)
    sharing_payload = load_shared_workspace_manifest(workspace)
    control_plane_payload = load_control_plane_manifest(workspace)
    connectors = list_connectors(workspace)
    jobs = list_jobs(workspace)
    sync_events = list_sync_events(workspace)
    runs = [read_json_manifest(path) for path in sorted(workspace.runs_dir.glob("*.json"))]
    collaboration_threads = [dict(item) for item in list(collaboration_payload.get("threads", []))]

    access_counts_by_role = _count_values(str(member.get("role", "viewer")) for member in list(access_payload.get("members", [])))
    collaboration_counts_by_status = _count_values(str(item.get("status", "unknown")) for item in collaboration_threads)
    connector_counts_by_kind = _count_values(str(item.get("kind", "unknown")) for item in connectors)
    job_counts_by_type = _count_values(str(item.get("job_type", "unknown")) for item in jobs)
    job_counts_by_status = _count_values(str(item.get("status", "unknown")) for item in jobs)
    run_counts_by_kind = _count_values(str(item.get("run_kind", "unknown")) for item in runs)
    run_counts_by_status = _count_values(str(item.get("status", "unknown")) for item in runs)
    storage_counts, storage_total_bytes = _measure_storage(workspace)

    summary = {
        "access_member_count": len(list(access_payload.get("members", []))),
        "shared_peer_count": len(list(sharing_payload.get("peers", []))),
        "accepted_shared_peer_count": sum(
            1 for peer in list(sharing_payload.get("peers", [])) if str(peer.get("status", "")) == "accepted"
        ),
        "control_plane_token_count": len(list(control_plane_payload.get("tokens", []))),
        "active_control_plane_token_count": sum(
            1 for token in list(control_plane_payload.get("tokens", [])) if str(token.get("status", "")) == "active"
        ),
        "access_counts_by_role": access_counts_by_role,
        "collaboration_thread_count": len(collaboration_threads),
        "collaboration_comment_count": sum(len(list(item.get("comments", []))) for item in collaboration_threads),
        "collaboration_decision_count": sum(len(list(item.get("decisions", []))) for item in collaboration_threads),
        "collaboration_request_count": sum(len(list(item.get("requests", []))) for item in collaboration_threads),
        "collaboration_counts_by_status": collaboration_counts_by_status,
        "connector_count": len(connectors),
        "connector_counts_by_kind": connector_counts_by_kind,
        "connector_result_count": sum(int(item.get("last_result_count", 0) or 0) for item in connectors),
        "job_count": len(jobs),
        "job_counts_by_type": job_counts_by_type,
        "job_counts_by_status": job_counts_by_status,
        "run_count": len(runs),
        "run_counts_by_kind": run_counts_by_kind,
        "run_counts_by_status": run_counts_by_status,
        "sync_event_count": len(sync_events),
        "sync_total_file_count": sum(int(item.get("file_count", 0) or 0) for item in sync_events),
        "storage_bytes_by_area": storage_counts,
        "storage_total_bytes": storage_total_bytes,
    }
    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "summary": summary,
    }


def _count_values(values) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _measure_storage(workspace: Workspace) -> tuple[Dict[str, int], int]:
    areas = {
        "raw": workspace.raw_dir,
        "wiki": workspace.wiki_dir,
        "outputs": workspace.outputs_dir,
        "state": workspace.state_dir,
    }
    counts: Dict[str, int] = {}
    total_bytes = 0
    for name, path in areas.items():
        size = 0
        if path.exists():
            for child in path.rglob("*"):
                if child.is_file():
                    size += child.stat().st_size
        counts[name] = size
        total_bytes += size
    return counts, total_bytes
