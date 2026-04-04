from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Dict, List

from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


def write_notifications_manifest(workspace: Workspace) -> str:
    payload = build_notification_manifest(workspace)
    workspace.notifications_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.notifications_manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return workspace.relative_path(workspace.notifications_manifest_path)


def render_notifications(workspace: Workspace) -> str:
    write_notifications_manifest(workspace)
    payload = json.loads(workspace.notifications_manifest_path.read_text(encoding="utf-8"))
    notifications = list(payload.get("notifications", []))
    summary = dict(payload.get("summary", {}))
    lines = [
        "# Notifications",
        "",
        f"- Notification count: `{summary.get('total_count', 0)}`",
        f"- Severities: `{json.dumps(summary.get('counts_by_severity', {}), sort_keys=True)}`",
        f"- Kinds: `{json.dumps(summary.get('counts_by_kind', {}), sort_keys=True)}`",
    ]
    if not notifications:
        lines.extend(["", "No active notifications found."])
        return "\n".join(lines)
    lines.extend(["", "## Inbox", ""])
    for item in notifications:
        lines.append(
            "- "
            f"`{item.get('severity', '')}` "
            f"`{item.get('kind', '')}` "
            f"{item.get('title', '')}"
        )
        lines.append(f"  path: `{item.get('path', '')}`")
        lines.append(f"  detail: {item.get('detail', '')}")
    return "\n".join(lines)


def build_notification_manifest(workspace: Workspace) -> Dict[str, object]:
    notifications: List[Dict[str, object]] = []

    review_payload = _read_json(workspace.review_queue_manifest_path)
    open_items = list(review_payload.get("open_items", []))
    if open_items:
        notifications.append(
            {
                "id": "review-queue-open-items",
                "kind": "review_queue",
                "severity": "warn",
                "title": f"Review queue has {len(open_items)} open item(s)",
                "detail": "Open review work is waiting in the durable review queue.",
                "path": workspace.relative_path(workspace.review_queue_manifest_path),
                "related_paths": [str(item.get("path", "")) for item in open_items[:8] if item.get("path")],
            }
        )

    queue_payload = _read_json(workspace.job_queue_manifest_path)
    queued_count = int(queue_payload.get("queued_count", 0) or 0)
    if queued_count:
        notifications.append(
            {
                "id": "job-queue-backlog",
                "kind": "job_backlog",
                "severity": "info",
                "title": f"Job queue has {queued_count} queued job(s)",
                "detail": "Queued work is waiting for the local worker loop.",
                "path": workspace.relative_path(workspace.job_queue_manifest_path),
                "related_paths": [
                    str(item.get("job_id", ""))
                    for item in list(queue_payload.get("jobs", []))[:8]
                    if str(item.get("status", "")) == "queued"
                ],
            }
        )
    for item in list(queue_payload.get("jobs", [])):
        if str(item.get("status", "")) != "failed":
            continue
        notifications.append(
            {
                "id": f"job-failed:{item.get('job_id', '')}",
                "kind": "job_failed",
                "severity": "high",
                "title": f"Job failed: {item.get('title', item.get('job_id', ''))}",
                "detail": "A persisted job reached a terminal failure state and may need retry or operator attention.",
                "path": workspace.relative_path(workspace.job_manifests_dir / f"{item.get('job_id', '')}.json"),
                "related_paths": [],
            }
        )

    for manifest_path in sorted(workspace.runs_dir.glob("*.json"), reverse=True):
        payload = _read_json(manifest_path)
        status = str(payload.get("status", ""))
        if status == "failed_validation":
            notifications.append(
                {
                    "id": f"run-failed-validation:{manifest_path.stem}",
                    "kind": "run_failed_validation",
                    "severity": "high",
                    "title": f"Research validation failed: {payload.get('question', payload.get('run_label', manifest_path.stem))}",
                    "detail": "A recorded run failed validation and needs remediation before it should be trusted downstream.",
                    "path": workspace.relative_path(manifest_path),
                    "related_paths": [str(payload.get("validation_report_path", ""))] if payload.get("validation_report_path") else [],
                }
            )
        elif status == "completed_with_warnings":
            notifications.append(
                {
                    "id": f"run-warning:{manifest_path.stem}",
                    "kind": "run_warning",
                    "severity": "warn",
                    "title": f"Run completed with warnings: {payload.get('question', payload.get('run_label', manifest_path.stem))}",
                    "detail": "A recorded run completed, but the validation layer still reported warnings.",
                    "path": workspace.relative_path(manifest_path),
                    "related_paths": [str(payload.get("validation_report_path", ""))] if payload.get("validation_report_path") else [],
                }
            )

    connector_payload = _read_json(workspace.connector_registry_path)
    connectors = list(connector_payload.get("connectors", []))
    unsynced_connectors = [item for item in connectors if not item.get("last_synced_at")]
    if unsynced_connectors:
        notifications.append(
            {
                "id": "connector-backlog",
                "kind": "connector_backlog",
                "severity": "info",
                "title": f"Connector registry has {len(unsynced_connectors)} unsynced connector(s)",
                "detail": "Registered connectors exist but have not been pulled into the workspace yet.",
                "path": workspace.relative_path(workspace.connector_registry_path),
                "related_paths": [str(item.get("connector_id", "")) for item in unsynced_connectors[:8]],
            }
        )
    due_connectors = [item for item in connectors if _connector_is_due(item)]
    if due_connectors:
        notifications.append(
            {
                "id": "connector-due",
                "kind": "connector_due",
                "severity": "info",
                "title": f"Connector registry has {len(due_connectors)} scheduled connector(s) due",
                "detail": "Scheduled connector subscriptions are ready for a sync pass.",
                "path": workspace.relative_path(workspace.connector_registry_path),
                "related_paths": [str(item.get("connector_id", "")) for item in due_connectors[:8]],
            }
        )

    counts_by_kind: Dict[str, int] = {}
    counts_by_severity: Dict[str, int] = {}
    for item in notifications:
        kind = str(item.get("kind", "unknown"))
        severity = str(item.get("severity", "info"))
        counts_by_kind[kind] = counts_by_kind.get(kind, 0) + 1
        counts_by_severity[severity] = counts_by_severity.get(severity, 0) + 1

    return {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "notifications": notifications,
        "summary": {
            "total_count": len(notifications),
            "counts_by_kind": dict(sorted(counts_by_kind.items())),
            "counts_by_severity": dict(sorted(counts_by_severity.items())),
        },
    }


def _read_json(path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _connector_is_due(connector: Dict[str, object]) -> bool:
    subscription = dict(connector.get("subscription", {}))
    if not bool(subscription.get("enabled", False)):
        return False
    next_sync_at = str(subscription.get("next_sync_at", "") or "").strip()
    if not next_sync_at:
        return True
    try:
        return datetime.fromisoformat(next_sync_at) <= datetime.now(timezone.utc)
    except ValueError:
        return True
