from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Dict, List, Optional

from cognisync.change_summaries import capture_change_state, write_change_summary
from cognisync.ingest import IngestError, IngestResult, ingest_repo, ingest_sitemap, ingest_url, ingest_urls
from cognisync.knowledge_surfaces import append_workspace_log
from cognisync.manifests import write_run_manifest, write_workspace_manifests
from cognisync.notifications import write_notifications_manifest
from cognisync.scanner import scan_workspace
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


SUPPORTED_CONNECTOR_KINDS = {"repo", "sitemap", "url", "urls"}
WEEKDAY_ORDER = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


class ConnectorError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConnectorSyncResult:
    connector_id: str
    connector_kind: str
    synced_count: int
    registry_path: Path
    change_summary_path: Path
    run_manifest_path: Path
    result_paths: List[Path]


@dataclass(frozen=True)
class ConnectorBatchSyncResult:
    connector_count: int
    synced_connector_count: int
    total_result_count: int
    registry_path: Path
    run_manifest_path: Path
    connector_results: List[ConnectorSyncResult]


def subscribe_connector(
    workspace: Workspace,
    connector_id: str,
    every_hours: Optional[int] = None,
    weekdays: Optional[List[str]] = None,
    hour: Optional[int] = None,
    minute: int = 0,
    actor: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    normalized_weekdays = _normalize_weekdays(weekdays or [])
    if every_hours is not None and every_hours < 1:
        raise ConnectorError("Connector subscription intervals must be at least 1 hour.")
    if every_hours is None and not normalized_weekdays:
        raise ConnectorError("Pass either --every-hours or at least one --weekday when subscribing a connector.")
    if every_hours is not None and normalized_weekdays:
        raise ConnectorError("Choose either an interval subscription or a weekly schedule, not both.")
    if normalized_weekdays and hour is None:
        raise ConnectorError("A weekly connector subscription requires --hour.")
    if minute < 0 or minute > 59:
        raise ConnectorError("Connector subscription minutes must be between 0 and 59.")
    if hour is not None and (hour < 0 or hour > 23):
        raise ConnectorError("Connector subscription hours must be between 0 and 23.")

    registry = _load_connector_registry(workspace)
    connectors = list(registry.get("connectors", []))
    connector = next((item for item in connectors if str(item.get("connector_id", "")) == connector_id), None)
    if connector is None:
        raise ConnectorError(f"Could not find connector '{connector_id}'.")

    subscribed_at = utc_timestamp()
    existing_subscription = _normalize_subscription(connector.get("subscription", {}))
    if every_hours is not None:
        connector["subscription"] = {
            "enabled": True,
            "schedule_type": "interval",
            "interval_hours": every_hours,
            "weekdays": [],
            "hour": None,
            "minute": None,
            "subscribed_at": subscribed_at,
            "next_sync_at": subscribed_at,
            "last_scheduled_sync_at": existing_subscription.get("last_scheduled_sync_at"),
            "last_scheduler_tick_at": existing_subscription.get("last_scheduler_tick_at"),
            "last_due_at": existing_subscription.get("last_due_at"),
            "last_tick_status": existing_subscription.get("last_tick_status"),
        }
    else:
        connector["subscription"] = {
            "enabled": True,
            "schedule_type": "weekly",
            "interval_hours": None,
            "weekdays": normalized_weekdays,
            "hour": int(hour) if hour is not None else None,
            "minute": int(minute),
            "subscribed_at": subscribed_at,
            "next_sync_at": _next_weekly_timestamp(normalized_weekdays, int(hour or 0), int(minute), subscribed_at),
            "last_scheduled_sync_at": existing_subscription.get("last_scheduled_sync_at"),
            "last_scheduler_tick_at": existing_subscription.get("last_scheduler_tick_at"),
            "last_due_at": existing_subscription.get("last_due_at"),
            "last_tick_status": existing_subscription.get("last_tick_status"),
        }
    connector["updated_at"] = subscribed_at
    connector["updated_by"] = _serialize_actor(actor)
    registry["connectors"] = sorted(connectors, key=lambda item: str(item.get("connector_id", "")))
    _write_connector_registry(workspace, registry)
    write_notifications_manifest(workspace)
    return connector


def unsubscribe_connector(
    workspace: Workspace,
    connector_id: str,
    actor: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    registry = _load_connector_registry(workspace)
    connectors = list(registry.get("connectors", []))
    connector = next((item for item in connectors if str(item.get("connector_id", "")) == connector_id), None)
    if connector is None:
        raise ConnectorError(f"Could not find connector '{connector_id}'.")

    subscription = _normalize_subscription(connector.get("subscription", {}))
    subscription["enabled"] = False
    subscription["schedule_type"] = "interval"
    subscription["interval_hours"] = None
    subscription["weekdays"] = []
    subscription["hour"] = None
    subscription["minute"] = None
    subscription["next_sync_at"] = None
    connector["subscription"] = subscription
    connector["updated_at"] = utc_timestamp()
    connector["updated_by"] = _serialize_actor(actor)
    registry["connectors"] = sorted(connectors, key=lambda item: str(item.get("connector_id", "")))
    _write_connector_registry(workspace, registry)
    write_notifications_manifest(workspace)
    return connector


def add_connector(
    workspace: Workspace,
    kind: str,
    source: str,
    name: Optional[str] = None,
    actor: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    normalized_kind = kind.strip().lower()
    if normalized_kind not in SUPPORTED_CONNECTOR_KINDS:
        raise ConnectorError(
            f"Unsupported connector kind '{kind}'. Expected one of: {', '.join(sorted(SUPPORTED_CONNECTOR_KINDS))}."
        )
    normalized_source = source.strip()
    if not normalized_source:
        raise ConnectorError("A connector source is required.")

    registry = _load_connector_registry(workspace)
    connectors = list(registry.get("connectors", []))
    connector_id = _unique_connector_id(connectors, normalized_kind, name or normalized_source)
    record = {
        "connector_id": connector_id,
        "kind": normalized_kind,
        "name": name or connector_id,
        "source": normalized_source,
        "created_at": utc_timestamp(),
        "updated_at": utc_timestamp(),
        "created_by": _serialize_actor(actor),
        "updated_by": _serialize_actor(actor),
        "last_synced_at": None,
        "last_synced_by": None,
        "last_change_summary_path": None,
        "last_run_manifest_path": None,
        "last_result_count": 0,
        "subscription": _normalize_subscription({}),
    }
    connectors.append(record)
    registry["connectors"] = sorted(connectors, key=lambda item: str(item.get("connector_id", "")))
    _write_connector_registry(workspace, registry)
    write_notifications_manifest(workspace)
    return record


def list_connectors(workspace: Workspace) -> List[Dict[str, object]]:
    registry = _load_connector_registry(workspace)
    connectors = list(registry.get("connectors", []))
    connectors.sort(key=lambda item: str(item.get("connector_id", "")))
    return connectors


def render_connector_list(workspace: Workspace) -> str:
    connectors = list_connectors(workspace)
    lines = [
        "# Connectors",
        "",
        f"- Connector count: `{len(connectors)}`",
    ]
    if not connectors:
        lines.extend(["", "No connector definitions found."])
        return "\n".join(lines)
    lines.extend(["", "## Registry", ""])
    for connector in connectors:
        lines.append(
            "- "
            f"`{connector.get('connector_id', '')}` "
            f"`{connector.get('kind', '')}` "
            f"{connector.get('source', '')}"
        )
        subscription = _normalize_subscription(connector.get("subscription", {}))
        if bool(subscription.get("enabled")):
            if str(subscription.get("schedule_type", "interval")) == "weekly":
                weekday_label = ",".join(str(item) for item in list(subscription.get("weekdays", []))) or "?"
                lines.append(
                    "  "
                    f"schedule: weekly {weekday_label} "
                    f"at {int(subscription.get('hour', 0) or 0):02d}:{int(subscription.get('minute', 0) or 0):02d}"
                    f" next {subscription.get('next_sync_at') or 'unscheduled'}"
                )
            else:
                lines.append(
                    "  "
                    f"schedule: every {subscription.get('interval_hours', '?')}h"
                    f" next {subscription.get('next_sync_at') or 'unscheduled'}"
                )
    return "\n".join(lines)


def list_due_connectors(workspace: Workspace) -> List[Dict[str, object]]:
    return [connector for connector in list_connectors(workspace) if connector_subscription_is_due(connector)]


def sync_connector(
    workspace: Workspace,
    connector_id: str,
    force: bool = False,
    scheduled: bool = False,
    actor: Optional[Dict[str, object]] = None,
) -> ConnectorSyncResult:
    registry = _load_connector_registry(workspace)
    connectors = list(registry.get("connectors", []))
    connector = next((item for item in connectors if str(item.get("connector_id", "")) == connector_id), None)
    if connector is None:
        raise ConnectorError(f"Could not find connector '{connector_id}'.")

    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        results = _run_connector_ingest(workspace, connector, force=force)
    except IngestError as error:
        raise ConnectorError(str(error)) from error

    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    run_manifest_path = write_run_manifest(
        workspace,
        "connector_sync",
        {
            "run_label": str(connector.get("name", connector_id)),
            "connector_id": connector_id,
            "connector_kind": str(connector.get("kind", "")),
            "connector_source": str(connector.get("source", "")),
            "actor": _serialize_actor(actor),
            "scheduled": scheduled,
            "result_paths": [workspace.relative_path(result.path) for result in results],
            "change_summary_path": workspace.relative_path(change_summary.path),
            "status": "completed",
        },
    )

    synced_at = utc_timestamp()
    connector["updated_at"] = synced_at
    connector["updated_by"] = _serialize_actor(actor)
    connector["last_synced_at"] = synced_at
    connector["last_synced_by"] = _serialize_actor(actor)
    connector["last_change_summary_path"] = workspace.relative_path(change_summary.path)
    connector["last_run_manifest_path"] = workspace.relative_path(run_manifest_path)
    connector["last_result_count"] = len(results)
    connector["subscription"] = _advance_subscription(connector.get("subscription", {}), synced_at, scheduled=scheduled)
    registry["connectors"] = sorted(connectors, key=lambda item: str(item.get("connector_id", "")))
    _write_connector_registry(workspace, registry)
    append_workspace_log(
        workspace,
        operation="ingest",
        title=f"Synced connector {connector_id}",
        details=[f"Pulled {len(results)} artifact(s) from the {str(connector.get('kind', 'connector'))} connector."],
        related_paths=[
            workspace.relative_path(change_summary.path),
            workspace.relative_path(run_manifest_path),
        ]
        + [workspace.relative_path(result.path) for result in results[:5]],
    )

    return ConnectorSyncResult(
        connector_id=connector_id,
        connector_kind=str(connector.get("kind", "")),
        synced_count=len(results),
        registry_path=workspace.connector_registry_path,
        change_summary_path=change_summary.path,
        run_manifest_path=run_manifest_path,
        result_paths=[result.path for result in results],
    )


def sync_all_connectors(
    workspace: Workspace,
    force: bool = False,
    limit: Optional[int] = None,
    scheduled_only: bool = False,
    actor: Optional[Dict[str, object]] = None,
) -> ConnectorBatchSyncResult:
    connectors = list_connectors(workspace)
    if not connectors:
        raise ConnectorError("No connector definitions found.")

    if scheduled_only:
        pending_connectors = [connector for connector in connectors if connector_subscription_is_due(connector)]
    else:
        pending_connectors = connectors if force else [connector for connector in connectors if not connector.get("last_synced_at")]
    selected_connectors = pending_connectors[:limit] if limit is not None else pending_connectors
    connector_results: List[ConnectorSyncResult] = []
    for connector in selected_connectors:
        connector_results.append(
            sync_connector(
                workspace,
                connector_id=str(connector.get("connector_id", "")),
                force=force,
                scheduled=scheduled_only,
                actor=actor,
            )
        )

    run_manifest_path = write_run_manifest(
        workspace,
        "connector_sync_all",
        {
            "run_label": "connector-sync-all",
            "actor": _serialize_actor(actor),
            "force": force,
            "scheduled_only": scheduled_only,
            "selected_connector_ids": [str(item.get("connector_id", "")) for item in selected_connectors],
            "connector_ids": [result.connector_id for result in connector_results],
            "registry_connector_count": len(connectors),
            "synced_connector_count": len(connector_results),
            "total_result_count": sum(result.synced_count for result in connector_results),
            "registry_path": workspace.relative_path(workspace.connector_registry_path),
            "connector_run_manifest_paths": [
                workspace.relative_path(result.run_manifest_path) for result in connector_results
            ],
            "connector_change_summary_paths": [
                workspace.relative_path(result.change_summary_path) for result in connector_results
            ],
            "status": "completed",
        },
    )

    return ConnectorBatchSyncResult(
        connector_count=len(connectors),
        synced_connector_count=len(connector_results),
        total_result_count=sum(result.synced_count for result in connector_results),
        registry_path=workspace.connector_registry_path,
        run_manifest_path=run_manifest_path,
        connector_results=connector_results,
    )


def _run_connector_ingest(workspace: Workspace, connector: Dict[str, object], force: bool) -> List[IngestResult]:
    kind = str(connector.get("kind", ""))
    source = str(connector.get("source", ""))
    name = str(connector.get("name", "")).strip() or None
    if kind == "url":
        return [ingest_url(workspace, url=source, name=name, force=force)]
    if kind == "urls":
        return ingest_urls(workspace, source_list=_resolve_local_source_path(workspace, source), force=force)
    if kind == "sitemap":
        sitemap_source = source if _looks_like_remote_source(source) else _resolve_local_source_path(workspace, source).as_posix()
        return ingest_sitemap(workspace, source=sitemap_source, force=force)
    if kind == "repo":
        repo_source = source if _looks_like_remote_source(source) else _resolve_local_source_path(workspace, source).as_posix()
        return [ingest_repo(workspace, repo_path=repo_source, name=name, force=force)]
    raise ConnectorError(f"Unsupported connector kind '{kind}'.")


def _load_connector_registry(workspace: Workspace) -> Dict[str, object]:
    if not workspace.connector_registry_path.exists():
        return {
            "schema_version": 1,
            "generated_at": utc_timestamp(),
            "connectors": [],
        }
    payload = json.loads(workspace.connector_registry_path.read_text(encoding="utf-8"))
    connectors = [_normalize_connector_record(item) for item in list(payload.get("connectors", []))]
    payload["connectors"] = sorted(connectors, key=lambda item: str(item.get("connector_id", "")))
    return payload


def _write_connector_registry(workspace: Workspace, payload: Dict[str, object]) -> None:
    payload = dict(payload)
    payload["schema_version"] = 1
    payload["generated_at"] = utc_timestamp()
    payload["connectors"] = [_normalize_connector_record(item) for item in list(payload.get("connectors", []))]
    workspace.connector_registry_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.connector_registry_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _unique_connector_id(connectors: List[Dict[str, object]], kind: str, seed: str) -> str:
    base = f"{kind}-{slugify(seed)[:40] or kind}"
    existing = {str(item.get("connector_id", "")) for item in connectors}
    if base not in existing:
        return base
    index = 2
    while f"{base}-{index}" in existing:
        index += 1
    return f"{base}-{index}"


def _resolve_local_source_path(workspace: Workspace, source: str) -> Path:
    candidate = Path(source).expanduser()
    if not candidate.is_absolute():
        candidate = workspace.root / candidate
    return candidate.resolve()


def _looks_like_remote_source(source: str) -> bool:
    normalized = source.strip().lower()
    return "://" in normalized or normalized.startswith("data:")


def _normalize_connector_record(connector: Dict[str, object]) -> Dict[str, object]:
    normalized = dict(connector)
    normalized["subscription"] = _normalize_subscription(connector.get("subscription", {}))
    normalized["created_by"] = _serialize_actor(connector.get("created_by"))
    normalized["updated_by"] = _serialize_actor(connector.get("updated_by"))
    normalized["last_synced_by"] = _serialize_actor(connector.get("last_synced_by"))
    return normalized


def _normalize_subscription(subscription: object) -> Dict[str, object]:
    payload = dict(subscription) if isinstance(subscription, dict) else {}
    interval_hours = payload.get("interval_hours")
    hour = payload.get("hour")
    minute = payload.get("minute")
    try:
        normalized_interval = int(interval_hours) if interval_hours is not None else None
    except (TypeError, ValueError):
        normalized_interval = None
    try:
        normalized_hour = int(hour) if hour is not None else None
    except (TypeError, ValueError):
        normalized_hour = None
    try:
        normalized_minute = int(minute) if minute is not None else None
    except (TypeError, ValueError):
        normalized_minute = None
    schedule_type = str(payload.get("schedule_type", "interval")).strip().lower() or "interval"
    if schedule_type not in {"interval", "weekly"}:
        schedule_type = "interval"
    return {
        "enabled": bool(payload.get("enabled", False)),
        "schedule_type": schedule_type,
        "interval_hours": normalized_interval,
        "weekdays": _normalize_weekdays(list(payload.get("weekdays", []))),
        "hour": normalized_hour,
        "minute": normalized_minute,
        "subscribed_at": str(payload.get("subscribed_at", "")) or None,
        "next_sync_at": str(payload.get("next_sync_at", "")) or None,
        "last_scheduled_sync_at": str(payload.get("last_scheduled_sync_at", "")) or None,
        "last_scheduler_tick_at": str(payload.get("last_scheduler_tick_at", "")) or None,
        "last_due_at": str(payload.get("last_due_at", "")) or None,
        "last_tick_status": str(payload.get("last_tick_status", "")) or None,
    }


def _advance_subscription(subscription: object, synced_at: str, scheduled: bool) -> Dict[str, object]:
    payload = _normalize_subscription(subscription)
    if not bool(payload.get("enabled")):
        return payload
    if scheduled:
        payload["last_scheduled_sync_at"] = synced_at
    if str(payload.get("schedule_type", "interval")) == "weekly":
        weekdays = _normalize_weekdays(list(payload.get("weekdays", [])))
        hour = int(payload.get("hour", 0) or 0)
        minute = int(payload.get("minute", 0) or 0)
        payload["next_sync_at"] = _next_weekly_timestamp(weekdays, hour, minute, synced_at)
        return payload
    interval_hours = payload.get("interval_hours")
    if interval_hours is None:
        return payload
    payload["next_sync_at"] = _future_timestamp(hours=int(interval_hours))
    return payload


def connector_subscription_is_due(connector: Dict[str, object]) -> bool:
    subscription = _normalize_subscription(connector.get("subscription", {}))
    if not bool(subscription.get("enabled")):
        return False
    next_sync_at = str(subscription.get("next_sync_at", "") or "").strip()
    if not next_sync_at:
        return True
    try:
        return datetime.fromisoformat(next_sync_at) <= datetime.now(timezone.utc)
    except ValueError:
        return True


def _normalize_weekdays(weekdays: List[str]) -> List[str]:
    normalized = []
    for weekday in weekdays:
        value = str(weekday).strip().lower()
        if not value:
            continue
        if value not in WEEKDAY_ORDER:
            raise ConnectorError(
                f"Unsupported weekday '{weekday}'. Expected one of: {', '.join(WEEKDAY_ORDER)}."
            )
        if value not in normalized:
            normalized.append(value)
    normalized.sort(key=WEEKDAY_ORDER.index)
    return normalized


def _next_weekly_timestamp(weekdays: List[str], hour: int, minute: int, reference_timestamp: str) -> str:
    reference = datetime.fromisoformat(reference_timestamp)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    reference = reference.astimezone(timezone.utc).replace(second=0, microsecond=0)
    weekday_indexes = sorted({WEEKDAY_ORDER.index(day) for day in _normalize_weekdays(weekdays)})
    if not weekday_indexes:
        return reference.isoformat()
    for offset in range(8):
        candidate_date = reference + timedelta(days=offset)
        if candidate_date.weekday() not in weekday_indexes:
            continue
        candidate = candidate_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate > reference:
            return candidate.isoformat()
    fallback = (reference + timedelta(days=7)).replace(hour=hour, minute=minute, second=0, microsecond=0)
    while fallback.weekday() not in weekday_indexes:
        fallback += timedelta(days=1)
    return fallback.isoformat()


def _future_timestamp(*, hours: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).replace(microsecond=0).isoformat()


def _serialize_actor(actor: object) -> Optional[Dict[str, str]]:
    if not isinstance(actor, dict):
        return None
    return {
        "principal_id": str(actor.get("principal_id", "")),
        "display_name": str(actor.get("display_name", "")),
        "role": str(actor.get("role", "")),
        "status": str(actor.get("status", "")),
    }
