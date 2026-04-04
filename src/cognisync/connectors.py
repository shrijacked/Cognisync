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
    every_hours: int,
    actor: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    if every_hours < 1:
        raise ConnectorError("Connector subscription intervals must be at least 1 hour.")

    registry = _load_connector_registry(workspace)
    connectors = list(registry.get("connectors", []))
    connector = next((item for item in connectors if str(item.get("connector_id", "")) == connector_id), None)
    if connector is None:
        raise ConnectorError(f"Could not find connector '{connector_id}'.")

    subscribed_at = utc_timestamp()
    connector["subscription"] = {
        "enabled": True,
        "interval_hours": every_hours,
        "subscribed_at": subscribed_at,
        "next_sync_at": subscribed_at,
        "last_scheduled_sync_at": str(dict(connector.get("subscription", {})).get("last_scheduled_sync_at", "")) or None,
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
    subscription["interval_hours"] = None
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
            lines.append(
                "  "
                f"schedule: every {subscription.get('interval_hours', '?')}h"
                f" next {subscription.get('next_sync_at') or 'unscheduled'}"
            )
    return "\n".join(lines)


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
        pending_connectors = [connector for connector in connectors if _connector_subscription_is_due(connector)]
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
    try:
        normalized_interval = int(interval_hours) if interval_hours is not None else None
    except (TypeError, ValueError):
        normalized_interval = None
    return {
        "enabled": bool(payload.get("enabled", False)),
        "interval_hours": normalized_interval,
        "subscribed_at": str(payload.get("subscribed_at", "")) or None,
        "next_sync_at": str(payload.get("next_sync_at", "")) or None,
        "last_scheduled_sync_at": str(payload.get("last_scheduled_sync_at", "")) or None,
    }


def _advance_subscription(subscription: object, synced_at: str, scheduled: bool) -> Dict[str, object]:
    payload = _normalize_subscription(subscription)
    if not bool(payload.get("enabled")):
        return payload
    interval_hours = payload.get("interval_hours")
    if interval_hours is None:
        return payload
    if scheduled:
        payload["last_scheduled_sync_at"] = synced_at
    payload["next_sync_at"] = _future_timestamp(hours=int(interval_hours))
    return payload


def _connector_subscription_is_due(connector: Dict[str, object]) -> bool:
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
