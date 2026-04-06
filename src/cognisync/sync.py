from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Dict, List, Optional

from cognisync.access import (
    DEFAULT_LOCAL_OPERATOR_ID,
    OPERATOR_ACTION_ROLES,
    ensure_access_manifest,
    require_access_role,
)
from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


class SyncError(RuntimeError):
    pass


@dataclass(frozen=True)
class SyncBundleResult:
    directory: Path
    manifest_path: Path
    file_count: int
    event_manifest_path: Path
    history_manifest_path: Path


@dataclass(frozen=True)
class SyncImportResult:
    manifest_path: Path
    file_count: int
    event_manifest_path: Path
    history_manifest_path: Path


def list_sync_events(workspace: Workspace) -> List[Dict[str, object]]:
    events: List[Dict[str, object]] = []
    if not workspace.sync_manifests_dir.exists():
        return events
    for manifest_path in sorted(workspace.sync_manifests_dir.glob("*.json"), reverse=True):
        events.append(json.loads(manifest_path.read_text(encoding="utf-8")))
    events.sort(key=lambda item: (str(item.get("generated_at", "")), str(item.get("sync_id", ""))), reverse=True)
    return events


def render_sync_history(workspace: Workspace) -> str:
    events = list_sync_events(workspace)
    lines = [
        "# Sync History",
        "",
        f"- Event count: `{len(events)}`",
    ]
    if not events:
        lines.extend(["", "No sync export or import events found."])
        return "\n".join(lines)
    operation_counts: Dict[str, int] = {}
    for event in events:
        operation = str(event.get("operation", "unknown"))
        operation_counts[operation] = operation_counts.get(operation, 0) + 1
    lines.append(f"- Operation counts: `{json.dumps(dict(sorted(operation_counts.items())), sort_keys=True)}`")
    lines.extend(["", "## Events", ""])
    for event in events:
        actor = dict(event.get("actor", {}))
        actor_label = str(actor.get("principal_id", "")) or "unknown-actor"
        lines.append(
            "- "
            f"`{event.get('sync_id', '')}` "
            f"`{event.get('operation', '')}` "
            f"`{event.get('status', '')}` "
            f"`{actor_label}` "
            f"{event.get('bundle_dir_relative', event.get('bundle_dir', ''))}"
        )
    return "\n".join(lines)


def export_sync_bundle(
    workspace: Workspace,
    output_dir: Optional[Path] = None,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> SyncBundleResult:
    ensure_access_manifest(workspace)
    actor = require_access_role(
        workspace,
        actor_id,
        OPERATOR_ACTION_ROLES,
        "export workspace sync bundles",
    )
    destination = output_dir or _next_sync_bundle_dir(workspace)
    destination.mkdir(parents=True, exist_ok=True)

    included_paths = [
        Path("raw"),
        Path("wiki"),
        Path("prompts"),
        Path(".cognisync"),
        Path("outputs") / "slides",
        Path("outputs") / "reports" / "change-summaries",
        Path("outputs") / "reports" / "research-jobs",
        Path("outputs") / "reports" / "remediation-jobs",
    ]
    copied_paths: List[str] = []
    file_count = 0

    for relative_path in included_paths:
        source_path = workspace.root / relative_path
        if not source_path.exists():
            continue
        target_path = destination / relative_path
        copied = _copy_path(source_path, target_path)
        if copied:
            copied_paths.append(relative_path.as_posix())
            file_count += copied

    manifest_payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "bundle_type": "workspace-sync-bundle",
        "actor": _serialize_actor(actor),
        "workspace_root": workspace.root.as_posix(),
        "included_paths": copied_paths,
        "file_count": file_count,
        "state_manifests": _state_manifest_paths(workspace),
    }
    manifest_path = destination / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    event_manifest_path = _record_sync_event(
        workspace,
        operation="export",
        bundle_dir=destination,
        bundle_manifest_path=manifest_path,
        file_count=file_count,
        included_paths=copied_paths,
        actor=actor,
    )
    history_manifest_path = _write_sync_history_manifest(workspace)
    return SyncBundleResult(
        directory=destination,
        manifest_path=manifest_path,
        file_count=file_count,
        event_manifest_path=event_manifest_path,
        history_manifest_path=history_manifest_path,
    )


def import_sync_bundle(
    workspace: Workspace,
    bundle_dir: Path,
    actor_id: str = DEFAULT_LOCAL_OPERATOR_ID,
) -> SyncImportResult:
    bundle_root = Path(bundle_dir).expanduser().resolve()
    manifest_path = bundle_root / "manifest.json"
    if not manifest_path.exists():
        raise SyncError(f"Could not find sync manifest at {manifest_path}.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    actor = require_access_role(
        workspace,
        actor_id,
        OPERATOR_ACTION_ROLES,
        "import workspace sync bundles",
    )
    workspace.initialize(name=workspace.root.name, force=False)

    file_count = 0
    for relative_string in list(manifest.get("included_paths", [])):
        relative_path = Path(str(relative_string))
        source_path = bundle_root / relative_path
        if not source_path.exists():
            continue
        target_path = workspace.root / relative_path
        file_count += _copy_path(source_path, target_path)

    event_manifest_path = _record_sync_event(
        workspace,
        operation="import",
        bundle_dir=bundle_root,
        bundle_manifest_path=manifest_path,
        file_count=file_count,
        included_paths=[str(item) for item in list(manifest.get("included_paths", []))],
        actor=actor,
        source_manifest=manifest,
    )
    history_manifest_path = _write_sync_history_manifest(workspace)
    return SyncImportResult(
        manifest_path=manifest_path,
        file_count=file_count,
        event_manifest_path=event_manifest_path,
        history_manifest_path=history_manifest_path,
    )


def _copy_path(source_path: Path, target_path: Path) -> int:
    if source_path.is_file():
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        return 1

    if not source_path.is_dir():
        return 0

    file_count = 0
    for child in source_path.rglob("*"):
        if not child.is_file():
            continue
        rel = child.relative_to(source_path)
        destination = target_path / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(child, destination)
        file_count += 1
    return file_count


def _next_sync_bundle_dir(workspace: Workspace) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    directory = workspace.sync_bundles_dir / f"sync-bundle-{stamp}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _record_sync_event(
    workspace: Workspace,
    operation: str,
    bundle_dir: Path,
    bundle_manifest_path: Path,
    file_count: int,
    included_paths: List[str],
    actor: Dict[str, object],
    source_manifest: Optional[Dict[str, object]] = None,
) -> Path:
    sync_id = _sync_event_id(operation)
    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "sync_id": sync_id,
        "operation": operation,
        "status": "completed",
        "actor": _serialize_actor(actor),
        "bundle_dir": bundle_dir.as_posix(),
        "bundle_dir_relative": workspace.relative_path(bundle_dir),
        "bundle_manifest_path": bundle_manifest_path.as_posix(),
        "bundle_manifest_relative": workspace.relative_path(bundle_manifest_path),
        "file_count": file_count,
        "included_paths": list(included_paths),
    }
    if source_manifest is not None:
        payload["source_bundle"] = {
            "generated_at": str(source_manifest.get("generated_at", "")),
            "bundle_type": str(source_manifest.get("bundle_type", "")),
            "actor": dict(source_manifest.get("actor", {})),
        }
    workspace.sync_manifests_dir.mkdir(parents=True, exist_ok=True)
    path = workspace.sync_manifests_dir / f"{sync_id}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _write_sync_history_manifest(workspace: Workspace) -> Path:
    events = list_sync_events(workspace)
    operation_counts: Dict[str, int] = {}
    for event in events:
        operation = str(event.get("operation", "unknown"))
        operation_counts[operation] = operation_counts.get(operation, 0) + 1
    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "event_count": len(events),
        "latest_event_id": str(events[0].get("sync_id", "")) if events else "",
        "operation_counts": dict(sorted(operation_counts.items())),
        "events": [
            {
                "sync_id": str(event.get("sync_id", "")),
                "operation": str(event.get("operation", "")),
                "status": str(event.get("status", "")),
                "generated_at": str(event.get("generated_at", "")),
                "actor_id": str(dict(event.get("actor", {})).get("principal_id", "")),
                "actor_role": str(dict(event.get("actor", {})).get("role", "")),
                "source_actor_id": str(dict(dict(event.get("source_bundle", {})).get("actor", {})).get("principal_id", "")),
                "file_count": int(event.get("file_count", 0) or 0),
                "bundle_dir_relative": str(event.get("bundle_dir_relative", "")),
                "event_manifest_path": workspace.relative_path(workspace.sync_manifests_dir / f"{event.get('sync_id', '')}.json"),
            }
            for event in events
        ],
    }
    workspace.sync_history_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    workspace.sync_history_manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return workspace.sync_history_manifest_path


def _sync_event_id(operation: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"sync-{operation}-{stamp}"


def _serialize_actor(actor: Dict[str, object]) -> Dict[str, str]:
    return {
        "principal_id": str(actor.get("principal_id", "")),
        "display_name": str(actor.get("display_name", "")),
        "role": str(actor.get("role", "")),
        "status": str(actor.get("status", "")),
    }


def _state_manifest_paths(workspace: Workspace) -> Dict[str, str]:
    manifest_paths = {
        "access": workspace.access_manifest_path,
        "audit": workspace.audit_manifest_path,
        "collaboration": workspace.collaboration_manifest_path,
        "control_plane": workspace.control_plane_manifest_path,
        "connectors": workspace.connector_registry_path,
        "graph": workspace.graph_manifest_path,
        "jobs_queue": workspace.job_queue_manifest_path,
        "notifications": workspace.notifications_manifest_path,
        "review_actions": workspace.review_actions_manifest_path,
        "review_queue": workspace.review_queue_manifest_path,
        "shared_workspace": workspace.shared_workspace_manifest_path,
        "sources": workspace.sources_manifest_path,
        "sync_history": workspace.sync_history_manifest_path,
        "usage": workspace.usage_manifest_path,
    }
    return {
        name: workspace.relative_path(path)
        for name, path in sorted(manifest_paths.items())
        if path.exists()
    }
