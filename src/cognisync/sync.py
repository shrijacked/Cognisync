from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
from typing import Dict, List, Optional

from cognisync.utils import utc_timestamp
from cognisync.workspace import Workspace


class SyncError(RuntimeError):
    pass


@dataclass(frozen=True)
class SyncBundleResult:
    directory: Path
    manifest_path: Path
    file_count: int


@dataclass(frozen=True)
class SyncImportResult:
    manifest_path: Path
    file_count: int


def export_sync_bundle(workspace: Workspace, output_dir: Optional[Path] = None) -> SyncBundleResult:
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
        "workspace_root": workspace.root.as_posix(),
        "included_paths": copied_paths,
        "file_count": file_count,
    }
    manifest_path = destination / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
    return SyncBundleResult(directory=destination, manifest_path=manifest_path, file_count=file_count)


def import_sync_bundle(workspace: Workspace, bundle_dir: Path) -> SyncImportResult:
    bundle_root = Path(bundle_dir).expanduser().resolve()
    manifest_path = bundle_root / "manifest.json"
    if not manifest_path.exists():
        raise SyncError(f"Could not find sync manifest at {manifest_path}.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    workspace.initialize(name=workspace.root.name, force=False)

    file_count = 0
    for relative_string in list(manifest.get("included_paths", [])):
        relative_path = Path(str(relative_string))
        source_path = bundle_root / relative_path
        if not source_path.exists():
            continue
        target_path = workspace.root / relative_path
        file_count += _copy_path(source_path, target_path)

    return SyncImportResult(manifest_path=manifest_path, file_count=file_count)


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
    stamp = utc_timestamp().replace(":", "").replace("-", "").replace("+", "").replace("T", "T")
    directory = workspace.sync_bundles_dir / f"sync-bundle-{stamp}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
