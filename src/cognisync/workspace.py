from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from cognisync.access import ensure_access_manifest
from cognisync.collaboration import ensure_collaboration_manifest
from cognisync.config import default_config, load_config, save_config
from cognisync.types import IndexSnapshot


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def wiki_dir(self) -> Path:
        return self.root / "wiki"

    @property
    def outputs_dir(self) -> Path:
        return self.root / "outputs"

    @property
    def schema_path(self) -> Path:
        return self.root / "AGENTS.md"

    @property
    def log_path(self) -> Path:
        return self.root / "log.md"

    @property
    def prompts_dir(self) -> Path:
        return self.root / "prompts"

    @property
    def state_dir(self) -> Path:
        return self.root / ".cognisync"

    @property
    def config_path(self) -> Path:
        return self.state_dir / "config.json"

    @property
    def index_path(self) -> Path:
        return self.state_dir / "index.json"

    @property
    def plans_dir(self) -> Path:
        return self.state_dir / "plans"

    @property
    def runs_dir(self) -> Path:
        return self.state_dir / "runs"

    @property
    def jobs_dir(self) -> Path:
        return self.state_dir / "jobs"

    @property
    def job_manifests_dir(self) -> Path:
        return self.jobs_dir / "manifests"

    @property
    def job_queue_manifest_path(self) -> Path:
        return self.jobs_dir / "queue.json"

    @property
    def worker_registry_path(self) -> Path:
        return self.jobs_dir / "workers.json"

    @property
    def sync_state_dir(self) -> Path:
        return self.state_dir / "sync"

    @property
    def sync_manifests_dir(self) -> Path:
        return self.sync_state_dir / "manifests"

    @property
    def sync_history_manifest_path(self) -> Path:
        return self.sync_state_dir / "history.json"

    @property
    def connector_registry_path(self) -> Path:
        return self.state_dir / "connectors.json"

    @property
    def notifications_manifest_path(self) -> Path:
        return self.state_dir / "notifications.json"

    @property
    def access_manifest_path(self) -> Path:
        return self.state_dir / "access.json"

    @property
    def collaboration_manifest_path(self) -> Path:
        return self.state_dir / "collaboration.json"

    @property
    def control_plane_manifest_path(self) -> Path:
        return self.state_dir / "control-plane.json"

    @property
    def shared_workspace_manifest_path(self) -> Path:
        return self.state_dir / "shared-workspace.json"

    @property
    def audit_manifest_path(self) -> Path:
        return self.state_dir / "audit.json"

    @property
    def usage_manifest_path(self) -> Path:
        return self.state_dir / "usage.json"

    @property
    def sources_manifest_path(self) -> Path:
        return self.state_dir / "sources.json"

    @property
    def graph_manifest_path(self) -> Path:
        return self.state_dir / "graph.json"

    @property
    def review_queue_manifest_path(self) -> Path:
        return self.state_dir / "review-queue.json"

    @property
    def review_actions_manifest_path(self) -> Path:
        return self.state_dir / "review-actions.json"

    @property
    def change_summaries_dir(self) -> Path:
        return self.outputs_dir / "reports" / "change-summaries"

    @property
    def review_exports_dir(self) -> Path:
        return self.outputs_dir / "reports" / "review-exports"

    @property
    def review_ui_dir(self) -> Path:
        return self.outputs_dir / "reports" / "review-ui"

    @property
    def export_artifacts_dir(self) -> Path:
        return self.outputs_dir / "reports" / "exports"

    @property
    def sync_bundles_dir(self) -> Path:
        return self.outputs_dir / "reports" / "sync-bundles"

    @property
    def research_jobs_dir(self) -> Path:
        return self.outputs_dir / "reports" / "research-jobs"

    @property
    def remediation_jobs_dir(self) -> Path:
        return self.outputs_dir / "reports" / "remediation-jobs"

    def initialize(self, name: Optional[str] = None, force: bool = False) -> None:
        directories = [
            self.raw_dir,
            self.wiki_dir / "sources",
            self.wiki_dir / "concepts",
            self.wiki_dir / "queries",
            self.outputs_dir / "reports",
            self.outputs_dir / "slides",
            self.prompts_dir,
            self.state_dir,
            self.plans_dir,
            self.runs_dir,
            self.jobs_dir,
            self.job_manifests_dir,
            self.sync_state_dir,
            self.sync_manifests_dir,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

        if force or not self.config_path.exists():
            save_config(self.config_path, default_config(name or self.root.name))

        ensure_access_manifest(self)
        ensure_collaboration_manifest(self)
        from cognisync.control_plane import ensure_control_plane_manifest
        from cognisync.sharing import ensure_shared_workspace_manifest
        ensure_control_plane_manifest(self)
        ensure_shared_workspace_manifest(self)
        from cognisync.knowledge_surfaces import append_workspace_log, ensure_workspace_log, write_workspace_schema

        write_workspace_schema(self, force=force)
        ensure_workspace_log(self, force=force)
        self.refresh_index()
        append_workspace_log(
            self,
            operation="init",
            title=f"Initialized {self.root.name}",
            details=["Workspace scaffolding and agent schema created."],
            related_paths=[self.relative_path(self.schema_path), self.relative_path(self.log_path)],
        )

    def load_config(self):
        return load_config(self.config_path)

    def relative_path(self, path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(self.root).as_posix()
        except ValueError:
            return resolved.as_posix()

    def write_index(self, snapshot: IndexSnapshot) -> None:
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(json.dumps(snapshot.to_dict(), indent=2, sort_keys=True), encoding="utf-8")

    def refresh_index(self) -> IndexSnapshot:
        from cognisync.knowledge_surfaces import ensure_workspace_log, write_wiki_navigation_surfaces, write_workspace_schema
        from cognisync.scanner import scan_workspace

        write_workspace_schema(self)
        ensure_workspace_log(self)
        provisional_snapshot = scan_workspace(self)
        write_wiki_navigation_surfaces(self, provisional_snapshot)
        final_snapshot = scan_workspace(self)
        self.write_index(final_snapshot)
        return final_snapshot

    def read_index(self) -> IndexSnapshot:
        data = json.loads(self.index_path.read_text(encoding="utf-8"))
        return IndexSnapshot.from_dict(data)

    def write_plan_json(self, name: str, plan) -> Path:
        path = self.plans_dir / f"{name}.json"
        path.write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return path
