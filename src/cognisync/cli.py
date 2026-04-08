from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from cognisync.access import (
    AccessError,
    DEFAULT_LOCAL_OPERATOR_ID,
    OPERATOR_ACTION_ROLES,
    VALID_ACCESS_ROLES,
    grant_access_member,
    require_access_role,
    render_access_roster,
    revoke_access_member,
)
from cognisync.adapters import (
    AdapterError,
    adapter_from_config,
    builtin_adapter_presets,
    install_builtin_adapter,
)
from cognisync.change_summaries import capture_change_state, write_change_summary
from cognisync.collaboration import (
    CollaborationError,
    add_comment,
    record_decision,
    render_collaboration_threads,
    request_review,
    resolve_review,
)
from cognisync.compile_flow import CompileError, run_compile_cycle
from cognisync.config import MaintenancePolicy, save_config
from cognisync.control_plane import (
    ControlPlaneError,
    accept_control_plane_invite,
    create_control_plane_invite,
    create_control_plane_server,
    disable_job_subscription,
    list_due_job_subscriptions,
    issue_control_plane_token,
    list_job_subscriptions,
    list_control_plane_tokens,
    render_control_plane_status,
    render_control_plane_workers,
    revoke_control_plane_token,
    run_scheduler_tick,
    schedule_job_subscription,
)
from cognisync.connectors import (
    ConnectorError,
    add_connector,
    list_due_connectors,
    render_connector_list,
    subscribe_connector,
    sync_all_connectors,
    sync_connector,
    unsubscribe_connector,
)
from cognisync.demo import DemoError, create_demo_workspace
from cognisync.doctor import doctor_exit_code, render_doctor_report, run_doctor
from cognisync.evaluation import evaluate_research_runs, export_feedback_bundle
from cognisync.exports import (
    ExportError,
    export_correction_bundle,
    export_finetune_bundle,
    export_presentations_bundle,
    export_research_jsonl,
    export_training_bundle,
)
from cognisync.ingest import (
    IngestError,
    ingest_batch,
    ingest_file,
    ingest_pdf,
    ingest_repo,
    ingest_sitemap,
    ingest_url,
    ingest_urls,
)
from cognisync.knowledge_surfaces import append_workspace_log
from cognisync.jobs import (
    JobError,
    claim_next_job,
    enqueue_compile_job,
    enqueue_connector_sync_job,
    enqueue_connector_sync_all_job,
    enqueue_ingest_repo_job,
    enqueue_ingest_sitemap_job,
    enqueue_ingest_url_job,
    enqueue_improve_research_job,
    enqueue_lint_job,
    enqueue_maintain_job,
    enqueue_remote_sync_pull_job,
    enqueue_research_job,
    enqueue_sync_export_job,
    heartbeat_job,
    retry_job,
    render_jobs_list,
    render_worker_registry,
    run_job_worker,
    run_next_job,
)
from cognisync.linter import lint_snapshot
from cognisync.maintenance import (
    MaintenanceError,
    accept_concept_candidate,
    apply_backlink_suggestion,
    clear_dismissed_review_item,
    dismiss_review_item,
    file_conflict_review,
    list_dismissed_review_items,
    reopen_review_item,
    resolve_entity_merge,
    run_maintenance_cycle,
)
from cognisync.manifests import write_workspace_manifests
from cognisync.notifications import render_notifications, write_notifications_manifest
from cognisync.observability import render_audit_history, render_usage_report, write_audit_manifest, write_usage_manifest
from cognisync.planner import build_compile_plan, render_compile_plan
from cognisync.remediation import RemediationError, remediate_research_runs
from cognisync.research import DEFAULT_RESEARCH_JOB_PROFILE, RESEARCH_JOB_PROFILES, ResearchError, run_research_cycle
from cognisync.remote_worker import RemoteWorkerError, run_remote_worker
from cognisync.review_exports import write_review_export
from cognisync.review_queue import build_review_queue, render_review_queue
from cognisync.review_ui import create_review_ui_server, write_review_ui_bundle
from cognisync.renderers import render_compile_packet, render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.sharing import (
    SharingError,
    accept_shared_peer,
    attach_remote_bundle,
    bind_shared_control_plane_url,
    detach_attached_remote,
    refresh_attached_remote_bundle,
    invite_shared_peer,
    issue_shared_peer_bundle,
    list_attached_remotes,
    list_due_attached_remote_pulls,
    list_due_shared_peer_syncs,
    list_shared_peers,
    pull_attached_remote,
    remove_shared_peer,
    render_shared_workspace_status,
    set_shared_peer_role,
    set_shared_trust_policy,
    subscribe_attached_remote_pull,
    subscribe_shared_peer_sync,
    suspend_attached_remote,
    suspend_shared_peer,
    unsubscribe_attached_remote_pull,
    unsubscribe_shared_peer_sync,
)
from cognisync.synthetic_data import (
    export_synthetic_contrastive_bundle,
    export_synthetic_graph_completion_bundle,
    export_synthetic_qa_bundle,
    export_synthetic_report_writing_bundle,
)
from cognisync.sync import SyncError, export_sync_bundle, import_sync_bundle, render_sync_history
from cognisync.training_loop import export_training_loop_bundle, improve_research_loop
from cognisync.workspace import Workspace


def _workspace_from_arg(path_arg: str) -> Workspace:
    return Workspace(Path(path_arg))


def _ensure_snapshot(workspace: Workspace):
    if workspace.index_path.exists():
        snapshot = workspace.read_index()
        write_workspace_manifests(workspace, snapshot)
        return snapshot
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    return snapshot


def _refresh_workspace_with_change_summary(workspace: Workspace, trigger: str, fallback_to_live_scan: bool = False):
    previous_state = capture_change_state(workspace, fallback_to_live_scan=fallback_to_live_scan)
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    return snapshot, write_change_summary(workspace, trigger, previous_state, snapshot)


def _require_operator_actor(workspace: Workspace, actor_id: str, action_label: str) -> dict:
    return require_access_role(workspace, actor_id, OPERATOR_ACTION_ROLES, action_label)


def _log_workspace_activity(
    workspace: Workspace,
    operation: str,
    title: str,
    details: list[str] | None = None,
    related_paths: list[str] | None = None,
) -> None:
    append_workspace_log(
        workspace,
        operation=operation,
        title=title,
        details=details or [],
        related_paths=related_paths or [],
    )


def cmd_init(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.path)
    workspace.initialize(name=args.name, force=args.force)
    print(f"Initialized Cognisync workspace at {workspace.root}")
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot, change_summary = _refresh_workspace_with_change_summary(workspace, "scan")
    _log_workspace_activity(
        workspace,
        operation="scan",
        title="Refreshed workspace snapshot",
        details=[f"Cataloged {len(snapshot.artifacts)} artifacts across raw, wiki, outputs, and prompts."],
        related_paths=[workspace.relative_path(change_summary.path), workspace.relative_path(workspace.index_path)],
    )
    print(f"Scanned {len(snapshot.artifacts)} artifacts into {workspace.index_path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.path)
    try:
        artifacts = create_demo_workspace(workspace, force=args.force)
    except DemoError as error:
        print(str(error), file=sys.stderr)
        return 2

    print(f"Demo workspace ready at {workspace.root}")
    print(f"Report: {artifacts['report']}")
    print(f"Slides: {artifacts['slides']}")
    print(f"Query packet: {artifacts['query_packet']}")
    print(f"Compile packet: {artifacts['compile_packet']}")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    plan = build_compile_plan(snapshot)
    workspace.write_plan_json("compile-plan", plan)
    plan_path = workspace.plans_dir / "compile-plan.md"
    plan_path.write_text(render_compile_plan(plan), encoding="utf-8")
    packet_path = render_compile_packet(workspace, plan, snapshot=snapshot)
    print(f"Wrote plan to {plan_path}")
    print(f"Wrote prompt packet to {packet_path}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    checks = run_doctor(workspace)
    print(render_doctor_report(checks))
    return doctor_exit_code(checks, strict=args.strict)


def cmd_lint(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    issues = lint_snapshot(snapshot, workspace=workspace)
    _log_workspace_activity(
        workspace,
        operation="lint",
        title="Checked workspace integrity",
        details=[f"Found {len(issues)} issue(s)."],
        related_paths=[workspace.relative_path(workspace.index_path)],
    )
    for issue in issues:
        print(f"[{issue.severity}] {issue.kind} {issue.path}: {issue.message}")
    if issues:
        return 1 if args.strict else 0
    print("No lint issues found.")
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    queue = build_review_queue(workspace, snapshot)
    print(render_review_queue(queue, limit=args.limit))
    print(f"Wrote review queue to {workspace.review_queue_manifest_path}")
    return 0


def cmd_review_export(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    output_file = None
    if args.output_file:
        output_file = Path(args.output_file).expanduser()
        if not output_file.is_absolute():
            output_file = workspace.root / output_file
        output_file = output_file.resolve()
    result = write_review_export(workspace, snapshot, output_file=output_file)
    print(f"Wrote review export to {result.path}")
    print(f"Open review items: {result.item_count}")
    print(f"Dismissed review items: {result.dismissed_count}")
    return 0


def cmd_jobs_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_jobs_list(workspace))
    print(f"Wrote queue summary to {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_workers(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_worker_registry(workspace))
    print(f"Worker registry: {workspace.worker_registry_path}")
    return 0


def cmd_jobs_enqueue_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_research_job(
            workspace,
            question=args.question,
            profile_name=args.profile,
            limit=args.limit,
            mode=args.mode,
            slides=args.slides,
            job_profile=args.job_profile,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued research job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_compile(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_compile_job(workspace, profile_name=args.profile, requested_by=actor)
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued compile job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_connector_sync(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_connector_sync_job(
            workspace,
            connector_id=args.connector_id,
            force=args.force,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued connector-sync job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_connector_sync_all(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_connector_sync_all_job(
            workspace,
            force=args.force,
            limit=args.limit,
            scheduled_only=args.scheduled_only,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued connector-sync-all job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_sync_export(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_sync_export_job(
            workspace,
            peer_ref=args.peer_ref,
            output_dir=args.output_dir,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued sync-export job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_remote_sync_pull(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_remote_sync_pull_job(
            workspace,
            remote_ref=args.remote_ref,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued remote-sync-pull job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_ingest_url(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_ingest_url_job(
            workspace,
            url=args.url,
            name=args.name,
            force=args.force,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued ingest-url job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_ingest_repo(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_ingest_repo_job(
            workspace,
            source=args.source,
            name=args.name,
            force=args.force,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued ingest-repo job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_ingest_sitemap(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_ingest_sitemap_job(
            workspace,
            source=args.source,
            force=args.force,
            limit=args.limit,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued ingest-sitemap job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_lint(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_lint_job(workspace, requested_by=actor)
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued lint job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_maintain(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_maintain_job(
            workspace,
            max_concepts=args.max_concepts,
            max_merges=args.max_merges,
            max_backlinks=args.max_backlinks,
            max_conflicts=args.max_conflicts,
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued maintain job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_improve_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "enqueue jobs")
        manifest_path = enqueue_improve_research_job(
            workspace,
            profile_name=args.profile,
            limit=args.limit,
            provider_formats=list(args.provider_format or []),
            requested_by=actor,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued improve-research job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_claim_next(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = claim_next_job(
            workspace,
            worker_id=args.worker_id,
            lease_seconds=args.lease_seconds,
            worker_capabilities=list(args.capability or []),
        )
    except (JobError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(
        f"Claimed job {result.job_id} ({result.job_type}) for worker {result.worker_id} "
        f"until {result.lease_expires_at}."
    )
    print(f"Job manifest: {result.job_manifest_path}")
    print(f"Queue summary: {result.queue_manifest_path}")
    return 0


def cmd_jobs_run_next(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = run_next_job(
            workspace,
            worker_id=args.worker_id,
            lease_seconds=args.lease_seconds,
            worker_capabilities=list(args.capability or []),
        )
    except JobError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Completed job {result.job_id} ({result.job_type}) with status {result.status}.")
    print(f"Job manifest: {result.job_manifest_path}")
    print(f"Queue summary: {result.queue_manifest_path}")
    return 0


def cmd_jobs_heartbeat(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = heartbeat_job(
            workspace,
            worker_id=args.worker_id,
            lease_seconds=args.lease_seconds,
            worker_capabilities=list(args.capability or []),
        )
    except JobError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(
        f"Renewed lease for job {result.job_id} ({result.job_type}) "
        f"for worker {result.worker_id} until {result.lease_expires_at}."
    )
    print(f"Job manifest: {result.job_manifest_path}")
    print(f"Queue summary: {result.queue_manifest_path}")
    return 0


def cmd_jobs_retry(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "retry jobs")
        manifest_path = retry_job(
            workspace,
            job_id=args.job_id,
            profile_name=args.profile,
            provider_formats=list(args.provider_format or []),
            requested_by=actor,
        )
    except JobError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Queued retry job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_work(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    result = run_job_worker(
        workspace,
        max_jobs=args.max_jobs,
        stop_on_error=args.stop_on_error,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        worker_capabilities=list(args.capability or []),
    )
    print(
        "Processed "
        f"{result.processed_count} job(s): "
        f"{result.completed_count} completed, "
        f"{result.failed_count} failed."
    )
    print(f"Queue summary: {result.queue_manifest_path}")
    return 0 if result.failed_count == 0 or not args.stop_on_error else 2


def cmd_sync_export(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    try:
        result = export_sync_bundle(
            workspace,
            output_dir=output_dir,
            actor_id=args.actor_id,
            peer_ref=args.for_peer,
        )
    except (SyncError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Wrote sync bundle to {result.directory}")
    print(f"Wrote sync manifest to {result.manifest_path}")
    print(f"Wrote sync event to {result.event_manifest_path}")
    print(f"Sync history: {result.history_manifest_path}")
    print(f"Copied {result.file_count} file(s) into the sync bundle.")
    return 0


def cmd_sync_import(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    bundle_dir = Path(args.bundle).expanduser()
    if not bundle_dir.is_absolute():
        bundle_dir = Path.cwd() / bundle_dir
    bundle_dir = bundle_dir.resolve()
    try:
        result = import_sync_bundle(
            workspace,
            bundle_dir=bundle_dir,
            actor_id=args.actor_id,
            from_peer=args.from_peer,
        )
    except (SyncError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Imported sync bundle into {workspace.root}")
    print(f"Source manifest: {result.manifest_path}")
    print(f"Wrote sync event to {result.event_manifest_path}")
    print(f"Sync history: {result.history_manifest_path}")
    print(f"Copied {result.file_count} file(s) from the bundle.")
    return 0


def cmd_sync_history(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_sync_history(workspace))
    print(f"Wrote sync history to {workspace.sync_history_manifest_path}")
    return 0


def cmd_notify_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    write_notifications_manifest(workspace)
    print(render_notifications(workspace))
    print(f"Wrote notifications to {workspace.notifications_manifest_path}")
    return 0


def cmd_audit_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    write_audit_manifest(workspace)
    print(render_audit_history(workspace))
    print(f"Wrote audit manifest to {workspace.audit_manifest_path}")
    return 0


def cmd_usage_report(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    write_usage_manifest(workspace)
    print(render_usage_report(workspace))
    print(f"Wrote usage manifest to {workspace.usage_manifest_path}")
    return 0


def cmd_access_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_access_roster(workspace))
    print(f"Access manifest: {workspace.access_manifest_path}")
    return 0


def cmd_access_grant(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        _require_operator_actor(workspace, args.actor_id, "grant workspace access")
        member = grant_access_member(
            workspace,
            principal_id=args.principal_id,
            role=args.role,
            display_name=args.name,
        )
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Granted {member['role']} access to {member['principal_id']}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    return 0


def cmd_access_revoke(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        _require_operator_actor(workspace, args.actor_id, "revoke workspace access")
        member = revoke_access_member(workspace, principal_id=args.principal_id)
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Revoked access for {member['principal_id']}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    return 0


def cmd_share_status(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_shared_workspace_status(workspace))
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_bind_control_plane(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        payload = bind_shared_control_plane_url(
            workspace,
            url=args.url,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Bound shared control-plane URL to {payload['published_control_plane_url']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_invite_peer(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = invite_shared_peer(
            workspace,
            peer_id=args.peer_id,
            role=args.role,
            actor_id=args.actor_id,
            base_url=args.base_url,
            capabilities=list(args.capability or []),
            display_name=args.name,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Invited peer {peer['peer_id']} as {peer['role']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_accept_peer(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = accept_shared_peer(
            workspace,
            peer_ref=args.peer_ref,
            actor_id=args.actor_id,
        )
    except SharingError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Accepted peer {peer['peer_id']} as {peer['role']}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_list_peers(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    peers = list_shared_peers(workspace, status=args.status)
    if not peers:
        print("No shared-workspace peers found.")
        print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
        return 0
    print(f"Shared-workspace peers: {len(peers)}")
    print("")
    for peer in peers:
        capabilities = ", ".join(str(item) for item in list(peer.get("capabilities", [])))
        print(f"{peer['peer_id']} [{peer['status']}] {peer['role']}")
        if peer.get("base_url"):
            print(f"base_url: {peer['base_url']}")
        if capabilities:
            print(f"capabilities: {capabilities}")
        print("")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_issue_peer_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_file = Path(args.output_file).expanduser()
    if not output_file.is_absolute():
        output_file = workspace.root / output_file
    output_file = output_file.resolve()
    try:
        bundle = issue_shared_peer_bundle(
            workspace,
            peer_ref=args.peer_ref,
            output_file=output_file.as_posix(),
            actor_id=args.actor_id,
            scopes=list(args.scope or []),
        )
    except (AccessError, SharingError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote peer bundle to {output_file}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_share_set_peer_role(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = set_shared_peer_role(
            workspace,
            peer_ref=args.peer_ref,
            role=args.role,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Updated peer {peer['peer_id']} to role {peer['role']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    return 0


def cmd_share_suspend_peer(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = suspend_shared_peer(
            workspace,
            peer_ref=args.peer_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Suspended peer {peer['peer_id']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_share_remove_peer(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = remove_shared_peer(
            workspace,
            peer_ref=args.peer_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Removed peer {peer['peer_id']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_share_set_policy(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    allow_remote_workers = None
    if args.allow_remote_workers:
        allow_remote_workers = True
    elif args.deny_remote_workers:
        allow_remote_workers = False
    allow_sync_imports = None
    if args.allow_sync_imports:
        allow_sync_imports = True
    elif args.deny_sync_imports:
        allow_sync_imports = False
    try:
        payload = set_shared_trust_policy(
            workspace,
            actor_id=args.actor_id,
            allow_remote_workers=allow_remote_workers,
            allow_sync_imports_from_peers=allow_sync_imports,
            default_peer_role=args.default_peer_role,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    trust_policy = dict(payload.get("trust_policy", {}))
    print("Updated shared-workspace trust policy.")
    print(f"allow_remote_workers: {trust_policy.get('allow_remote_workers', True)}")
    print(f"allow_sync_imports_from_peers: {trust_policy.get('allow_sync_imports_from_peers', True)}")
    print(f"default_peer_role: {trust_policy.get('default_peer_role', 'viewer')}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_subscribe_sync(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = subscribe_shared_peer_sync(
            workspace,
            peer_ref=args.peer_ref,
            every_hours=args.every_hours,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    subscription = dict(peer.get("sync_subscription", {}))
    print(
        f"Subscribed peer {peer['peer_id']} for sync export every "
        f"{subscription.get('interval_hours', '?')} hour(s)."
    )
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_unsubscribe_sync(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        peer = unsubscribe_shared_peer_sync(
            workspace,
            peer_ref=args.peer_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Disabled sync export scheduling for peer {peer['peer_id']}.")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_attach_remote_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        remote = attach_remote_bundle(
            workspace,
            bundle_file=args.bundle_file,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Attached remote {remote['principal_id']} from {remote['workspace_name']}")
    print(f"Remote id: {remote['remote_id']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_list_attached_remotes(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    remotes = list_attached_remotes(workspace, status=args.status)
    if not remotes:
        print("No attached remotes found.")
        print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
        return 0
    print(f"Attached remotes: {len(remotes)}")
    print("")
    for remote in remotes:
        scopes = ", ".join(str(item) for item in list(remote.get("scopes", [])))
        print(f"{remote['principal_id']} [{remote['status']}] {remote['workspace_name']}")
        print(f"remote_id: {remote['remote_id']}")
        print(f"server_url: {remote['server_url']}")
        if scopes:
            print(f"scopes: {scopes}")
        if remote.get("last_pull_at"):
            print(f"last_pull_at: {remote['last_pull_at']}")
        if remote.get("last_pull_status"):
            print(f"last_pull_status: {remote['last_pull_status']}")
        print("")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_refresh_remote_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        remote = refresh_attached_remote_bundle(
            workspace,
            bundle_file=args.bundle_file,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Refreshed remote {remote['principal_id']} from {remote['workspace_name']}")
    print(f"Remote id: {remote['remote_id']}")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_pull_remote(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = pull_attached_remote(
            workspace,
            remote_ref=args.remote_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError, SyncError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Pulled remote {result['remote']['principal_id']} into {workspace.root}")
    print(f"Source manifest: {result['manifest_path']}")
    print(f"Sync event: {result['event_manifest_path']}")
    print(f"Sync history: {result['history_manifest_path']}")
    print(f"Copied {result['file_count']} file(s) from the remote bundle.")
    return 0


def cmd_share_suspend_remote(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        remote = suspend_attached_remote(
            workspace,
            remote_ref=args.remote_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Suspended attached remote {remote['principal_id']}.")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_detach_remote(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        remote = detach_attached_remote(
            workspace,
            remote_ref=args.remote_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Detached remote {remote['principal_id']} from {remote['workspace_name']}.")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_subscribe_remote_pull(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        remote = subscribe_attached_remote_pull(
            workspace,
            remote_ref=args.remote_ref,
            every_hours=args.every_hours,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    subscription = dict(remote.get("pull_subscription", {}))
    print(
        f"Subscribed remote {remote['principal_id']} for pull every "
        f"{subscription.get('interval_hours', '?')} hour(s)."
    )
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_share_unsubscribe_remote_pull(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        remote = unsubscribe_attached_remote_pull(
            workspace,
            remote_ref=args.remote_ref,
            actor_id=args.actor_id,
        )
    except (AccessError, SharingError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Disabled scheduled pulls for remote {remote['principal_id']}.")
    print(f"Shared-workspace manifest: {workspace.shared_workspace_manifest_path}")
    return 0


def cmd_control_plane_status(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_control_plane_status(workspace))
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_invite(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        invite = create_control_plane_invite(
            workspace,
            principal_id=args.principal_id,
            role=args.role,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Created invite {invite['invite_id']} for {invite['principal_id']} as {invite['role']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_accept_invite(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        invite = accept_control_plane_invite(
            workspace,
            invite_ref=args.invite_ref,
            actor_id=args.actor_id,
        )
    except ControlPlaneError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Accepted invite {invite['invite_id']} for {invite['principal_id']}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_issue_token(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_file = None
    if args.output_file:
        output_file = Path(args.output_file).expanduser()
        if not output_file.is_absolute():
            output_file = workspace.root / output_file
        output_file = output_file.resolve()
    try:
        token_metadata, token_value = issue_control_plane_token(
            workspace,
            principal_id=args.principal_id,
            scopes=list(args.scope or []),
            actor_id=args.actor_id,
            description=args.description,
            expires_in_hours=args.expires_in_hours,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2

    payload = {
        "token": token_value,
        "token_id": token_metadata["token_id"],
        "principal_id": token_metadata["principal_id"],
        "role": token_metadata["role"],
        "scopes": list(token_metadata.get("scopes", [])),
        "expires_at": str(token_metadata.get("expires_at", "")),
        "manifest_path": workspace.relative_path(workspace.control_plane_manifest_path),
    }
    if output_file is not None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Wrote control-plane token payload to {output_file}")
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_list_tokens(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    tokens = list_control_plane_tokens(workspace)
    if not tokens:
        print("No control-plane tokens found.")
        print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
        return 0
    print(f"Control-plane tokens: {len(tokens)}")
    print("")
    for token in tokens:
        scopes = ", ".join(str(item) for item in list(token.get("scopes", [])))
        print(f"{token['token_id']} [{token['status']}] {token['principal_id']} ({token['role']})")
        print(f"prefix: {token.get('token_prefix', '')}")
        if scopes:
            print(f"scopes: {scopes}")
        expires_at = str(token.get("expires_at", "")).strip()
        if expires_at:
            print(f"expires-at: {expires_at}")
        description = str(token.get("description", "")).strip()
        if description:
            print(f"description: {description}")
        print("")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_revoke_token(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        token = revoke_control_plane_token(
            workspace,
            token_id=args.token_id,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Revoked token {token['token_id']} for {token['principal_id']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_schedule_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        subscription = schedule_job_subscription(
            workspace,
            job_type="research",
            every_hours=args.every_hours,
            parameters={
                "question": args.question,
                "profile_name": args.profile_name,
                "limit": args.limit,
                "mode": args.mode,
                "slides": args.slides,
                "job_profile": args.job_profile,
            },
            label=args.label,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Scheduled research job {subscription['subscription_id']}")
    print(f"Question: {subscription['parameters']['question']}")
    print(f"Every hours: {subscription['interval_hours']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_schedule_compile(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        subscription = schedule_job_subscription(
            workspace,
            job_type="compile",
            every_hours=args.every_hours,
            parameters={"profile_name": args.profile_name},
            label=args.label,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Scheduled compile job {subscription['subscription_id']}")
    print(f"Every hours: {subscription['interval_hours']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_schedule_lint(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        subscription = schedule_job_subscription(
            workspace,
            job_type="lint",
            every_hours=args.every_hours,
            parameters={},
            label=args.label,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Scheduled lint job {subscription['subscription_id']}")
    print(f"Every hours: {subscription['interval_hours']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_schedule_maintain(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        subscription = schedule_job_subscription(
            workspace,
            job_type="maintain",
            every_hours=args.every_hours,
            parameters={
                "max_concepts": args.max_concepts,
                "max_merges": args.max_merges,
                "max_backlinks": args.max_backlinks,
                "max_conflicts": args.max_conflicts,
            },
            label=args.label,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Scheduled maintain job {subscription['subscription_id']}")
    print(f"Every hours: {subscription['interval_hours']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_list_scheduled_jobs(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    subscriptions = list_job_subscriptions(workspace)
    if not subscriptions:
        print("No scheduled jobs found.")
        print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
        return 0
    print(f"Scheduled jobs: {len(subscriptions)}")
    print("")
    for subscription in subscriptions:
        status = "enabled" if subscription.get("enabled", False) else "disabled"
        print(
            f"{subscription['subscription_id']} "
            f"[{status}] "
            f"{subscription['job_type']} "
            f"every:{subscription.get('interval_hours', '')}h"
        )
        label = str(subscription.get("label", "")).strip()
        if label:
            print(f"label: {label}")
        next_run_at = str(subscription.get("next_run_at", "")).strip()
        if next_run_at:
            print(f"next-run: {next_run_at}")
        print("")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_remove_scheduled_job(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        subscription = disable_job_subscription(
            workspace,
            subscription_id=args.subscription_id,
            actor_id=args.actor_id,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Disabled scheduled job {subscription['subscription_id']}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_scheduler_status(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    payload = json.loads(workspace.control_plane_manifest_path.read_text(encoding="utf-8")) if workspace.control_plane_manifest_path.exists() else {}
    scheduler = dict(payload.get("scheduler", {}))
    due_connectors = [connector["connector_id"] for connector in list_due_connectors(workspace)]
    due_peer_syncs = [peer["peer_id"] for peer in list_due_shared_peer_syncs(workspace)]
    due_remote_pulls = [remote["principal_id"] for remote in list_due_attached_remote_pulls(workspace)]
    due_job_subscriptions = [subscription["subscription_id"] for subscription in list_due_job_subscriptions(workspace)]
    print("# Scheduler Status")
    print("")
    print(f"- Due connectors: `{len(due_connectors)}`")
    print(f"- Due peer sync exports: `{len(due_peer_syncs)}`")
    print(f"- Due attached remote pulls: `{len(due_remote_pulls)}`")
    print(f"- Due scheduled jobs: `{len(due_job_subscriptions)}`")
    print(f"- Last tick at: `{scheduler.get('last_tick_at', '')}`")
    print(f"- Last action: `{scheduler.get('last_action', '')}`")
    if due_connectors:
        print("")
        print("## Due Connectors")
        print("")
        for connector_id in due_connectors:
            print(f"- `{connector_id}`")
    if due_peer_syncs:
        print("")
        print("## Due Peer Sync Exports")
        print("")
        for peer_id in due_peer_syncs:
            print(f"- `{peer_id}`")
    if due_remote_pulls:
        print("")
        print("## Due Attached Remote Pulls")
        print("")
        for remote_id in due_remote_pulls:
            print(f"- `{remote_id}`")
    if due_job_subscriptions:
        print("")
        print("## Due Scheduled Jobs")
        print("")
        for subscription_id in due_job_subscriptions:
            print(f"- `{subscription_id}`")
    history = list(scheduler.get("history", []))
    if history:
        print("")
        print("## History")
        print("")
        for entry in history[:10]:
            due_connectors_label = ",".join(str(item) for item in list(entry.get("due_connector_ids", []))) or "none"
            due_peers_label = ",".join(str(item) for item in list(entry.get("due_peer_sync_ids", []))) or "none"
            due_jobs_label = ",".join(str(item) for item in list(entry.get("due_job_subscription_ids", []))) or "none"
            print(
                f"- `{entry.get('tick_at', '')}` "
                f"`{entry.get('action', '')}` "
                f"connectors:{due_connectors_label} "
                f"peers:{due_peers_label} "
                f"scheduled:{due_jobs_label}"
            )
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_workers(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_control_plane_workers(workspace))
    print(f"Worker registry: {workspace.workers_registry_path}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_scheduler_tick(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = run_scheduler_tick(
            workspace,
            actor_id=args.actor_id,
            enqueue_only=args.enqueue_only,
            force=args.force,
            limit=args.limit,
        )
    except (AccessError, ControlPlaneError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Scheduler action: {result.action}")
    print(f"Due connectors: {result.due_connector_count}")
    print(f"Due scheduled jobs: {result.due_job_subscription_count}")
    if result.due_connector_ids:
        print("Connector ids:")
        for connector_id in result.due_connector_ids:
            print(f"- {connector_id}")
    if result.due_job_subscription_ids:
        print("Scheduled job ids:")
        for subscription_id in result.due_job_subscription_ids:
            print(f"- {subscription_id}")
    if result.enqueued_job_ids:
        print("Enqueued jobs:")
        for job_id in result.enqueued_job_ids:
            print(f"- {job_id}")
    if result.executed_run_manifest_path is not None:
        print(f"Executed run manifest: {result.executed_run_manifest_path}")
    print(f"Control-plane manifest: {workspace.control_plane_manifest_path}")
    return 0


def cmd_control_plane_serve(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    server = create_control_plane_server(workspace=workspace, host=args.host, port=args.port)
    host, port = server.server_address
    print(f"Serving control plane at http://{host}:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopped control-plane server.")
    finally:
        server.server_close()
    return 0


def cmd_worker_remote(args: argparse.Namespace) -> int:
    try:
        result = run_remote_worker(
            server_url=args.server_url,
            token=args.token,
            worker_id=args.worker_id,
            max_jobs=args.max_jobs,
            lease_seconds=args.lease_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
            max_idle_polls=args.max_idle_polls,
            worker_capabilities=list(args.capability or []),
            workspace_root=Path(args.workspace).expanduser().resolve() if args.workspace else None,
        )
    except RemoteWorkerError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Processed jobs: {result.processed_count}")
    print(f"Completed jobs: {result.completed_count}")
    print(f"Stopped reason: {result.stopped_reason}")
    return 0


def cmd_collab_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_collaboration_threads(workspace))
    print(f"Collaboration manifest: {workspace.collaboration_manifest_path}")
    return 0


def cmd_collab_request_review(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        thread = request_review(
            workspace,
            artifact_path=args.artifact_path,
            actor_id=args.actor_id,
            assignee_ids=list(args.assign or []),
            note=args.note,
        )
    except (CollaborationError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    write_notifications_manifest(workspace)
    print(f"Requested review for {thread['artifact_path']}")
    print(f"Thread status: {thread['status']}")
    print(f"Collaboration manifest: {workspace.collaboration_manifest_path}")
    return 0


def cmd_collab_comment(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        comment = add_comment(
            workspace,
            artifact_path=args.artifact_path,
            actor_id=args.actor_id,
            message=args.message,
        )
    except (CollaborationError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    write_notifications_manifest(workspace)
    print(f"Added comment {comment['comment_id']} to {args.artifact_path}")
    print(f"Collaboration manifest: {workspace.collaboration_manifest_path}")
    return 0


def cmd_collab_approve(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        decision = record_decision(
            workspace,
            artifact_path=args.artifact_path,
            actor_id=args.actor_id,
            decision="approved",
            summary=args.summary,
        )
    except (CollaborationError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    write_notifications_manifest(workspace)
    print(f"Recorded approval {decision['decision_id']} for {args.artifact_path}")
    print(f"Collaboration manifest: {workspace.collaboration_manifest_path}")
    return 0


def cmd_collab_request_changes(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        decision = record_decision(
            workspace,
            artifact_path=args.artifact_path,
            actor_id=args.actor_id,
            decision="changes_requested",
            summary=args.summary,
        )
    except (CollaborationError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    write_notifications_manifest(workspace)
    print(f"Recorded change request {decision['decision_id']} for {args.artifact_path}")
    print(f"Collaboration manifest: {workspace.collaboration_manifest_path}")
    return 0


def cmd_collab_resolve(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        thread = resolve_review(
            workspace,
            artifact_path=args.artifact_path,
            actor_id=args.actor_id,
        )
    except (CollaborationError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    write_notifications_manifest(workspace)
    print(f"Resolved collaboration thread for {thread['artifact_path']}")
    print(f"Collaboration manifest: {workspace.collaboration_manifest_path}")
    return 0


def cmd_connector_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_connector_list(workspace))
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_connector_add(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "register connectors")
        connector = add_connector(
            workspace,
            kind=args.kind,
            source=args.source,
            name=args.name,
            actor=actor,
        )
    except (ConnectorError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Registered connector {connector['connector_id']} ({connector['kind']})")
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_connector_sync(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "sync connectors")
        result = sync_connector(
            workspace,
            connector_id=args.connector_id,
            force=args.force,
            actor=actor,
        )
    except (ConnectorError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Synced connector {result.connector_id} ({result.connector_kind})")
    print(f"Imported {result.synced_count} source artifact(s).")
    print(f"Wrote change summary to {result.change_summary_path}")
    print(f"Wrote run manifest to {result.run_manifest_path}")
    print(f"Connector registry: {result.registry_path}")
    return 0


def cmd_connector_sync_all(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "sync connectors")
        result = sync_all_connectors(
            workspace,
            force=args.force,
            limit=args.limit,
            scheduled_only=args.scheduled_only,
            actor=actor,
        )
    except (ConnectorError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Synced {result.synced_connector_count} connector(s).")
    print(f"Imported {result.total_result_count} source artifact(s).")
    print(f"Wrote batch run manifest to {result.run_manifest_path}")
    print(f"Connector registry: {result.registry_path}")
    return 0


def cmd_connector_subscribe(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "manage connector subscriptions")
        connector = subscribe_connector(
            workspace,
            connector_id=args.connector_id,
            every_hours=args.every_hours,
            weekdays=list(args.weekday or []),
            hour=args.hour,
            minute=args.minute,
            actor=actor,
        )
    except (ConnectorError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    subscription = connector["subscription"]
    if subscription["schedule_type"] == "weekly":
        weekday_label = ",".join(subscription["weekdays"])
        print(
            f"Subscribed connector {connector['connector_id']} "
            f"for weekly schedule {weekday_label} at "
            f"{int(subscription['hour'] or 0):02d}:{int(subscription['minute'] or 0):02d}."
        )
    else:
        print(
            f"Subscribed connector {connector['connector_id']} "
            f"for every {connector['subscription']['interval_hours']} hour(s)."
        )
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_connector_unsubscribe(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        actor = _require_operator_actor(workspace, args.actor_id, "manage connector subscriptions")
        connector = unsubscribe_connector(workspace, connector_id=args.connector_id, actor=actor)
    except (ConnectorError, AccessError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Unsubscribed connector {connector['connector_id']}")
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_ui_review(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    output_file = None
    if args.output_file:
        output_file = Path(args.output_file).expanduser()
        if not output_file.is_absolute():
            output_file = workspace.root / output_file
        output_file = output_file.resolve()

    result = write_review_ui_bundle(workspace, snapshot, output_file=output_file, actor_id=args.actor_id)
    print(f"Wrote review UI to {result.html_path}")
    print(f"Wrote review UI export to {result.export_path}")
    print(f"Wrote review UI state to {result.state_path}")

    if not args.serve:
        return 0

    server = create_review_ui_server(
        result.html_path.parent,
        host=args.host,
        port=args.port,
        index_name=result.html_path.name,
        workspace=workspace,
        actor_id=args.actor_id,
    )
    host, port = server.server_address
    print(f"Serving review UI at http://{host}:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopped review UI server.")
    finally:
        server.server_close()
    return 0


def cmd_export_jsonl(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_file = None
    if args.output_file:
        output_file = Path(args.output_file).expanduser()
        if not output_file.is_absolute():
            output_file = workspace.root / output_file
        output_file = output_file.resolve()
    result = export_research_jsonl(workspace, output_file=output_file)
    print(f"Wrote JSONL export to {result.path}")
    print(f"Exported {result.record_count} research run(s).")
    return 0


def cmd_export_presentations(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_presentations_bundle(workspace, output_dir=output_dir)
    print(f"Wrote presentation export to {result.directory}")
    print(f"Wrote presentation manifest to {result.manifest_path}")
    print(f"Bundled {result.presentation_count} presentation(s).")
    return 0


def cmd_export_training_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_training_bundle(workspace, output_dir=output_dir)
    print(f"Wrote training export to {result.directory}")
    print(f"Wrote training dataset to {result.dataset_path}")
    print(f"Wrote training manifest to {result.manifest_path}")
    print(f"Bundled {result.record_count} research run(s).")
    return 0


def cmd_export_finetune_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    try:
        result = export_finetune_bundle(
            workspace,
            output_dir=output_dir,
            provider_formats=list(args.provider_format or []),
        )
    except ExportError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Wrote finetune export to {result.directory}")
    print(f"Wrote supervised dataset to {result.supervised_path}")
    print(f"Wrote retrieval dataset to {result.retrieval_path}")
    print(f"Wrote finetune manifest to {result.manifest_path}")
    for provider_name, provider_path in sorted(result.provider_exports.items()):
        print(f"Wrote provider export {provider_name} to {provider_path}")
    print(
        "Bundled "
        f"{result.supervised_count} supervised example(s) and "
        f"{result.retrieval_count} retrieval example(s)."
    )
    return 0


def cmd_export_feedback_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_feedback_bundle(workspace, output_dir=output_dir)
    print(f"Wrote feedback export to {result.directory}")
    print(f"Wrote remediation dataset to {result.dataset_path}")
    print(f"Wrote feedback manifest to {result.manifest_path}")
    print(f"Bundled {result.record_count} remediation record(s).")
    return 0


def cmd_export_correction_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_correction_bundle(workspace, output_dir=output_dir)
    print(f"Wrote correction export to {result.directory}")
    print(f"Wrote correction dataset to {result.dataset_path}")
    print(f"Wrote correction manifest to {result.manifest_path}")
    print(f"Bundled {result.record_count} correction record(s).")
    return 0


def cmd_export_training_loop_bundle(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    try:
        result = export_training_loop_bundle(
            workspace,
            output_dir=output_dir,
            provider_formats=list(args.provider_format or []),
        )
    except ExportError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Wrote training-loop bundle to {result.directory}")
    print(f"Wrote training-loop manifest to {result.manifest_path}")
    print(f"Wrote evaluation report to {result.evaluation_report_path}")
    print(f"Wrote evaluation payload to {result.evaluation_payload_path}")
    print(f"Wrote feedback manifest to {result.feedback_manifest_path}")
    print(f"Wrote correction manifest to {result.correction_manifest_path}")
    print(f"Wrote finetune manifest to {result.finetune_manifest_path}")
    return 0


def cmd_improve_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    try:
        result = improve_research_loop(
            workspace,
            profile_name=args.profile,
            limit=args.limit,
            output_dir=output_dir,
            provider_formats=list(args.provider_format or []),
        )
    except (ExportError, RemediationError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Improved {result.remediation.remediated_count} research run(s).")
    if result.remediation.manifest_paths:
        for manifest_path in result.remediation.manifest_paths:
            print(f"- {manifest_path}")
    else:
        print("No remediation candidates were available, but the training-loop bundle was still refreshed.")
    print(f"Wrote training-loop bundle to {result.bundle.directory}")
    print(f"Wrote training-loop manifest to {result.bundle.manifest_path}")
    return 0


def cmd_remediate_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = remediate_research_runs(
            workspace,
            profile_name=args.profile,
            limit=args.limit,
        )
    except RemediationError as error:
        print(str(error), file=sys.stderr)
        return 2
    if not result.remediated_count:
        print("No remediation candidates found.")
        return 0
    print(f"Remediated {result.remediated_count} research run(s).")
    for manifest_path in result.manifest_paths:
        print(f"- {manifest_path}")
    return 0


def cmd_eval_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_file = None
    payload_file = None
    if args.output_file:
        output_file = Path(args.output_file).expanduser()
        if not output_file.is_absolute():
            output_file = workspace.root / output_file
        output_file = output_file.resolve()
    if args.payload_file:
        payload_file = Path(args.payload_file).expanduser()
        if not payload_file.is_absolute():
            payload_file = workspace.root / payload_file
        payload_file = payload_file.resolve()
    result = evaluate_research_runs(workspace, output_file=output_file, payload_file=payload_file)
    print(f"Wrote research evaluation report to {result.report_path}")
    print(f"Wrote research evaluation payload to {result.payload_path}")
    print(f"Evaluated {result.run_count} research run(s).")
    return 0


def cmd_synth_qa(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_synthetic_qa_bundle(workspace, output_dir=output_dir)
    print(f"Wrote synthetic QA bundle to {result.directory}")
    print(f"Wrote synthetic dataset to {result.dataset_path}")
    print(f"Wrote synthetic manifest to {result.manifest_path}")
    print(f"Generated {result.record_count} synthetic QA record(s).")
    return 0


def cmd_synth_contrastive(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_synthetic_contrastive_bundle(workspace, output_dir=output_dir)
    print(f"Wrote synthetic contrastive bundle to {result.directory}")
    print(f"Wrote synthetic dataset to {result.dataset_path}")
    print(f"Wrote synthetic manifest to {result.manifest_path}")
    print(f"Generated {result.record_count} synthetic contrastive record(s).")
    return 0


def cmd_synth_graph_completion(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_synthetic_graph_completion_bundle(workspace, output_dir=output_dir)
    print(f"Wrote synthetic graph-completion bundle to {result.directory}")
    print(f"Wrote synthetic dataset to {result.dataset_path}")
    print(f"Wrote synthetic manifest to {result.manifest_path}")
    print(f"Generated {result.record_count} synthetic graph-completion record(s).")
    return 0


def cmd_synth_report_writing(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_dir = None
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser()
        if not output_dir.is_absolute():
            output_dir = workspace.root / output_dir
        output_dir = output_dir.resolve()
    result = export_synthetic_report_writing_bundle(workspace, output_dir=output_dir)
    print(f"Wrote synthetic report-writing bundle to {result.directory}")
    print(f"Wrote synthetic dataset to {result.dataset_path}")
    print(f"Wrote synthetic manifest to {result.manifest_path}")
    print(f"Generated {result.record_count} synthetic report-writing record(s).")
    return 0


def cmd_review_accept_concept(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        path = accept_concept_candidate(workspace, args.slug)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Accepted concept candidate into {path}")
    return 0


def cmd_review_resolve_merge(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        path = resolve_entity_merge(workspace, args.canonical_label, preferred_label=args.preferred_label)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Resolved merge candidate into {path}")
    return 0


def cmd_review_apply_backlink(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        path = apply_backlink_suggestion(workspace, args.target_path)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Applied backlink suggestion through {path}")
    return 0


def cmd_review_file_conflict(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        path = file_conflict_review(workspace, args.subject)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Filed conflict note into {path}")
    return 0


def cmd_review_dismiss(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        entry = dismiss_review_item(workspace, args.review_id, args.reason)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Dismissed review item {args.review_id} ({entry['kind']})")
    print(f"Reason: {entry['reason']}")
    return 0


def cmd_review_reopen(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        entry = reopen_review_item(workspace, args.review_id)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Reopened review item {args.review_id} ({entry['kind']})")
    return 0


def cmd_review_list_dismissed(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    entries = list_dismissed_review_items(workspace)
    if not entries:
        print("No dismissed review items found.")
        return 0
    print(f"Dismissed review items: {len(entries)}")
    print("")
    for entry in entries:
        print(f"{entry['review_id']} [{entry['kind']}]")
        reason = str(entry.get("reason", "")).strip()
        if reason:
            print(f"reason: {reason}")
        path = str(entry.get("path", "")).strip()
        if path:
            print(f"path: {path}")
        print("")
    return 0


def cmd_review_clear_dismissed(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        entry = clear_dismissed_review_item(workspace, args.review_id)
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Cleared dismissed review item {args.review_id} ({entry['kind']})")
    return 0


def cmd_maintain(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    config = workspace.load_config()
    policy = _resolve_maintenance_policy(config.maintenance_policy, args)
    try:
        result = run_maintenance_cycle(
            workspace,
            max_concepts=args.max_concepts,
            max_merges=args.max_merges,
            max_backlinks=args.max_backlinks,
            max_conflicts=args.max_conflicts,
            policy=policy,
        )
    except MaintenanceError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(
        "Maintenance applied "
        f"{len(result.accepted_concept_paths)} concept(s), "
        f"{len(result.resolved_merge_keys)} merge resolution(s), "
        f"{len(result.applied_backlink_targets)} backlink(s), and "
        f"{len(result.filed_conflict_keys)} conflict filing(s)."
    )
    print(f"Remaining review items: {result.remaining_review_count}")
    print(f"Lint issue count after maintenance: {result.issue_count}")
    print(f"Wrote change summary to {result.change_summary_path}")
    print(f"Wrote maintenance run manifest to {result.run_manifest_path}")
    return 0


def cmd_ingest_file(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        result = ingest_file(
            workspace,
            source=Path(args.source),
            category="files",
            name=args.name,
            force=args.force,
        )
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title=f"Filed source {result.path.name}",
        details=["Imported a local file into the raw corpus."],
        related_paths=[workspace.relative_path(result.path), workspace.relative_path(change_summary.path)],
    )
    print(f"Ingested file into {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_ingest_pdf(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        result = ingest_pdf(workspace, source=Path(args.source), name=args.name, force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title=f"Filed PDF {result.path.name}",
        details=["Imported a PDF and refreshed the compiled workspace index."],
        related_paths=[workspace.relative_path(result.path), workspace.relative_path(change_summary.path)],
    )
    print(f"Ingested pdf into {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_ingest_url(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        result = ingest_url(workspace, url=args.url, name=args.name, force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title=f"Captured URL source {result.path.name}",
        details=[f"Imported {args.url} into the raw URL corpus."],
        related_paths=[workspace.relative_path(result.path), workspace.relative_path(change_summary.path)],
    )
    print(f"Ingested url into {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_ingest_repo(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        result = ingest_repo(workspace, repo_path=args.source, name=args.name, force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title=f"Captured repository source {result.path.name}",
        details=[f"Imported repository metadata from {args.source}."],
        related_paths=[workspace.relative_path(result.path), workspace.relative_path(change_summary.path)],
    )
    print(f"Ingested repo manifest into {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_ingest_urls(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        results = ingest_urls(workspace, source_list=Path(args.source), force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title="Captured URL batch",
        details=[f"Imported {len(results)} URL source(s) from a list."],
        related_paths=[workspace.relative_path(change_summary.path)] + [workspace.relative_path(result.path) for result in results[:5]],
    )
    print(f"Ingested {len(results)} URL source(s).")
    for result in results:
        print(f"- {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_ingest_sitemap(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        results = ingest_sitemap(workspace, source=args.source, force=args.force, limit=args.limit)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title="Captured sitemap batch",
        details=[f"Imported {len(results)} URL source(s) from sitemap {args.source}."],
        related_paths=[workspace.relative_path(change_summary.path)] + [workspace.relative_path(result.path) for result in results[:5]],
    )
    print(f"Ingested {len(results)} URL source(s).")
    for result in results:
        print(f"- {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def cmd_ingest_batch(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
    try:
        results = ingest_batch(workspace, manifest_path=Path(args.manifest), force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
    _log_workspace_activity(
        workspace,
        operation="ingest",
        title="Executed batch ingest manifest",
        details=[f"Imported {len(results)} source(s) from {args.manifest}."],
        related_paths=[workspace.relative_path(change_summary.path)] + [workspace.relative_path(result.path) for result in results[:5]],
    )
    print(f"Batch ingested {len(results)} source(s).")
    for result in results:
        print(f"- {result.kind}: {result.path}")
    print(f"Wrote change summary to {change_summary.path}")
    return 0


def _resolve_maintenance_policy(base_policy: MaintenancePolicy, args: argparse.Namespace) -> MaintenancePolicy:
    deny_concepts = set(base_policy.deny_concepts)
    for value in list(getattr(args, "deny_concept", []) or []):
        deny_concepts.update(_split_csv_items(value))
    min_concept_support = (
        int(args.min_concept_support)
        if getattr(args, "min_concept_support", None) is not None
        else int(base_policy.min_concept_support)
    )
    require_entity_evidence = bool(base_policy.require_entity_evidence_for_short_concepts)
    if getattr(args, "allow_short_concepts_without_entity", False):
        require_entity_evidence = False
    return MaintenancePolicy(
        min_concept_support=max(1, min_concept_support),
        require_entity_evidence_for_short_concepts=require_entity_evidence,
        deny_concepts=sorted(deny_concepts),
    )


def _split_csv_items(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def cmd_query(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    engine = SearchEngine.from_workspace(workspace, snapshot)
    hits = engine.search(args.question, limit=args.limit)
    report_path = render_query_report(workspace, args.question, hits, snapshot=snapshot)
    packet_path = render_query_packet(workspace, args.question, hits, snapshot=snapshot)
    print(f"Wrote report to {report_path}")
    print(f"Wrote prompt packet to {packet_path}")
    if args.slides:
        slide_path = render_marp_slides(workspace, args.question, hits)
        print(f"Wrote slide deck to {slide_path}")
    return 0


def cmd_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_file = Path(args.output_file).resolve() if args.output_file else None
    if args.resume and args.question:
        print("Pass either a new question or --resume, not both.", file=sys.stderr)
        return 2
    if not args.resume and not args.question:
        print("A question is required unless you pass --resume.", file=sys.stderr)
        return 2
    try:
        result = run_research_cycle(
            workspace,
            question=args.question,
            limit=args.limit,
            profile_name=args.profile,
            output_file=output_file,
            slides=args.slides,
            mode=args.mode,
            resume=args.resume,
            job_profile=args.job_profile,
        )
    except ResearchError as error:
        print(str(error), file=sys.stderr)
        return 2

    if result.resumed:
        print(f"Resumed research run from {result.run_manifest_path}")
    print(f"Wrote research plan to {result.plan_path}")
    print(f"Wrote report to {result.report_path}")
    print(f"Wrote prompt packet to {result.packet_path}")
    print(f"Wrote research notes to {result.notes_dir}")
    print(f"Wrote source packet to {result.source_packet_path}")
    print(f"Wrote checkpoints to {result.checkpoints_path}")
    print(f"Wrote validation report to {result.validation_report_path}")
    print(f"Wrote change summary to {result.change_summary_path}")
    print(f"Wrote run manifest to {result.run_manifest_path}")
    if result.slide_path is not None:
        print(f"Wrote slide deck to {result.slide_path}")
    if result.answer_path is not None:
        print(f"Wrote filed answer to {result.answer_path}")
    elif not result.ran_profile:
        print("No profile provided. Research report and prompt packet generated but not executed.")
    if result.warning_count:
        print(f"Research verification reported {result.warning_count} warning(s).")
    return 0


def cmd_compile(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    output_file = Path(args.output_file).resolve() if args.output_file else None
    try:
        result = run_compile_cycle(workspace, profile_name=args.profile, output_file=output_file)
    except CompileError as error:
        print(str(error), file=sys.stderr)
        return 2

    print(f"Wrote plan to {result.plan_path}")
    print(f"Wrote prompt packet to {result.packet_path}")
    if result.ran_profile and result.output_file:
        print(f"Wrote compile output to {result.output_file}")
    elif not result.ran_profile:
        print("No profile provided. Compile packet generated but not executed.")

    if result.issue_count:
        print(f"Compile finished with {result.issue_count} lint issue(s).")
        return 1 if args.strict else 0

    print("Compile finished with no lint issues.")
    return 0


def cmd_run_packet(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    config = workspace.load_config()
    try:
        adapter = adapter_from_config(config, args.profile)
    except AdapterError as error:
        print(str(error), file=sys.stderr)
        return 2

    prompt_file = Path(args.prompt_file).resolve()
    output_file = Path(args.output_file).resolve() if args.output_file else None
    if output_file is not None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
    result = adapter.run(prompt_file=prompt_file, workspace_root=workspace.root, output_file=output_file)
    if output_file and not adapter.output_file_flag and result.stdout:
        output_file.write_text(result.stdout, encoding="utf-8")
        print(f"Wrote output to {output_file}")
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    if output_file and adapter.output_file_flag and output_file.exists():
        print(f"Wrote output to {output_file}")
    return result.returncode


def cmd_adapter_list(args: argparse.Namespace) -> int:
    presets = builtin_adapter_presets()
    for name in sorted(presets):
        preset = presets[name]
        print(f"{name}: {preset.summary}")
    return 0


def cmd_adapter_install(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    config = workspace.load_config()
    try:
        profile = install_builtin_adapter(
            config=config,
            preset_name=args.name,
            profile_name=args.profile,
            force=args.force,
        )
    except AdapterError as error:
        print(str(error), file=sys.stderr)
        return 2

    save_config(workspace.config_path, config)
    installed_name = args.profile or args.name
    print(f"Installed builtin adapter '{args.name}' as profile '{installed_name}' in {workspace.config_path}")
    if profile.description:
        print(profile.description)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cognisync", description="Cognisync knowledge-base framework CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize a Cognisync workspace")
    init_parser.add_argument("path", nargs="?", default=".")
    init_parser.add_argument("--name", default=None)
    init_parser.add_argument("--force", action="store_true")
    init_parser.set_defaults(func=cmd_init)

    scan_parser = subparsers.add_parser("scan", help="Scan workspace files and build an index")
    scan_parser.add_argument("--workspace", default=".")
    scan_parser.set_defaults(func=cmd_scan)

    demo_parser = subparsers.add_parser("demo", help="Create a polished demo knowledge garden")
    demo_parser.add_argument("path", nargs="?", default="examples/research-garden")
    demo_parser.add_argument("--force", action="store_true")
    demo_parser.set_defaults(func=cmd_demo)

    doctor_parser = subparsers.add_parser("doctor", help="Validate workspace and adapter readiness")
    doctor_parser.add_argument("--workspace", default=".")
    doctor_parser.add_argument("--strict", action="store_true")
    doctor_parser.set_defaults(func=cmd_doctor)

    notify_parser = subparsers.add_parser("notify", help="Inspect the filesystem-native operator notification inbox")
    notify_subparsers = notify_parser.add_subparsers(dest="notify_command", required=True)

    notify_list_parser = notify_subparsers.add_parser("list", help="List active notifications")
    notify_list_parser.add_argument("--workspace", default=".")
    notify_list_parser.set_defaults(func=cmd_notify_list)

    audit_parser = subparsers.add_parser("audit", help="Inspect the filesystem-native audit index")
    audit_subparsers = audit_parser.add_subparsers(dest="audit_command", required=True)

    audit_list_parser = audit_subparsers.add_parser("list", help="List derived audit events from persisted manifests")
    audit_list_parser.add_argument("--workspace", default=".")
    audit_list_parser.set_defaults(func=cmd_audit_list)

    usage_parser = subparsers.add_parser("usage", help="Inspect derived workspace usage accounting")
    usage_subparsers = usage_parser.add_subparsers(dest="usage_command", required=True)

    usage_report_parser = usage_subparsers.add_parser("report", help="Write a usage summary manifest for the workspace")
    usage_report_parser.add_argument("--workspace", default=".")
    usage_report_parser.set_defaults(func=cmd_usage_report)

    access_parser = subparsers.add_parser("access", help="Manage file-native workspace roles and permissions")
    access_subparsers = access_parser.add_subparsers(dest="access_command", required=True)

    access_list_parser = access_subparsers.add_parser("list", help="List the persisted workspace access roster")
    access_list_parser.add_argument("--workspace", default=".")
    access_list_parser.set_defaults(func=cmd_access_list)

    access_grant_parser = access_subparsers.add_parser("grant", help="Grant or update a workspace access member")
    access_grant_parser.add_argument("principal_id")
    access_grant_parser.add_argument("role", choices=VALID_ACCESS_ROLES)
    access_grant_parser.add_argument("--workspace", default=".")
    access_grant_parser.add_argument("--name", default=None)
    access_grant_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    access_grant_parser.set_defaults(func=cmd_access_grant)

    access_revoke_parser = access_subparsers.add_parser("revoke", help="Remove a workspace access member")
    access_revoke_parser.add_argument("principal_id")
    access_revoke_parser.add_argument("--workspace", default=".")
    access_revoke_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    access_revoke_parser.set_defaults(func=cmd_access_revoke)

    share_parser = subparsers.add_parser(
        "share",
        help="Manage shared-workspace peer state and published control-plane bindings",
    )
    share_subparsers = share_parser.add_subparsers(dest="share_command", required=True)

    share_status_parser = share_subparsers.add_parser("status", help="Inspect the shared-workspace manifest")
    share_status_parser.add_argument("--workspace", default=".")
    share_status_parser.set_defaults(func=cmd_share_status)

    share_bind_parser = share_subparsers.add_parser(
        "bind-control-plane",
        help="Publish the control-plane URL for this workspace",
    )
    share_bind_parser.add_argument("url")
    share_bind_parser.add_argument("--workspace", default=".")
    share_bind_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_bind_parser.set_defaults(func=cmd_share_bind_control_plane)

    share_invite_parser = share_subparsers.add_parser(
        "invite-peer",
        help="Invite a remote peer into the shared workspace",
    )
    share_invite_parser.add_argument("peer_id")
    share_invite_parser.add_argument("role", choices=VALID_ACCESS_ROLES)
    share_invite_parser.add_argument("--workspace", default=".")
    share_invite_parser.add_argument("--name", default=None)
    share_invite_parser.add_argument("--base-url", default=None)
    share_invite_parser.add_argument("--capability", action="append", default=[])
    share_invite_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_invite_parser.set_defaults(func=cmd_share_invite_peer)

    share_accept_parser = share_subparsers.add_parser(
        "accept-peer",
        help="Accept a pending shared-workspace peer by peer id",
    )
    share_accept_parser.add_argument("peer_ref")
    share_accept_parser.add_argument("--workspace", default=".")
    share_accept_parser.add_argument("--actor-id", default=None)
    share_accept_parser.set_defaults(func=cmd_share_accept_peer)

    share_list_parser = share_subparsers.add_parser(
        "list-peers",
        help="List persisted shared-workspace peers",
    )
    share_list_parser.add_argument("--workspace", default=".")
    share_list_parser.add_argument("--status", default=None, choices=["accepted", "pending", "suspended"])
    share_list_parser.set_defaults(func=cmd_share_list_peers)

    share_issue_bundle_parser = share_subparsers.add_parser(
        "issue-peer-bundle",
        help="Issue a remote peer bundle with a control-plane token",
    )
    share_issue_bundle_parser.add_argument("peer_ref")
    share_issue_bundle_parser.add_argument("--workspace", default=".")
    share_issue_bundle_parser.add_argument("--output-file", required=True)
    share_issue_bundle_parser.add_argument("--scope", action="append", default=[])
    share_issue_bundle_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_issue_bundle_parser.set_defaults(func=cmd_share_issue_peer_bundle)

    share_attach_remote_parser = share_subparsers.add_parser(
        "attach-remote-bundle",
        help="Attach a remote workspace bundle for control-plane pull syncs",
    )
    share_attach_remote_parser.add_argument("bundle_file")
    share_attach_remote_parser.add_argument("--workspace", default=".")
    share_attach_remote_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_attach_remote_parser.set_defaults(func=cmd_share_attach_remote_bundle)

    share_refresh_remote_parser = share_subparsers.add_parser(
        "refresh-remote-bundle",
        help="Refresh an attached remote bundle with rotated control-plane credentials",
    )
    share_refresh_remote_parser.add_argument("bundle_file")
    share_refresh_remote_parser.add_argument("--workspace", default=".")
    share_refresh_remote_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_refresh_remote_parser.set_defaults(func=cmd_share_refresh_remote_bundle)

    share_list_attached_parser = share_subparsers.add_parser(
        "list-attached-remotes",
        help="List attached remote workspaces imported from peer bundles",
    )
    share_list_attached_parser.add_argument("--workspace", default=".")
    share_list_attached_parser.add_argument("--status", default=None, choices=["attached", "suspended"])
    share_list_attached_parser.set_defaults(func=cmd_share_list_attached_remotes)

    share_pull_remote_parser = share_subparsers.add_parser(
        "pull-remote",
        help="Pull and import a sync bundle from an attached remote workspace",
    )
    share_pull_remote_parser.add_argument("remote_ref")
    share_pull_remote_parser.add_argument("--workspace", default=".")
    share_pull_remote_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_pull_remote_parser.set_defaults(func=cmd_share_pull_remote)

    share_suspend_remote_parser = share_subparsers.add_parser(
        "suspend-remote",
        help="Suspend an attached remote and disable its scheduled pull imports",
    )
    share_suspend_remote_parser.add_argument("remote_ref")
    share_suspend_remote_parser.add_argument("--workspace", default=".")
    share_suspend_remote_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_suspend_remote_parser.set_defaults(func=cmd_share_suspend_remote)

    share_detach_remote_parser = share_subparsers.add_parser(
        "detach-remote",
        help="Detach an attached remote from the local workspace",
    )
    share_detach_remote_parser.add_argument("remote_ref")
    share_detach_remote_parser.add_argument("--workspace", default=".")
    share_detach_remote_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_detach_remote_parser.set_defaults(func=cmd_share_detach_remote)

    share_set_peer_role_parser = share_subparsers.add_parser(
        "set-peer-role",
        help="Update a shared peer role and rebind its workspace access",
    )
    share_set_peer_role_parser.add_argument("peer_ref")
    share_set_peer_role_parser.add_argument("role", choices=VALID_ACCESS_ROLES)
    share_set_peer_role_parser.add_argument("--workspace", default=".")
    share_set_peer_role_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_set_peer_role_parser.set_defaults(func=cmd_share_set_peer_role)

    share_suspend_peer_parser = share_subparsers.add_parser(
        "suspend-peer",
        help="Suspend a shared peer and revoke its active access",
    )
    share_suspend_peer_parser.add_argument("peer_ref")
    share_suspend_peer_parser.add_argument("--workspace", default=".")
    share_suspend_peer_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_suspend_peer_parser.set_defaults(func=cmd_share_suspend_peer)

    share_remove_peer_parser = share_subparsers.add_parser(
        "remove-peer",
        help="Remove a shared peer and revoke its active access",
    )
    share_remove_peer_parser.add_argument("peer_ref")
    share_remove_peer_parser.add_argument("--workspace", default=".")
    share_remove_peer_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_remove_peer_parser.set_defaults(func=cmd_share_remove_peer)

    share_set_policy_parser = share_subparsers.add_parser(
        "set-policy",
        help="Update shared-workspace trust policy controls",
    )
    share_set_policy_parser.add_argument("--workspace", default=".")
    share_set_policy_parser.add_argument("--allow-remote-workers", action="store_true")
    share_set_policy_parser.add_argument("--deny-remote-workers", action="store_true")
    share_set_policy_parser.add_argument("--allow-sync-imports", action="store_true")
    share_set_policy_parser.add_argument("--deny-sync-imports", action="store_true")
    share_set_policy_parser.add_argument("--default-peer-role", default=None, choices=VALID_ACCESS_ROLES)
    share_set_policy_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_set_policy_parser.set_defaults(func=cmd_share_set_policy)

    share_subscribe_sync_parser = share_subparsers.add_parser(
        "subscribe-sync",
        help="Subscribe an accepted peer to scheduled workspace sync exports",
    )
    share_subscribe_sync_parser.add_argument("peer_ref")
    share_subscribe_sync_parser.add_argument("--workspace", default=".")
    share_subscribe_sync_parser.add_argument("--every-hours", type=int, required=True)
    share_subscribe_sync_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_subscribe_sync_parser.set_defaults(func=cmd_share_subscribe_sync)

    share_unsubscribe_sync_parser = share_subparsers.add_parser(
        "unsubscribe-sync",
        help="Disable scheduled workspace sync exports for a peer",
    )
    share_unsubscribe_sync_parser.add_argument("peer_ref")
    share_unsubscribe_sync_parser.add_argument("--workspace", default=".")
    share_unsubscribe_sync_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_unsubscribe_sync_parser.set_defaults(func=cmd_share_unsubscribe_sync)

    share_subscribe_remote_pull_parser = share_subparsers.add_parser(
        "subscribe-remote-pull",
        help="Subscribe an attached remote for scheduled pull imports",
    )
    share_subscribe_remote_pull_parser.add_argument("remote_ref")
    share_subscribe_remote_pull_parser.add_argument("--workspace", default=".")
    share_subscribe_remote_pull_parser.add_argument("--every-hours", type=int, required=True)
    share_subscribe_remote_pull_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_subscribe_remote_pull_parser.set_defaults(func=cmd_share_subscribe_remote_pull)

    share_unsubscribe_remote_pull_parser = share_subparsers.add_parser(
        "unsubscribe-remote-pull",
        help="Disable scheduled pull imports for an attached remote",
    )
    share_unsubscribe_remote_pull_parser.add_argument("remote_ref")
    share_unsubscribe_remote_pull_parser.add_argument("--workspace", default=".")
    share_unsubscribe_remote_pull_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    share_unsubscribe_remote_pull_parser.set_defaults(func=cmd_share_unsubscribe_remote_pull)

    control_plane_parser = subparsers.add_parser(
        "control-plane",
        help="Manage hosted-alpha control-plane state, tokens, and scheduler actions",
    )
    control_plane_subparsers = control_plane_parser.add_subparsers(dest="control_plane_command", required=True)

    control_plane_status_parser = control_plane_subparsers.add_parser(
        "status",
        help="Inspect the persisted control-plane manifest",
    )
    control_plane_status_parser.add_argument("--workspace", default=".")
    control_plane_status_parser.set_defaults(func=cmd_control_plane_status)

    control_plane_scheduler_status_parser = control_plane_subparsers.add_parser(
        "scheduler-status",
        help="Inspect due connectors and scheduler history",
    )
    control_plane_scheduler_status_parser.add_argument("--workspace", default=".")
    control_plane_scheduler_status_parser.set_defaults(func=cmd_control_plane_scheduler_status)

    control_plane_workers_parser = control_plane_subparsers.add_parser(
        "workers",
        help="Inspect the current remote worker registry",
    )
    control_plane_workers_parser.add_argument("--workspace", default=".")
    control_plane_workers_parser.set_defaults(func=cmd_control_plane_workers)

    control_plane_invite_parser = control_plane_subparsers.add_parser(
        "invite",
        help="Create a control-plane invite for a workspace principal",
    )
    control_plane_invite_parser.add_argument("principal_id")
    control_plane_invite_parser.add_argument("role", choices=VALID_ACCESS_ROLES)
    control_plane_invite_parser.add_argument("--workspace", default=".")
    control_plane_invite_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_invite_parser.set_defaults(func=cmd_control_plane_invite)

    control_plane_accept_invite_parser = control_plane_subparsers.add_parser(
        "accept-invite",
        help="Accept a pending control-plane invite by invite id or principal id",
    )
    control_plane_accept_invite_parser.add_argument("invite_ref")
    control_plane_accept_invite_parser.add_argument("--workspace", default=".")
    control_plane_accept_invite_parser.add_argument("--actor-id", default=None)
    control_plane_accept_invite_parser.set_defaults(func=cmd_control_plane_accept_invite)

    control_plane_issue_token_parser = control_plane_subparsers.add_parser(
        "issue-token",
        help="Issue a bearer token for control-plane access",
    )
    control_plane_issue_token_parser.add_argument("principal_id")
    control_plane_issue_token_parser.add_argument("--workspace", default=".")
    control_plane_issue_token_parser.add_argument("--scope", action="append", default=[])
    control_plane_issue_token_parser.add_argument("--description", default="")
    control_plane_issue_token_parser.add_argument("--expires-in-hours", type=int, default=None)
    control_plane_issue_token_parser.add_argument("--output-file", default=None)
    control_plane_issue_token_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_issue_token_parser.set_defaults(func=cmd_control_plane_issue_token)

    control_plane_list_tokens_parser = control_plane_subparsers.add_parser(
        "list-tokens",
        help="List persisted control-plane tokens without exposing raw token values",
    )
    control_plane_list_tokens_parser.add_argument("--workspace", default=".")
    control_plane_list_tokens_parser.set_defaults(func=cmd_control_plane_list_tokens)

    control_plane_revoke_token_parser = control_plane_subparsers.add_parser(
        "revoke-token",
        help="Revoke a previously issued control-plane token",
    )
    control_plane_revoke_token_parser.add_argument("token_id")
    control_plane_revoke_token_parser.add_argument("--workspace", default=".")
    control_plane_revoke_token_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_revoke_token_parser.set_defaults(func=cmd_control_plane_revoke_token)

    control_plane_schedule_research_parser = control_plane_subparsers.add_parser(
        "schedule-research",
        help="Schedule a recurring research job in the control-plane manifest",
    )
    control_plane_schedule_research_parser.add_argument("question")
    control_plane_schedule_research_parser.add_argument("--workspace", default=".")
    control_plane_schedule_research_parser.add_argument("--every-hours", type=int, required=True)
    control_plane_schedule_research_parser.add_argument("--profile-name", default=None)
    control_plane_schedule_research_parser.add_argument("--limit", type=int, default=5)
    control_plane_schedule_research_parser.add_argument("--mode", default="wiki")
    control_plane_schedule_research_parser.add_argument("--slides", action="store_true")
    control_plane_schedule_research_parser.add_argument("--job-profile", default=DEFAULT_RESEARCH_JOB_PROFILE)
    control_plane_schedule_research_parser.add_argument("--label", default=None)
    control_plane_schedule_research_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_schedule_research_parser.set_defaults(func=cmd_control_plane_schedule_research)

    control_plane_schedule_compile_parser = control_plane_subparsers.add_parser(
        "schedule-compile",
        help="Schedule a recurring compile job in the control-plane manifest",
    )
    control_plane_schedule_compile_parser.add_argument("--workspace", default=".")
    control_plane_schedule_compile_parser.add_argument("--every-hours", type=int, required=True)
    control_plane_schedule_compile_parser.add_argument("--profile-name", default=None)
    control_plane_schedule_compile_parser.add_argument("--label", default=None)
    control_plane_schedule_compile_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_schedule_compile_parser.set_defaults(func=cmd_control_plane_schedule_compile)

    control_plane_schedule_lint_parser = control_plane_subparsers.add_parser(
        "schedule-lint",
        help="Schedule a recurring lint job in the control-plane manifest",
    )
    control_plane_schedule_lint_parser.add_argument("--workspace", default=".")
    control_plane_schedule_lint_parser.add_argument("--every-hours", type=int, required=True)
    control_plane_schedule_lint_parser.add_argument("--label", default=None)
    control_plane_schedule_lint_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_schedule_lint_parser.set_defaults(func=cmd_control_plane_schedule_lint)

    control_plane_schedule_maintain_parser = control_plane_subparsers.add_parser(
        "schedule-maintain",
        help="Schedule a recurring maintenance job in the control-plane manifest",
    )
    control_plane_schedule_maintain_parser.add_argument("--workspace", default=".")
    control_plane_schedule_maintain_parser.add_argument("--every-hours", type=int, required=True)
    control_plane_schedule_maintain_parser.add_argument("--max-concepts", type=int, default=10)
    control_plane_schedule_maintain_parser.add_argument("--max-merges", type=int, default=10)
    control_plane_schedule_maintain_parser.add_argument("--max-backlinks", type=int, default=10)
    control_plane_schedule_maintain_parser.add_argument("--max-conflicts", type=int, default=10)
    control_plane_schedule_maintain_parser.add_argument("--label", default=None)
    control_plane_schedule_maintain_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_schedule_maintain_parser.set_defaults(func=cmd_control_plane_schedule_maintain)

    control_plane_list_scheduled_jobs_parser = control_plane_subparsers.add_parser(
        "list-scheduled-jobs",
        help="List recurring scheduled jobs in the control-plane manifest",
    )
    control_plane_list_scheduled_jobs_parser.add_argument("--workspace", default=".")
    control_plane_list_scheduled_jobs_parser.set_defaults(func=cmd_control_plane_list_scheduled_jobs)

    control_plane_remove_scheduled_job_parser = control_plane_subparsers.add_parser(
        "remove-scheduled-job",
        help="Disable a recurring scheduled job in the control-plane manifest",
    )
    control_plane_remove_scheduled_job_parser.add_argument("subscription_id")
    control_plane_remove_scheduled_job_parser.add_argument("--workspace", default=".")
    control_plane_remove_scheduled_job_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_remove_scheduled_job_parser.set_defaults(func=cmd_control_plane_remove_scheduled_job)

    control_plane_scheduler_parser = control_plane_subparsers.add_parser(
        "scheduler-tick",
        help="Run a hosted-alpha scheduler tick over subscribed connectors",
    )
    control_plane_scheduler_parser.add_argument("--workspace", default=".")
    control_plane_scheduler_parser.add_argument("--enqueue-only", action="store_true")
    control_plane_scheduler_parser.add_argument("--force", action="store_true")
    control_plane_scheduler_parser.add_argument("--limit", type=int, default=None)
    control_plane_scheduler_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    control_plane_scheduler_parser.set_defaults(func=cmd_control_plane_scheduler_tick)

    control_plane_serve_parser = control_plane_subparsers.add_parser(
        "serve",
        help="Serve the local control-plane HTTP interface",
    )
    control_plane_serve_parser.add_argument("--workspace", default=".")
    control_plane_serve_parser.add_argument("--host", default="127.0.0.1")
    control_plane_serve_parser.add_argument("--port", type=int, default=8766)
    control_plane_serve_parser.set_defaults(func=cmd_control_plane_serve)

    collab_parser = subparsers.add_parser("collab", help="Manage file-native artifact collaboration threads")
    collab_subparsers = collab_parser.add_subparsers(dest="collab_command", required=True)

    collab_list_parser = collab_subparsers.add_parser("list", help="List persisted artifact review threads")
    collab_list_parser.add_argument("--workspace", default=".")
    collab_list_parser.set_defaults(func=cmd_collab_list)

    collab_request_parser = collab_subparsers.add_parser("request-review", help="Request review for an artifact path")
    collab_request_parser.add_argument("artifact_path")
    collab_request_parser.add_argument("--workspace", default=".")
    collab_request_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    collab_request_parser.add_argument("--assign", action="append", default=[])
    collab_request_parser.add_argument("--note", default="")
    collab_request_parser.set_defaults(func=cmd_collab_request_review)

    collab_comment_parser = collab_subparsers.add_parser("comment", help="Add a review comment to an artifact thread")
    collab_comment_parser.add_argument("artifact_path")
    collab_comment_parser.add_argument("--workspace", default=".")
    collab_comment_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    collab_comment_parser.add_argument("--message", required=True)
    collab_comment_parser.set_defaults(func=cmd_collab_comment)

    collab_approve_parser = collab_subparsers.add_parser("approve", help="Approve an artifact review thread")
    collab_approve_parser.add_argument("artifact_path")
    collab_approve_parser.add_argument("--workspace", default=".")
    collab_approve_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    collab_approve_parser.add_argument("--summary", default="")
    collab_approve_parser.set_defaults(func=cmd_collab_approve)

    collab_changes_parser = collab_subparsers.add_parser(
        "request-changes",
        help="Request changes on an artifact review thread",
    )
    collab_changes_parser.add_argument("artifact_path")
    collab_changes_parser.add_argument("--workspace", default=".")
    collab_changes_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    collab_changes_parser.add_argument("--summary", default="")
    collab_changes_parser.set_defaults(func=cmd_collab_request_changes)

    collab_resolve_parser = collab_subparsers.add_parser("resolve", help="Resolve an artifact collaboration thread")
    collab_resolve_parser.add_argument("artifact_path")
    collab_resolve_parser.add_argument("--workspace", default=".")
    collab_resolve_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    collab_resolve_parser.set_defaults(func=cmd_collab_resolve)

    jobs_parser = subparsers.add_parser("jobs", help="Manage persisted local job queues for remote-style execution")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command", required=True)

    jobs_list_parser = jobs_subparsers.add_parser("list", help="List queued and historical jobs")
    jobs_list_parser.add_argument("--workspace", default=".")
    jobs_list_parser.set_defaults(func=cmd_jobs_list)

    jobs_workers_parser = jobs_subparsers.add_parser("workers", help="List the derived worker registry for queue activity")
    jobs_workers_parser.add_argument("--workspace", default=".")
    jobs_workers_parser.set_defaults(func=cmd_jobs_workers)

    jobs_enqueue_parser = jobs_subparsers.add_parser("enqueue", help="Queue a persisted job manifest")
    jobs_enqueue_subparsers = jobs_enqueue_parser.add_subparsers(dest="jobs_enqueue_command", required=True)

    jobs_enqueue_research_parser = jobs_enqueue_subparsers.add_parser(
        "research",
        help="Queue a research run for later worker execution",
    )
    jobs_enqueue_research_parser.add_argument("--workspace", default=".")
    jobs_enqueue_research_parser.add_argument("--profile", default=None)
    jobs_enqueue_research_parser.add_argument("--limit", type=int, default=5)
    jobs_enqueue_research_parser.add_argument("--mode", default="wiki")
    jobs_enqueue_research_parser.add_argument("--slides", action="store_true")
    jobs_enqueue_research_parser.add_argument("--job-profile", default=DEFAULT_RESEARCH_JOB_PROFILE)
    jobs_enqueue_research_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_research_parser.add_argument("question")
    jobs_enqueue_research_parser.set_defaults(func=cmd_jobs_enqueue_research)

    jobs_enqueue_compile_parser = jobs_enqueue_subparsers.add_parser(
        "compile",
        help="Queue a compile planning run for later worker execution",
    )
    jobs_enqueue_compile_parser.add_argument("--workspace", default=".")
    jobs_enqueue_compile_parser.add_argument("--profile", default=None)
    jobs_enqueue_compile_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_compile_parser.set_defaults(func=cmd_jobs_enqueue_compile)

    jobs_enqueue_connector_sync_parser = jobs_enqueue_subparsers.add_parser(
        "connector-sync",
        help="Queue a connector sync job for later worker execution",
    )
    jobs_enqueue_connector_sync_parser.add_argument("connector_id")
    jobs_enqueue_connector_sync_parser.add_argument("--workspace", default=".")
    jobs_enqueue_connector_sync_parser.add_argument("--force", action="store_true")
    jobs_enqueue_connector_sync_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_connector_sync_parser.set_defaults(func=cmd_jobs_enqueue_connector_sync)

    jobs_enqueue_connector_sync_all_parser = jobs_enqueue_subparsers.add_parser(
        "connector-sync-all",
        help="Queue a full connector-registry sync for later worker execution",
    )
    jobs_enqueue_connector_sync_all_parser.add_argument("--workspace", default=".")
    jobs_enqueue_connector_sync_all_parser.add_argument("--force", action="store_true")
    jobs_enqueue_connector_sync_all_parser.add_argument("--limit", type=int, default=None)
    jobs_enqueue_connector_sync_all_parser.add_argument("--scheduled-only", action="store_true")
    jobs_enqueue_connector_sync_all_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_connector_sync_all_parser.set_defaults(func=cmd_jobs_enqueue_connector_sync_all)

    jobs_enqueue_sync_export_parser = jobs_enqueue_subparsers.add_parser(
        "sync-export",
        help="Queue a workspace sync export for later worker execution",
    )
    jobs_enqueue_sync_export_parser.add_argument("peer_ref", nargs="?", default=None)
    jobs_enqueue_sync_export_parser.add_argument("--workspace", default=".")
    jobs_enqueue_sync_export_parser.add_argument("--output-dir", default=None)
    jobs_enqueue_sync_export_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_sync_export_parser.set_defaults(func=cmd_jobs_enqueue_sync_export)

    jobs_enqueue_remote_sync_pull_parser = jobs_enqueue_subparsers.add_parser(
        "remote-sync-pull",
        help="Queue a pull/import cycle for an attached remote workspace",
    )
    jobs_enqueue_remote_sync_pull_parser.add_argument("remote_ref")
    jobs_enqueue_remote_sync_pull_parser.add_argument("--workspace", default=".")
    jobs_enqueue_remote_sync_pull_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_remote_sync_pull_parser.set_defaults(func=cmd_jobs_enqueue_remote_sync_pull)

    jobs_enqueue_ingest_url_parser = jobs_enqueue_subparsers.add_parser(
        "ingest-url",
        help="Queue a URL ingest job for later worker execution",
    )
    jobs_enqueue_ingest_url_parser.add_argument("url")
    jobs_enqueue_ingest_url_parser.add_argument("--workspace", default=".")
    jobs_enqueue_ingest_url_parser.add_argument("--name", default=None)
    jobs_enqueue_ingest_url_parser.add_argument("--force", action="store_true")
    jobs_enqueue_ingest_url_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_ingest_url_parser.set_defaults(func=cmd_jobs_enqueue_ingest_url)

    jobs_enqueue_ingest_repo_parser = jobs_enqueue_subparsers.add_parser(
        "ingest-repo",
        help="Queue a repository ingest job for later worker execution",
    )
    jobs_enqueue_ingest_repo_parser.add_argument("source")
    jobs_enqueue_ingest_repo_parser.add_argument("--workspace", default=".")
    jobs_enqueue_ingest_repo_parser.add_argument("--name", default=None)
    jobs_enqueue_ingest_repo_parser.add_argument("--force", action="store_true")
    jobs_enqueue_ingest_repo_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_ingest_repo_parser.set_defaults(func=cmd_jobs_enqueue_ingest_repo)

    jobs_enqueue_ingest_sitemap_parser = jobs_enqueue_subparsers.add_parser(
        "ingest-sitemap",
        help="Queue a sitemap ingest job for later worker execution",
    )
    jobs_enqueue_ingest_sitemap_parser.add_argument("source")
    jobs_enqueue_ingest_sitemap_parser.add_argument("--workspace", default=".")
    jobs_enqueue_ingest_sitemap_parser.add_argument("--force", action="store_true")
    jobs_enqueue_ingest_sitemap_parser.add_argument("--limit", type=int, default=None)
    jobs_enqueue_ingest_sitemap_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_ingest_sitemap_parser.set_defaults(func=cmd_jobs_enqueue_ingest_sitemap)

    jobs_enqueue_lint_parser = jobs_enqueue_subparsers.add_parser(
        "lint",
        help="Queue a lint pass for later worker execution",
    )
    jobs_enqueue_lint_parser.add_argument("--workspace", default=".")
    jobs_enqueue_lint_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_lint_parser.set_defaults(func=cmd_jobs_enqueue_lint)

    jobs_enqueue_maintain_parser = jobs_enqueue_subparsers.add_parser(
        "maintain",
        help="Queue a maintenance pass for later worker execution",
    )
    jobs_enqueue_maintain_parser.add_argument("--workspace", default=".")
    jobs_enqueue_maintain_parser.add_argument("--max-concepts", type=int, default=10)
    jobs_enqueue_maintain_parser.add_argument("--max-merges", type=int, default=10)
    jobs_enqueue_maintain_parser.add_argument("--max-backlinks", type=int, default=10)
    jobs_enqueue_maintain_parser.add_argument("--max-conflicts", type=int, default=10)
    jobs_enqueue_maintain_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_maintain_parser.set_defaults(func=cmd_jobs_enqueue_maintain)

    jobs_enqueue_improve_parser = jobs_enqueue_subparsers.add_parser(
        "improve-research",
        help="Queue a one-shot research improvement loop for later worker execution",
    )
    jobs_enqueue_improve_parser.add_argument("--workspace", default=".")
    jobs_enqueue_improve_parser.add_argument("--profile", required=True)
    jobs_enqueue_improve_parser.add_argument("--limit", type=int, default=5)
    jobs_enqueue_improve_parser.add_argument("--provider-format", action="append", default=[])
    jobs_enqueue_improve_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_enqueue_improve_parser.set_defaults(func=cmd_jobs_enqueue_improve_research)

    jobs_claim_next_parser = jobs_subparsers.add_parser(
        "claim-next",
        help="Claim the oldest available job for a specific worker under a lease",
    )
    jobs_claim_next_parser.add_argument("--workspace", default=".")
    jobs_claim_next_parser.add_argument("--worker-id", default="local-worker")
    jobs_claim_next_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_claim_next_parser.add_argument("--capability", action="append", default=[])
    jobs_claim_next_parser.set_defaults(func=cmd_jobs_claim_next)

    jobs_run_next_parser = jobs_subparsers.add_parser("run-next", help="Run the oldest queued job")
    jobs_run_next_parser.add_argument("--workspace", default=".")
    jobs_run_next_parser.add_argument("--worker-id", default="local-worker")
    jobs_run_next_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_run_next_parser.add_argument("--capability", action="append", default=[])
    jobs_run_next_parser.set_defaults(func=cmd_jobs_run_next)

    jobs_heartbeat_parser = jobs_subparsers.add_parser(
        "heartbeat",
        help="Renew the active lease for a worker's currently claimed or running job",
    )
    jobs_heartbeat_parser.add_argument("--workspace", default=".")
    jobs_heartbeat_parser.add_argument("--worker-id", default="local-worker")
    jobs_heartbeat_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_heartbeat_parser.add_argument("--capability", action="append", default=[])
    jobs_heartbeat_parser.set_defaults(func=cmd_jobs_heartbeat)

    jobs_retry_parser = jobs_subparsers.add_parser("retry", help="Re-queue a terminal job for another attempt")
    jobs_retry_parser.add_argument("job_id")
    jobs_retry_parser.add_argument("--workspace", default=".")
    jobs_retry_parser.add_argument("--profile", default=None)
    jobs_retry_parser.add_argument("--provider-format", action="append", default=[])
    jobs_retry_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    jobs_retry_parser.set_defaults(func=cmd_jobs_retry)

    jobs_work_parser = jobs_subparsers.add_parser(
        "work",
        help="Run queued jobs sequentially until the queue is empty or a limit is reached",
    )
    jobs_work_parser.add_argument("--workspace", default=".")
    jobs_work_parser.add_argument("--max-jobs", type=int, default=None)
    jobs_work_parser.add_argument("--stop-on-error", action="store_true")
    jobs_work_parser.add_argument("--worker-id", default="local-worker")
    jobs_work_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_work_parser.add_argument("--capability", action="append", default=[])
    jobs_work_parser.set_defaults(func=cmd_jobs_work)

    worker_parser = subparsers.add_parser(
        "worker",
        help="Run remote or local worker entrypoints against the persisted queue model",
    )
    worker_subparsers = worker_parser.add_subparsers(dest="worker_command", required=True)

    worker_remote_parser = worker_subparsers.add_parser(
        "remote",
        help="Poll a control-plane server and execute queued jobs remotely",
    )
    worker_remote_parser.add_argument("--server-url", required=True)
    worker_remote_parser.add_argument("--token", required=True)
    worker_remote_parser.add_argument("--worker-id", default="remote-worker")
    worker_remote_parser.add_argument("--max-jobs", type=int, default=None)
    worker_remote_parser.add_argument("--lease-seconds", type=int, default=300)
    worker_remote_parser.add_argument("--poll-interval-seconds", type=float, default=0.0)
    worker_remote_parser.add_argument("--max-idle-polls", type=int, default=0)
    worker_remote_parser.add_argument("--capability", action="append", default=[])
    worker_remote_parser.add_argument("--workspace", default=None)
    worker_remote_parser.set_defaults(func=cmd_worker_remote)

    sync_parser = subparsers.add_parser("sync", help="Export or import portable workspace sync bundles")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    sync_history_parser = sync_subparsers.add_parser("history", help="List recorded sync export and import events")
    sync_history_parser.add_argument("--workspace", default=".")
    sync_history_parser.set_defaults(func=cmd_sync_history)

    sync_export_parser = sync_subparsers.add_parser("export", help="Write a portable workspace sync bundle")
    sync_export_parser.add_argument("--workspace", default=".")
    sync_export_parser.add_argument("--output-dir", default=None)
    sync_export_parser.add_argument("--for-peer", default=None)
    sync_export_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    sync_export_parser.set_defaults(func=cmd_sync_export)

    sync_import_parser = sync_subparsers.add_parser("import", help="Import a portable workspace sync bundle")
    sync_import_parser.add_argument("bundle")
    sync_import_parser.add_argument("--workspace", default=".")
    sync_import_parser.add_argument("--from-peer", default=None)
    sync_import_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    sync_import_parser.set_defaults(func=cmd_sync_import)

    connector_parser = subparsers.add_parser("connector", help="Manage file-native source connector definitions")
    connector_subparsers = connector_parser.add_subparsers(dest="connector_command", required=True)

    connector_list_parser = connector_subparsers.add_parser("list", help="List registered connectors")
    connector_list_parser.add_argument("--workspace", default=".")
    connector_list_parser.set_defaults(func=cmd_connector_list)

    connector_add_parser = connector_subparsers.add_parser("add", help="Register a connector definition in the workspace")
    connector_add_parser.add_argument("kind", choices=["repo", "sitemap", "url", "urls"])
    connector_add_parser.add_argument("source")
    connector_add_parser.add_argument("--workspace", default=".")
    connector_add_parser.add_argument("--name", default=None)
    connector_add_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    connector_add_parser.set_defaults(func=cmd_connector_add)

    connector_sync_parser = connector_subparsers.add_parser("sync", help="Run a connector immediately")
    connector_sync_parser.add_argument("connector_id")
    connector_sync_parser.add_argument("--workspace", default=".")
    connector_sync_parser.add_argument("--force", action="store_true")
    connector_sync_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    connector_sync_parser.set_defaults(func=cmd_connector_sync)

    connector_sync_all_parser = connector_subparsers.add_parser(
        "sync-all",
        help="Run every registered connector immediately",
    )
    connector_sync_all_parser.add_argument("--workspace", default=".")
    connector_sync_all_parser.add_argument("--force", action="store_true")
    connector_sync_all_parser.add_argument("--limit", type=int, default=None)
    connector_sync_all_parser.add_argument("--scheduled-only", action="store_true")
    connector_sync_all_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    connector_sync_all_parser.set_defaults(func=cmd_connector_sync_all)

    connector_subscribe_parser = connector_subparsers.add_parser(
        "subscribe",
        help="Enable scheduled syncs for a connector in the local registry",
    )
    connector_subscribe_parser.add_argument("connector_id")
    connector_subscribe_parser.add_argument("--workspace", default=".")
    connector_subscribe_parser.add_argument("--every-hours", type=int, default=None)
    connector_subscribe_parser.add_argument(
        "--weekday",
        action="append",
        default=[],
        choices=["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
    )
    connector_subscribe_parser.add_argument("--hour", type=int, default=None)
    connector_subscribe_parser.add_argument("--minute", type=int, default=0)
    connector_subscribe_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    connector_subscribe_parser.set_defaults(func=cmd_connector_subscribe)

    connector_unsubscribe_parser = connector_subparsers.add_parser(
        "unsubscribe",
        help="Disable scheduled syncs for a connector",
    )
    connector_unsubscribe_parser.add_argument("connector_id")
    connector_unsubscribe_parser.add_argument("--workspace", default=".")
    connector_unsubscribe_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    connector_unsubscribe_parser.set_defaults(func=cmd_connector_unsubscribe)

    ingest_parser = subparsers.add_parser("ingest", help="Bring source material into raw/")
    ingest_subparsers = ingest_parser.add_subparsers(dest="ingest_command", required=True)

    ingest_file_parser = ingest_subparsers.add_parser("file", help="Copy a local file into raw/files")
    ingest_file_parser.add_argument("source")
    ingest_file_parser.add_argument("--workspace", default=".")
    ingest_file_parser.add_argument("--name", default=None)
    ingest_file_parser.add_argument("--force", action="store_true")
    ingest_file_parser.set_defaults(func=cmd_ingest_file)

    ingest_pdf_parser = ingest_subparsers.add_parser("pdf", help="Copy a local PDF into raw/pdfs")
    ingest_pdf_parser.add_argument("source")
    ingest_pdf_parser.add_argument("--workspace", default=".")
    ingest_pdf_parser.add_argument("--name", default=None)
    ingest_pdf_parser.add_argument("--force", action="store_true")
    ingest_pdf_parser.set_defaults(func=cmd_ingest_pdf)

    ingest_url_parser = ingest_subparsers.add_parser("url", help="Fetch a URL into raw/urls as Markdown")
    ingest_url_parser.add_argument("url")
    ingest_url_parser.add_argument("--workspace", default=".")
    ingest_url_parser.add_argument("--name", default=None)
    ingest_url_parser.add_argument("--force", action="store_true")
    ingest_url_parser.set_defaults(func=cmd_ingest_url)

    ingest_repo_parser = ingest_subparsers.add_parser("repo", help="Create a repository manifest in raw/repos")
    ingest_repo_parser.add_argument("source")
    ingest_repo_parser.add_argument("--workspace", default=".")
    ingest_repo_parser.add_argument("--name", default=None)
    ingest_repo_parser.add_argument("--force", action="store_true")
    ingest_repo_parser.set_defaults(func=cmd_ingest_repo)

    ingest_urls_parser = ingest_subparsers.add_parser("urls", help="Ingest a text or JSON list of URLs into raw/urls")
    ingest_urls_parser.add_argument("source")
    ingest_urls_parser.add_argument("--workspace", default=".")
    ingest_urls_parser.add_argument("--force", action="store_true")
    ingest_urls_parser.set_defaults(func=cmd_ingest_urls)

    ingest_sitemap_parser = ingest_subparsers.add_parser("sitemap", help="Ingest all URLs from a sitemap into raw/urls")
    ingest_sitemap_parser.add_argument("source")
    ingest_sitemap_parser.add_argument("--workspace", default=".")
    ingest_sitemap_parser.add_argument("--limit", type=int, default=None)
    ingest_sitemap_parser.add_argument("--force", action="store_true")
    ingest_sitemap_parser.set_defaults(func=cmd_ingest_sitemap)

    ingest_batch_parser = ingest_subparsers.add_parser("batch", help="Ingest a manifest of sources into raw/")
    ingest_batch_parser.add_argument("manifest")
    ingest_batch_parser.add_argument("--workspace", default=".")
    ingest_batch_parser.add_argument("--force", action="store_true")
    ingest_batch_parser.set_defaults(func=cmd_ingest_batch)

    plan_parser = subparsers.add_parser("plan", help="Build a compile plan from the current workspace")
    plan_parser.add_argument("--workspace", default=".")
    plan_parser.set_defaults(func=cmd_plan)

    compile_parser = subparsers.add_parser("compile", help="Run scan, plan, packet execution, and lint as one loop")
    compile_parser.add_argument("--workspace", default=".")
    compile_parser.add_argument("--profile", default=None)
    compile_parser.add_argument("--output-file", default=None)
    compile_parser.add_argument("--strict", action="store_true")
    compile_parser.set_defaults(func=cmd_compile)

    lint_parser = subparsers.add_parser("lint", help="Lint workspace integrity")
    lint_parser.add_argument("--workspace", default=".")
    lint_parser.add_argument("--strict", action="store_true")
    lint_parser.set_defaults(func=cmd_lint)

    review_parser = subparsers.add_parser("review", help="Render or apply the graph-backed review queue")
    review_parser.add_argument("--workspace", default=".")
    review_parser.add_argument("--limit", type=int, default=20)
    review_parser.set_defaults(func=cmd_review)
    review_subparsers = review_parser.add_subparsers(dest="review_command", required=False)

    review_accept_parser = review_subparsers.add_parser("accept-concept", help="Accept a concept candidate into wiki/concepts")
    review_accept_parser.add_argument("slug")
    review_accept_parser.add_argument("--workspace", default=".")
    review_accept_parser.set_defaults(func=cmd_review_accept_concept)

    review_merge_parser = review_subparsers.add_parser("resolve-merge", help="Resolve an entity merge candidate")
    review_merge_parser.add_argument("canonical_label")
    review_merge_parser.add_argument("--workspace", default=".")
    review_merge_parser.add_argument("--preferred-label", default=None)
    review_merge_parser.set_defaults(func=cmd_review_resolve_merge)

    review_backlink_parser = review_subparsers.add_parser("apply-backlink", help="Apply a backlink suggestion through a stable navigation page")
    review_backlink_parser.add_argument("target_path")
    review_backlink_parser.add_argument("--workspace", default=".")
    review_backlink_parser.set_defaults(func=cmd_review_apply_backlink)

    review_conflict_parser = review_subparsers.add_parser("file-conflict", help="File a deterministic note for a conflict review item")
    review_conflict_parser.add_argument("subject")
    review_conflict_parser.add_argument("--workspace", default=".")
    review_conflict_parser.set_defaults(func=cmd_review_file_conflict)

    review_dismiss_parser = review_subparsers.add_parser("dismiss", help="Dismiss a review item with a persisted reason")
    review_dismiss_parser.add_argument("review_id")
    review_dismiss_parser.add_argument("--reason", required=True)
    review_dismiss_parser.add_argument("--workspace", default=".")
    review_dismiss_parser.set_defaults(func=cmd_review_dismiss)

    review_reopen_parser = review_subparsers.add_parser("reopen", help="Reopen a previously dismissed review item")
    review_reopen_parser.add_argument("review_id")
    review_reopen_parser.add_argument("--workspace", default=".")
    review_reopen_parser.set_defaults(func=cmd_review_reopen)

    review_list_dismissed_parser = review_subparsers.add_parser("list-dismissed", help="List dismissed review items")
    review_list_dismissed_parser.add_argument("--workspace", default=".")
    review_list_dismissed_parser.set_defaults(func=cmd_review_list_dismissed)

    review_clear_dismissed_parser = review_subparsers.add_parser(
        "clear-dismissed", help="Remove a dismissal record so the review item can surface again"
    )
    review_clear_dismissed_parser.add_argument("review_id")
    review_clear_dismissed_parser.add_argument("--workspace", default=".")
    review_clear_dismissed_parser.set_defaults(func=cmd_review_clear_dismissed)

    review_export_parser = review_subparsers.add_parser(
        "export", help="Write a machine-readable review artifact for other tools and agents"
    )
    review_export_parser.add_argument("--workspace", default=".")
    review_export_parser.add_argument("--output-file", default=None)
    review_export_parser.set_defaults(func=cmd_review_export)

    maintain_parser = subparsers.add_parser("maintain", help="Apply graph-driven maintenance actions automatically")
    maintain_parser.add_argument("--workspace", default=".")
    maintain_parser.add_argument("--max-concepts", type=int, default=10)
    maintain_parser.add_argument("--max-merges", type=int, default=10)
    maintain_parser.add_argument("--max-backlinks", type=int, default=10)
    maintain_parser.add_argument("--max-conflicts", type=int, default=10)
    maintain_parser.add_argument("--min-concept-support", type=int, default=None)
    maintain_parser.add_argument("--deny-concept", action="append", default=[])
    maintain_parser.add_argument("--allow-short-concepts-without-entity", action="store_true")
    maintain_parser.set_defaults(func=cmd_maintain)

    query_parser = subparsers.add_parser("query", help="Search the workspace and render a research brief")
    query_parser.add_argument("--workspace", default=".")
    query_parser.add_argument("--slides", action="store_true")
    query_parser.add_argument("--limit", type=int, default=5)
    query_parser.add_argument("question")
    query_parser.set_defaults(func=cmd_query)

    research_parser = subparsers.add_parser("research", help="Run search, packet generation, and optional answer filing")
    research_parser.add_argument("--workspace", default=".")
    research_parser.add_argument("--slides", action="store_true")
    research_parser.add_argument(
        "--mode",
        default="wiki",
        choices=["brief", "memo", "report", "slides", "wiki"],
        help="Shape the filed answer artifact.",
    )
    research_parser.add_argument(
        "--job-profile",
        default=DEFAULT_RESEARCH_JOB_PROFILE,
        choices=sorted(RESEARCH_JOB_PROFILES),
        help="Choose the orchestration plan used to scaffold intermediate research notes.",
    )
    research_parser.add_argument("--limit", type=int, default=5)
    research_parser.add_argument("--profile", default=None)
    research_parser.add_argument("--resume", default=None, help="Resume a research run from a manifest path or `latest`.")
    research_parser.add_argument("--output-file", default=None)
    research_parser.add_argument("question", nargs="?")
    research_parser.set_defaults(func=cmd_research)

    run_parser = subparsers.add_parser("run-packet", help="Execute a prompt packet through a configured LLM profile")
    run_parser.add_argument("prompt_file")
    run_parser.add_argument("--workspace", default=".")
    run_parser.add_argument("--profile", default="default")
    run_parser.add_argument("--output-file", default=None)
    run_parser.set_defaults(func=cmd_run_packet)

    adapter_parser = subparsers.add_parser("adapter", help="Manage builtin adapter presets")
    adapter_subparsers = adapter_parser.add_subparsers(dest="adapter_command", required=True)

    adapter_list_parser = adapter_subparsers.add_parser("list", help="List builtin adapter presets")
    adapter_list_parser.set_defaults(func=cmd_adapter_list)

    adapter_install_parser = adapter_subparsers.add_parser("install", help="Install a builtin adapter preset")
    adapter_install_parser.add_argument("name")
    adapter_install_parser.add_argument("--workspace", default=".")
    adapter_install_parser.add_argument("--profile", default=None)
    adapter_install_parser.add_argument("--force", action="store_true")
    adapter_install_parser.set_defaults(func=cmd_adapter_install)

    export_parser = subparsers.add_parser("export", help="Export workspace artifacts into bridge-friendly bundles")
    export_subparsers = export_parser.add_subparsers(dest="export_command", required=True)

    export_jsonl_parser = export_subparsers.add_parser(
        "jsonl", help="Export research runs as a JSONL dataset artifact"
    )
    export_jsonl_parser.add_argument("--workspace", default=".")
    export_jsonl_parser.add_argument("--output-file", default=None)
    export_jsonl_parser.set_defaults(func=cmd_export_jsonl)

    export_presentations_parser = export_subparsers.add_parser(
        "presentations", help="Bundle generated slide decks and companion reports"
    )
    export_presentations_parser.add_argument("--workspace", default=".")
    export_presentations_parser.add_argument("--output-dir", default=None)
    export_presentations_parser.set_defaults(func=cmd_export_presentations)

    export_training_parser = export_subparsers.add_parser(
        "training-bundle", help="Export research runs as a training-ready dataset bundle"
    )
    export_training_parser.add_argument("--workspace", default=".")
    export_training_parser.add_argument("--output-dir", default=None)
    export_training_parser.set_defaults(func=cmd_export_training_bundle)

    export_finetune_parser = export_subparsers.add_parser(
        "finetune-bundle",
        help="Export supervised and retrieval datasets for downstream finetuning pipelines",
    )
    export_finetune_parser.add_argument("--workspace", default=".")
    export_finetune_parser.add_argument("--output-dir", default=None)
    export_finetune_parser.add_argument(
        "--provider-format",
        action="append",
        default=[],
        help="Optionally emit provider-specific supervised exports such as openai-chat",
    )
    export_finetune_parser.set_defaults(func=cmd_export_finetune_bundle)

    export_feedback_parser = export_subparsers.add_parser(
        "feedback-bundle",
        help="Export remediation-ready records for low-quality research runs",
    )
    export_feedback_parser.add_argument("--workspace", default=".")
    export_feedback_parser.add_argument("--output-dir", default=None)
    export_feedback_parser.set_defaults(func=cmd_export_feedback_bundle)

    export_correction_parser = export_subparsers.add_parser(
        "correction-bundle",
        help="Export validated remediation jobs as correction-training records",
    )
    export_correction_parser.add_argument("--workspace", default=".")
    export_correction_parser.add_argument("--output-dir", default=None)
    export_correction_parser.set_defaults(func=cmd_export_correction_bundle)

    export_training_loop_parser = export_subparsers.add_parser(
        "training-loop-bundle",
        help="Package evaluation, remediation, correction, and finetune artifacts into one training-loop bundle",
    )
    export_training_loop_parser.add_argument("--workspace", default=".")
    export_training_loop_parser.add_argument("--output-dir", default=None)
    export_training_loop_parser.add_argument(
        "--provider-format",
        action="append",
        default=[],
        help="Optionally emit provider-specific supervised exports such as openai-chat inside the finetune subtree",
    )
    export_training_loop_parser.set_defaults(func=cmd_export_training_loop_bundle)

    remediate_parser = subparsers.add_parser("remediate", help="Replay weak research runs through remediation prompts")
    remediate_subparsers = remediate_parser.add_subparsers(dest="remediate_command", required=True)

    remediate_research_parser = remediate_subparsers.add_parser(
        "research",
        help="Remediate low-quality research runs with a configured adapter profile",
    )
    remediate_research_parser.add_argument("--workspace", default=".")
    remediate_research_parser.add_argument("--profile", required=True)
    remediate_research_parser.add_argument("--limit", type=int, default=5)
    remediate_research_parser.set_defaults(func=cmd_remediate_research)

    improve_parser = subparsers.add_parser(
        "improve",
        help="Run higher-level improvement loops over persisted Cognisync artifacts",
    )
    improve_subparsers = improve_parser.add_subparsers(dest="improve_command", required=True)

    improve_research_parser = improve_subparsers.add_parser(
        "research",
        help="Remediate weak research runs and package a refreshed training-loop bundle",
    )
    improve_research_parser.add_argument("--workspace", default=".")
    improve_research_parser.add_argument("--profile", required=True)
    improve_research_parser.add_argument("--limit", type=int, default=5)
    improve_research_parser.add_argument("--output-dir", default=None)
    improve_research_parser.add_argument(
        "--provider-format",
        action="append",
        default=[],
        help="Optionally emit provider-specific supervised exports such as openai-chat inside the bundle",
    )
    improve_research_parser.set_defaults(func=cmd_improve_research)

    eval_parser = subparsers.add_parser("eval", help="Evaluate persisted Cognisync artifacts")
    eval_subparsers = eval_parser.add_subparsers(dest="eval_command", required=True)

    eval_research_parser = eval_subparsers.add_parser(
        "research", help="Score persisted research runs and write an evaluation report"
    )
    eval_research_parser.add_argument("--workspace", default=".")
    eval_research_parser.add_argument("--output-file", default=None)
    eval_research_parser.add_argument("--payload-file", default=None)
    eval_research_parser.set_defaults(func=cmd_eval_research)

    synth_parser = subparsers.add_parser("synth", help="Generate synthetic datasets from persisted corpus structure")
    synth_subparsers = synth_parser.add_subparsers(dest="synth_command", required=True)

    synth_qa_parser = synth_subparsers.add_parser(
        "qa", help="Generate assertion-grounded synthetic QA examples from the graph"
    )
    synth_qa_parser.add_argument("--workspace", default=".")
    synth_qa_parser.add_argument("--output-dir", default=None)
    synth_qa_parser.set_defaults(func=cmd_synth_qa)

    synth_contrastive_parser = synth_subparsers.add_parser(
        "contrastive", help="Generate contrastive retrieval pairs from assertion support paths"
    )
    synth_contrastive_parser.add_argument("--workspace", default=".")
    synth_contrastive_parser.add_argument("--output-dir", default=None)
    synth_contrastive_parser.set_defaults(func=cmd_synth_contrastive)

    synth_graph_completion_parser = synth_subparsers.add_parser(
        "graph-completion", help="Generate missing-edge completion records from assertion graph edges"
    )
    synth_graph_completion_parser.add_argument("--workspace", default=".")
    synth_graph_completion_parser.add_argument("--output-dir", default=None)
    synth_graph_completion_parser.set_defaults(func=cmd_synth_graph_completion)

    synth_report_writing_parser = synth_subparsers.add_parser(
        "report-writing", help="Generate report-writing examples from persisted research runs"
    )
    synth_report_writing_parser.add_argument("--workspace", default=".")
    synth_report_writing_parser.add_argument("--output-dir", default=None)
    synth_report_writing_parser.set_defaults(func=cmd_synth_report_writing)

    ui_parser = subparsers.add_parser("ui", help="Generate or serve lightweight Cognisync web interfaces")
    ui_subparsers = ui_parser.add_subparsers(dest="ui_command", required=True)

    ui_review_parser = ui_subparsers.add_parser("review", help="Build or serve the review dashboard")
    ui_review_parser.add_argument("--workspace", default=".")
    ui_review_parser.add_argument("--output-file", default=None)
    ui_review_parser.add_argument("--serve", action="store_true")
    ui_review_parser.add_argument("--host", default="127.0.0.1")
    ui_review_parser.add_argument("--port", type=int, default=8765)
    ui_review_parser.add_argument("--actor-id", default=DEFAULT_LOCAL_OPERATOR_ID)
    ui_review_parser.set_defaults(func=cmd_ui_review)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
