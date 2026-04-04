from __future__ import annotations

import argparse
from pathlib import Path
import sys

from cognisync.access import (
    AccessError,
    DEFAULT_LOCAL_OPERATOR_ID,
    VALID_ACCESS_ROLES,
    grant_access_member,
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
from cognisync.compile_flow import CompileError, run_compile_cycle
from cognisync.config import MaintenancePolicy, save_config
from cognisync.connectors import (
    ConnectorError,
    add_connector,
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
from cognisync.jobs import (
    JobError,
    claim_next_job,
    enqueue_compile_job,
    enqueue_connector_sync_job,
    enqueue_connector_sync_all_job,
    enqueue_improve_research_job,
    enqueue_lint_job,
    enqueue_maintain_job,
    enqueue_research_job,
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
from cognisync.review_exports import write_review_export
from cognisync.review_queue import build_review_queue, render_review_queue
from cognisync.review_ui import create_review_ui_server, write_review_ui_bundle
from cognisync.renderers import render_compile_packet, render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.synthetic_data import export_synthetic_contrastive_bundle, export_synthetic_qa_bundle
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    return snapshot


def _refresh_workspace_with_change_summary(workspace: Workspace, trigger: str, fallback_to_live_scan: bool = False):
    previous_state = capture_change_state(workspace, fallback_to_live_scan=fallback_to_live_scan)
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    return snapshot, write_change_summary(workspace, trigger, previous_state, snapshot)


def cmd_init(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.path)
    workspace.initialize(name=args.name, force=args.force)
    print(f"Initialized Cognisync workspace at {workspace.root}")
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot, change_summary = _refresh_workspace_with_change_summary(workspace, "scan")
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
    manifest_path = enqueue_research_job(
        workspace,
        question=args.question,
        profile_name=args.profile,
        limit=args.limit,
        mode=args.mode,
        slides=args.slides,
        job_profile=args.job_profile,
    )
    print(f"Queued research job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_compile(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    manifest_path = enqueue_compile_job(workspace, profile_name=args.profile)
    print(f"Queued compile job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_connector_sync(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    manifest_path = enqueue_connector_sync_job(
        workspace,
        connector_id=args.connector_id,
        force=args.force,
    )
    print(f"Queued connector-sync job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_connector_sync_all(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    manifest_path = enqueue_connector_sync_all_job(
        workspace,
        force=args.force,
        limit=args.limit,
        scheduled_only=args.scheduled_only,
    )
    print(f"Queued connector-sync-all job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_lint(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    manifest_path = enqueue_lint_job(workspace)
    print(f"Queued lint job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_maintain(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    manifest_path = enqueue_maintain_job(
        workspace,
        max_concepts=args.max_concepts,
        max_merges=args.max_merges,
        max_backlinks=args.max_backlinks,
        max_conflicts=args.max_conflicts,
    )
    print(f"Queued maintain job at {manifest_path}")
    print(f"Queue summary: {workspace.job_queue_manifest_path}")
    return 0


def cmd_jobs_enqueue_improve_research(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    manifest_path = enqueue_improve_research_job(
        workspace,
        profile_name=args.profile,
        limit=args.limit,
        provider_formats=list(args.provider_format or []),
    )
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
        )
    except JobError as error:
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
        manifest_path = retry_job(
            workspace,
            job_id=args.job_id,
            profile_name=args.profile,
            provider_formats=list(args.provider_format or []),
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
    result = export_sync_bundle(workspace, output_dir=output_dir)
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
        result = import_sync_bundle(workspace, bundle_dir=bundle_dir)
    except SyncError as error:
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
        member = revoke_access_member(workspace, principal_id=args.principal_id)
    except AccessError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Revoked access for {member['principal_id']}")
    print(f"Access manifest: {workspace.access_manifest_path}")
    return 0


def cmd_connector_list(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    print(render_connector_list(workspace))
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_connector_add(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        connector = add_connector(
            workspace,
            kind=args.kind,
            source=args.source,
            name=args.name,
        )
    except ConnectorError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(f"Registered connector {connector['connector_id']} ({connector['kind']})")
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_connector_sync(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = sync_connector(
            workspace,
            connector_id=args.connector_id,
            force=args.force,
        )
    except ConnectorError as error:
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
        result = sync_all_connectors(
            workspace,
            force=args.force,
            limit=args.limit,
            scheduled_only=args.scheduled_only,
        )
    except ConnectorError as error:
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
        connector = subscribe_connector(
            workspace,
            connector_id=args.connector_id,
            every_hours=args.every_hours,
        )
    except ConnectorError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(
        f"Subscribed connector {connector['connector_id']} "
        f"for every {connector['subscription']['interval_hours']} hour(s)."
    )
    print(f"Connector registry: {workspace.connector_registry_path}")
    return 0


def cmd_connector_unsubscribe(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        connector = unsubscribe_connector(workspace, connector_id=args.connector_id)
    except ConnectorError as error:
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "ingest", previous_state, snapshot)
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
    access_grant_parser.set_defaults(func=cmd_access_grant)

    access_revoke_parser = access_subparsers.add_parser("revoke", help="Remove a workspace access member")
    access_revoke_parser.add_argument("principal_id")
    access_revoke_parser.add_argument("--workspace", default=".")
    access_revoke_parser.set_defaults(func=cmd_access_revoke)

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
    jobs_enqueue_research_parser.add_argument("question")
    jobs_enqueue_research_parser.set_defaults(func=cmd_jobs_enqueue_research)

    jobs_enqueue_compile_parser = jobs_enqueue_subparsers.add_parser(
        "compile",
        help="Queue a compile planning run for later worker execution",
    )
    jobs_enqueue_compile_parser.add_argument("--workspace", default=".")
    jobs_enqueue_compile_parser.add_argument("--profile", default=None)
    jobs_enqueue_compile_parser.set_defaults(func=cmd_jobs_enqueue_compile)

    jobs_enqueue_connector_sync_parser = jobs_enqueue_subparsers.add_parser(
        "connector-sync",
        help="Queue a connector sync job for later worker execution",
    )
    jobs_enqueue_connector_sync_parser.add_argument("connector_id")
    jobs_enqueue_connector_sync_parser.add_argument("--workspace", default=".")
    jobs_enqueue_connector_sync_parser.add_argument("--force", action="store_true")
    jobs_enqueue_connector_sync_parser.set_defaults(func=cmd_jobs_enqueue_connector_sync)

    jobs_enqueue_connector_sync_all_parser = jobs_enqueue_subparsers.add_parser(
        "connector-sync-all",
        help="Queue a full connector-registry sync for later worker execution",
    )
    jobs_enqueue_connector_sync_all_parser.add_argument("--workspace", default=".")
    jobs_enqueue_connector_sync_all_parser.add_argument("--force", action="store_true")
    jobs_enqueue_connector_sync_all_parser.add_argument("--limit", type=int, default=None)
    jobs_enqueue_connector_sync_all_parser.add_argument("--scheduled-only", action="store_true")
    jobs_enqueue_connector_sync_all_parser.set_defaults(func=cmd_jobs_enqueue_connector_sync_all)

    jobs_enqueue_lint_parser = jobs_enqueue_subparsers.add_parser(
        "lint",
        help="Queue a lint pass for later worker execution",
    )
    jobs_enqueue_lint_parser.add_argument("--workspace", default=".")
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
    jobs_enqueue_maintain_parser.set_defaults(func=cmd_jobs_enqueue_maintain)

    jobs_enqueue_improve_parser = jobs_enqueue_subparsers.add_parser(
        "improve-research",
        help="Queue a one-shot research improvement loop for later worker execution",
    )
    jobs_enqueue_improve_parser.add_argument("--workspace", default=".")
    jobs_enqueue_improve_parser.add_argument("--profile", required=True)
    jobs_enqueue_improve_parser.add_argument("--limit", type=int, default=5)
    jobs_enqueue_improve_parser.add_argument("--provider-format", action="append", default=[])
    jobs_enqueue_improve_parser.set_defaults(func=cmd_jobs_enqueue_improve_research)

    jobs_claim_next_parser = jobs_subparsers.add_parser(
        "claim-next",
        help="Claim the oldest available job for a specific worker under a lease",
    )
    jobs_claim_next_parser.add_argument("--workspace", default=".")
    jobs_claim_next_parser.add_argument("--worker-id", default="local-worker")
    jobs_claim_next_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_claim_next_parser.set_defaults(func=cmd_jobs_claim_next)

    jobs_run_next_parser = jobs_subparsers.add_parser("run-next", help="Run the oldest queued job")
    jobs_run_next_parser.add_argument("--workspace", default=".")
    jobs_run_next_parser.add_argument("--worker-id", default="local-worker")
    jobs_run_next_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_run_next_parser.set_defaults(func=cmd_jobs_run_next)

    jobs_heartbeat_parser = jobs_subparsers.add_parser(
        "heartbeat",
        help="Renew the active lease for a worker's currently claimed or running job",
    )
    jobs_heartbeat_parser.add_argument("--workspace", default=".")
    jobs_heartbeat_parser.add_argument("--worker-id", default="local-worker")
    jobs_heartbeat_parser.add_argument("--lease-seconds", type=int, default=300)
    jobs_heartbeat_parser.set_defaults(func=cmd_jobs_heartbeat)

    jobs_retry_parser = jobs_subparsers.add_parser("retry", help="Re-queue a terminal job for another attempt")
    jobs_retry_parser.add_argument("job_id")
    jobs_retry_parser.add_argument("--workspace", default=".")
    jobs_retry_parser.add_argument("--profile", default=None)
    jobs_retry_parser.add_argument("--provider-format", action="append", default=[])
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
    jobs_work_parser.set_defaults(func=cmd_jobs_work)

    sync_parser = subparsers.add_parser("sync", help="Export or import portable workspace sync bundles")
    sync_subparsers = sync_parser.add_subparsers(dest="sync_command", required=True)

    sync_history_parser = sync_subparsers.add_parser("history", help="List recorded sync export and import events")
    sync_history_parser.add_argument("--workspace", default=".")
    sync_history_parser.set_defaults(func=cmd_sync_history)

    sync_export_parser = sync_subparsers.add_parser("export", help="Write a portable workspace sync bundle")
    sync_export_parser.add_argument("--workspace", default=".")
    sync_export_parser.add_argument("--output-dir", default=None)
    sync_export_parser.set_defaults(func=cmd_sync_export)

    sync_import_parser = sync_subparsers.add_parser("import", help="Import a portable workspace sync bundle")
    sync_import_parser.add_argument("bundle")
    sync_import_parser.add_argument("--workspace", default=".")
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
    connector_add_parser.set_defaults(func=cmd_connector_add)

    connector_sync_parser = connector_subparsers.add_parser("sync", help="Run a connector immediately")
    connector_sync_parser.add_argument("connector_id")
    connector_sync_parser.add_argument("--workspace", default=".")
    connector_sync_parser.add_argument("--force", action="store_true")
    connector_sync_parser.set_defaults(func=cmd_connector_sync)

    connector_sync_all_parser = connector_subparsers.add_parser(
        "sync-all",
        help="Run every registered connector immediately",
    )
    connector_sync_all_parser.add_argument("--workspace", default=".")
    connector_sync_all_parser.add_argument("--force", action="store_true")
    connector_sync_all_parser.add_argument("--limit", type=int, default=None)
    connector_sync_all_parser.add_argument("--scheduled-only", action="store_true")
    connector_sync_all_parser.set_defaults(func=cmd_connector_sync_all)

    connector_subscribe_parser = connector_subparsers.add_parser(
        "subscribe",
        help="Enable scheduled syncs for a connector in the local registry",
    )
    connector_subscribe_parser.add_argument("connector_id")
    connector_subscribe_parser.add_argument("--workspace", default=".")
    connector_subscribe_parser.add_argument("--every-hours", type=int, required=True)
    connector_subscribe_parser.set_defaults(func=cmd_connector_subscribe)

    connector_unsubscribe_parser = connector_subparsers.add_parser(
        "unsubscribe",
        help="Disable scheduled syncs for a connector",
    )
    connector_unsubscribe_parser.add_argument("connector_id")
    connector_unsubscribe_parser.add_argument("--workspace", default=".")
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
