from __future__ import annotations

import argparse
from pathlib import Path
import sys

from cognisync.adapters import (
    AdapterError,
    adapter_from_config,
    builtin_adapter_presets,
    install_builtin_adapter,
)
from cognisync.compile_flow import CompileError, run_compile_cycle
from cognisync.config import save_config
from cognisync.demo import DemoError, create_demo_workspace
from cognisync.doctor import doctor_exit_code, render_doctor_report, run_doctor
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
from cognisync.linter import lint_snapshot
from cognisync.maintenance import (
    MaintenanceError,
    accept_concept_candidate,
    apply_backlink_suggestion,
    file_conflict_review,
    resolve_entity_merge,
    run_maintenance_cycle,
)
from cognisync.manifests import write_workspace_manifests
from cognisync.planner import build_compile_plan, render_compile_plan
from cognisync.research import ResearchError, run_research_cycle
from cognisync.review_queue import build_review_queue, render_review_queue
from cognisync.renderers import render_compile_packet, render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
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


def cmd_init(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.path)
    workspace.initialize(name=args.name, force=args.force)
    print(f"Initialized Cognisync workspace at {workspace.root}")
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Scanned {len(snapshot.artifacts)} artifacts into {workspace.index_path}")
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


def cmd_maintain(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = run_maintenance_cycle(
            workspace,
            max_concepts=args.max_concepts,
            max_merges=args.max_merges,
            max_backlinks=args.max_backlinks,
            max_conflicts=args.max_conflicts,
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
    print(f"Wrote maintenance run manifest to {result.run_manifest_path}")
    return 0


def cmd_ingest_file(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
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
    print(f"Ingested file into {result.path}")
    return 0


def cmd_ingest_pdf(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = ingest_pdf(workspace, source=Path(args.source), name=args.name, force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Ingested pdf into {result.path}")
    return 0


def cmd_ingest_url(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = ingest_url(workspace, url=args.url, name=args.name, force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Ingested url into {result.path}")
    return 0


def cmd_ingest_repo(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        result = ingest_repo(workspace, repo_path=args.source, name=args.name, force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Ingested repo manifest into {result.path}")
    return 0


def cmd_ingest_urls(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        results = ingest_urls(workspace, source_list=Path(args.source), force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Ingested {len(results)} URL source(s).")
    for result in results:
        print(f"- {result.path}")
    return 0


def cmd_ingest_sitemap(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        results = ingest_sitemap(workspace, source=args.source, force=args.force, limit=args.limit)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Ingested {len(results)} URL source(s).")
    for result in results:
        print(f"- {result.path}")
    return 0


def cmd_ingest_batch(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    try:
        results = ingest_batch(workspace, manifest_path=Path(args.manifest), force=args.force)
    except IngestError as error:
        print(str(error), file=sys.stderr)
        return 2
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    print(f"Batch ingested {len(results)} source(s).")
    for result in results:
        print(f"- {result.kind}: {result.path}")
    return 0


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
        )
    except ResearchError as error:
        print(str(error), file=sys.stderr)
        return 2

    if result.resumed:
        print(f"Resumed research run from {result.run_manifest_path}")
    print(f"Wrote research plan to {result.plan_path}")
    print(f"Wrote report to {result.report_path}")
    print(f"Wrote prompt packet to {result.packet_path}")
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

    maintain_parser = subparsers.add_parser("maintain", help="Apply graph-driven maintenance actions automatically")
    maintain_parser.add_argument("--workspace", default=".")
    maintain_parser.add_argument("--max-concepts", type=int, default=10)
    maintain_parser.add_argument("--max-merges", type=int, default=10)
    maintain_parser.add_argument("--max-backlinks", type=int, default=10)
    maintain_parser.add_argument("--max-conflicts", type=int, default=10)
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

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
