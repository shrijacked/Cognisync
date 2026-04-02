from __future__ import annotations

import argparse
from pathlib import Path
import sys

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.linter import lint_snapshot
from cognisync.planner import build_compile_plan, render_compile_plan
from cognisync.renderers import render_compile_packet, render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.workspace import Workspace


def _workspace_from_arg(path_arg: str) -> Workspace:
    return Workspace(Path(path_arg))


def _ensure_snapshot(workspace: Workspace):
    if workspace.index_path.exists():
        return workspace.read_index()
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
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
    print(f"Scanned {len(snapshot.artifacts)} artifacts into {workspace.index_path}")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    plan = build_compile_plan(snapshot)
    workspace.write_plan_json("compile-plan", plan)
    plan_path = workspace.plans_dir / "compile-plan.md"
    plan_path.write_text(render_compile_plan(plan), encoding="utf-8")
    packet_path = render_compile_packet(workspace, plan)
    print(f"Wrote plan to {plan_path}")
    print(f"Wrote prompt packet to {packet_path}")
    return 0


def cmd_lint(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    issues = lint_snapshot(snapshot)
    for issue in issues:
        print(f"[{issue.severity}] {issue.kind} {issue.path}: {issue.message}")
    if issues:
        return 1 if args.strict else 0
    print("No lint issues found.")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    workspace = _workspace_from_arg(args.workspace)
    snapshot = _ensure_snapshot(workspace)
    engine = SearchEngine.from_workspace(workspace, snapshot)
    hits = engine.search(args.question, limit=args.limit)
    report_path = render_query_report(workspace, args.question, hits)
    packet_path = render_query_packet(workspace, args.question, hits)
    print(f"Wrote report to {report_path}")
    print(f"Wrote prompt packet to {packet_path}")
    if args.slides:
        slide_path = render_marp_slides(workspace, args.question, hits)
        print(f"Wrote slide deck to {slide_path}")
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
    result = adapter.run(prompt_file=prompt_file, workspace_root=workspace.root, output_file=output_file)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    return result.returncode


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

    plan_parser = subparsers.add_parser("plan", help="Build a compile plan from the current workspace")
    plan_parser.add_argument("--workspace", default=".")
    plan_parser.set_defaults(func=cmd_plan)

    lint_parser = subparsers.add_parser("lint", help="Lint workspace integrity")
    lint_parser.add_argument("--workspace", default=".")
    lint_parser.add_argument("--strict", action="store_true")
    lint_parser.set_defaults(func=cmd_lint)

    query_parser = subparsers.add_parser("query", help="Search the workspace and render a research brief")
    query_parser.add_argument("--workspace", default=".")
    query_parser.add_argument("--slides", action="store_true")
    query_parser.add_argument("--limit", type=int, default=5)
    query_parser.add_argument("question")
    query_parser.set_defaults(func=cmd_query)

    run_parser = subparsers.add_parser("run-packet", help="Execute a prompt packet through a configured LLM profile")
    run_parser.add_argument("prompt_file")
    run_parser.add_argument("--workspace", default=".")
    run_parser.add_argument("--profile", default="default")
    run_parser.add_argument("--output-file", default=None)
    run_parser.set_defaults(func=cmd_run_packet)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
