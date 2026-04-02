from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.linter import lint_snapshot
from cognisync.manifests import write_run_manifest, write_workspace_manifests
from cognisync.planner import build_compile_plan, render_compile_plan
from cognisync.renderers import render_compile_packet
from cognisync.workspace import Workspace
from cognisync.scanner import scan_workspace


class CompileError(RuntimeError):
    pass


@dataclass(frozen=True)
class CompileRunResult:
    plan_path: Path
    packet_path: Path
    output_file: Optional[Path]
    run_manifest_path: Path
    issue_count: int
    task_count: int
    ran_profile: bool


def run_compile_cycle(
    workspace: Workspace,
    profile_name: Optional[str] = None,
    output_file: Optional[Path] = None,
) -> CompileRunResult:
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)

    plan = build_compile_plan(snapshot)
    workspace.write_plan_json("compile-plan", plan)
    plan_path = workspace.plans_dir / "compile-plan.md"
    plan_path.write_text(render_compile_plan(plan), encoding="utf-8")
    packet_path = render_compile_packet(workspace, plan, snapshot=snapshot)

    ran_profile = False
    resolved_output = output_file
    if profile_name:
        config = workspace.load_config()
        try:
            adapter = adapter_from_config(config, profile_name)
        except AdapterError as error:
            raise CompileError(str(error)) from error

        if resolved_output is None:
            resolved_output = workspace.outputs_dir / "reports" / "compile-session.md"
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        result = adapter.run(prompt_file=packet_path, workspace_root=workspace.root, output_file=resolved_output)
        if result.returncode != 0:
            raise CompileError(f"Adapter '{profile_name}' exited with code {result.returncode}.")
        if resolved_output and not adapter.output_file_flag and result.stdout:
            resolved_output.write_text(result.stdout, encoding="utf-8")
        ran_profile = True

    final_snapshot = scan_workspace(workspace)
    workspace.write_index(final_snapshot)
    write_workspace_manifests(workspace, final_snapshot)
    issues = lint_snapshot(final_snapshot)
    run_manifest_path = write_run_manifest(
        workspace,
        "compile",
        {
            "run_label": "compile-plan",
            "profile": profile_name,
            "plan_path": workspace.relative_path(plan_path),
            "packet_path": workspace.relative_path(packet_path),
            "output_file": workspace.relative_path(resolved_output) if resolved_output else None,
            "issue_count": len(issues),
            "task_count": len(plan.tasks),
            "status": "completed" if not issues else "completed_with_issues",
        },
    )
    return CompileRunResult(
        plan_path=plan_path,
        packet_path=packet_path,
        output_file=resolved_output,
        run_manifest_path=run_manifest_path,
        issue_count=len(issues),
        task_count=len(plan.tasks),
        ran_profile=ran_profile,
    )
