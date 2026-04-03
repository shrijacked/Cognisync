from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, List, Optional, Sequence

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.manifests import read_json_manifest, write_run_manifest, write_workspace_manifests
from cognisync.renderers import render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.types import ResearchPlan, ResearchPlanStep, SearchHit
from cognisync.utils import slugify, utc_timestamp
from cognisync.workspace import Workspace


class ResearchError(RuntimeError):
    pass


CITATION_RE = re.compile(r"\[(S\d+)\]")
RESEARCH_OUTPUT_MODES = {"brief", "memo", "report", "slides", "wiki"}


@dataclass(frozen=True)
class ResearchRunResult:
    plan_path: Path
    report_path: Path
    packet_path: Path
    answer_path: Optional[Path]
    slide_path: Optional[Path]
    run_manifest_path: Path
    hit_count: int
    ran_profile: bool
    resumed: bool


def run_research_cycle(
    workspace: Workspace,
    question: Optional[str] = None,
    limit: int = 5,
    profile_name: Optional[str] = None,
    output_file: Optional[Path] = None,
    slides: bool = False,
    mode: str = "wiki",
    resume: Optional[str] = None,
) -> ResearchRunResult:
    if mode not in RESEARCH_OUTPUT_MODES:
        raise ResearchError(
            f"Unsupported research mode '{mode}'. Expected one of: {', '.join(sorted(RESEARCH_OUTPUT_MODES))}."
        )
    if resume:
        return _resume_research_cycle(
            workspace=workspace,
            resume=resume,
            profile_name=profile_name,
            output_file=output_file,
        )
    if not question:
        raise ResearchError("A question is required unless you resume an existing research run.")

    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)

    engine = SearchEngine.from_workspace(workspace, snapshot)
    hits = engine.search(question, limit=limit)

    report_path = render_query_report(workspace, question, hits, snapshot=snapshot)
    packet_path = render_query_packet(workspace, question, hits, snapshot=snapshot, mode=mode)
    slide_path = render_marp_slides(workspace, question, hits) if slides or mode == "slides" else None
    answer_path = output_file or _default_answer_path(workspace, question, mode)
    sources = [_hit_to_manifest_entry(hit, index) for index, hit in enumerate(hits, start=1)]

    execution_status = "pending"
    validation_status = "pending"
    filing_status = "pending"
    if profile_name:
        execution_status = "completed"
        validation_status = "completed"
        filing_status = "completed"

    plan = _build_research_plan(
        question=question,
        mode=mode,
        sources=sources,
        report_path=workspace.relative_path(report_path),
        packet_path=workspace.relative_path(packet_path),
        answer_path=workspace.relative_path(answer_path),
        slide_path=workspace.relative_path(slide_path) if slide_path else None,
        execution_status=execution_status,
        validation_status=validation_status,
        filing_status=filing_status,
    )
    plan_path, plan_json_path = _write_research_plan(workspace, question, plan)

    base_payload = {
        "run_label": question,
        "question": question,
        "mode": mode,
        "profile": profile_name,
        "plan_path": workspace.relative_path(plan_path),
        "plan_json_path": workspace.relative_path(plan_json_path),
        "report_path": workspace.relative_path(report_path),
        "packet_path": workspace.relative_path(packet_path),
        "answer_path": workspace.relative_path(answer_path),
        "slide_path": workspace.relative_path(slide_path) if slide_path else None,
        "status": "planned" if not profile_name else "running",
        "resume_supported": True,
        "attempt_count": 0,
        "resume_count": 0,
        "sources": sources,
        "citations": {
            "available": _available_citations(sources),
            "used": [],
        },
        "validation": {
            "passed": False,
            "errors": [],
            "status": "pending",
        },
    }
    run_manifest_path = write_run_manifest(workspace, "research", base_payload)

    if not profile_name:
        return ResearchRunResult(
            plan_path=plan_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=None,
            slide_path=slide_path,
            run_manifest_path=run_manifest_path,
            hit_count=len(hits),
            ran_profile=False,
            resumed=False,
        )

    return _execute_research_run(
        workspace=workspace,
        question=question,
        mode=mode,
        profile_name=profile_name,
        output_file=answer_path,
        report_path=report_path,
        packet_path=packet_path,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        slide_path=slide_path,
        run_manifest_path=run_manifest_path,
        run_id=run_manifest_path.stem,
        sources=sources,
        resume_count=0,
        attempt_count=1,
        resumed=False,
    )


def _resume_research_cycle(
    workspace: Workspace,
    resume: str,
    profile_name: Optional[str],
    output_file: Optional[Path],
) -> ResearchRunResult:
    run_manifest_path = _resolve_research_manifest_path(workspace, resume)
    manifest = read_json_manifest(run_manifest_path)
    if manifest.get("run_kind") != "research":
        raise ResearchError(f"Run manifest is not a research run: {run_manifest_path}")

    question = str(manifest.get("question", "")).strip()
    mode = str(manifest.get("mode", "wiki")).strip() or "wiki"
    effective_profile = profile_name or _optional_text(manifest.get("profile"))
    if not effective_profile:
        raise ResearchError("Resuming a research run requires a profile, either from the manifest or --profile.")

    report_path = _workspace_path(workspace, manifest.get("report_path"))
    packet_path = _workspace_path(workspace, manifest.get("packet_path"))
    plan_path = _workspace_path(workspace, manifest.get("plan_path"))
    plan_json_path = _workspace_path(workspace, manifest.get("plan_json_path"))
    slide_path = _workspace_path(workspace, manifest.get("slide_path")) if manifest.get("slide_path") else None
    answer_path = output_file or _workspace_path(workspace, manifest.get("answer_path"))
    if answer_path is None:
        answer_path = _default_answer_path(workspace, question, mode)

    if not packet_path or not packet_path.exists():
        raise ResearchError(f"Prompt packet is missing for resume: {packet_path}")
    if not plan_path or not plan_path.exists():
        raise ResearchError(f"Research plan is missing for resume: {plan_path}")
    if not plan_json_path or not plan_json_path.exists():
        raise ResearchError(f"Research plan JSON is missing for resume: {plan_json_path}")

    sources = list(manifest.get("sources", []))
    resume_count = int(manifest.get("resume_count", 0)) + 1
    attempt_count = int(manifest.get("attempt_count", 0)) + 1

    return _execute_research_run(
        workspace=workspace,
        question=question,
        mode=mode,
        profile_name=effective_profile,
        output_file=answer_path,
        report_path=report_path,
        packet_path=packet_path,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        slide_path=slide_path,
        run_manifest_path=run_manifest_path,
        run_id=run_manifest_path.stem,
        sources=sources,
        resume_count=resume_count,
        attempt_count=attempt_count,
        resumed=True,
    )


def _execute_research_run(
    workspace: Workspace,
    question: str,
    mode: str,
    profile_name: str,
    output_file: Path,
    report_path: Path,
    packet_path: Path,
    plan_path: Path,
    plan_json_path: Path,
    slide_path: Optional[Path],
    run_manifest_path: Path,
    run_id: str,
    sources: List[Dict[str, object]],
    resume_count: int,
    attempt_count: int,
    resumed: bool,
) -> ResearchRunResult:
    config = workspace.load_config()
    try:
        adapter = adapter_from_config(config, profile_name)
    except AdapterError as error:
        failed_plan = _build_research_plan(
            question=question,
            mode=mode,
            sources=sources,
            report_path=workspace.relative_path(report_path),
            packet_path=workspace.relative_path(packet_path),
            answer_path=workspace.relative_path(output_file),
            slide_path=workspace.relative_path(slide_path) if slide_path else None,
            execution_status="failed",
            validation_status="pending",
            filing_status="pending",
        )
        _persist_existing_research_plan(workspace, plan_path, plan_json_path, failed_plan)
        _write_research_run_state(
            workspace=workspace,
            run_id=run_id,
            question=question,
            mode=mode,
            profile_name=profile_name,
            plan_path=plan_path,
            plan_json_path=plan_json_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=output_file,
            slide_path=slide_path,
            status="adapter_failed",
            sources=sources,
            citations_used=[],
            validation_passed=False,
            validation_errors=[str(error)],
            validation_status="pending",
            resume_count=resume_count,
            attempt_count=attempt_count,
        )
        raise ResearchError(str(error)) from error

    output_file.parent.mkdir(parents=True, exist_ok=True)
    running_plan = _build_research_plan(
        question=question,
        mode=mode,
        sources=sources,
        report_path=workspace.relative_path(report_path),
        packet_path=workspace.relative_path(packet_path),
        answer_path=workspace.relative_path(output_file),
        slide_path=workspace.relative_path(slide_path) if slide_path else None,
        execution_status="running",
        validation_status="pending",
        filing_status="pending",
    )
    _persist_existing_research_plan(workspace, plan_path, plan_json_path, running_plan)
    _write_research_run_state(
        workspace=workspace,
        run_id=run_id,
        question=question,
        mode=mode,
        profile_name=profile_name,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        status="running",
        sources=sources,
        citations_used=[],
        validation_passed=False,
        validation_errors=[],
        validation_status="pending",
        resume_count=resume_count,
        attempt_count=attempt_count,
    )

    result = adapter.run(prompt_file=packet_path, workspace_root=workspace.root, output_file=output_file)
    if result.returncode != 0:
        failed_plan = _build_research_plan(
            question=question,
            mode=mode,
            sources=sources,
            report_path=workspace.relative_path(report_path),
            packet_path=workspace.relative_path(packet_path),
            answer_path=workspace.relative_path(output_file),
            slide_path=workspace.relative_path(slide_path) if slide_path else None,
            execution_status="failed",
            validation_status="pending",
            filing_status="pending",
        )
        _persist_existing_research_plan(workspace, plan_path, plan_json_path, failed_plan)
        _write_research_run_state(
            workspace=workspace,
            run_id=run_id,
            question=question,
            mode=mode,
            profile_name=profile_name,
            plan_path=plan_path,
            plan_json_path=plan_json_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=output_file,
            slide_path=slide_path,
            status="adapter_failed",
            sources=sources,
            citations_used=[],
            validation_passed=False,
            validation_errors=[f"Adapter '{profile_name}' exited with code {result.returncode}."],
            validation_status="pending",
            resume_count=resume_count,
            attempt_count=attempt_count,
        )
        raise ResearchError(f"Adapter '{profile_name}' exited with code {result.returncode}.")

    if not adapter.output_file_flag and result.stdout:
        output_file.write_text(result.stdout, encoding="utf-8")

    answer_text = output_file.read_text(encoding="utf-8", errors="ignore") if output_file.exists() else ""
    validation = _validate_citations(answer_text, _available_citations(sources))

    final_snapshot = scan_workspace(workspace)
    workspace.write_index(final_snapshot)
    write_workspace_manifests(workspace, final_snapshot)

    final_plan = _build_research_plan(
        question=question,
        mode=mode,
        sources=sources,
        report_path=workspace.relative_path(report_path),
        packet_path=workspace.relative_path(packet_path),
        answer_path=workspace.relative_path(output_file),
        slide_path=workspace.relative_path(slide_path) if slide_path else None,
        execution_status="completed",
        validation_status="completed" if validation["passed"] else "failed",
        filing_status="completed" if output_file.exists() else "pending",
    )
    _persist_existing_research_plan(workspace, plan_path, plan_json_path, final_plan)
    _write_research_run_state(
        workspace=workspace,
        run_id=run_id,
        question=question,
        mode=mode,
        profile_name=profile_name,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        status="completed" if validation["passed"] else "failed_validation",
        sources=sources,
        citations_used=validation["used"],
        validation_passed=validation["passed"],
        validation_errors=validation["errors"],
        validation_status="completed" if validation["passed"] else "failed",
        resume_count=resume_count,
        attempt_count=attempt_count,
    )
    if not validation["passed"]:
        raise ResearchError("Citation validation failed: " + "; ".join(validation["errors"]))

    return ResearchRunResult(
        plan_path=plan_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        run_manifest_path=run_manifest_path,
        hit_count=len(sources),
        ran_profile=True,
        resumed=resumed,
    )


def _write_research_run_state(
    workspace: Workspace,
    run_id: str,
    question: str,
    mode: str,
    profile_name: Optional[str],
    plan_path: Path,
    plan_json_path: Path,
    report_path: Path,
    packet_path: Path,
    answer_path: Path,
    slide_path: Optional[Path],
    status: str,
    sources: List[Dict[str, object]],
    citations_used: List[str],
    validation_passed: bool,
    validation_errors: List[str],
    validation_status: str,
    resume_count: int,
    attempt_count: int,
) -> Path:
    return write_run_manifest(
        workspace,
        "research",
        {
            "run_label": question,
            "question": question,
            "mode": mode,
            "profile": profile_name,
            "plan_path": workspace.relative_path(plan_path),
            "plan_json_path": workspace.relative_path(plan_json_path),
            "report_path": workspace.relative_path(report_path),
            "packet_path": workspace.relative_path(packet_path),
            "answer_path": workspace.relative_path(answer_path),
            "slide_path": workspace.relative_path(slide_path) if slide_path else None,
            "status": status,
            "resume_supported": True,
            "attempt_count": attempt_count,
            "resume_count": resume_count,
            "sources": sources,
            "citations": {
                "available": _available_citations(sources),
                "used": citations_used,
            },
            "validation": {
                "passed": validation_passed,
                "errors": validation_errors,
                "status": validation_status,
            },
        },
        run_id=run_id,
    )


def _build_research_plan(
    question: str,
    mode: str,
    sources: List[Dict[str, object]],
    report_path: str,
    packet_path: str,
    answer_path: str,
    slide_path: Optional[str],
    execution_status: str,
    validation_status: str,
    filing_status: str,
) -> ResearchPlan:
    return ResearchPlan(
        generated_at=utc_timestamp(),
        question=question,
        mode=mode,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=answer_path,
        slide_path=slide_path,
        sources=sources,
        steps=[
            ResearchPlanStep(
                step_id="retrieve-sources",
                kind="retrieve_sources",
                title="Retrieve relevant sources",
                status="completed",
                detail=f"Selected {len(sources)} source(s) from the current workspace snapshot.",
            ),
            ResearchPlanStep(
                step_id="render-artifacts",
                kind="render_artifacts",
                title="Render report and prompt packet",
                status="completed",
                detail="The cited report and prompt packet are ready on disk.",
            ),
            ResearchPlanStep(
                step_id="execute-profile",
                kind="execute_profile",
                title="Execute adapter profile",
                status=execution_status,
                detail="Execute the prompt packet through the selected adapter profile.",
            ),
            ResearchPlanStep(
                step_id="validate-citations",
                kind="validate_citations",
                title="Validate inline citations",
                status=validation_status,
                detail="Every inline citation must resolve to one of the retrieved sources.",
            ),
            ResearchPlanStep(
                step_id="file-answer",
                kind="file_answer",
                title="File the answer artifact",
                status=filing_status,
                detail="Persist the final research artifact back into the workspace.",
            ),
        ],
    )


def _write_research_plan(workspace: Workspace, question: str, plan: ResearchPlan) -> tuple[Path, Path]:
    name = f"research-{slugify(question)}"
    json_path = workspace.write_plan_json(name, plan)
    markdown_path = workspace.plans_dir / f"{name}.md"
    markdown_path.write_text(render_research_plan(plan), encoding="utf-8")
    return markdown_path, json_path


def _persist_existing_research_plan(
    workspace: Workspace,
    plan_path: Path,
    plan_json_path: Path,
    plan: ResearchPlan,
) -> None:
    workspace.write_plan_json(plan_json_path.stem, plan)
    plan_path.write_text(render_research_plan(plan), encoding="utf-8")


def render_research_plan(plan: ResearchPlan) -> str:
    lines = [
        "# Research Plan",
        "",
        f"Generated: {plan.generated_at}",
        f"Question: {plan.question}",
        f"Mode: {plan.mode}",
        "",
        "## Artifacts",
        "",
        f"- Report: `{plan.report_path}`",
        f"- Prompt packet: `{plan.packet_path}`",
        f"- Answer target: `{plan.answer_path}`",
    ]
    if plan.slide_path:
        lines.append(f"- Slides: `{plan.slide_path}`")
    lines.extend(["", "## Sources", ""])
    if not plan.sources:
        lines.append("No sources were selected for this plan.")
        lines.append("")
    else:
        for source in plan.sources:
            lines.append(
                f"- [{source['citation']}] {source['title']} "
                f"(`{source['source_kind']}`) -> `{source['path']}`"
            )
        lines.append("")
    lines.extend(["## Steps", ""])
    for step in plan.steps:
        lines.extend(
            [
                f"### {step.title}",
                "",
                f"- Kind: `{step.kind}`",
                f"- Status: `{step.status}`",
                f"- Detail: {step.detail}",
                "",
            ]
        )
    return "\n".join(lines)


def _default_answer_path(workspace: Workspace, question: str, mode: str) -> Path:
    slug = slugify(question)
    if mode == "brief":
        return workspace.outputs_dir / "reports" / f"{slug}-brief.md"
    if mode == "memo":
        return workspace.outputs_dir / "reports" / f"{slug}-memo.md"
    if mode == "report":
        return workspace.outputs_dir / "reports" / f"{slug}.md"
    if mode == "slides":
        return workspace.outputs_dir / "slides" / f"{slug}.md"
    return workspace.wiki_dir / "queries" / f"{slug}.md"


def _validate_citations(answer_text: str, available_citations: Sequence[str]) -> dict:
    available = set(available_citations)
    used = CITATION_RE.findall(answer_text)
    unique_used = sorted(set(used), key=lambda item: int(item[1:]))
    errors = []
    if available and not unique_used:
        errors.append("The answer did not include any inline citations.")
    unknown = [citation for citation in unique_used if citation not in available]
    if unknown:
        errors.append("Unknown citations: " + ", ".join(unknown))
    return {
        "passed": not errors,
        "errors": errors,
        "used": unique_used,
    }


def _hit_to_manifest_entry(hit: SearchHit, index: int) -> dict:
    return {
        "citation": f"S{index}",
        "path": hit.path,
        "title": hit.title,
        "source_kind": hit.source_kind,
        "score": hit.score,
        "retrieval_reason": hit.retrieval_reason,
    }


def _available_citations(sources: Sequence[Dict[str, object]]) -> List[str]:
    return [str(source["citation"]) for source in sources]


def _resolve_research_manifest_path(workspace: Workspace, resume: str) -> Path:
    if resume == "latest":
        manifests = sorted(workspace.runs_dir.glob("research-*.json"))
        if not manifests:
            raise ResearchError("No research run manifests are available to resume.")
        return manifests[-1]
    path = Path(resume).expanduser()
    if not path.is_absolute():
        path = (workspace.root / path).resolve()
    if not path.exists():
        raise ResearchError(f"Research run manifest does not exist: {path}")
    return path


def _workspace_path(workspace: Workspace, raw_path: object) -> Optional[Path]:
    text = _optional_text(raw_path)
    if not text:
        return None
    path = Path(text)
    if path.is_absolute():
        return path
    return workspace.root / text


def _optional_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
