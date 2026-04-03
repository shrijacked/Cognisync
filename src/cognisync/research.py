from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, List, Optional, Sequence

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.change_summaries import ChangeState, capture_change_state, write_change_summary
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
CLAIM_RE = re.compile(
    r"^\s*(?:[-*]\s+)?([A-Za-z][A-Za-z0-9 /_-]{2,60}?)\s+"
    r"(is|are|uses|supports|requires|prefers)\s+"
    r"([A-Za-z][A-Za-z0-9 /_-]{2,80}?)(?:[.!?]|$)",
    re.IGNORECASE,
)
RESEARCH_OUTPUT_MODES = {"brief", "memo", "report", "slides", "wiki"}
CONFLICT_ACK_MARKERS = {"conflict", "disagree", "however", "contradict", "tension", "different", "vs"}


@dataclass(frozen=True)
class ResearchRunResult:
    plan_path: Path
    report_path: Path
    packet_path: Path
    answer_path: Optional[Path]
    slide_path: Optional[Path]
    change_summary_path: Path
    run_manifest_path: Path
    hit_count: int
    ran_profile: bool
    resumed: bool
    status: str
    warning_count: int


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

    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)
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
        "validation": _pending_validation_payload(_available_citations(sources)),
    }
    run_manifest_path = write_run_manifest(workspace, "research", base_payload)

    if not profile_name:
        change_summary_path = _write_research_change_summary(workspace, previous_state)
        run_manifest_path = _write_research_run_state(
            workspace=workspace,
            run_id=run_manifest_path.stem,
            question=question,
            mode=mode,
            profile_name=profile_name,
            plan_path=plan_path,
            plan_json_path=plan_json_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=answer_path,
            slide_path=slide_path,
            change_summary_path=change_summary_path,
            status="planned",
            sources=sources,
            validation=_pending_validation_payload(_available_citations(sources)),
            resume_count=0,
            attempt_count=0,
        )
        return ResearchRunResult(
            plan_path=plan_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=None,
            slide_path=slide_path,
            change_summary_path=change_summary_path,
            run_manifest_path=run_manifest_path,
            hit_count=len(hits),
            ran_profile=False,
            resumed=False,
            status="planned",
            warning_count=0,
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
        previous_state=previous_state,
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
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)

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
        previous_state=previous_state,
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
    previous_state: ChangeState,
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
            change_summary_path=_write_research_change_summary(workspace, previous_state),
            status="adapter_failed",
            sources=sources,
            validation=_failed_validation_payload(_available_citations(sources), [str(error)]),
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
        change_summary_path=None,
        status="running",
        sources=sources,
        validation=_pending_validation_payload(_available_citations(sources)),
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
            change_summary_path=_write_research_change_summary(workspace, previous_state),
            status="adapter_failed",
            sources=sources,
            validation=_failed_validation_payload(
                _available_citations(sources),
                [f"Adapter '{profile_name}' exited with code {result.returncode}."],
            ),
            resume_count=resume_count,
            attempt_count=attempt_count,
        )
        raise ResearchError(f"Adapter '{profile_name}' exited with code {result.returncode}.")

    if not adapter.output_file_flag and result.stdout:
        output_file.write_text(result.stdout, encoding="utf-8")

    answer_text = output_file.read_text(encoding="utf-8", errors="ignore") if output_file.exists() else ""
    validation = _verify_research_answer(workspace, answer_text, sources)
    change_summary_path = _write_research_change_summary(workspace, previous_state)

    final_status = "completed"
    validation_step_status = "completed"
    if validation["errors"]:
        final_status = "failed_validation"
        validation_step_status = "failed"
    elif validation["warnings"]:
        final_status = "completed_with_warnings"
        validation_step_status = "warning"

    final_plan = _build_research_plan(
        question=question,
        mode=mode,
        sources=sources,
        report_path=workspace.relative_path(report_path),
        packet_path=workspace.relative_path(packet_path),
        answer_path=workspace.relative_path(output_file),
        slide_path=workspace.relative_path(slide_path) if slide_path else None,
        execution_status="completed",
        validation_status=validation_step_status,
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
        change_summary_path=change_summary_path,
        status=final_status,
        sources=sources,
        validation=validation,
        resume_count=resume_count,
        attempt_count=attempt_count,
    )
    if not validation["passed"]:
        raise ResearchError("Research verification failed: " + "; ".join(validation["errors"]))

    return ResearchRunResult(
        plan_path=plan_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        change_summary_path=change_summary_path,
        run_manifest_path=run_manifest_path,
        hit_count=len(sources),
        ran_profile=True,
        resumed=resumed,
        status=final_status,
        warning_count=len(validation["warnings"]),
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
    change_summary_path: Optional[Path],
    status: str,
    sources: List[Dict[str, object]],
    validation: Dict[str, object],
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
            "change_summary_path": workspace.relative_path(change_summary_path) if change_summary_path else None,
            "status": status,
            "resume_supported": True,
            "attempt_count": attempt_count,
            "resume_count": resume_count,
            "sources": sources,
            "citations": {
                "available": _available_citations(sources),
                "used": list(validation.get("used", [])),
            },
            "validation": validation,
        },
        run_id=run_id,
    )


def _write_research_change_summary(workspace: Workspace, previous_state: ChangeState) -> Path:
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "research", previous_state, snapshot)
    return change_summary.path


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


def _verify_research_answer(workspace: Workspace, answer_text: str, sources: Sequence[Dict[str, object]]) -> Dict[str, object]:
    available_citations = _available_citations(sources)
    citations = _validate_citations(answer_text, available_citations)
    answer_lint = _lint_answer(answer_text)
    unsupported_claims = _detect_unsupported_claims(answer_text)
    source_conflicts = _detect_source_conflicts(workspace, sources, answer_text, citations["used"])
    errors = citations["errors"] + answer_lint["errors"] + unsupported_claims["errors"] + source_conflicts["errors"]
    warnings = citations["warnings"] + answer_lint["warnings"] + unsupported_claims["warnings"] + source_conflicts["warnings"]
    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "status": "failed" if errors else ("warning" if warnings else "completed"),
        "used": citations["used"],
        "checks": {
            "citations": citations,
            "answer_lint": answer_lint,
            "unsupported_claims": unsupported_claims,
            "source_conflicts": source_conflicts,
        },
    }


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
        "warnings": [],
        "status": "failed" if errors else "completed",
        "used": unique_used,
    }


def _lint_answer(answer_text: str) -> Dict[str, object]:
    errors: List[str] = []
    warnings: List[str] = []
    stripped = answer_text.strip()
    if not stripped:
        errors.append("The answer is empty.")
    if stripped and not re.search(r"(?m)^#\s+\S+", stripped):
        errors.append("The answer is missing a top-level Markdown heading.")
    if stripped.count("```") % 2 != 0:
        errors.append("The answer has an unmatched fenced code block.")
    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "status": "failed" if errors else "completed",
    }


def _detect_unsupported_claims(answer_text: str) -> Dict[str, object]:
    errors: List[str] = []
    warnings: List[str] = []
    in_code = False
    for raw_line in answer_text.splitlines():
        line = raw_line.strip()
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code or not line:
            continue
        if line.startswith("#") or line.startswith(">"):
            continue
        normalized = line[2:].strip() if line.startswith(("- ", "* ")) else line
        lowered = normalized.lower()
        if lowered.startswith(("source:", "generated:", "open questions", "follow-up", "follow up", "next steps")):
            continue
        if len(re.findall(r"[A-Za-z0-9_]+", normalized)) < 5:
            continue
        if CITATION_RE.search(normalized):
            continue
        errors.append(f"Unsupported claim without citation: {normalized}")
    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "status": "failed" if errors else "completed",
    }


def _detect_source_conflicts(
    workspace: Workspace,
    sources: Sequence[Dict[str, object]],
    answer_text: str,
    used_citations: Sequence[str],
) -> Dict[str, object]:
    errors: List[str] = []
    warnings: List[str] = []
    claims: Dict[tuple[str, str], Dict[str, List[str]]] = {}
    for source in sources:
        citation = str(source.get("citation", ""))
        path = str(source.get("path", "")).strip()
        if not path:
            continue
        absolute_path = workspace.root / path
        if not absolute_path.exists():
            continue
        text = _strip_frontmatter(absolute_path.read_text(encoding="utf-8", errors="ignore"))
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            match = CLAIM_RE.match(line)
            if not match:
                continue
            subject = " ".join(match.group(1).lower().split())
            verb = match.group(2).lower()
            obj = " ".join(match.group(3).lower().split())
            claims.setdefault((subject, verb), {}).setdefault(obj, []).append(citation)

    narrative_text = "\n".join(
        line.strip()
        for line in answer_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ).lower()
    acknowledged = any(marker in narrative_text for marker in CONFLICT_ACK_MARKERS)
    used_citation_set = set(used_citations)
    for (subject, verb), objects in sorted(claims.items()):
        if len(objects) < 2:
            continue
        variants = []
        cited_variants = 0
        for obj, citations in sorted(objects.items()):
            unique_citations = sorted(set(citations))
            variants.append(f"{obj} ({', '.join(unique_citations)})")
            if used_citation_set.intersection(unique_citations):
                cited_variants += 1
        if acknowledged and cited_variants >= 2:
            continue
        errors.append(
            f"Retrieved sources disagree about '{subject} {verb}': "
            + "; ".join(variants)
            + ". The answer must acknowledge the disagreement and cite both sides."
        )
    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "status": "failed" if errors else ("warning" if warnings else "completed"),
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


def _pending_validation_payload(available_citations: Sequence[str]) -> Dict[str, object]:
    return {
        "passed": False,
        "errors": [],
        "warnings": [],
        "status": "pending",
        "used": [],
        "checks": {
            "citations": {"passed": False, "errors": [], "warnings": [], "status": "pending", "used": []},
            "answer_lint": {"passed": False, "errors": [], "warnings": [], "status": "pending"},
            "unsupported_claims": {"passed": False, "errors": [], "warnings": [], "status": "pending"},
            "source_conflicts": {"passed": False, "errors": [], "warnings": [], "status": "pending"},
        },
    }


def _failed_validation_payload(available_citations: Sequence[str], errors: List[str]) -> Dict[str, object]:
    payload = _pending_validation_payload(available_citations)
    payload["errors"] = errors
    payload["status"] = "failed"
    return payload


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


def _strip_frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    return text[end + 5 :]
