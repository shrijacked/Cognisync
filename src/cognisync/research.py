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
RESEARCH_JOB_PROFILES = {
    "synthesis-report": {
        "steps": [
            (
                "build-working-set",
                "build_working_set",
                "Build working set",
                "Capture the current source set and the most relevant evidence in a working note.",
                "working-set.md",
            ),
            (
                "capture-open-questions",
                "capture_open_questions",
                "Capture open questions",
                "List unresolved questions, missing evidence, and weak spots before drafting the answer.",
                "open-questions.md",
            ),
            (
                "shape-synthesis-outline",
                "shape_synthesis_outline",
                "Shape synthesis outline",
                "Turn the evidence into a section outline before the final answer is executed.",
                "synthesis-outline.md",
            ),
        ],
    },
    "literature-review": {
        "steps": [
            (
                "build-working-set",
                "build_working_set",
                "Build working set",
                "Capture the current source set and the most relevant evidence in a working note.",
                "working-set.md",
            ),
            (
                "build-paper-matrix",
                "build_paper_matrix",
                "Build paper matrix",
                "Compare the retrieved sources across claims, methods, and limitations.",
                "paper-matrix.md",
            ),
            (
                "capture-open-questions",
                "capture_open_questions",
                "Capture open questions",
                "List unresolved questions, gaps, and follow-up reading paths before writing the answer.",
                "open-questions.md",
            ),
        ],
    },
    "repo-analysis": {
        "steps": [
            (
                "build-working-set",
                "build_working_set",
                "Build working set",
                "Capture the current source set and the most relevant evidence in a working note.",
                "working-set.md",
            ),
            (
                "map-code-surfaces",
                "map_code_surfaces",
                "Map code surfaces",
                "Identify the main modules, packages, and interfaces that answer the question.",
                "code-surfaces.md",
            ),
            (
                "capture-risks-and-interfaces",
                "capture_risks_and_interfaces",
                "Capture risks and interfaces",
                "Summarize important interfaces, constraints, and likely integration risks.",
                "risks-and-interfaces.md",
            ),
        ],
    },
    "contradiction-finding": {
        "steps": [
            (
                "build-working-set",
                "build_working_set",
                "Build working set",
                "Capture the current source set and the most relevant evidence in a working note.",
                "working-set.md",
            ),
            (
                "build-claim-ledger",
                "build_claim_ledger",
                "Build claim ledger",
                "List the competing claims and which sources support each side.",
                "claim-ledger.md",
            ),
            (
                "build-resolution-checklist",
                "build_resolution_checklist",
                "Build resolution checklist",
                "Capture what the final answer must acknowledge before it can reconcile the disagreement.",
                "resolution-checklist.md",
            ),
        ],
    },
    "market-scan": {
        "steps": [
            (
                "build-working-set",
                "build_working_set",
                "Build working set",
                "Capture the current source set and the most relevant evidence in a working note.",
                "working-set.md",
            ),
            (
                "build-competitor-grid",
                "build_competitor_grid",
                "Build competitor grid",
                "Compare the retrieved subjects across product shape, strengths, and tradeoffs.",
                "competitor-grid.md",
            ),
            (
                "capture-positioning-questions",
                "capture_positioning_questions",
                "Capture positioning questions",
                "List unresolved positioning questions or missing evidence before the final write-up.",
                "positioning-questions.md",
            ),
        ],
    },
}
DEFAULT_RESEARCH_JOB_PROFILE = "synthesis-report"
CONFLICT_ACK_MARKERS = {"conflict", "disagree", "however", "contradict", "tension", "different", "vs"}


@dataclass(frozen=True)
class ResearchRunResult:
    plan_path: Path
    report_path: Path
    packet_path: Path
    answer_path: Optional[Path]
    slide_path: Optional[Path]
    notes_dir: Path
    validation_report_path: Path
    change_summary_path: Path
    run_manifest_path: Path
    hit_count: int
    ran_profile: bool
    resumed: bool
    status: str
    warning_count: int


@dataclass(frozen=True)
class ResearchJobArtifacts:
    notes_dir: Path
    note_paths: List[Path]
    validation_report_path: Path


def run_research_cycle(
    workspace: Workspace,
    question: Optional[str] = None,
    limit: int = 5,
    profile_name: Optional[str] = None,
    output_file: Optional[Path] = None,
    slides: bool = False,
    mode: str = "wiki",
    resume: Optional[str] = None,
    job_profile: str = DEFAULT_RESEARCH_JOB_PROFILE,
) -> ResearchRunResult:
    if mode not in RESEARCH_OUTPUT_MODES:
        raise ResearchError(
            f"Unsupported research mode '{mode}'. Expected one of: {', '.join(sorted(RESEARCH_OUTPUT_MODES))}."
        )
    if job_profile not in RESEARCH_JOB_PROFILES:
        raise ResearchError(
            f"Unsupported research job profile '{job_profile}'. Expected one of: {', '.join(sorted(RESEARCH_JOB_PROFILES))}."
        )
    if resume:
        return _resume_research_cycle(
            workspace=workspace,
            resume=resume,
            profile_name=profile_name,
            output_file=output_file,
            job_profile=job_profile,
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
    answer_path = output_file or _default_answer_path(workspace, question, mode)
    sources = [_hit_to_manifest_entry(hit, index) for index, hit in enumerate(hits, start=1)]
    initial_validation = _pending_validation_payload(_available_citations(sources))

    execution_status = "pending"
    validation_status = "pending"
    filing_status = "pending"
    if profile_name:
        execution_status = "completed"
        validation_status = "completed"
        filing_status = "completed"

    base_payload = {
        "run_label": question,
        "question": question,
        "mode": mode,
        "profile": profile_name,
        "job_profile": job_profile,
        "status": "planned" if not profile_name else "running",
        "resume_supported": True,
        "attempt_count": 0,
        "resume_count": 0,
        "sources": sources,
        "citations": {
            "available": _available_citations(sources),
            "used": [],
        },
        "validation": initial_validation,
    }
    run_manifest_path = write_run_manifest(workspace, "research", base_payload)
    run_id = run_manifest_path.stem
    job_artifacts = _write_research_job_artifacts(
        workspace=workspace,
        run_id=run_id,
        question=question,
        job_profile=job_profile,
        sources=sources,
        validation=initial_validation,
    )
    packet_path = render_query_packet(
        workspace,
        question,
        hits,
        snapshot=snapshot,
        mode=mode,
        job_profile=job_profile,
        note_paths=[workspace.relative_path(path) for path in job_artifacts.note_paths],
    )
    slide_path = render_marp_slides(workspace, question, hits) if slides or mode == "slides" else None
    plan = _build_research_plan(
        question=question,
        mode=mode,
        job_profile=job_profile,
        notes_dir=workspace.relative_path(job_artifacts.notes_dir),
        note_paths=[workspace.relative_path(path) for path in job_artifacts.note_paths],
        validation_report_path=workspace.relative_path(job_artifacts.validation_report_path),
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

    if not profile_name:
        change_summary_path = _write_research_change_summary(workspace, previous_state)
        run_manifest_path = _write_research_run_state(
            workspace=workspace,
            run_id=run_manifest_path.stem,
            question=question,
            mode=mode,
            profile_name=profile_name,
            job_profile=job_profile,
            plan_path=plan_path,
            plan_json_path=plan_json_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=answer_path,
            slide_path=slide_path,
            notes_dir=job_artifacts.notes_dir,
            note_paths=job_artifacts.note_paths,
            validation_report_path=job_artifacts.validation_report_path,
            change_summary_path=change_summary_path,
            status="planned",
            sources=sources,
            validation=initial_validation,
            resume_count=0,
            attempt_count=0,
        )
        return ResearchRunResult(
            plan_path=plan_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=None,
            slide_path=slide_path,
            notes_dir=job_artifacts.notes_dir,
            validation_report_path=job_artifacts.validation_report_path,
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
        job_profile=job_profile,
        output_file=answer_path,
        report_path=report_path,
        packet_path=packet_path,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        slide_path=slide_path,
        notes_dir=job_artifacts.notes_dir,
        note_paths=job_artifacts.note_paths,
        validation_report_path=job_artifacts.validation_report_path,
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
    job_profile: str,
) -> ResearchRunResult:
    run_manifest_path = _resolve_research_manifest_path(workspace, resume)
    manifest = read_json_manifest(run_manifest_path)
    if manifest.get("run_kind") != "research":
        raise ResearchError(f"Run manifest is not a research run: {run_manifest_path}")

    question = str(manifest.get("question", "")).strip()
    mode = str(manifest.get("mode", "wiki")).strip() or "wiki"
    effective_profile = profile_name or _optional_text(manifest.get("profile"))
    effective_job_profile = _optional_text(manifest.get("job_profile")) or job_profile or DEFAULT_RESEARCH_JOB_PROFILE
    if effective_job_profile not in RESEARCH_JOB_PROFILES:
        raise ResearchError(
            f"Unsupported research job profile '{effective_job_profile}'. Expected one of: {', '.join(sorted(RESEARCH_JOB_PROFILES))}."
        )
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
    notes_dir = _workspace_path(workspace, manifest.get("notes_dir")) or (workspace.research_jobs_dir / run_manifest_path.stem)
    note_paths = [
        _workspace_path(workspace, value)
        for value in list(manifest.get("note_paths", []))
        if _workspace_path(workspace, value) is not None
    ]
    validation_report_path = _workspace_path(workspace, manifest.get("validation_report_path")) or (
        notes_dir / "validation-report.md"
    )
    job_artifacts = _write_research_job_artifacts(
        workspace=workspace,
        run_id=run_manifest_path.stem,
        question=question,
        job_profile=effective_job_profile,
        sources=sources,
        validation=dict(manifest.get("validation", _pending_validation_payload(_available_citations(sources)))),
        existing_notes_dir=notes_dir,
        existing_note_paths=[path for path in note_paths if path is not None],
        validation_report_path=validation_report_path,
    )
    resume_count = int(manifest.get("resume_count", 0)) + 1
    attempt_count = int(manifest.get("attempt_count", 0)) + 1
    previous_state = capture_change_state(workspace, fallback_to_live_scan=True)

    return _execute_research_run(
        workspace=workspace,
        question=question,
        mode=mode,
        profile_name=effective_profile,
        job_profile=effective_job_profile,
        output_file=answer_path,
        report_path=report_path,
        packet_path=packet_path,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        slide_path=slide_path,
        notes_dir=job_artifacts.notes_dir,
        note_paths=job_artifacts.note_paths,
        validation_report_path=job_artifacts.validation_report_path,
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
    job_profile: str,
    output_file: Path,
    report_path: Path,
    packet_path: Path,
    plan_path: Path,
    plan_json_path: Path,
    slide_path: Optional[Path],
    notes_dir: Path,
    note_paths: List[Path],
    validation_report_path: Path,
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
            job_profile=job_profile,
            notes_dir=workspace.relative_path(notes_dir),
            note_paths=[workspace.relative_path(path) for path in note_paths],
            validation_report_path=workspace.relative_path(validation_report_path),
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
            job_profile=job_profile,
            plan_path=plan_path,
            plan_json_path=plan_json_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=output_file,
            slide_path=slide_path,
            notes_dir=notes_dir,
            note_paths=note_paths,
            validation_report_path=validation_report_path,
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
        job_profile=job_profile,
        notes_dir=workspace.relative_path(notes_dir),
        note_paths=[workspace.relative_path(path) for path in note_paths],
        validation_report_path=workspace.relative_path(validation_report_path),
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
        job_profile=job_profile,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        notes_dir=notes_dir,
        note_paths=note_paths,
        validation_report_path=validation_report_path,
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
            job_profile=job_profile,
            notes_dir=workspace.relative_path(notes_dir),
            note_paths=[workspace.relative_path(path) for path in note_paths],
            validation_report_path=workspace.relative_path(validation_report_path),
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
            job_profile=job_profile,
            plan_path=plan_path,
            plan_json_path=plan_json_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=output_file,
            slide_path=slide_path,
            notes_dir=notes_dir,
            note_paths=note_paths,
            validation_report_path=validation_report_path,
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
    _write_validation_report(workspace, validation_report_path, question, job_profile, sources, validation)
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
        job_profile=job_profile,
        notes_dir=workspace.relative_path(notes_dir),
        note_paths=[workspace.relative_path(path) for path in note_paths],
        validation_report_path=workspace.relative_path(validation_report_path),
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
        job_profile=job_profile,
        plan_path=plan_path,
        plan_json_path=plan_json_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        notes_dir=notes_dir,
        note_paths=note_paths,
        validation_report_path=validation_report_path,
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
        notes_dir=notes_dir,
        validation_report_path=validation_report_path,
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
    job_profile: str,
    plan_path: Path,
    plan_json_path: Path,
    report_path: Path,
    packet_path: Path,
    answer_path: Path,
    slide_path: Optional[Path],
    notes_dir: Path,
    note_paths: List[Path],
    validation_report_path: Path,
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
            "job_profile": job_profile,
            "plan_path": workspace.relative_path(plan_path),
            "plan_json_path": workspace.relative_path(plan_json_path),
            "report_path": workspace.relative_path(report_path),
            "packet_path": workspace.relative_path(packet_path),
            "answer_path": workspace.relative_path(answer_path),
            "slide_path": workspace.relative_path(slide_path) if slide_path else None,
            "notes_dir": workspace.relative_path(notes_dir),
            "note_paths": [workspace.relative_path(path) for path in note_paths],
            "validation_report_path": workspace.relative_path(validation_report_path),
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
    job_profile: str,
    notes_dir: str,
    note_paths: List[str],
    validation_report_path: str,
    sources: List[Dict[str, object]],
    report_path: str,
    packet_path: str,
    answer_path: str,
    slide_path: Optional[str],
    execution_status: str,
    validation_status: str,
    filing_status: str,
) -> ResearchPlan:
    profile_definition = RESEARCH_JOB_PROFILES[job_profile]
    note_lookup = {Path(path).name: path for path in note_paths}
    steps = [
        ResearchPlanStep(
            step_id="retrieve-sources",
            kind="retrieve_sources",
            title="Retrieve relevant sources",
            status="completed",
            detail=f"Selected {len(sources)} source(s) from the current workspace snapshot.",
        )
    ]
    for step_id, kind, title, detail, file_name in profile_definition["steps"]:
        steps.append(
            ResearchPlanStep(
                step_id=step_id,
                kind=kind,
                title=title,
                status="completed",
                detail=detail,
                owner="planner",
                output_path=note_lookup.get(file_name),
                depends_on=["retrieve-sources"],
            )
        )
    last_profile_step = profile_definition["steps"][-1][0]
    steps.extend(
        [
            ResearchPlanStep(
                step_id="render-artifacts",
                kind="render_artifacts",
                title="Render report and prompt packet",
                status="completed",
                detail="The cited report and prompt packet are ready on disk.",
                owner="planner",
                depends_on=[last_profile_step],
            ),
            ResearchPlanStep(
                step_id="execute-profile",
                kind="execute_profile",
                title="Execute adapter profile",
                status=execution_status,
                detail="Execute the prompt packet through the selected adapter profile.",
                owner="adapter",
                depends_on=["render-artifacts"],
            ),
            ResearchPlanStep(
                step_id="validate-citations",
                kind="validate_citations",
                title="Validate inline citations",
                status=validation_status,
                detail="Every inline citation must resolve to one of the retrieved sources.",
                owner="validator",
                output_path=validation_report_path,
                depends_on=["execute-profile"],
            ),
            ResearchPlanStep(
                step_id="file-answer",
                kind="file_answer",
                title="File the answer artifact",
                status=filing_status,
                detail="Persist the final research artifact back into the workspace.",
                owner="filer",
                depends_on=["validate-citations"],
            ),
        ]
    )
    return ResearchPlan(
        generated_at=utc_timestamp(),
        question=question,
        mode=mode,
        job_profile=job_profile,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=answer_path,
        slide_path=slide_path,
        notes_dir=notes_dir,
        note_paths=note_paths,
        validation_report_path=validation_report_path,
        sources=sources,
        steps=steps,
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
        f"Job profile: {plan.job_profile}",
        "",
        "## Artifacts",
        "",
        f"- Report: `{plan.report_path}`",
        f"- Prompt packet: `{plan.packet_path}`",
        f"- Answer target: `{plan.answer_path}`",
    ]
    if plan.slide_path:
        lines.append(f"- Slides: `{plan.slide_path}`")
    if plan.notes_dir:
        lines.append(f"- Notes directory: `{plan.notes_dir}`")
    if plan.validation_report_path:
        lines.append(f"- Validation report: `{plan.validation_report_path}`")
    if plan.note_paths:
        lines.extend(["", "## Intermediate Notes", ""])
        for path in plan.note_paths:
            lines.append(f"- `{path}`")
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
                f"- Owner: `{step.owner}`",
                f"- Detail: {step.detail}",
            ]
        )
        if step.output_path:
            lines.append(f"- Output: `{step.output_path}`")
        if step.depends_on:
            lines.append(f"- Depends on: {', '.join(f'`{step_id}`' for step_id in step.depends_on)}")
        lines.append("")
    return "\n".join(lines)


def _write_research_job_artifacts(
    workspace: Workspace,
    run_id: str,
    question: str,
    job_profile: str,
    sources: Sequence[Dict[str, object]],
    validation: Dict[str, object],
    existing_notes_dir: Optional[Path] = None,
    existing_note_paths: Optional[Sequence[Path]] = None,
    validation_report_path: Optional[Path] = None,
) -> ResearchJobArtifacts:
    profile_definition = RESEARCH_JOB_PROFILES[job_profile]
    notes_dir = existing_notes_dir or (workspace.research_jobs_dir / run_id)
    notes_dir.mkdir(parents=True, exist_ok=True)

    existing_lookup = {path.name: path for path in list(existing_note_paths or [])}
    note_paths: List[Path] = []
    for _, _, title, detail, file_name in profile_definition["steps"]:
        note_path = existing_lookup.get(file_name) or (notes_dir / file_name)
        if not note_path.exists():
            note_path.write_text(
                _render_research_note(question, job_profile, title, detail, sources, file_name),
                encoding="utf-8",
            )
        note_paths.append(note_path)

    validation_path = validation_report_path or (notes_dir / "validation-report.md")
    _write_validation_report(workspace, validation_path, question, job_profile, sources, validation)
    return ResearchJobArtifacts(
        notes_dir=notes_dir,
        note_paths=note_paths + [validation_path],
        validation_report_path=validation_path,
    )


def _render_research_note(
    question: str,
    job_profile: str,
    title: str,
    detail: str,
    sources: Sequence[Dict[str, object]],
    file_name: str,
) -> str:
    lines = [
        f"# {title}",
        "",
        f"Question: {question}",
        f"Job profile: {job_profile}",
        "",
        detail,
        "",
        "## Source Coverage",
        "",
    ]
    if not sources:
        lines.append("No sources were selected for this run.")
    else:
        for source in sources:
            lines.append(
                f"- [{source['citation']}] {source['title']} "
                f"(`{source['source_kind']}`) -> `{source['path']}`"
            )
    lines.extend(["", "## Working Notes", ""])
    if file_name == "paper-matrix.md":
        lines.extend(
            [
                "| Source | Main claim | Method | Limitation |",
                "| --- | --- | --- | --- |",
            ]
        )
    elif file_name == "claim-ledger.md":
        lines.extend(
            [
                "| Claim | Source | Evidence | Counterpoint |",
                "| --- | --- | --- | --- |",
            ]
        )
    elif file_name == "competitor-grid.md":
        lines.extend(
            [
                "| Subject | Positioning | Strength | Risk |",
                "| --- | --- | --- | --- |",
            ]
        )
    else:
        lines.extend(
            [
                "- Fill this note with grounded observations only.",
                "- Preserve source ids inline so later validation can trace the reasoning.",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def _write_validation_report(
    workspace: Workspace,
    path: Path,
    question: str,
    job_profile: str,
    sources: Sequence[Dict[str, object]],
    validation: Dict[str, object],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Validation Report",
        "",
        f"Question: {question}",
        f"Job profile: {job_profile}",
        f"Validation Status: {validation.get('status', 'pending')}",
        f"Passed: `{bool(validation.get('passed', False))}`",
        "",
        "## Citations",
        "",
        f"- Available: {', '.join(_available_citations(sources)) or '-'}",
        f"- Used: {', '.join(list(validation.get('used', []))) or '-'}",
        "",
        "## Errors",
        "",
    ]
    errors = list(validation.get("errors", []))
    warnings = list(validation.get("warnings", []))
    if errors:
        lines.extend(f"- {error}" for error in errors)
    else:
        lines.append("- None")
    lines.extend(["", "## Warnings", ""])
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


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
