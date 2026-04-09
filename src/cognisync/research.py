from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Dict, List, Optional, Sequence

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.change_summaries import ChangeState, capture_change_state, write_change_summary
from cognisync.knowledge_surfaces import append_workspace_log
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
VALID_RESEARCH_STEP_REVIEW_STATUSES = {"approved", "changes_requested", "needs_follow_up"}
DEFAULT_RESEARCH_STEP_EXECUTION_STATUS = "not_run"
DEFAULT_RESEARCH_STEP_REVIEW_STATUS = "unreviewed"


@dataclass(frozen=True)
class ResearchRunResult:
    plan_path: Path
    report_path: Path
    packet_path: Path
    answer_path: Optional[Path]
    slide_path: Optional[Path]
    notes_dir: Path
    source_packet_path: Path
    checkpoints_path: Path
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
    source_packet_path: Path
    checkpoints_path: Path
    validation_report_path: Path


@dataclass(frozen=True)
class ResearchStepExecutionResult:
    run_manifest_path: Path
    checkpoints_path: Path
    step_id: str
    profile_name: str
    output_path: Path
    stdout: str
    stderr: str
    returncode: int


@dataclass(frozen=True)
class ResearchStepReviewResult:
    run_manifest_path: Path
    checkpoints_path: Path
    step_id: str
    review_status: str
    reviewer: str


@dataclass(frozen=True)
class ResearchStepDispatchResult:
    run_manifest_path: Path
    checkpoints_path: Path
    dispatch_manifest_path: Path
    executed_steps: List[str]
    skipped_steps: List[str]
    failed_step: Optional[str]
    status: str


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
    snapshot = workspace.refresh_index()
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
        source_packet_path=workspace.relative_path(job_artifacts.source_packet_path),
        checkpoints_path=workspace.relative_path(job_artifacts.checkpoints_path),
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
    execution_packet_paths = _write_research_execution_packets(workspace, plan, job_artifacts.notes_dir)
    _write_research_checkpoints(workspace, job_artifacts.checkpoints_path, plan, execution_packet_paths)

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
            execution_packet_paths=list(execution_packet_paths.values()),
            source_packet_path=job_artifacts.source_packet_path,
            checkpoints_path=job_artifacts.checkpoints_path,
            validation_report_path=job_artifacts.validation_report_path,
            change_summary_path=change_summary_path,
            status="planned",
            sources=sources,
            validation=initial_validation,
            resume_count=0,
            attempt_count=0,
        )
        append_workspace_log(
            workspace,
            operation="query",
            title=question,
            details=["Planned a research run without executing a model profile yet."],
            related_paths=[
                workspace.relative_path(plan_path),
                workspace.relative_path(report_path),
                workspace.relative_path(change_summary_path),
                workspace.relative_path(run_manifest_path),
            ],
        )
        return ResearchRunResult(
            plan_path=plan_path,
            report_path=report_path,
            packet_path=packet_path,
            answer_path=None,
            slide_path=slide_path,
            notes_dir=job_artifacts.notes_dir,
            source_packet_path=job_artifacts.source_packet_path,
            checkpoints_path=job_artifacts.checkpoints_path,
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
        source_packet_path=job_artifacts.source_packet_path,
        checkpoints_path=job_artifacts.checkpoints_path,
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
    source_packet_path = _workspace_path(workspace, manifest.get("source_packet_path")) or (notes_dir / "source-packet.md")
    checkpoints_path = _workspace_path(workspace, manifest.get("checkpoints_path")) or (notes_dir / "checkpoints.json")
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
        source_packet_path=source_packet_path,
        checkpoints_path=checkpoints_path,
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
        source_packet_path=job_artifacts.source_packet_path,
        checkpoints_path=job_artifacts.checkpoints_path,
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
    source_packet_path: Path,
    checkpoints_path: Path,
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
            source_packet_path=workspace.relative_path(source_packet_path),
            checkpoints_path=workspace.relative_path(checkpoints_path),
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
        execution_packet_paths = _write_research_execution_packets(workspace, failed_plan, notes_dir)
        _write_research_checkpoints(workspace, checkpoints_path, failed_plan, execution_packet_paths)
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
            execution_packet_paths=list(execution_packet_paths.values()),
            source_packet_path=source_packet_path,
            checkpoints_path=checkpoints_path,
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
        source_packet_path=workspace.relative_path(source_packet_path),
        checkpoints_path=workspace.relative_path(checkpoints_path),
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
    execution_packet_paths = _write_research_execution_packets(workspace, running_plan, notes_dir)
    _write_research_checkpoints(workspace, checkpoints_path, running_plan, execution_packet_paths)
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
        execution_packet_paths=list(execution_packet_paths.values()),
        source_packet_path=source_packet_path,
        checkpoints_path=checkpoints_path,
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
            source_packet_path=workspace.relative_path(source_packet_path),
            checkpoints_path=workspace.relative_path(checkpoints_path),
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
        execution_packet_paths = _write_research_execution_packets(workspace, failed_plan, notes_dir)
        _write_research_checkpoints(workspace, checkpoints_path, failed_plan, execution_packet_paths)
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
            execution_packet_paths=list(execution_packet_paths.values()),
            source_packet_path=source_packet_path,
            checkpoints_path=checkpoints_path,
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
        source_packet_path=workspace.relative_path(source_packet_path),
        checkpoints_path=workspace.relative_path(checkpoints_path),
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
    execution_packet_paths = _write_research_execution_packets(workspace, final_plan, notes_dir)
    _write_research_checkpoints(workspace, checkpoints_path, final_plan, execution_packet_paths)
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
        execution_packet_paths=list(execution_packet_paths.values()),
        source_packet_path=source_packet_path,
        checkpoints_path=checkpoints_path,
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

    append_workspace_log(
        workspace,
        operation="query",
        title=question,
        details=[f"Completed research in `{mode}` mode with status `{final_status}` and {len(sources)} cited source(s)."],
        related_paths=[
            workspace.relative_path(report_path),
            workspace.relative_path(output_file),
            workspace.relative_path(change_summary_path),
            workspace.relative_path(run_manifest_path),
        ],
    )
    return ResearchRunResult(
        plan_path=plan_path,
        report_path=report_path,
        packet_path=packet_path,
        answer_path=output_file,
        slide_path=slide_path,
        notes_dir=notes_dir,
        source_packet_path=source_packet_path,
        checkpoints_path=checkpoints_path,
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
    execution_packet_paths: List[Path],
    source_packet_path: Path,
    checkpoints_path: Path,
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
            "execution_packet_paths": [workspace.relative_path(path) for path in execution_packet_paths],
            "source_packet_path": workspace.relative_path(source_packet_path),
            "checkpoints_path": workspace.relative_path(checkpoints_path),
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
    snapshot = workspace.refresh_index()
    write_workspace_manifests(workspace, snapshot)
    change_summary = write_change_summary(workspace, "research", previous_state, snapshot)
    return change_summary.path


def _build_research_plan(
    question: str,
    mode: str,
    job_profile: str,
    notes_dir: str,
    note_paths: List[str],
    source_packet_path: str,
    checkpoints_path: str,
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
        source_packet_path=source_packet_path,
        answer_path=answer_path,
        slide_path=slide_path,
        notes_dir=notes_dir,
        note_paths=note_paths,
        checkpoints_path=checkpoints_path,
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
        f"- Source packet: `{plan.source_packet_path}`" if plan.source_packet_path else "- Source packet: `-`",
        f"- Answer target: `{plan.answer_path}`",
    ]
    if plan.slide_path:
        lines.append(f"- Slides: `{plan.slide_path}`")
    if plan.notes_dir:
        lines.append(f"- Notes directory: `{plan.notes_dir}`")
    if plan.validation_report_path:
        lines.append(f"- Validation report: `{plan.validation_report_path}`")
    if plan.checkpoints_path:
        lines.append(f"- Checkpoints: `{plan.checkpoints_path}`")
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
    source_packet_path: Optional[Path] = None,
    checkpoints_path: Optional[Path] = None,
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

    resolved_source_packet_path = source_packet_path or (notes_dir / "source-packet.md")
    _write_source_packet(resolved_source_packet_path, question, job_profile, sources)
    resolved_checkpoints_path = checkpoints_path or (notes_dir / "checkpoints.json")
    if not resolved_checkpoints_path.exists():
        resolved_checkpoints_path.write_text(
            '{"schema_version": 1, "status": "pending", "steps": []}\n',
            encoding="utf-8",
        )

    validation_path = validation_report_path or (notes_dir / "validation-report.md")
    _write_validation_report(workspace, validation_path, question, job_profile, sources, validation)
    return ResearchJobArtifacts(
        notes_dir=notes_dir,
        note_paths=note_paths + [validation_path],
        source_packet_path=resolved_source_packet_path,
        checkpoints_path=resolved_checkpoints_path,
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


def _write_source_packet(
    path: Path,
    question: str,
    job_profile: str,
    sources: Sequence[Dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Research Source Packet",
        "",
        f"Question: {question}",
        f"Job profile: {job_profile}",
        "",
        "## Retrieved Sources",
        "",
    ]
    if not sources:
        lines.append("No sources were selected for this run.")
    else:
        for source in sources:
            lines.extend(
                [
                    f"### [{source['citation']}] {source['title']}",
                    "",
                    f"- Path: `{source['path']}`",
                    f"- Source kind: `{source['source_kind']}`",
                    f"- Score: `{source.get('score', '')}`",
                    f"- Retrieval: {source.get('retrieval_reason', 'lexical match')}",
                    f"- Snippet: {source.get('snippet', '')}",
                    "",
                ]
            )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_research_execution_packets(workspace: Workspace, plan: ResearchPlan, notes_dir: Path) -> Dict[str, Path]:
    packet_dir = notes_dir / "execution-packets"
    packet_dir.mkdir(parents=True, exist_ok=True)
    packet_paths: Dict[str, Path] = {}
    for step in plan.steps:
        packet_path = packet_dir / f"{step.step_id}.md"
        packet_path.write_text(_render_research_execution_packet(plan, step), encoding="utf-8")
        packet_paths[step.step_id] = packet_path
    return packet_paths


def _render_research_execution_packet(plan: ResearchPlan, step: ResearchPlanStep) -> str:
    lines = [
        "# Research Execution Packet",
        "",
        f"Question: {plan.question}",
        f"Mode: {plan.mode}",
        f"Job profile: {plan.job_profile}",
        f"Step: {step.title}",
        f"Step id: `{step.step_id}`",
        f"Kind: `{step.kind}`",
        f"Owner: `{step.owner}`",
        f"Status: `{step.status}`",
        "",
        "## Instruction",
        "",
        step.detail,
        "",
        "## Inputs",
        "",
        f"- Primary prompt packet: `{plan.packet_path}`",
        f"- Source packet: `{plan.source_packet_path}`",
        f"- Report draft: `{plan.report_path}`",
        f"- Checkpoints: `{plan.checkpoints_path}`",
        f"- Validation report: `{plan.validation_report_path}`",
    ]
    if step.depends_on:
        lines.append(f"- Depends on: {', '.join(f'`{step_id}`' for step_id in step.depends_on)}")
    lines.extend(["", "## Expected Output", ""])
    if step.output_path:
        lines.append(f"Write or revise the step artifact at `{step.output_path}`.")
    else:
        lines.append("Return a concise status update and preserve any durable artifact named in the primary plan.")
    lines.extend(["", "## Retrieved Sources", ""])
    if not plan.sources:
        lines.append("No sources were selected for this run.")
    else:
        for source in plan.sources:
            lines.append(f"- [{source['citation']}] {source['title']} -> `{source['path']}`")
    lines.extend(
        [
            "",
            "## Grounding Rule",
            "",
            "Use only the source packet, the primary prompt packet, and the referenced workspace artifacts as evidence. Preserve inline citations like `[S1]` when drafting answer text.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_research_checkpoints(
    workspace: Workspace,
    path: Path,
    plan: ResearchPlan,
    execution_packet_paths: Optional[Dict[str, Path]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    packet_paths = execution_packet_paths or {}
    existing_payload: Dict[str, object] = {}
    existing_steps: Dict[str, Dict[str, object]] = {}
    if path.exists():
        try:
            existing_payload = read_json_manifest(path)
        except json.JSONDecodeError:
            existing_payload = {}
    for item in list(existing_payload.get("steps", [])):
        if isinstance(item, dict) and item.get("step_id") is not None:
            existing_steps[str(item["step_id"])] = item
    statuses = [step.status for step in plan.steps]
    if any(status == "failed" for status in statuses):
        overall_status = "failed"
    elif any(status == "warning" for status in statuses):
        overall_status = "warning"
    elif all(status == "completed" for status in statuses):
        overall_status = "completed"
    else:
        overall_status = "in_progress"
    payload = {
        "schema_version": 1,
        "generated_at": _optional_text(existing_payload.get("generated_at")) or utc_timestamp(),
        "updated_at": utc_timestamp(),
        "question": plan.question,
        "job_profile": plan.job_profile,
        "status": overall_status,
        "dispatch_history": [str(item) for item in list(existing_payload.get("dispatch_history", [])) if str(item).strip()],
        "steps": [
            _build_research_checkpoint_step_payload(
                workspace=workspace,
                step=step,
                execution_packet_path=packet_paths.get(step.step_id),
                existing_step=existing_steps.get(step.step_id),
            )
            for step in plan.steps
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _build_research_checkpoint_step_payload(
    workspace: Workspace,
    step: ResearchPlanStep,
    execution_packet_path: Optional[Path],
    existing_step: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    existing = existing_step or {}
    return {
        "step_id": step.step_id,
        "kind": step.kind,
        "title": step.title,
        "status": step.status,
        "owner": step.owner,
        "output_path": step.output_path,
        "execution_packet_path": workspace.relative_path(execution_packet_path) if execution_packet_path else None,
        "depends_on": step.depends_on,
        "execution_status": _optional_text(existing.get("execution_status")) or DEFAULT_RESEARCH_STEP_EXECUTION_STATUS,
        "execution_profile": _optional_text(existing.get("execution_profile")),
        "execution_output_path": _optional_text(existing.get("execution_output_path")),
        "executed_at": _optional_text(existing.get("executed_at")),
        "execution_return_code": _optional_int(existing.get("execution_return_code")),
        "review_status": _optional_text(existing.get("review_status")) or DEFAULT_RESEARCH_STEP_REVIEW_STATUS,
        "reviewed_at": _optional_text(existing.get("reviewed_at")),
        "reviewed_by": _optional_text(existing.get("reviewed_by")),
        "review_note": _optional_text(existing.get("review_note")),
    }


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


def render_research_step_status(workspace: Workspace, resume: str = "latest") -> str:
    run_manifest_path, manifest, checkpoints_path, checkpoint_payload = _load_research_checkpoint_context(workspace, resume)
    lines = [
        "# Research Step Queue",
        "",
        f"Run manifest: `{workspace.relative_path(run_manifest_path)}`",
        f"Question: {manifest.get('question', '')}",
        f"Job profile: `{manifest.get('job_profile', DEFAULT_RESEARCH_JOB_PROFILE)}`",
        f"Checkpoints: `{workspace.relative_path(checkpoints_path)}`",
        "",
        "## Steps",
        "",
    ]
    for step in list(checkpoint_payload.get("steps", [])):
        output_path = _optional_text(step.get("execution_output_path")) or _optional_text(step.get("output_path")) or "-"
        packet_path = _optional_text(step.get("execution_packet_path")) or "-"
        lines.append(
            f"- {step['step_id']} | {step['title']} | owner={step['owner']} | planned={step['status']} "
            f"| execution={_research_step_execution_status(step)} | review={_research_step_review_status(step)} "
            f"| output=`{output_path}` | packet=`{packet_path}`"
        )
    return "\n".join(lines)


def run_research_step(
    workspace: Workspace,
    resume: str,
    step_id: str,
    profile_name: str,
    output_file: Optional[Path] = None,
) -> ResearchStepExecutionResult:
    run_manifest_path, _, checkpoints_path, checkpoint_payload = _load_research_checkpoint_context(workspace, resume)
    step = _resolve_research_step_payload(checkpoint_payload, step_id)
    packet_path = _resolve_research_step_packet_path(workspace, step)

    config = workspace.load_config()
    try:
        adapter = adapter_from_config(config, profile_name)
    except AdapterError as error:
        raise ResearchError(str(error)) from error

    resolved_output_path = output_file
    if resolved_output_path is not None and not resolved_output_path.is_absolute():
        resolved_output_path = (workspace.root / resolved_output_path).resolve()
    if resolved_output_path is None:
        resolved_output_path = _workspace_path(workspace, step.get("output_path"))
    if resolved_output_path is None:
        raise ResearchError(
            f"Research step '{step_id}' does not declare an output path. Pass --output-file to capture the result."
        )

    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = adapter.run(prompt_file=packet_path, workspace_root=workspace.root, output_file=resolved_output_path)
    except OSError as error:
        raise ResearchError(f"Failed to execute adapter profile '{profile_name}': {error}") from error

    if not adapter.output_file_flag and result.stdout and result.returncode == 0:
        resolved_output_path.write_text(result.stdout, encoding="utf-8")

    step["execution_profile"] = profile_name
    step["execution_status"] = "completed" if result.returncode == 0 else "failed"
    step["execution_output_path"] = workspace.relative_path(resolved_output_path)
    step["executed_at"] = utc_timestamp()
    step["execution_return_code"] = result.returncode
    step["review_status"] = "pending_review" if result.returncode == 0 else DEFAULT_RESEARCH_STEP_REVIEW_STATUS
    step["reviewed_at"] = None
    step["reviewed_by"] = None
    step["review_note"] = None
    _write_research_checkpoint_payload(checkpoints_path, checkpoint_payload)

    append_workspace_log(
        workspace,
        operation="research-step-run",
        title=f"Executed research step {step_id}",
        details=[
            f"Profile `{profile_name}` returned code {result.returncode}.",
            f"Execution status: `{step['execution_status']}`.",
            f"Review status: `{step['review_status']}`.",
        ],
        related_paths=[
            workspace.relative_path(packet_path),
            workspace.relative_path(resolved_output_path),
            workspace.relative_path(checkpoints_path),
        ],
    )
    return ResearchStepExecutionResult(
        run_manifest_path=run_manifest_path,
        checkpoints_path=checkpoints_path,
        step_id=step_id,
        profile_name=profile_name,
        output_path=resolved_output_path,
        stdout=result.stdout,
        stderr=result.stderr,
        returncode=result.returncode,
    )


def review_research_step(
    workspace: Workspace,
    resume: str,
    step_id: str,
    review_status: str,
    reviewer: str,
    note: Optional[str] = None,
) -> ResearchStepReviewResult:
    if review_status not in VALID_RESEARCH_STEP_REVIEW_STATUSES:
        raise ResearchError(
            f"Unsupported review status '{review_status}'. Expected one of: "
            f"{', '.join(sorted(VALID_RESEARCH_STEP_REVIEW_STATUSES))}."
        )
    run_manifest_path, _, checkpoints_path, checkpoint_payload = _load_research_checkpoint_context(workspace, resume)
    step = _resolve_research_step_payload(checkpoint_payload, step_id)
    if _research_step_execution_status(step) == DEFAULT_RESEARCH_STEP_EXECUTION_STATUS:
        raise ResearchError(f"Research step '{step_id}' must be executed before it can be reviewed.")

    step["review_status"] = review_status
    step["reviewed_at"] = utc_timestamp()
    step["reviewed_by"] = reviewer
    step["review_note"] = note.strip() if note and note.strip() else None
    _write_research_checkpoint_payload(checkpoints_path, checkpoint_payload)

    related_paths = [workspace.relative_path(checkpoints_path)]
    packet_path = _workspace_path(workspace, step.get("execution_packet_path"))
    if packet_path is not None:
        related_paths.append(workspace.relative_path(packet_path))
    output_path = _workspace_path(workspace, step.get("execution_output_path") or step.get("output_path"))
    if output_path is not None:
        related_paths.append(workspace.relative_path(output_path))
    append_workspace_log(
        workspace,
        operation="research-step-review",
        title=f"Reviewed research step {step_id}",
        details=[
            f"Reviewer: `{reviewer}`.",
            f"Status: `{review_status}`.",
            f"Note: {step['review_note'] or 'none'}",
        ],
        related_paths=related_paths,
    )
    return ResearchStepReviewResult(
        run_manifest_path=run_manifest_path,
        checkpoints_path=checkpoints_path,
        step_id=step_id,
        review_status=review_status,
        reviewer=reviewer,
    )


def dispatch_research_steps(
    workspace: Workspace,
    resume: str,
    default_profile: str,
    profile_routes: Optional[Dict[str, str]] = None,
    retry_failed: bool = False,
) -> ResearchStepDispatchResult:
    routes = dict(profile_routes or {})
    run_manifest_path, manifest, checkpoints_path, checkpoint_payload = _load_research_checkpoint_context(workspace, resume)
    steps = _dispatchable_research_steps(checkpoint_payload)
    step_ids = {str(step["step_id"]) for step in steps}
    unknown_routes = sorted(set(routes) - step_ids)
    if unknown_routes:
        raise ResearchError(
            f"Unknown dispatch step route(s): {', '.join(unknown_routes)}."
        )

    steps_by_id = {
        str(step["step_id"]): step
        for step in list(checkpoint_payload.get("steps", []))
        if isinstance(step, dict) and step.get("step_id") is not None
    }
    executed_steps: List[str] = []
    skipped_steps: List[str] = []
    records: List[Dict[str, object]] = []
    failed_step: Optional[str] = None

    for step in steps:
        step_id = str(step["step_id"])
        skip_reason = _research_step_dispatch_skip_reason(step, steps_by_id, retry_failed=retry_failed)
        if skip_reason is not None:
            skipped_steps.append(step_id)
            records.append(
                {
                    "step_id": step_id,
                    "title": step.get("title", ""),
                    "status": "skipped",
                    "reason": skip_reason,
                    "profile": routes.get(step_id, default_profile),
                }
            )
            continue

        profile_name = routes.get(step_id, default_profile)
        result = run_research_step(
            workspace,
            resume=resume,
            step_id=step_id,
            profile_name=profile_name,
        )
        executed_steps.append(step_id)
        step["execution_status"] = "completed" if result.returncode == 0 else "failed"
        records.append(
            {
                "step_id": step_id,
                "title": step.get("title", ""),
                "status": "completed" if result.returncode == 0 else "failed",
                "profile": profile_name,
                "output_path": workspace.relative_path(result.output_path),
                "returncode": result.returncode,
            }
        )
        if result.returncode != 0:
            failed_step = step_id
            break

    notes_dir = _workspace_path(workspace, manifest.get("notes_dir")) or checkpoints_path.parent
    dispatch_manifest_path = _write_research_step_dispatch_manifest(
        workspace=workspace,
        notes_dir=notes_dir,
        run_manifest_path=run_manifest_path,
        question=str(manifest.get("question", "")),
        job_profile=str(manifest.get("job_profile", DEFAULT_RESEARCH_JOB_PROFILE)),
        default_profile=default_profile,
        profile_routes=routes,
        executed_steps=executed_steps,
        skipped_steps=skipped_steps,
        failed_step=failed_step,
        records=records,
    )
    latest_checkpoint_payload = read_json_manifest(checkpoints_path)
    dispatch_history = [
        str(item)
        for item in list(latest_checkpoint_payload.get("dispatch_history", []))
        if str(item).strip()
    ]
    dispatch_history.append(workspace.relative_path(dispatch_manifest_path))
    latest_checkpoint_payload["dispatch_history"] = dispatch_history
    _write_research_checkpoint_payload(checkpoints_path, latest_checkpoint_payload)

    append_workspace_log(
        workspace,
        operation="research-step-dispatch",
        title=f"Dispatched research steps for {run_manifest_path.stem}",
        details=[
            f"Executed {len(executed_steps)} step(s).",
            f"Skipped {len(skipped_steps)} step(s).",
            f"Failed step: `{failed_step}`." if failed_step else "Dispatch completed without failed steps.",
        ],
        related_paths=[
            workspace.relative_path(run_manifest_path),
            workspace.relative_path(checkpoints_path),
            workspace.relative_path(dispatch_manifest_path),
        ],
    )
    return ResearchStepDispatchResult(
        run_manifest_path=run_manifest_path,
        checkpoints_path=checkpoints_path,
        dispatch_manifest_path=dispatch_manifest_path,
        executed_steps=executed_steps,
        skipped_steps=skipped_steps,
        failed_step=failed_step,
        status="failed" if failed_step else ("noop" if not executed_steps else "completed"),
    )


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


def _optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _research_step_execution_status(step: Dict[str, object]) -> str:
    return _optional_text(step.get("execution_status")) or DEFAULT_RESEARCH_STEP_EXECUTION_STATUS


def _research_step_review_status(step: Dict[str, object]) -> str:
    return _optional_text(step.get("review_status")) or DEFAULT_RESEARCH_STEP_REVIEW_STATUS


def _dispatchable_research_steps(checkpoint_payload: Dict[str, object]) -> List[Dict[str, object]]:
    steps: List[Dict[str, object]] = []
    for step in list(checkpoint_payload.get("steps", [])):
        if isinstance(step, dict) and _is_research_step_dispatchable(step):
            steps.append(step)
    return steps


def _is_research_step_dispatchable(step: Dict[str, object]) -> bool:
    return (
        _optional_text(step.get("execution_packet_path")) is not None
        and _optional_text(step.get("output_path")) is not None
        and _optional_text(step.get("owner")) == "planner"
        and _optional_text(step.get("step_id")) != "retrieve-sources"
    )


def _research_step_dispatch_skip_reason(
    step: Dict[str, object],
    steps_by_id: Dict[str, Dict[str, object]],
    *,
    retry_failed: bool,
) -> Optional[str]:
    execution_status = _research_step_execution_status(step)
    if execution_status == "completed":
        return "already_completed"
    if execution_status == "failed" and not retry_failed:
        return "previously_failed"

    blocked_dependencies: List[str] = []
    for dependency_id in list(step.get("depends_on", [])):
        dependency = steps_by_id.get(str(dependency_id))
        if dependency is None:
            continue
        if _is_research_step_dispatchable(dependency):
            if _research_step_execution_status(dependency) != "completed":
                blocked_dependencies.append(str(dependency_id))
        elif _optional_text(dependency.get("status")) != "completed":
            blocked_dependencies.append(str(dependency_id))
    if blocked_dependencies:
        return f"waiting_on:{','.join(blocked_dependencies)}"
    return None


def _load_research_checkpoint_context(
    workspace: Workspace,
    resume: str,
) -> tuple[Path, Dict[str, object], Path, Dict[str, object]]:
    run_manifest_path = _resolve_research_manifest_path(workspace, resume)
    manifest = read_json_manifest(run_manifest_path)
    if manifest.get("run_kind") != "research":
        raise ResearchError(f"Run manifest is not a research run: {run_manifest_path}")

    checkpoints_path = _workspace_path(workspace, manifest.get("checkpoints_path"))
    if checkpoints_path is None or not checkpoints_path.exists():
        raise ResearchError(f"Research checkpoints are missing for run manifest: {run_manifest_path}")
    checkpoint_payload = read_json_manifest(checkpoints_path)
    if not isinstance(checkpoint_payload.get("steps"), list):
        raise ResearchError(f"Research checkpoints are malformed: {checkpoints_path}")
    return run_manifest_path, manifest, checkpoints_path, checkpoint_payload


def _resolve_research_step_payload(checkpoint_payload: Dict[str, object], step_id: str) -> Dict[str, object]:
    for step in list(checkpoint_payload.get("steps", [])):
        if isinstance(step, dict) and str(step.get("step_id")) == step_id:
            return step
    raise ResearchError(f"Research step '{step_id}' does not exist in this run.")


def _resolve_research_step_packet_path(workspace: Workspace, step: Dict[str, object]) -> Path:
    packet_path = _workspace_path(workspace, step.get("execution_packet_path"))
    if packet_path is None or not packet_path.exists():
        raise ResearchError(f"Execution packet is missing for research step '{step.get('step_id', '')}'.")
    return packet_path


def _write_research_checkpoint_payload(path: Path, payload: Dict[str, object]) -> None:
    payload["updated_at"] = utc_timestamp()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_research_step_dispatch_manifest(
    workspace: Workspace,
    notes_dir: Path,
    run_manifest_path: Path,
    question: str,
    job_profile: str,
    default_profile: str,
    profile_routes: Dict[str, str],
    executed_steps: List[str],
    skipped_steps: List[str],
    failed_step: Optional[str],
    records: List[Dict[str, object]],
) -> Path:
    dispatch_dir = notes_dir / "dispatch-runs"
    dispatch_dir.mkdir(parents=True, exist_ok=True)
    base_name = f"dispatch-{slugify(utc_timestamp())}"
    dispatch_manifest_path = dispatch_dir / f"{base_name}.json"
    suffix = 2
    while dispatch_manifest_path.exists():
        dispatch_manifest_path = dispatch_dir / f"{base_name}-{suffix}.json"
        suffix += 1
    payload = {
        "schema_version": 1,
        "generated_at": utc_timestamp(),
        "run_manifest_path": workspace.relative_path(run_manifest_path),
        "question": question,
        "job_profile": job_profile,
        "default_profile": default_profile,
        "step_profiles": {step_id: profile_routes.get(step_id, default_profile) for step_id in executed_steps},
        "executed_steps": executed_steps,
        "skipped_steps": skipped_steps,
        "failed_step": failed_step,
        "status": "failed" if failed_step else ("noop" if not executed_steps else "completed"),
        "results": records,
    }
    dispatch_manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return dispatch_manifest_path


def _strip_frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    return text[end + 5 :]
