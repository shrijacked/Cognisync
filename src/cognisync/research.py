from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Optional

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.manifests import write_run_manifest, write_workspace_manifests
from cognisync.renderers import render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.types import SearchHit
from cognisync.utils import slugify
from cognisync.workspace import Workspace


class ResearchError(RuntimeError):
    pass


CITATION_RE = re.compile(r"\[(S\d+)\]")
RESEARCH_OUTPUT_MODES = {"brief", "memo", "report", "slides", "wiki"}


@dataclass(frozen=True)
class ResearchRunResult:
    report_path: Path
    packet_path: Path
    answer_path: Optional[Path]
    slide_path: Optional[Path]
    run_manifest_path: Path
    hit_count: int
    ran_profile: bool


def run_research_cycle(
    workspace: Workspace,
    question: str,
    limit: int = 5,
    profile_name: Optional[str] = None,
    output_file: Optional[Path] = None,
    slides: bool = False,
    mode: str = "wiki",
) -> ResearchRunResult:
    if mode not in RESEARCH_OUTPUT_MODES:
        raise ResearchError(
            f"Unsupported research mode '{mode}'. Expected one of: {', '.join(sorted(RESEARCH_OUTPUT_MODES))}."
        )

    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)
    write_workspace_manifests(workspace, snapshot)

    engine = SearchEngine.from_workspace(workspace, snapshot)
    hits = engine.search(question, limit=limit)

    report_path = render_query_report(workspace, question, hits, snapshot=snapshot)
    packet_path = render_query_packet(workspace, question, hits, snapshot=snapshot, mode=mode)
    slide_path = render_marp_slides(workspace, question, hits) if slides or mode == "slides" else None

    answer_path = None
    ran_profile = False
    validation = {
        "passed": True,
        "errors": [],
    }
    citations = {
        "used": [],
        "available": [f"S{index}" for index in range(1, len(hits) + 1)],
    }
    if profile_name:
        config = workspace.load_config()
        try:
            adapter = adapter_from_config(config, profile_name)
        except AdapterError as error:
            raise ResearchError(str(error)) from error

        answer_path = output_file or _default_answer_path(workspace, question, mode)
        answer_path.parent.mkdir(parents=True, exist_ok=True)
        result = adapter.run(prompt_file=packet_path, workspace_root=workspace.root, output_file=answer_path)
        if result.returncode != 0:
            raise ResearchError(f"Adapter '{profile_name}' exited with code {result.returncode}.")
        if not adapter.output_file_flag and result.stdout:
            answer_path.write_text(result.stdout, encoding="utf-8")
        if answer_path.exists():
            answer_text = answer_path.read_text(encoding="utf-8", errors="ignore")
            validation = _validate_citations(answer_text, hits)
            citations["used"] = validation.get("used", [])
        ran_profile = True

    final_snapshot = scan_workspace(workspace)
    workspace.write_index(final_snapshot)
    write_workspace_manifests(workspace, final_snapshot)
    run_manifest_path = write_run_manifest(
        workspace,
        "research",
        {
            "run_label": question,
            "question": question,
            "mode": mode,
            "profile": profile_name,
            "report_path": workspace.relative_path(report_path),
            "packet_path": workspace.relative_path(packet_path),
            "answer_path": workspace.relative_path(answer_path) if answer_path else None,
            "slide_path": workspace.relative_path(slide_path) if slide_path else None,
            "status": "failed_validation" if not validation["passed"] else "completed",
            "sources": [_hit_to_manifest_entry(hit, index) for index, hit in enumerate(hits, start=1)],
            "citations": citations,
            "validation": {
                "passed": validation["passed"],
                "errors": validation["errors"],
            },
        },
    )
    if profile_name and not validation["passed"]:
        raise ResearchError("Citation validation failed: " + "; ".join(validation["errors"]))
    return ResearchRunResult(
        report_path=report_path,
        packet_path=packet_path,
        answer_path=answer_path,
        slide_path=slide_path,
        run_manifest_path=run_manifest_path,
        hit_count=len(hits),
        ran_profile=ran_profile,
    )


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


def _validate_citations(answer_text: str, hits) -> dict:
    available = {f"S{index}" for index in range(1, len(hits) + 1)}
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
