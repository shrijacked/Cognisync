from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional

from cognisync.graph_intelligence import extract_claim_tuples
from cognisync.planner import render_compile_plan
from cognisync.types import ArtifactRecord, CompilePlan, IndexSnapshot, SearchHit
from cognisync.utils import relative_markdown_path, slugify, utc_timestamp
from cognisync.workspace import Workspace


TEXTUAL_KINDS = {"markdown", "text", "data", "code"}
RESEARCH_MODE_INSTRUCTIONS = {
    "brief": "Write the final answer as a concise research brief with crisp findings and cited bullets.",
    "memo": "Write the final answer as a research memo with sections for findings, evidence, and follow-up work.",
    "report": "Write the final answer as a polished research report with clear sections and explicit inline citations.",
    "slides": "Write the final answer as a Marp slide deck with inline citations in slide bullets or notes.",
    "wiki": "Write the final answer as a reusable wiki page that can be filed back into `wiki/queries/`.",
}
RESEARCH_JOB_PROFILE_INSTRUCTIONS = {
    "synthesis-report": (
        "Work like a synthesis lead: map the working set, capture open questions, and converge on a tight outline "
        "before answering."
    ),
    "literature-review": (
        "Work like a literature review agent: compare sources explicitly, build a paper matrix, and call out gaps "
        "or disagreements."
    ),
    "repo-analysis": (
        "Work like a codebase analyst: isolate key modules, interfaces, and risks before making claims about the repo."
    ),
    "contradiction-finding": (
        "Work like a contradiction analyst: surface competing claims, preserve both sides, and avoid collapsing "
        "disagreements prematurely."
    ),
    "market-scan": (
        "Work like a market scan operator: compare alternatives, track positioning signals, and separate evidence "
        "from interpretation."
    ),
}


def _literalize_snippet(text: str) -> str:
    return text.replace("[", "&#91;").replace("]", "&#93;")


def _strip_frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    return text[end + 5 :]


def _compact_excerpt(text: str, limit: int = 500) -> str:
    compact = " ".join(_strip_frontmatter(text).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _artifact_map(snapshot: Optional[IndexSnapshot]) -> Dict[str, ArtifactRecord]:
    if snapshot is None:
        return {}
    return {artifact.path: artifact for artifact in snapshot.artifacts}


def _fact_blocks_for_hits(workspace: Workspace, hits: List[SearchHit]) -> List[Dict[str, object]]:
    support: Dict[tuple[str, str, str], Dict[str, object]] = {}
    for index, hit in enumerate(hits, start=1):
        citation = f"S{index}"
        path = workspace.root / hit.path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for subject, verb, obj in extract_claim_tuples(text):
            key = (subject, verb, obj)
            entry = support.setdefault(
                key,
                {
                    "subject": subject,
                    "verb": verb,
                    "object": obj,
                    "citations": [],
                    "paths": [],
                },
            )
            if citation not in entry["citations"]:
                entry["citations"].append(citation)
            if hit.path not in entry["paths"]:
                entry["paths"].append(hit.path)
    fact_blocks = list(support.values())
    fact_blocks.sort(key=lambda item: (-len(item["citations"]), item["subject"], item["verb"], item["object"]))
    return fact_blocks


def _input_context_lines(workspace: Workspace, plan: CompilePlan, snapshot: IndexSnapshot) -> List[str]:
    artifact_map = _artifact_map(snapshot)
    seen = set()
    lines = ["## Input Context", ""]
    for task in plan.tasks:
        for input_path in task.inputs:
            if input_path in seen:
                continue
            seen.add(input_path)
            artifact = artifact_map.get(input_path)
            lines.append(f"### {input_path}")
            lines.append("")
            if artifact is not None:
                lines.append(f"- Title: `{artifact.title}`")
                lines.append(f"- Kind: `{artifact.kind}`")
                lines.append(f"- Word count: `{artifact.word_count}`")
                if artifact.tags:
                    lines.append(f"- Tags: {', '.join(f'`#{tag}`' for tag in artifact.tags)}")
                if artifact.images:
                    lines.append(f"- Embedded images: {', '.join(f'`{image}`' for image in artifact.images[:5])}")
            source_path = workspace.root / input_path
            if artifact is not None and artifact.kind in TEXTUAL_KINDS and source_path.exists():
                lines.extend(["", "```md", _compact_excerpt(source_path.read_text(encoding='utf-8', errors='ignore')), "```"])
            lines.append("")
    if len(lines) == 2:
        lines.extend(["No additional input context is available.", ""])
    return lines


def render_query_report(
    workspace: Workspace,
    question: str,
    hits: Iterable[SearchHit],
    snapshot: Optional[IndexSnapshot] = None,
) -> Path:
    output_path = workspace.outputs_dir / "reports" / f"{slugify(question)}.md"
    lines = [
        f"# Research Brief: {question}",
        "",
        f"Generated: {utc_timestamp()}",
        "",
        "## Evidence Summary",
        "",
    ]
    hit_list = list(hits)
    artifact_map = _artifact_map(snapshot)
    if not hit_list:
        lines.extend(
            [
                "No relevant sources were found in the current workspace.",
                "",
            ]
        )
    else:
        for index, hit in enumerate(hit_list, start=1):
            citation = f"S{index}"
            lines.extend(
                [
                    f"- [{citation}] {hit.title}: {_literalize_snippet(hit.snippet)}",
                    "",
                ]
            )

        fact_blocks = _fact_blocks_for_hits(workspace, hit_list)
        lines.extend(["## Fact Blocks", ""])
        if not fact_blocks:
            lines.extend(["No source-backed fact blocks were extracted from the retrieved hits.", ""])
        else:
            for block in fact_blocks:
                claim = f"{block['subject']} {block['verb']} {block['object']}"
                display_claim = claim[:1].upper() + claim[1:]
                lines.extend(
                    [
                        f"### {display_claim}",
                        "",
                        f"- Claim: `{claim}`",
                        f"- Supported by: {', '.join(f'[{citation}]' for citation in block['citations'])}",
                        f"- Support count: `{len(block['citations'])}`",
                        "",
                    ]
                )

        lines.extend(["## Source Blocks", ""])
        for index, hit in enumerate(hit_list, start=1):
            citation = f"S{index}"
            target = workspace.root / hit.path
            artifact = artifact_map.get(hit.path)
            lines.extend(
                [
                    f"### [{citation}] {hit.title}",
                    "",
                    f"- Source: [{hit.path}]({relative_markdown_path(output_path, target)})",
                    f"- Source kind: `{hit.source_kind}`",
                    f"- Score: `{hit.score}`",
                    f"- Snippet: {_literalize_snippet(hit.snippet)}",
                ]
            )
            if hit.retrieval_reason:
                lines.append(f"- Retrieval: {hit.retrieval_reason}")
            if artifact is not None and artifact.tags:
                lines.append(f"- Tags: {', '.join(f'`#{tag}`' for tag in artifact.tags)}")
            if artifact is not None and artifact.images:
                lines.append(f"- Embedded images: {', '.join(f'`{image}`' for image in artifact.images[:5])}")
            lines.extend(
                [
                    "",
                ]
            )

    lines.extend(
        [
            "## Suggested Workflow",
            "",
            "- Use the prompt packet in `prompts/` to hand the question to an external LLM.",
            "- File the resulting answer into `wiki/queries/` or `outputs/reports/`.",
            "- Re-run `cognisync lint` and `cognisync plan` after incorporating new findings.",
            "",
        ]
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def render_marp_slides(workspace: Workspace, question: str, hits: Iterable[SearchHit]) -> Path:
    output_path = workspace.outputs_dir / "slides" / f"{slugify(question)}.md"
    hit_list = list(hits)
    lines = [
        "---",
        "marp: true",
        "theme: gaia",
        "paginate: true",
        "---",
        "",
        f"# {question}",
        "",
        "Cognisync generated briefing deck",
        "",
        "---",
        "",
        "## Top Sources",
        "",
    ]
    if not hit_list:
        lines.extend(["No relevant sources found.", ""])
    else:
        for hit in hit_list:
            lines.append(f"- {hit.title}")
        lines.extend(["", "---", ""])
        for hit in hit_list:
            target = workspace.root / hit.path
            lines.extend(
                [
                    f"## {hit.title}",
                    "",
                    _literalize_snippet(hit.snippet),
                    "",
                    f"[Open source]({relative_markdown_path(output_path, target)})",
                    "",
                    "---",
                    "",
                ]
            )
    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output_path


def render_query_packet(
    workspace: Workspace,
    question: str,
    hits: Iterable[SearchHit],
    snapshot: Optional[IndexSnapshot] = None,
    mode: str = "wiki",
    job_profile: str = "synthesis-report",
    note_paths: Optional[Iterable[str]] = None,
) -> Path:
    output_path = workspace.prompts_dir / f"query-{slugify(question)}.md"
    hit_list = list(hits)
    artifact_map = _artifact_map(snapshot)
    mode_instruction = RESEARCH_MODE_INSTRUCTIONS.get(mode, RESEARCH_MODE_INSTRUCTIONS["wiki"])
    profile_instruction = RESEARCH_JOB_PROFILE_INSTRUCTIONS.get(
        job_profile, RESEARCH_JOB_PROFILE_INSTRUCTIONS["synthesis-report"]
    )
    lines = [
        "# Query Packet",
        "",
        f"Question: {question}",
        f"Output mode: {mode}",
        f"Research job profile: {job_profile}",
        "",
        "## Instructions",
        "",
        "Answer the question using the listed workspace sources.",
        mode_instruction,
        profile_instruction,
        "Cite evidence inline as [S1], [S2], and suggest follow-up pages or concepts when useful.",
        "Do not invent citations. Every citation must map to one of the listed sources.",
        "",
    ]
    normalized_note_paths = [str(path).strip() for path in list(note_paths or []) if str(path).strip()]
    fact_blocks = _fact_blocks_for_hits(workspace, hit_list)
    if normalized_note_paths:
        lines.extend(
            [
                "## Intermediate Artifacts",
                "",
                "Use the following job notes as intermediate checkpoints:",
                "",
            ]
        )
        lines.extend(f"- `{path}`" for path in normalized_note_paths)
        lines.extend(["", "## Fact Blocks", ""])
    else:
        lines.extend(["## Fact Blocks", ""])
    if not fact_blocks:
        lines.extend(["No source-backed fact blocks were extracted from the retrieved hits.", "", "## Source Context", ""])
    else:
        for block in fact_blocks:
            claim = f"{block['subject']} {block['verb']} {block['object']}"
            lines.extend(
                [
                    f"- `{claim}` supported by {', '.join(f'[{citation}]' for citation in block['citations'])}",
                ]
            )
        lines.extend(["", "## Source Context", ""])
    if not hit_list:
        lines.extend(["No relevant sources were found by the deterministic search pass.", ""])
    else:
        for index, hit in enumerate(hit_list, start=1):
            target = workspace.root / hit.path
            artifact = artifact_map.get(hit.path)
            lines.extend(
                [
                    f"### [S{index}] {hit.title}",
                    "",
                    f"- Source: [{hit.path}]({relative_markdown_path(output_path, target)})",
                    f"- Source kind: `{hit.source_kind}`",
                    f"- Score: `{hit.score}`",
                    f"- Snippet: {_literalize_snippet(hit.snippet)}",
                ]
            )
            if hit.retrieval_reason:
                lines.append(f"- Retrieval: {hit.retrieval_reason}")
            if artifact is not None and artifact.images:
                lines.append(f"- Embedded images: {', '.join(f'`{image}`' for image in artifact.images[:5])}")
            lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def render_compile_packet(workspace: Workspace, plan: CompilePlan, snapshot: Optional[IndexSnapshot] = None) -> Path:
    output_path = workspace.prompts_dir / "compile-plan.md"
    lines = [
        "# Compile Packet",
        "",
        "Execute the following tasks against the workspace.",
        "Write changes directly into `wiki/` and `outputs/` where appropriate.",
        "",
        render_compile_plan(plan),
        "",
    ]
    if snapshot is not None:
        lines.extend(_input_context_lines(workspace, plan, snapshot))
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path
