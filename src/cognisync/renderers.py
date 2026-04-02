from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional

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
) -> Path:
    output_path = workspace.prompts_dir / f"query-{slugify(question)}.md"
    hit_list = list(hits)
    artifact_map = _artifact_map(snapshot)
    mode_instruction = RESEARCH_MODE_INSTRUCTIONS.get(mode, RESEARCH_MODE_INSTRUCTIONS["wiki"])
    lines = [
        "# Query Packet",
        "",
        f"Question: {question}",
        f"Output mode: {mode}",
        "",
        "## Instructions",
        "",
        "Answer the question using the listed workspace sources.",
        mode_instruction,
        "Cite evidence inline as [S1], [S2], and suggest follow-up pages or concepts when useful.",
        "Do not invent citations. Every citation must map to one of the listed sources.",
        "",
        "## Source Context",
        "",
    ]
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
