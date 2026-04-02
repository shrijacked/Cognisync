from __future__ import annotations

from pathlib import Path
from typing import Iterable

from cognisync.planner import render_compile_plan
from cognisync.types import CompilePlan, SearchHit
from cognisync.utils import relative_markdown_path, slugify, utc_timestamp
from cognisync.workspace import Workspace


def render_query_report(workspace: Workspace, question: str, hits: Iterable[SearchHit]) -> Path:
    output_path = workspace.outputs_dir / "reports" / f"{slugify(question)}.md"
    lines = [
        f"# Research Brief: {question}",
        "",
        f"Generated: {utc_timestamp()}",
        "",
        "## Top Sources",
        "",
    ]
    hit_list = list(hits)
    if not hit_list:
        lines.extend(
            [
                "No relevant sources were found in the current workspace.",
                "",
            ]
        )
    else:
        for index, hit in enumerate(hit_list, start=1):
            target = workspace.root / hit.path
            lines.extend(
                [
                    f"{index}. [{hit.title}]({relative_markdown_path(output_path, target)})",
                    f"   Score: {hit.score}",
                    f"   Snippet: {hit.snippet}",
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
                    hit.snippet,
                    "",
                    f"[Open source]({relative_markdown_path(output_path, target)})",
                    "",
                    "---",
                    "",
                ]
            )
    output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output_path


def render_query_packet(workspace: Workspace, question: str, hits: Iterable[SearchHit]) -> Path:
    output_path = workspace.prompts_dir / f"query-{slugify(question)}.md"
    hit_list = list(hits)
    lines = [
        "# Query Packet",
        "",
        f"Question: {question}",
        "",
        "## Instructions",
        "",
        "Answer the question using the listed workspace sources.",
        "Write the final answer as Markdown that can be filed back into the knowledge base.",
        "Preserve source attributions and suggest follow-up pages or concepts when useful.",
        "",
        "## Source Context",
        "",
    ]
    if not hit_list:
        lines.extend(["No relevant sources were found by the deterministic search pass.", ""])
    else:
        for hit in hit_list:
            target = workspace.root / hit.path
            lines.extend(
                [
                    f"- [{hit.title}]({relative_markdown_path(output_path, target)})",
                    f"  - Score: {hit.score}",
                    f"  - Snippet: {hit.snippet}",
                ]
            )
        lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def render_compile_packet(workspace: Workspace, plan: CompilePlan) -> Path:
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
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path
