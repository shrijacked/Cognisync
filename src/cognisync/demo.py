from __future__ import annotations

from pathlib import Path
from typing import Dict

from cognisync.manifests import write_workspace_manifests
from cognisync.planner import build_compile_plan, render_compile_plan
from cognisync.renderers import render_compile_packet, render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.workspace import Workspace


DEMO_QUESTION = "How should a team build an LLM-maintained research garden?"


class DemoError(RuntimeError):
    pass


def create_demo_workspace(workspace: Workspace, force: bool = False) -> Dict[str, Path]:
    if workspace.root.exists() and any(workspace.root.iterdir()) and not force:
        raise DemoError(
            f"Refusing to populate non-empty directory {workspace.root}. "
            "Re-run with --force if you want to overwrite the demo content."
        )

    workspace.initialize(name="Cognisync Demo Garden", force=force)
    _write_demo_files(workspace, force=True)

    seeded_snapshot = scan_workspace(workspace)
    write_workspace_manifests(workspace, seeded_snapshot)
    engine = SearchEngine.from_workspace(workspace, seeded_snapshot)
    hits = engine.search(DEMO_QUESTION, limit=5)
    report_path = render_query_report(workspace, DEMO_QUESTION, hits)
    slides_path = render_marp_slides(workspace, DEMO_QUESTION, hits)
    query_packet_path = render_query_packet(workspace, DEMO_QUESTION, hits)

    _write_file(
        workspace.root / "wiki" / "queries" / "research-garden-brief.md",
        (
            "# Research Garden Brief\n\n"
            "This seeded answer shows how a team can treat the wiki as the durable product surface.\n\n"
            "## Core Pattern\n\n"
            "- collect source material into `raw/`\n"
            "- compile summaries and concepts into `wiki/`\n"
            "- ask reusable questions and file the outputs back into the garden\n\n"
            "## Key Concepts\n\n"
            "- [[knowledge-gardens]]\n"
            "- [[agent-loops]]\n\n"
            "## Generated Artifacts\n\n"
            f"- [Research brief](../../{report_path.relative_to(workspace.root).as_posix()})\n"
            f"- [Slide deck](../../{slides_path.relative_to(workspace.root).as_posix()})\n"
            f"- [Prompt packet](../../{query_packet_path.relative_to(workspace.root).as_posix()})\n"
        ),
        force=force,
    )

    snapshot = scan_workspace(workspace)
    plan = build_compile_plan(snapshot)
    workspace.write_plan_json("compile-plan", plan)
    plan_path = workspace.plans_dir / "compile-plan.md"
    plan_path.write_text(render_compile_plan(plan), encoding="utf-8")
    compile_packet_path = render_compile_packet(workspace, plan)

    final_snapshot = scan_workspace(workspace)
    workspace.write_index(final_snapshot)
    write_workspace_manifests(workspace, final_snapshot)

    return {
        "report": report_path,
        "slides": slides_path,
        "query_packet": query_packet_path,
        "compile_packet": compile_packet_path,
        "index": workspace.index_path,
    }


def _write_demo_files(workspace: Workspace, force: bool) -> None:
    for relative_path, content in _demo_file_map().items():
        _write_file(workspace.root / relative_path, content, force=force)


def _write_file(path: Path, content: str, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        return
    path.write_text(content, encoding="utf-8")


def _demo_file_map() -> Dict[str, str]:
    return {
        "README.md": (
            "# Research Garden Demo\n\n"
            "This directory is a seeded Cognisync workspace that demonstrates the full filesystem\n"
            "contract in a compact form.\n\n"
            "## Start Here\n\n"
            "- `raw/` contains the source notes\n"
            "- `wiki/sources/` contains compiled summaries\n"
            "- `wiki/concepts/` contains cross-source synthesis\n"
            "- `wiki/queries/` shows how an answer can be filed back into the garden\n"
            "- `outputs/` and `prompts/` show the generated artifacts\n"
        ),
        "raw/agentic-workflows.md": (
            "---\n"
            "title: Agentic Workflows\n"
            "tags: [agents, orchestration, loops]\n"
            "---\n"
            "# Agentic Workflows\n\n"
            "Agentic workflows split research into planning, execution, and synthesis loops.\n"
            "The most reliable teams keep the intermediate artifacts inspectable so the model can\n"
            "revisit its own trail of evidence and repair weak conclusions.\n\n"
            "These workflows become much more useful when the outputs are filed into a durable\n"
            "workspace instead of disappearing into a transient chat session.\n"
        ),
        "raw/knowledge-gardens.md": (
            "---\n"
            "title: Knowledge Gardens\n"
            "tags: [knowledge-gardens, markdown, obsidian]\n"
            "---\n"
            "# Knowledge Gardens\n\n"
            "A knowledge garden is a living wiki that grows through repeated synthesis rather than\n"
            "one-off note taking. Markdown files, backlinks, and local media make the corpus easy to\n"
            "inspect in tools like Obsidian while staying friendly to automated agents.\n"
        ),
        "raw/evaluation-loops.md": (
            "---\n"
            "title: Evaluation Loops\n"
            "tags: [evaluation, loops, reliability]\n"
            "---\n"
            "# Evaluation Loops\n\n"
            "Evaluation loops help a team detect broken links, missing summaries, stale claims, and\n"
            "weakly supported conclusions. The best loop writes its own follow-up tasks back into the\n"
            "workspace so quality improves over time.\n"
        ),
        "wiki/index.md": (
            "# Knowledge Base Index\n\n"
            "Cognisync demo garden for an LLM-maintained research workflow.\n\n"
            "## Entry Points\n\n"
            "- [Sources](sources.md)\n"
            "- [Concepts](concepts.md)\n"
            "- [Queries](queries.md)\n"
            "- [[knowledge-gardens]]\n"
            "- [[agent-loops]]\n"
        ),
        "wiki/sources.md": (
            "# Sources\n\n"
            "- [Agentic Workflows](sources/agentic-workflows.md)\n"
            "- [Knowledge Gardens](sources/knowledge-gardens.md)\n"
            "- [Evaluation Loops](sources/evaluation-loops.md)\n"
        ),
        "wiki/concepts.md": (
            "# Concepts\n\n"
            "- [Knowledge Gardens](concepts/knowledge-gardens.md)\n"
            "- [Agent Loops](concepts/agent-loops.md)\n"
        ),
        "wiki/queries.md": (
            "# Queries\n\n"
            "- [[research-garden-brief]]\n"
        ),
        "wiki/sources/agentic-workflows.md": (
            "# Agentic Workflows\n\n"
            "This source argues that LLM workflows become more reliable when they externalize their\n"
            "thinking into inspectable artifacts instead of hiding everything inside context windows.\n\n"
            "## Highlights\n\n"
            "- planning and synthesis are separate but connected loops\n"
            "- intermediate artifacts make review and recovery easier\n"
            "- durable outputs are a prerequisite for reusable research automation\n\n"
            "## Related Concepts\n\n"
            "- [[agent-loops]]\n"
            "- [[knowledge-gardens]]\n\n"
            "## Source File\n\n"
            "- [Raw source](../../raw/agentic-workflows.md)\n"
        ),
        "wiki/sources/knowledge-gardens.md": (
            "# Knowledge Gardens\n\n"
            "This source frames the knowledge base itself as the product surface. Markdown files,\n"
            "backlinks, and local assets make the system legible to both humans and LLMs.\n\n"
            "## Highlights\n\n"
            "- files are durable, inspectable, and versionable\n"
            "- local-first tools like Obsidian stay useful because the wiki remains plain text\n"
            "- new answers can be filed back into the same corpus\n\n"
            "## Related Concepts\n\n"
            "- [[knowledge-gardens]]\n"
            "- [[agent-loops]]\n\n"
            "## Source File\n\n"
            "- [Raw source](../../raw/knowledge-gardens.md)\n"
        ),
        "wiki/sources/evaluation-loops.md": (
            "# Evaluation Loops\n\n"
            "This source emphasizes that a knowledge garden needs maintenance passes, not just data\n"
            "ingestion. Health checks, missing-data detection, and follow-up tasks make the corpus more\n"
            "useful over time.\n\n"
            "## Highlights\n\n"
            "- linting is part of the product, not just a developer convenience\n"
            "- every issue can become a new compile or research task\n"
            "- quality improves when checks run repeatedly instead of once\n\n"
            "## Related Concepts\n\n"
            "- [[agent-loops]]\n\n"
            "## Source File\n\n"
            "- [Raw source](../../raw/evaluation-loops.md)\n"
        ),
        "wiki/concepts/knowledge-gardens.md": (
            "# Knowledge Garden Pattern\n\n"
            "A knowledge garden is a file-backed research environment where raw material, compiled wiki\n"
            "pages, and generated outputs all live together. The advantage is not just retrieval. The\n"
            "advantage is compounding structure: each pass leaves behind better summaries, links, and\n"
            "questions for the next pass.\n\n"
            "## Signals In This Demo\n\n"
            "- [[Agentic Workflows]] show why durable artifacts matter\n"
            "- [[Knowledge Gardens]] explains why Markdown and backlinks are the right substrate\n"
            "- [[Evaluation Loops]] turns maintenance into an explicit workflow\n"
        ),
        "wiki/concepts/agent-loops.md": (
            "# Agent Loops\n\n"
            "Agent loops are repeatable cycles of planning, searching, synthesizing, and checking. In a\n"
            "research garden they work best when every loop writes back new structure: summaries,\n"
            "concept pages, query reports, and quality issues.\n\n"
            "## Supporting Sources\n\n"
            "- [[Agentic Workflows]]\n"
            "- [[Evaluation Loops]]\n"
            "- [[Knowledge Gardens]]\n"
        ),
    }
