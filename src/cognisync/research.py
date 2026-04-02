from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from cognisync.adapters import AdapterError, adapter_from_config
from cognisync.renderers import render_marp_slides, render_query_packet, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.utils import slugify
from cognisync.workspace import Workspace


class ResearchError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResearchRunResult:
    report_path: Path
    packet_path: Path
    answer_path: Optional[Path]
    slide_path: Optional[Path]
    hit_count: int
    ran_profile: bool


def run_research_cycle(
    workspace: Workspace,
    question: str,
    limit: int = 5,
    profile_name: Optional[str] = None,
    output_file: Optional[Path] = None,
    slides: bool = False,
) -> ResearchRunResult:
    snapshot = scan_workspace(workspace)
    workspace.write_index(snapshot)

    engine = SearchEngine.from_workspace(workspace, snapshot)
    hits = engine.search(question, limit=limit)

    report_path = render_query_report(workspace, question, hits, snapshot=snapshot)
    packet_path = render_query_packet(workspace, question, hits, snapshot=snapshot)
    slide_path = render_marp_slides(workspace, question, hits) if slides else None

    answer_path = None
    ran_profile = False
    if profile_name:
        config = workspace.load_config()
        try:
            adapter = adapter_from_config(config, profile_name)
        except AdapterError as error:
            raise ResearchError(str(error)) from error

        answer_path = output_file or workspace.wiki_dir / "queries" / f"{slugify(question)}.md"
        answer_path.parent.mkdir(parents=True, exist_ok=True)
        result = adapter.run(prompt_file=packet_path, workspace_root=workspace.root, output_file=answer_path)
        if result.returncode != 0:
            raise ResearchError(f"Adapter '{profile_name}' exited with code {result.returncode}.")
        if not adapter.output_file_flag and result.stdout:
            answer_path.write_text(result.stdout, encoding="utf-8")
        ran_profile = True

    final_snapshot = scan_workspace(workspace)
    workspace.write_index(final_snapshot)
    return ResearchRunResult(
        report_path=report_path,
        packet_path=packet_path,
        answer_path=answer_path,
        slide_path=slide_path,
        hit_count=len(hits),
        ran_profile=ran_profile,
    )
