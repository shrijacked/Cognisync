from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from cognisync.graph_intelligence import build_concept_candidates
from cognisync.linter import lint_snapshot
from cognisync.types import CompilePlan, IndexSnapshot, PlanTask
from cognisync.utils import utc_timestamp


def build_compile_plan(snapshot: IndexSnapshot) -> CompilePlan:
    tasks: List[PlanTask] = []
    existing_paths = set(snapshot.artifact_paths())

    for artifact in snapshot.artifacts:
        if artifact.collection != "raw" or not artifact.summary_target:
            continue
        if artifact.summary_target in existing_paths:
            continue
        tasks.append(
            PlanTask(
                task_id=f"summarize:{artifact.path}",
                kind="summarize_source",
                title=f"Summarize {artifact.title}",
                inputs=[artifact.path],
                output_path=artifact.summary_target,
                rationale="This raw source has no compiled summary page in the wiki yet.",
                prompt_hint="Write a source summary with key points, quotes, backlinks, and follow-up questions.",
            )
        )

    for candidate in build_concept_candidates(snapshot):
        output_path = str(candidate["output_path"])
        if output_path in existing_paths or candidate.get("resolved"):
            continue
        tasks.append(
            PlanTask(
                task_id=str(candidate["id"]),
                kind="create_concept_page",
                title=f"Create concept page for {candidate['title']}",
                inputs=list(candidate["support_paths"]),
                output_path=output_path,
                rationale=(
                    "This concept candidate appears across multiple sources but has no dedicated concept page."
                ),
                prompt_hint="Synthesize the recurring concept, cite supporting sources, and add backlinks.",
            )
        )

    repair_targets: Dict[str, List[str]] = defaultdict(list)
    for issue in lint_snapshot(snapshot):
        if issue.kind == "broken_link":
            repair_targets[issue.path].append(issue.message)
        elif issue.kind == "stale_summary":
            source_path = next(
                (
                    artifact.path
                    for artifact in snapshot.artifacts
                    if artifact.collection == "raw" and artifact.summary_target == issue.path
                ),
                issue.path,
            )
            tasks.append(
                PlanTask(
                    task_id=f"refresh-summary:{issue.path}",
                    kind="refresh_source_summary",
                    title=f"Refresh summary {issue.path}",
                    inputs=[source_path, issue.path],
                    output_path=issue.path,
                    rationale=(
                        f"Refresh `{issue.path}` from changed source `{source_path}` because the source is newer "
                        "than the compiled summary."
                    ),
                    prompt_hint=(
                        f"Refresh the compiled summary from `{source_path}`. Preserve backlinks in `{issue.path}` "
                        "while updating claims, citations, and follow-up questions."
                    ),
                )
            )
        elif issue.kind == "orphan_page":
            tasks.append(
                PlanTask(
                    task_id=f"connect:{issue.path}",
                    kind="connect_orphan_page",
                    title=f"Connect orphan page {issue.path}",
                    inputs=[issue.path],
                    output_path=issue.path,
                    rationale=issue.message,
                    prompt_hint="Add backlinks from relevant index, source, or concept pages.",
                )
            )

    for path, messages in sorted(repair_targets.items()):
        tasks.append(
            PlanTask(
                task_id=f"repair-links:{path}",
                kind="repair_broken_links",
                title=f"Repair broken links in {path}",
                inputs=[path],
                output_path=path,
                rationale="Broken internal links were detected in this page.",
                prompt_hint="Fix or replace broken links and preserve the document's intent.",
            )
        )

    tasks.sort(key=lambda task: (task.kind, task.output_path, task.task_id))
    return CompilePlan(generated_at=utc_timestamp(), tasks=tasks)


def render_compile_plan(plan: CompilePlan) -> str:
    lines = [
        "# Compile Plan",
        "",
        f"Generated: {plan.generated_at}",
        "",
    ]
    if not plan.tasks:
        lines.extend(
            [
                "No compile tasks are currently pending.",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(["## Tasks", ""])
    for task in plan.tasks:
        lines.extend(
            [
                f"### {task.title}",
                "",
                f"- Kind: `{task.kind}`",
                f"- Output: `{task.output_path}`",
                f"- Inputs: {', '.join(f'`{item}`' for item in task.inputs)}",
                f"- Why: {task.rationale}",
                f"- Prompt Hint: {task.prompt_hint}",
                "",
            ]
        )
    return "\n".join(lines)
