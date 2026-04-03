from __future__ import annotations

from collections import defaultdict
from typing import List, Optional

from cognisync.review_queue import build_review_queue, canonicalize_review_label
from cognisync.types import IndexSnapshot, LintIssue
from cognisync.workspace import Workspace


def lint_snapshot(snapshot: IndexSnapshot, workspace: Optional[Workspace] = None) -> List[LintIssue]:
    issues: List[LintIssue] = []
    existing_paths = set(snapshot.artifact_paths())

    for artifact in snapshot.artifacts:
        for index, link in enumerate(artifact.links):
            if link.external or not link.resolved_path:
                continue
            if link.resolved_path not in existing_paths:
                issues.append(
                    LintIssue(
                        issue_id=f"broken-link:{artifact.path}:{index}",
                        kind="broken_link",
                        severity="error",
                        path=artifact.path,
                        message=f"Broken internal link '{link.raw_target}' in {artifact.path}.",
                        suggestion="Update the link target or create the missing page.",
                    )
                )

    for artifact in snapshot.artifacts:
        if artifact.collection == "raw" and artifact.summary_target and artifact.summary_target not in existing_paths:
            issues.append(
                LintIssue(
                    issue_id=f"missing-summary:{artifact.path}",
                    kind="missing_summary",
                    severity="warning",
                    path=artifact.path,
                    message=f"Missing summary page for raw source {artifact.path}.",
                    suggestion=f"Create {artifact.summary_target} to summarize the source.",
                )
            )

    titles = defaultdict(list)
    for artifact in snapshot.artifacts:
        if artifact.collection != "wiki" or artifact.kind != "markdown":
            continue
        titles[artifact.title.strip().lower()].append(artifact.path)

    for title, paths in titles.items():
        if title and len(paths) > 1:
            for path in paths:
                issues.append(
                    LintIssue(
                        issue_id=f"duplicate-title:{title}:{path}",
                        kind="duplicate_title",
                        severity="warning",
                        path=path,
                        message=f"Duplicate title '{title}' appears in multiple wiki pages.",
                        suggestion="Rename or merge duplicate pages so links stay unambiguous.",
                    )
                )

    for artifact in snapshot.artifacts:
        if artifact.collection != "wiki" or artifact.kind != "markdown":
            continue
        if artifact.path == "wiki/index.md":
            continue
        if snapshot.backlinks.get(artifact.path):
            continue
        issues.append(
            LintIssue(
                issue_id=f"orphan-page:{artifact.path}",
                kind="orphan_page",
                severity="info",
                path=artifact.path,
                message=f"Wiki page {artifact.path} has no backlinks.",
                suggestion="Link the page from index pages, source summaries, or concept articles.",
            )
        )

    if workspace is not None:
        issues.extend(_lint_graph_integrity(snapshot, workspace))

    return sorted(issues, key=lambda issue: (issue.severity, issue.kind, issue.path, issue.issue_id))


def _lint_graph_integrity(snapshot: IndexSnapshot, workspace: Workspace) -> List[LintIssue]:
    issues: List[LintIssue] = []

    for artifact in snapshot.artifacts:
        if artifact.collection != "raw":
            continue
        if artifact.kind not in {"markdown", "text", "data", "code"}:
            continue
        if artifact.tags or artifact.headings:
            continue
        issues.append(
            LintIssue(
                issue_id=f"missing-metadata:{artifact.path}",
                kind="missing_metadata",
                severity="warning",
                path=artifact.path,
                message=f"Raw source {artifact.path} lacks both tags and headings.",
                suggestion="Add frontmatter tags or a top-level heading so retrieval and graph maintenance have metadata to work with.",
            )
        )

    concept_paths = defaultdict(list)
    for artifact in snapshot.artifacts:
        if artifact.collection != "wiki" or artifact.kind != "markdown":
            continue
        if not artifact.path.startswith("wiki/concepts/"):
            continue
        canonical = canonicalize_review_label(artifact.title)
        if canonical:
            concept_paths[canonical].append(artifact.path)

    for canonical, paths in concept_paths.items():
        if len(paths) < 2:
            continue
        for path in sorted(paths):
            issues.append(
                LintIssue(
                    issue_id=f"duplicate-concept:{canonical}:{path}",
                    kind="duplicate_concept",
                    severity="warning",
                    path=path,
                    message=f"Multiple concept pages map to the same canonical label '{canonical}'.",
                    suggestion="Merge the concept pages or keep one as an alias so concept links stay stable.",
                )
            )

    review_queue = build_review_queue(workspace, snapshot)
    for item in review_queue["items"]:
        if item["kind"] != "conflict_review":
            continue
        issues.append(
            LintIssue(
                issue_id=f"conflicting-claim:{item['review_id']}",
                kind="conflicting_claim",
                severity="warning",
                path=str(item["path"]),
                message=str(item["detail"]),
                suggestion=str(item["suggestion"]),
            )
        )

    return issues
