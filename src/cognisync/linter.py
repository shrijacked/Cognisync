from __future__ import annotations

from collections import defaultdict
from typing import List

from cognisync.types import IndexSnapshot, LintIssue


def lint_snapshot(snapshot: IndexSnapshot) -> List[LintIssue]:
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

    return sorted(issues, key=lambda issue: (issue.severity, issue.kind, issue.path, issue.issue_id))
