import tempfile
import unittest
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.linter import lint_snapshot
from cognisync.planner import build_compile_plan
from cognisync.scanner import scan_workspace
from cognisync.workspace import Workspace


class PlannerAndLinterTests(unittest.TestCase):
    def test_plan_and_lint_surface_missing_summaries_concepts_and_integrity_issues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Planner Test")

            (workspace.raw_dir / "agents-overview.md").write_text(
                """---
title: Agents Overview
tags: [agents, loops]
---
# Agents Overview

Agents benefit from loops and memory.
""",
                encoding="utf-8",
            )
            (workspace.raw_dir / "agent-patterns.md").write_text(
                """---
title: Agent Patterns
tags: [agents]
---
# Agent Patterns

Patterns include planning and execution.
""",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "sources" / "agents-overview.md").write_text(
                "# Agents Overview\n\nSummary already exists.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "index.md").write_text(
                "# Index\n\n[Missing](missing-page.md)\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "queries" / "duplicate-a.md").write_text(
                "# Duplicate Title\n\nFirst page.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "queries" / "duplicate-b.md").write_text(
                "# Duplicate Title\n\nSecond page.\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)
            plan = build_compile_plan(snapshot)
            issues = lint_snapshot(snapshot)

            task_kinds = [task.kind for task in plan.tasks]
            issue_kinds = [issue.kind for issue in issues]

            self.assertIn("summarize_source", task_kinds)
            self.assertIn("create_concept_page", task_kinds)
            self.assertIn("repair_broken_links", task_kinds)
            self.assertIn("missing_summary", issue_kinds)
            self.assertIn("broken_link", issue_kinds)
            self.assertIn("duplicate_title", issue_kinds)


if __name__ == "__main__":
    unittest.main()
