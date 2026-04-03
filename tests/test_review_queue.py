import json
import tempfile
import unittest
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.linter import lint_snapshot
from cognisync.scanner import scan_workspace
from cognisync.workspace import Workspace


class ReviewQueueTests(unittest.TestCase):
    def test_scan_writes_review_queue_manifest_with_actionable_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Review Queue Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Retrieval Systems\n\n"
                "## Vector Database\n\n"
                "Vector Database improves agent recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Memory Systems\n\n"
                "## Vector Databases\n\n"
                "Vector Databases improve persistent memory.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "queries" / "agent-memory.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Agent Memory\n\n"
                "Memory workflows need operator review.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)

            review_queue = json.loads((workspace.state_dir / "review-queue.json").read_text(encoding="utf-8"))
            item_kinds = {item["kind"] for item in review_queue["items"]}

            self.assertIn("concept_candidate", item_kinds)
            self.assertIn("entity_merge_candidate", item_kinds)
            self.assertIn("conflict_review", item_kinds)
            self.assertIn("backlink_suggestion", item_kinds)

            concept_item = next(
                item
                for item in review_queue["items"]
                if item["kind"] == "concept_candidate"
                and set(item["related_paths"]) == {"raw/retrieval.md", "raw/memory.md"}
            )
            self.assertIn("vector database", concept_item["title"].lower())
            self.assertTrue(concept_item["target_path"].startswith("wiki/concepts/"))

    def test_lint_uses_graph_review_queue_for_metadata_conflicts_and_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Graph Aware Lint Test")

            (workspace.raw_dir / "metadata-gap.md").write_text(
                "A raw note without headings or tags.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "cloud.md").write_text(
                "# Cloud First\n\nThe deployment model is cloud only.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "local.md").write_text(
                "# Local First\n\nThe deployment model is local first.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "concepts" / "vector-database.md").write_text(
                "# Vector Database\n\nSingular concept page.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "concepts" / "vector-databases.md").write_text(
                "# Vector Databases\n\nPlural concept page.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            snapshot = scan_workspace(workspace)
            issues = lint_snapshot(snapshot, workspace=workspace)
            issue_kinds = {issue.kind for issue in issues}

            self.assertIn("missing_metadata", issue_kinds)
            self.assertIn("conflicting_claim", issue_kinds)
            self.assertIn("duplicate_concept", issue_kinds)


if __name__ == "__main__":
    unittest.main()
