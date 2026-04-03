import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
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

    def test_review_accept_concept_creates_concept_page_and_records_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Accept Concept Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Databases\n\nVector Databases improve recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistence.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["review", "accept-concept", "vector-databases", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            concept_path = workspace.wiki_dir / "concepts" / "vector-databases.md"
            self.assertTrue(concept_path.exists())
            concept_text = concept_path.read_text(encoding="utf-8")
            self.assertIn("# Vector Databases", concept_text)
            self.assertIn("raw/retrieval.md", concept_text)
            self.assertIn("raw/memory.md", concept_text)

            actions = json.loads((workspace.state_dir / "review-actions.json").read_text(encoding="utf-8"))
            self.assertIn("vector-databases", actions["accepted_concepts"])
            self.assertIn("Accepted concept candidate", stdout.getvalue())

            queue = json.loads((workspace.state_dir / "review-queue.json").read_text(encoding="utf-8"))
            concept_items = [item for item in queue["items"] if item["kind"] == "concept_candidate"]
            self.assertFalse(any(item["slug"] == "vector-databases" for item in concept_items))

    def test_review_resolve_merge_records_aliases_and_updates_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Resolve Merge Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Database\n\nVector Database improves recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistence.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["review", "resolve-merge", "vector database", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            actions = json.loads((workspace.state_dir / "review-actions.json").read_text(encoding="utf-8"))
            resolution = actions["resolved_entity_merges"]["vector database"]
            self.assertEqual(resolution["preferred_label"], "Vector Databases")
            self.assertIn("Vector Database", resolution["aliases"])
            self.assertIn("Resolved merge candidate", stdout.getvalue())

            concept_text = (workspace.wiki_dir / "concepts" / "vector-databases.md").read_text(encoding="utf-8")
            self.assertIn("aliases:", concept_text)
            self.assertIn("Vector Database", concept_text)

            graph_payload = json.loads((workspace.state_dir / "graph.json").read_text(encoding="utf-8"))
            entity_titles = {node["title"] for node in graph_payload["nodes"] if node["kind"] == "entity"}
            self.assertIn("Vector Databases", entity_titles)
            self.assertNotIn("Vector Database", entity_titles)

            queue = json.loads((workspace.state_dir / "review-queue.json").read_text(encoding="utf-8"))
            merge_items = [item for item in queue["items"] if item["kind"] == "entity_merge_candidate"]
            self.assertFalse(merge_items)

    def test_maintain_applies_review_actions_and_writes_run_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Maintenance Test")

            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval Systems\n\n## Vector Database\n\nVector Database improves recall.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "memory.md").write_text(
                "# Memory Systems\n\n## Vector Databases\n\nVector Databases help persistence.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["maintain", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            self.assertTrue((workspace.wiki_dir / "concepts" / "vector-databases.md").exists())
            actions = json.loads((workspace.state_dir / "review-actions.json").read_text(encoding="utf-8"))
            self.assertIn("vector-databases", actions["accepted_concepts"])
            self.assertIn("vector database", actions["resolved_entity_merges"])

            maintenance_manifests = sorted((workspace.state_dir / "runs").glob("maintenance-*.json"))
            self.assertTrue(maintenance_manifests)
            manifest = json.loads(maintenance_manifests[-1].read_text(encoding="utf-8"))
            self.assertEqual(manifest["run_kind"], "maintenance")
            self.assertEqual(manifest["status"], "completed")
            self.assertGreaterEqual(manifest["accepted_concept_count"], 1)
            self.assertGreaterEqual(manifest["resolved_merge_count"], 1)
            self.assertIn("Maintenance applied", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
