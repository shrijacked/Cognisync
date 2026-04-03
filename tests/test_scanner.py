import tempfile
import unittest
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.scanner import scan_workspace
from cognisync.workspace import Workspace


class ScannerTests(unittest.TestCase):
    def test_scan_extracts_titles_tags_links_and_backlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Scanner Test")

            article = workspace.raw_dir / "transformers.md"
            article.write_text(
                """---
title: Transformers
tags: [llm, architecture]
---
# Transformers

Transformers rely on attention and influence [[agents]].

![Attention Figure](images/attention.png)
""",
                encoding="utf-8",
            )
            (workspace.raw_dir / "images").mkdir(parents=True, exist_ok=True)
            (workspace.raw_dir / "images" / "attention.png").write_bytes(b"png")
            (workspace.wiki_dir / "concepts" / "agents.md").write_text(
                "# Agents\n\nAgent loops coordinate tools.\n",
                encoding="utf-8",
            )
            (workspace.wiki_dir / "sources" / "transformers.md").write_text(
                "# Transformers Summary\n\nThis summary links to [[agents]].\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)
            artifact = snapshot.artifact_by_path("raw/transformers.md")

            self.assertEqual(artifact.title, "Transformers")
            self.assertIn("llm", artifact.tags)
            self.assertIn("Transformers", artifact.headings)
            self.assertIn("wiki/concepts/agents.md", [link.resolved_path for link in artifact.links])
            self.assertIn("raw/transformers.md", snapshot.backlinks["wiki/concepts/agents.md"])

    def test_scan_ignores_change_summary_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Scanner Ignore Change Summary Test")

            (workspace.raw_dir / "note.md").write_text("# Note\n\nCorpus content.\n", encoding="utf-8")
            workspace.change_summaries_dir.mkdir(parents=True, exist_ok=True)
            (workspace.change_summaries_dir / "scan-1.md").write_text(
                "# Scan Change Summary\n\nOperator telemetry.\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)

            self.assertIn("raw/note.md", snapshot.artifact_paths())
            self.assertNotIn("outputs/reports/change-summaries/scan-1.md", snapshot.artifact_paths())

    def test_scan_ignores_review_export_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Scanner Ignore Review Export Test")

            (workspace.raw_dir / "note.md").write_text("# Note\n\nCorpus content.\n", encoding="utf-8")
            review_exports_dir = workspace.outputs_dir / "reports" / "review-exports"
            review_exports_dir.mkdir(parents=True, exist_ok=True)
            (review_exports_dir / "review-export-1.json").write_text(
                "{\n  \"schema_version\": 1\n}\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)

            self.assertIn("raw/note.md", snapshot.artifact_paths())
            self.assertNotIn("outputs/reports/review-exports/review-export-1.json", snapshot.artifact_paths())

    def test_scan_ignores_review_ui_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Scanner Ignore Review UI Test")

            (workspace.raw_dir / "note.md").write_text("# Note\n\nCorpus content.\n", encoding="utf-8")
            workspace.review_ui_dir.mkdir(parents=True, exist_ok=True)
            (workspace.review_ui_dir / "index.html").write_text("<html><body>ui</body></html>", encoding="utf-8")
            (workspace.review_ui_dir / "review-export.json").write_text("{\n  \"schema_version\": 1\n}\n", encoding="utf-8")

            snapshot = scan_workspace(workspace)

            self.assertIn("raw/note.md", snapshot.artifact_paths())
            self.assertNotIn("outputs/reports/review-ui/index.html", snapshot.artifact_paths())
            self.assertNotIn("outputs/reports/review-ui/review-export.json", snapshot.artifact_paths())

    def test_scan_ignores_general_export_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Scanner Ignore Export Artifact Test")

            (workspace.raw_dir / "note.md").write_text("# Note\n\nCorpus content.\n", encoding="utf-8")
            exports_dir = workspace.outputs_dir / "reports" / "exports"
            exports_dir.mkdir(parents=True, exist_ok=True)
            (exports_dir / "research-dataset-1.jsonl").write_text(
                "{\"question\": \"hello\"}\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)

            self.assertIn("raw/note.md", snapshot.artifact_paths())
            self.assertNotIn("outputs/reports/exports/research-dataset-1.jsonl", snapshot.artifact_paths())


if __name__ == "__main__":
    unittest.main()
