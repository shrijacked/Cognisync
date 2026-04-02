import tempfile
import unittest
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.renderers import render_marp_slides, render_query_report
from cognisync.scanner import scan_workspace
from cognisync.search import SearchEngine
from cognisync.workspace import Workspace


class SearchAndRenderersTests(unittest.TestCase):
    def test_query_workflow_renders_report_and_slides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)
            workspace.initialize(name="Query Test")

            (workspace.raw_dir / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning, execution, and reflection.\n",
                encoding="utf-8",
            )
            (workspace.raw_dir / "retrieval.md").write_text(
                "# Retrieval\n\nRetrieval organizes relevant context for answer generation.\n",
                encoding="utf-8",
            )

            snapshot = scan_workspace(workspace)
            engine = SearchEngine.from_workspace(workspace, snapshot)
            hits = engine.search("agent loops planning", limit=2)

            report_path = render_query_report(
                workspace,
                question="How do agent loops work?",
                hits=hits,
            )
            slides_path = render_marp_slides(
                workspace,
                question="How do agent loops work?",
                hits=hits,
            )

            report_text = report_path.read_text(encoding="utf-8")
            slides_text = slides_path.read_text(encoding="utf-8")

            self.assertGreaterEqual(len(hits), 1)
            self.assertEqual(hits[0].path, "raw/agent-loops.md")
            self.assertIn("How do agent loops work?", report_text)
            self.assertIn("## Top Sources", report_text)
            self.assertIn("marp: true", slides_text)
            self.assertIn("Agent Loops", slides_text)


if __name__ == "__main__":
    unittest.main()
