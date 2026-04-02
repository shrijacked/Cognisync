import tempfile
import unittest
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.linter import lint_snapshot
from cognisync.config import load_config
from cognisync.scanner import scan_workspace
from cognisync.workspace import Workspace


class WorkspaceTests(unittest.TestCase):
    def test_initialize_creates_expected_layout_and_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = Workspace(root)

            workspace.initialize(name="Research Garden")

            self.assertTrue(workspace.raw_dir.exists())
            self.assertTrue(workspace.wiki_dir.exists())
            self.assertTrue((workspace.wiki_dir / "sources").exists())
            self.assertTrue((workspace.wiki_dir / "concepts").exists())
            self.assertTrue((workspace.wiki_dir / "queries").exists())
            self.assertTrue((workspace.outputs_dir / "reports").exists())
            self.assertTrue((workspace.outputs_dir / "slides").exists())
            self.assertTrue(workspace.prompts_dir.exists())
            self.assertTrue(workspace.state_dir.exists())
            self.assertTrue((workspace.wiki_dir / "index.md").exists())
            self.assertTrue(workspace.config_path.exists())

            config = load_config(workspace.config_path)

            self.assertEqual(config.workspace_name, "Research Garden")
            self.assertEqual(config.summary_directory, "wiki/sources")
            self.assertEqual(config.concept_directory, "wiki/concepts")

    def test_initialize_creates_navigation_pages_without_broken_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Workspace(Path(tmp))

            workspace.initialize(name="Link Cleanliness")

            snapshot = scan_workspace(workspace)
            issues = lint_snapshot(snapshot)
            broken_links = [issue for issue in issues if issue.kind == "broken_link"]

            self.assertEqual(broken_links, [])


if __name__ == "__main__":
    unittest.main()
