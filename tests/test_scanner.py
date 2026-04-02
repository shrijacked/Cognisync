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


if __name__ == "__main__":
    unittest.main()
