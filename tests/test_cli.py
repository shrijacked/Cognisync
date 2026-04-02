import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main


class CliTests(unittest.TestCase):
    def test_cli_flow_initializes_scans_plans_lints_and_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            init_stdout = io.StringIO()
            with redirect_stdout(init_stdout):
                exit_code = main(["init", str(root), "--name", "CLI Workspace"])
            self.assertEqual(exit_code, 0)

            (root / "raw" / "doc.md").write_text(
                "# CLI Doc\n\nThis document covers orchestration workflows.\n",
                encoding="utf-8",
            )
            (root / "wiki" / "sources" / "doc.md").write_text(
                "# CLI Doc\n\nSummary page exists.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(root)]), 0)
            self.assertEqual(main(["plan", "--workspace", str(root)]), 0)
            self.assertEqual(main(["lint", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(["query", "--workspace", str(root), "orchestration workflows"]),
                0,
            )

            report_files = list((root / "outputs" / "reports").glob("*.md"))
            self.assertTrue(report_files)


if __name__ == "__main__":
    unittest.main()
