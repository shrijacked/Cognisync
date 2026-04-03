import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main


class ExportTests(unittest.TestCase):
    def test_export_jsonl_writes_research_dataset_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Export Workspace"]), 0)

            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory helps agent loops persist findings.\n",
                encoding="utf-8",
            )

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--slides",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["export", "jsonl", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            export_files = sorted((root / "outputs" / "reports" / "exports").glob("research-dataset-*.jsonl"))
            self.assertTrue(export_files)
            records = [json.loads(line) for line in export_files[-1].read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["question"], "how do agent loops use memory")
            self.assertEqual(records[0]["run_kind"], "research")
            self.assertIn("report_text", records[0])
            self.assertIn("source blocks", records[0]["report_text"].lower())
            self.assertIn("slide_path", records[0])
            self.assertIn("Wrote JSONL export to", stdout.getvalue())

    def test_export_presentations_writes_bundle_manifest_and_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Presentation Export Workspace"]), 0)

            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory helps agent loops persist findings.\n",
                encoding="utf-8",
            )

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--slides",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["export", "presentations", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            export_dirs = sorted((root / "outputs" / "reports" / "exports").glob("presentations-*"))
            self.assertTrue(export_dirs)
            manifest_path = export_dirs[-1] / "manifest.json"
            self.assertTrue(manifest_path.exists())
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["presentation_count"], 1)
            self.assertEqual(payload["presentations"][0]["question"], "how do agent loops use memory")
            slide_copy = export_dirs[-1] / payload["presentations"][0]["slide_file"]
            report_copy = export_dirs[-1] / payload["presentations"][0]["report_file"]
            self.assertTrue(slide_copy.exists())
            self.assertTrue(report_copy.exists())
            self.assertIn("Wrote presentation export to", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
