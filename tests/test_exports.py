import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config


class ExportTests(unittest.TestCase):
    def test_export_training_bundle_writes_dataset_manifest_and_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Training Export Workspace"]), 0)

            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory helps agent loops persist findings.\n",
                encoding="utf-8",
            )

            config = load_config(root / ".cognisync" / "config.json")
            config.llm_profiles["researcher"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ]
            )
            save_config(root / ".cognisync" / "config.json", config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "researcher",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["export", "training-bundle", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            export_dirs = sorted((root / "outputs" / "reports" / "exports").glob("training-bundle-*"))
            self.assertTrue(export_dirs)
            dataset_path = export_dirs[-1] / "dataset.jsonl"
            manifest_path = export_dirs[-1] / "manifest.json"
            self.assertTrue(dataset_path.exists())
            self.assertTrue(manifest_path.exists())

            records = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(records), 1)
            self.assertTrue(records[0]["labels"]["validation_passed"])
            self.assertIn("packet_text", records[0])
            self.assertIn("answer_text", records[0])
            self.assertGreaterEqual(records[0]["source_count"], 2)

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["record_count"], 1)
            self.assertEqual(manifest["label_counts"]["validation_passed"], 1)
            self.assertIn("Wrote training export to", stdout.getvalue())

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
            self.assertEqual(records[0]["job_profile"], "synthesis-report")
            self.assertIn("report_text", records[0])
            self.assertIn("source blocks", records[0]["report_text"].lower())
            self.assertIn("slide_path", records[0])
            self.assertIn("note_paths", records[0])
            self.assertIn("source_packet_path", records[0])
            self.assertIn("checkpoints_path", records[0])
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

    def test_eval_research_writes_scorecard_report_and_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Evaluation Workspace"]), 0)

            (root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nMemory helps agent loops persist findings.\n",
                encoding="utf-8",
            )

            config = load_config(root / ".cognisync" / "config.json")
            config.llm_profiles["passer"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ]
            )
            config.llm_profiles["failing"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops always require vector databases.')",
                ]
            )
            save_config(root / ".cognisync" / "config.json", config)

            self.assertEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "passer",
                        "how do agent loops use memory",
                    ]
                ),
                0,
            )
            self.assertNotEqual(
                main(
                    [
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "failing",
                        "do agent loops always require vector databases",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["eval", "research", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            report_files = sorted((root / "outputs" / "reports" / "exports").glob("research-eval-*.md"))
            payload_files = sorted((root / "outputs" / "reports" / "exports").glob("research-eval-*.json"))
            self.assertTrue(report_files)
            self.assertTrue(payload_files)

            report_text = report_files[-1].read_text(encoding="utf-8")
            payload = json.loads(payload_files[-1].read_text(encoding="utf-8"))
            self.assertIn("## Scorecard", report_text)
            self.assertIn("Validation pass rate", report_text)
            self.assertEqual(payload["run_count"], 2)
            self.assertEqual(payload["validation_pass_count"], 1)
            self.assertEqual(payload["failed_validation_count"], 1)
            self.assertIn("Wrote research evaluation report to", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
