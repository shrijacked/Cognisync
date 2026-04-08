import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main


class SyntheticDataTests(unittest.TestCase):
    def test_synth_qa_writes_assertion_grounded_dataset_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Synthetic QA Workspace"]), 0)

            (root / "raw" / "memory-a.md").write_text(
                "# Memory A\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory-b.md").write_text(
                "# Memory B\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["synth", "qa", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            bundle_dirs = sorted((root / "outputs" / "reports" / "exports").glob("synthetic-qa-*"))
            self.assertTrue(bundle_dirs)
            dataset_path = bundle_dirs[-1] / "dataset.jsonl"
            manifest_path = bundle_dirs[-1] / "manifest.json"
            self.assertTrue(dataset_path.exists())
            self.assertTrue(manifest_path.exists())

            records = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertTrue(records)
            self.assertEqual(records[0]["task_type"], "synthetic_qa")
            self.assertIn("agent memory", records[0]["question"].lower())
            self.assertIn("[S1]", records[0]["answer"])
            self.assertEqual(records[0]["support_count"], 2)
            self.assertIn("Wrote synthetic QA bundle to", stdout.getvalue())

    def test_synth_contrastive_writes_positive_negative_pairs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Synthetic Contrastive Workspace"]), 0)

            (root / "raw" / "memory-a.md").write_text(
                "# Memory A\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory-b.md").write_text(
                "# Memory B\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )
            (root / "raw" / "planning.md").write_text(
                "# Planning\n\nAgent planning prefers explicit checkpoints.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["synth", "contrastive", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            bundle_dirs = sorted((root / "outputs" / "reports" / "exports").glob("synthetic-contrastive-*"))
            self.assertTrue(bundle_dirs)
            dataset_path = bundle_dirs[-1] / "dataset.jsonl"
            manifest_path = bundle_dirs[-1] / "manifest.json"
            self.assertTrue(dataset_path.exists())
            self.assertTrue(manifest_path.exists())

            records = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertTrue(records)
            self.assertEqual(records[0]["task_type"], "contrastive_retrieval")
            self.assertNotEqual(records[0]["positive_path"], records[0]["negative_path"])
            self.assertIn("Wrote synthetic contrastive bundle to", stdout.getvalue())

    def test_synth_graph_completion_writes_missing_edge_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Synthetic Graph Workspace"]), 0)

            (root / "raw" / "memory-a.md").write_text(
                "# Memory A\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )
            (root / "raw" / "memory-b.md").write_text(
                "# Memory B\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["synth", "graph-completion", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            bundle_dirs = sorted((root / "outputs" / "reports" / "exports").glob("synthetic-graph-completion-*"))
            self.assertTrue(bundle_dirs)
            dataset_path = bundle_dirs[-1] / "dataset.jsonl"
            records = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertTrue(records)
            self.assertEqual(records[0]["task_type"], "graph_completion")
            self.assertEqual(records[0]["response"], "vector databases")
            self.assertIn("Complete the missing graph edge", records[0]["prompt"])
            self.assertIn("Wrote synthetic graph-completion bundle to", stdout.getvalue())

    def test_synth_report_writing_writes_examples_from_research_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(main(["init", str(root), "--name", "Synthetic Report Workspace"]), 0)

            (root / "raw" / "memory.md").write_text(
                "# Memory\n\nAgent memory uses vector databases.\n",
                encoding="utf-8",
            )
            self.assertEqual(
                main(
                    [
                        "research",
                        "how do agent memory systems work",
                        "--workspace",
                        str(root),
                        "--mode",
                        "memo",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = main(["synth", "report-writing", "--workspace", str(root)])

            self.assertEqual(exit_code, 0)
            bundle_dirs = sorted((root / "outputs" / "reports" / "exports").glob("synthetic-report-writing-*"))
            self.assertTrue(bundle_dirs)
            dataset_path = bundle_dirs[-1] / "dataset.jsonl"
            records = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertTrue(records)
            self.assertEqual(records[0]["task_type"], "report_writing")
            self.assertIn("Write a memo", records[0]["prompt"])
            self.assertTrue(records[0]["response"].strip())
            self.assertIn("Wrote synthetic report-writing bundle to", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
