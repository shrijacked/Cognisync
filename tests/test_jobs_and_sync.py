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


class JobsAndSyncTests(unittest.TestCase):
    def test_jobs_queue_runs_research_and_improvement_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Job Queue Workspace"]), 0)

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
            config.llm_profiles["healer"] = LLMProfile(
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('# Research Memo\\n\\nAgent loops use memory to retain findings. [S1]')",
                ]
            )
            save_config(root / ".cognisync" / "config.json", config)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                enqueue_exit = main(
                    [
                        "jobs",
                        "enqueue",
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "passer",
                        "how do agent loops use memory",
                    ]
                )
            self.assertEqual(enqueue_exit, 0)
            self.assertIn("Queued research job", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                run_exit = main(["jobs", "run-next", "--workspace", str(root)])
            self.assertEqual(run_exit, 0)
            self.assertIn("Completed job", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                failing_enqueue_exit = main(
                    [
                        "jobs",
                        "enqueue",
                        "research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "failing",
                        "do agent loops always require vector databases",
                    ]
                )
            self.assertEqual(failing_enqueue_exit, 0)
            self.assertIn("Queued research job", stdout.getvalue())

            self.assertEqual(main(["jobs", "run-next", "--workspace", str(root)]), 2)

            job_manifest_paths = sorted((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            failed_job_payload = next(
                json.loads(path.read_text(encoding="utf-8"))
                for path in job_manifest_paths
                if json.loads(path.read_text(encoding="utf-8"))["status"] == "failed"
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                retry_exit = main(
                    [
                        "jobs",
                        "retry",
                        failed_job_payload["job_id"],
                        "--workspace",
                        str(root),
                        "--profile",
                        "healer",
                    ]
                )
            self.assertEqual(retry_exit, 0)
            self.assertIn("Queued retry job", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                retried_exit = main(["jobs", "run-next", "--workspace", str(root)])
            self.assertEqual(retried_exit, 0)
            self.assertIn("Completed job", stdout.getvalue())

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
                enqueue_improve_exit = main(
                    [
                        "jobs",
                        "enqueue",
                        "improve-research",
                        "--workspace",
                        str(root),
                        "--profile",
                        "healer",
                        "--limit",
                        "1",
                        "--provider-format",
                        "openai-chat",
                    ]
                )
            self.assertEqual(enqueue_improve_exit, 0)
            self.assertIn("Queued improve-research job", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                improve_exit = main(["jobs", "run-next", "--workspace", str(root)])
            self.assertEqual(improve_exit, 0)
            self.assertIn("Completed job", stdout.getvalue())

            jobs_dir = root / ".cognisync" / "jobs"
            queue_path = jobs_dir / "queue.json"
            manifest_paths = sorted((jobs_dir / "manifests").glob("*.json"))
            self.assertTrue(queue_path.exists())
            self.assertEqual(len(manifest_paths), 4)

            queue_payload = json.loads(queue_path.read_text(encoding="utf-8"))
            self.assertEqual(queue_payload["status_counts"]["completed"], 3)
            self.assertEqual(queue_payload["status_counts"]["failed"], 1)
            self.assertEqual(queue_payload["queued_count"], 0)

            manifest_payloads = [json.loads(path.read_text(encoding="utf-8")) for path in manifest_paths]
            retried_manifest = next(
                payload
                for payload in manifest_payloads
                if payload["status"] == "completed" and payload.get("retry_of_job_id") == failed_job_payload["job_id"]
            )
            self.assertEqual(retried_manifest["job_type"], "research")
            improve_manifest = next(payload for payload in manifest_payloads if payload["job_type"] == "improve_research")
            self.assertEqual(improve_manifest["status"], "completed")
            self.assertEqual(improve_manifest["job_type"], "improve_research")
            self.assertTrue(improve_manifest["result"]["training_loop_manifest_path"])
            training_loop_manifest = root / improve_manifest["result"]["training_loop_manifest_path"]
            self.assertTrue(training_loop_manifest.exists())

    def test_sync_bundle_exports_and_imports_workspace_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source_root = Path(tmp) / "source"
            target_root = Path(tmp) / "target"
            self.assertEqual(main(["init", str(source_root), "--name", "Source Workspace"]), 0)

            (source_root / "raw" / "agent-loops.md").write_text(
                "# Agent Loops\n\nAgent loops coordinate planning and reflection.\n",
                encoding="utf-8",
            )
            (source_root / "wiki" / "concepts" / "agent-loops.md").write_text(
                "# Agent Loops\n\nA compiled concept page.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["scan", "--workspace", str(source_root)]), 0)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                export_exit = main(["sync", "export", "--workspace", str(source_root)])
            self.assertEqual(export_exit, 0)
            self.assertIn("Wrote sync bundle to", stdout.getvalue())

            bundle_dirs = sorted((source_root / "outputs" / "reports" / "sync-bundles").glob("sync-bundle-*"))
            self.assertTrue(bundle_dirs)
            bundle_dir = bundle_dirs[-1]
            manifest_path = bundle_dir / "manifest.json"
            self.assertTrue(manifest_path.exists())
            self.assertTrue((source_root / ".cognisync" / "sync" / "history.json").exists())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                history_exit = main(["sync", "history", "--workspace", str(source_root)])
            self.assertEqual(history_exit, 0)
            self.assertIn("Sync History", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                import_exit = main(["sync", "import", str(bundle_dir), "--workspace", str(target_root)])
            self.assertEqual(import_exit, 0)
            self.assertIn("Imported sync bundle into", stdout.getvalue())

            self.assertTrue((target_root / "raw" / "agent-loops.md").exists())
            self.assertTrue((target_root / "wiki" / "concepts" / "agent-loops.md").exists())
            self.assertTrue((target_root / ".cognisync" / "sources.json").exists())
            self.assertTrue((target_root / ".cognisync" / "sync" / "history.json").exists())
            imported_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertGreaterEqual(imported_manifest["file_count"], 3)

            source_history = json.loads((source_root / ".cognisync" / "sync" / "history.json").read_text(encoding="utf-8"))
            target_history = json.loads((target_root / ".cognisync" / "sync" / "history.json").read_text(encoding="utf-8"))
            self.assertEqual(source_history["operation_counts"]["export"], 1)
            self.assertEqual(target_history["operation_counts"]["import"], 1)
            self.assertEqual(source_history["event_count"], 1)
            self.assertEqual(target_history["event_count"], 1)


if __name__ == "__main__":
    unittest.main()
