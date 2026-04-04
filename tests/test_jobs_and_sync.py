import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path

from tests import support  # noqa: F401

from cognisync.cli import main
from cognisync.config import LLMProfile, load_config, save_config


class JobsAndSyncTests(unittest.TestCase):
    def test_job_heartbeat_extends_the_active_worker_lease(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Heartbeat Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\nAgent memory benefits from explicit links.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "claim-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                        "--lease-seconds",
                        "60",
                    ]
                ),
                0,
            )

            manifest_path = next((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            original_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            original_expiry = datetime.fromisoformat(original_payload["lease"]["lease_expires_at"])
            self.assertNotIn("last_heartbeat_at", original_payload["lease"])

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                heartbeat_exit = main(
                    [
                        "jobs",
                        "heartbeat",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                        "--lease-seconds",
                        "600",
                    ]
                )
            self.assertEqual(heartbeat_exit, 0)
            self.assertIn("Renewed lease", stdout.getvalue())

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            renewed_expiry = datetime.fromisoformat(payload["lease"]["lease_expires_at"])
            self.assertGreater(renewed_expiry, original_expiry)
            self.assertEqual(payload["lease"]["worker_id"], "worker-a")
            self.assertEqual(payload["lease"]["claim_count"], 1)
            self.assertTrue(payload["lease"]["last_heartbeat_at"])

            queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
            queued_job = queue_payload["jobs"][0]
            self.assertEqual(queued_job["worker_id"], "worker-a")
            self.assertEqual(queued_job["status"], "claimed")
            self.assertEqual(queued_job["last_heartbeat_at"], payload["lease"]["last_heartbeat_at"])

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "run-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-b",
                    ]
                ),
                2,
            )

    def test_worker_registry_tracks_claim_heartbeat_and_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Worker Registry Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\nAgent memory benefits from explicit links.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "claim-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                    ]
                ),
                0,
            )

            workers_path = root / ".cognisync" / "jobs" / "workers.json"
            workers_payload = json.loads(workers_path.read_text(encoding="utf-8"))
            worker = workers_payload["workers"][0]
            self.assertEqual(worker["worker_id"], "worker-a")
            self.assertEqual(worker["status"], "claimed")
            self.assertTrue(worker["current_job_id"])
            self.assertTrue(worker["lease_expires_at"])

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "heartbeat",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                        "--lease-seconds",
                        "600",
                    ]
                ),
                0,
            )

            workers_payload = json.loads(workers_path.read_text(encoding="utf-8"))
            worker = workers_payload["workers"][0]
            self.assertEqual(worker["status"], "claimed")
            self.assertTrue(worker["last_seen_at"])
            self.assertEqual(worker["current_job_type"], "lint")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(main(["jobs", "workers", "--workspace", str(root)]), 0)
            self.assertIn("worker-a", stdout.getvalue())

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "run-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                    ]
                ),
                0,
            )

            workers_payload = json.loads(workers_path.read_text(encoding="utf-8"))
            worker = workers_payload["workers"][0]
            self.assertEqual(worker["status"], "idle")
            self.assertEqual(worker["current_job_id"], "")
            self.assertEqual(worker["current_job_type"], "")

    def test_jobs_can_be_claimed_and_run_by_a_specific_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Claimed Job Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\nAgent memory benefits from explicit links.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                claim_exit = main(
                    [
                        "jobs",
                        "claim-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                        "--lease-seconds",
                        "120",
                    ]
                )
            self.assertEqual(claim_exit, 0)
            self.assertIn("Claimed job", stdout.getvalue())

            queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
            self.assertEqual(queue_payload["claimed_count"], 1)
            self.assertEqual(queue_payload["status_counts"]["claimed"], 1)
            self.assertIn("worker-a", queue_payload["active_worker_ids"])

            manifest_path = next((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "claimed")
            self.assertEqual(payload["lease"]["worker_id"], "worker-a")

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "run-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-b",
                    ]
                ),
                2,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                run_exit = main(
                    [
                        "jobs",
                        "run-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                    ]
                )
            self.assertEqual(run_exit, 0)
            self.assertIn("Completed job", stdout.getvalue())

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["lease"]["worker_id"], "worker-a")

    def test_expired_job_leases_can_be_reclaimed_by_another_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Lease Reclaim Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "# Retrieval Systems\n\nAgent memory benefits from explicit links.\n",
                encoding="utf-8",
            )
            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "claim-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-a",
                        "--lease-seconds",
                        "60",
                    ]
                ),
                0,
            )

            manifest_path = next((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload["lease"]["lease_expires_at"] = "2000-01-01T00:00:00+00:00"
            manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                reclaim_exit = main(
                    [
                        "jobs",
                        "claim-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-b",
                        "--lease-seconds",
                        "60",
                    ]
                )
            self.assertEqual(reclaim_exit, 0)
            self.assertIn("Claimed job", stdout.getvalue())

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "claimed")
            self.assertEqual(payload["lease"]["worker_id"], "worker-b")
            self.assertEqual(payload["lease"]["claim_count"], 2)

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "run-next",
                        "--workspace",
                        str(root),
                        "--worker-id",
                        "worker-b",
                    ]
                ),
                0,
            )

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
            self.assertEqual(
                main(
                    [
                        "access",
                        "grant",
                        "reviewer-1",
                        "reviewer",
                        "--workspace",
                        str(source_root),
                        "--name",
                        "Reviewer One",
                    ]
                ),
                0,
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
            self.assertTrue((target_root / ".cognisync" / "access.json").exists())
            self.assertTrue((target_root / ".cognisync" / "sync" / "history.json").exists())
            imported_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertGreaterEqual(imported_manifest["file_count"], 3)
            self.assertEqual(imported_manifest["state_manifests"]["access"], ".cognisync/access.json")

            target_access = json.loads((target_root / ".cognisync" / "access.json").read_text(encoding="utf-8"))
            members = {item["principal_id"]: item for item in target_access["members"]}
            self.assertIn("reviewer-1", members)
            self.assertEqual(members["reviewer-1"]["role"], "reviewer")

            source_history = json.loads((source_root / ".cognisync" / "sync" / "history.json").read_text(encoding="utf-8"))
            target_history = json.loads((target_root / ".cognisync" / "sync" / "history.json").read_text(encoding="utf-8"))
            self.assertEqual(source_history["operation_counts"]["export"], 1)
            self.assertEqual(target_history["operation_counts"]["import"], 1)
            self.assertEqual(source_history["event_count"], 1)
            self.assertEqual(target_history["event_count"], 1)

    def test_jobs_worker_drains_compile_lint_and_maintain_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Worker Queue Workspace"]), 0)

            (root / "raw" / "retrieval.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Retrieval Systems\n\n"
                "## Agent Memory\n\n"
                "Agent Memory benefits from explicit links.\n",
                encoding="utf-8",
            )
            (root / "wiki" / "queries" / "agent-memory.md").write_text(
                "---\n"
                "tags: [agents]\n"
                "---\n"
                "# Agent Memory\n\n"
                "Operator note without backlinks yet.\n",
                encoding="utf-8",
            )

            self.assertEqual(main(["jobs", "enqueue", "lint", "--workspace", str(root)]), 0)
            self.assertEqual(main(["jobs", "enqueue", "compile", "--workspace", str(root)]), 0)
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "maintain",
                        "--workspace",
                        str(root),
                        "--max-concepts",
                        "1",
                        "--max-backlinks",
                        "1",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                work_exit = main(["jobs", "work", "--workspace", str(root), "--max-jobs", "3"])
            self.assertEqual(work_exit, 0)
            self.assertIn("Processed 3 job(s): 3 completed, 0 failed.", stdout.getvalue())

            queue_payload = json.loads((root / ".cognisync" / "jobs" / "queue.json").read_text(encoding="utf-8"))
            self.assertEqual(queue_payload["status_counts"]["completed"], 3)
            self.assertEqual(queue_payload["queued_count"], 0)

            manifest_payloads = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            ]
            self.assertEqual({payload["job_type"] for payload in manifest_payloads}, {"lint", "compile", "maintain"})
            self.assertTrue(any(payload["result"].get("run_manifest_path") for payload in manifest_payloads))
            self.assertTrue((root / "wiki" / "concepts" / "agent-memory.md").exists())


if __name__ == "__main__":
    unittest.main()
