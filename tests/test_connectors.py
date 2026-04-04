import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from tests import support  # noqa: F401

from cognisync.cli import main


class ConnectorTests(unittest.TestCase):
    def test_connector_cli_enforces_operator_role_for_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Access Workspace"]), 0)
            self.assertEqual(
                main(
                    [
                        "access",
                        "grant",
                        "reviewer-1",
                        "reviewer",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            url = "data:text/html;charset=utf-8," + quote("<html><head><title>Denied Source</title></head><body><p>One.</p></body></html>")

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "connector",
                        "add",
                        "url",
                        url,
                        "--workspace",
                        str(root),
                        "--name",
                        "denied-source",
                        "--actor-id",
                        "reviewer-1",
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("does not have permission", stderr.getvalue())

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        url,
                        "--workspace",
                        str(root),
                        "--name",
                        "allowed-source",
                    ]
                ),
                0,
            )

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "connector",
                        "sync",
                        "url-allowed-source",
                        "--workspace",
                        str(root),
                        "--actor-id",
                        "reviewer-1",
                    ]
                )
            self.assertEqual(exit_code, 2)
            self.assertIn("does not have permission", stderr.getvalue())

    def test_connector_subscriptions_limit_scheduled_syncs_to_due_connectors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Subscription Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Scheduled First</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Scheduled Second</title></head><body><p>Two.</p></body></html>")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        first_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "scheduled-first",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "scheduled-second",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "connector",
                        "subscribe",
                        "url-scheduled-first",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "6",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "subscribe",
                        "url-scheduled-second",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "12",
                    ]
                ),
                0,
            )

            registry_path = root / ".cognisync" / "connectors.json"
            registry = json.loads(registry_path.read_text(encoding="utf-8"))
            for connector in registry["connectors"]:
                subscription = connector["subscription"]
                if connector["connector_id"] == "url-scheduled-first":
                    subscription["next_sync_at"] = "2000-01-01T00:00:00+00:00"
                if connector["connector_id"] == "url-scheduled-second":
                    subscription["next_sync_at"] = (
                        datetime.now(timezone.utc) + timedelta(hours=1)
                    ).replace(microsecond=0).isoformat()
            registry_path.write_text(json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                sync_exit = main(["connector", "sync-all", "--workspace", str(root), "--scheduled-only"])
            self.assertEqual(sync_exit, 0)
            self.assertIn("Synced 1 connector(s).", stdout.getvalue())
            self.assertIn("Imported 1 source artifact(s).", stdout.getvalue())

            registry = json.loads(registry_path.read_text(encoding="utf-8"))
            connectors = {item["connector_id"]: item for item in registry["connectors"]}
            self.assertTrue(connectors["url-scheduled-first"]["last_synced_at"])
            self.assertIsNone(connectors["url-scheduled-second"]["last_synced_at"])
            self.assertTrue(connectors["url-scheduled-first"]["subscription"]["last_scheduled_sync_at"])
            self.assertTrue(connectors["url-scheduled-first"]["subscription"]["next_sync_at"])
            self.assertEqual(connectors["url-scheduled-second"]["subscription"]["interval_hours"], 12)
            self.assertTrue((root / "raw" / "urls" / "scheduled-first.md").exists())
            self.assertFalse((root / "raw" / "urls" / "scheduled-second.md").exists())

    def test_connector_sync_all_runs_every_registered_connector(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Sync All Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>First Source</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Second Source</title></head><body><p>Two.</p></body></html>")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        first_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "first-source",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "second-source",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                sync_exit = main(["connector", "sync-all", "--workspace", str(root)])
            self.assertEqual(sync_exit, 0)
            self.assertIn("Synced 2 connector(s).", stdout.getvalue())
            self.assertIn("Imported 2 source artifact(s).", stdout.getvalue())

            self.assertTrue((root / "raw" / "urls" / "first-source.md").exists())
            self.assertTrue((root / "raw" / "urls" / "second-source.md").exists())

            registry = json.loads((root / ".cognisync" / "connectors.json").read_text(encoding="utf-8"))
            connectors = sorted(registry["connectors"], key=lambda item: item["connector_id"])
            self.assertEqual(len(connectors), 2)
            self.assertTrue(all(connector["last_synced_at"] for connector in connectors))
            self.assertEqual(sum(int(connector["last_result_count"]) for connector in connectors), 2)

    def test_connector_add_list_and_sync_repo_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Workspace"]), 0)

            repo_dir = Path(tmp) / "sample-repo"
            repo_dir.mkdir()
            (repo_dir / "README.md").write_text("# Sample Repo\n\nConnector sync.\n", encoding="utf-8")
            subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "checkout", "-b", "main"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "seed repo"], cwd=repo_dir, check=True, capture_output=True, text=True)

            remote_source = repo_dir.resolve().as_uri()
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "repo",
                        remote_source,
                        "--workspace",
                        str(root),
                        "--name",
                        "remote-sample",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                list_exit = main(["connector", "list", "--workspace", str(root)])
            self.assertEqual(list_exit, 0)
            self.assertIn("repo-remote-sample", stdout.getvalue())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                sync_exit = main(["connector", "sync", "repo-remote-sample", "--workspace", str(root)])
            self.assertEqual(sync_exit, 0)
            self.assertIn("Synced connector repo-remote-sample", stdout.getvalue())

            registry = json.loads((root / ".cognisync" / "connectors.json").read_text(encoding="utf-8"))
            connector = registry["connectors"][0]
            self.assertEqual(connector["connector_id"], "repo-remote-sample")
            self.assertTrue(connector["last_synced_at"])
            self.assertEqual(connector["last_result_count"], 1)
            self.assertTrue((root / "raw" / "repos" / "remote-sample.md").exists())

    def test_connector_sync_jobs_flow_through_the_worker_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Job Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>First Source</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Second Source</title></head><body><p>Two.</p></body></html>")
            url_list = Path(tmp) / "urls.txt"
            url_list.write_text(first_url + "\n" + second_url + "\n", encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "urls",
                        str(url_list),
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-sources",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "connector-sync",
                        "urls-batch-sources",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                work_exit = main(["jobs", "work", "--workspace", str(root), "--max-jobs", "1"])
            self.assertEqual(work_exit, 0)
            self.assertIn("Processed 1 job(s): 1 completed, 0 failed.", stdout.getvalue())

            self.assertTrue((root / "raw" / "urls" / "first-source.md").exists())
            self.assertTrue((root / "raw" / "urls" / "second-source.md").exists())

            registry = json.loads((root / ".cognisync" / "connectors.json").read_text(encoding="utf-8"))
            connector = registry["connectors"][0]
            self.assertEqual(connector["connector_id"], "urls-batch-sources")
            self.assertEqual(connector["last_result_count"], 2)

            manifest_payloads = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            ]
            self.assertEqual(manifest_payloads[0]["job_type"], "connector_sync")
            self.assertEqual(manifest_payloads[0]["result"]["connector_kind"], "urls")

    def test_connector_sync_all_jobs_flow_through_the_worker_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Batch Job Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Batch First</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Batch Second</title></head><body><p>Two.</p></body></html>")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        first_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-first",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "batch-second",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "connector-sync-all",
                        "--workspace",
                        str(root),
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                work_exit = main(["jobs", "work", "--workspace", str(root), "--max-jobs", "1"])
            self.assertEqual(work_exit, 0)
            self.assertIn("Processed 1 job(s): 1 completed, 0 failed.", stdout.getvalue())

            self.assertTrue((root / "raw" / "urls" / "batch-first.md").exists())
            self.assertTrue((root / "raw" / "urls" / "batch-second.md").exists())

            manifest_payloads = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted((root / ".cognisync" / "jobs" / "manifests").glob("*.json"))
            ]
            self.assertEqual(manifest_payloads[0]["job_type"], "connector_sync_all")
            self.assertEqual(manifest_payloads[0]["result"]["synced_connector_count"], 2)
            self.assertEqual(manifest_payloads[0]["result"]["total_result_count"], 2)

    def test_scheduled_connector_sync_all_jobs_only_run_due_connectors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "workspace"
            self.assertEqual(main(["init", str(root), "--name", "Connector Scheduled Job Workspace"]), 0)

            first_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Due Source</title></head><body><p>One.</p></body></html>")
            second_url = "data:text/html;charset=utf-8," + quote("<html><head><title>Future Source</title></head><body><p>Two.</p></body></html>")

            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        first_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "due-source",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "add",
                        "url",
                        second_url,
                        "--workspace",
                        str(root),
                        "--name",
                        "future-source",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "subscribe",
                        "url-due-source",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "2",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "connector",
                        "subscribe",
                        "url-future-source",
                        "--workspace",
                        str(root),
                        "--every-hours",
                        "2",
                    ]
                ),
                0,
            )

            registry_path = root / ".cognisync" / "connectors.json"
            registry = json.loads(registry_path.read_text(encoding="utf-8"))
            for connector in registry["connectors"]:
                if connector["connector_id"] == "url-due-source":
                    connector["subscription"]["next_sync_at"] = "2000-01-01T00:00:00+00:00"
                if connector["connector_id"] == "url-future-source":
                    connector["subscription"]["next_sync_at"] = (
                        datetime.now(timezone.utc) + timedelta(hours=4)
                    ).replace(microsecond=0).isoformat()
            registry_path.write_text(json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "jobs",
                        "enqueue",
                        "connector-sync-all",
                        "--workspace",
                        str(root),
                        "--scheduled-only",
                    ]
                ),
                0,
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                work_exit = main(["jobs", "work", "--workspace", str(root), "--max-jobs", "1"])
            self.assertEqual(work_exit, 0)
            self.assertIn("Processed 1 job(s): 1 completed, 0 failed.", stdout.getvalue())

            manifest_payload = json.loads(
                next((root / ".cognisync" / "jobs" / "manifests").glob("*.json")).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest_payload["job_type"], "connector_sync_all")
            self.assertTrue(manifest_payload["parameters"]["scheduled_only"])
            self.assertEqual(manifest_payload["result"]["synced_connector_count"], 1)
            self.assertEqual(manifest_payload["result"]["total_result_count"], 1)
            self.assertTrue((root / "raw" / "urls" / "due-source.md").exists())
            self.assertFalse((root / "raw" / "urls" / "future-source.md").exists())


if __name__ == "__main__":
    unittest.main()
